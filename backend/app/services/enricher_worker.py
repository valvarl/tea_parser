from __future__ import annotations

"""Kafka worker: enriches products and writes structured docs to MongoDB.

Collections used
----------------
- index       : minimal SKU registry (already created by indexer)
- candidates  : detailed product cards (this worker populates)
- prices      : one snapshot per capture (price history)
- tea_reviews : full‑text reviews (optional)

Schema hints (see README for full spec):

candidates
~~~~~~~~~~
{
    _id: ObjectId,
    sku: str,
    created_at: datetime,
    updated_at: datetime,
    basic: {
        title: str, price_current: int | None, price_old: int | None,
        rating_avg: float | None, reviews_count: int | None,
    },
    stock: {
        qty: int | None, max_qty: int | None,
        delivery_date: datetime | None, delivery_label_raw: str | None,
    },
    media: { images: [str], video: [str] },
    attributes: { … },
    raw: { charcs_json: str | None, description: str | None },
    reviews_agg: { last_count: int | None, last_rating: float | None },
}

prices
~~~~~~
{ _id, candidate_id, captured_at, price_current, price_old, currency }
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.logging import configure_logging
from app.db.mongo import db
from app.services.enricher import ProductEnricher  # scraping / parsing class

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_ENRICHER_CMD = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")
CONCURRENCY_DEF = int(os.getenv("ENRICH_CONCURRENCY", 6))
REVIEWS_DEF = os.getenv("ENRICH_REVIEWS", "false").lower() == "true"
REVIEWS_LIMIT_DEF = int(os.getenv("ENRICH_REVIEWS_LIMIT", 20))
CURRENCY = "RUB"

configure_logging()
logger = logging.getLogger("enricher-worker")

enricher = ProductEnricher(
    concurrency=CONCURRENCY_DEF,
    headless=True,
    reviews=REVIEWS_DEF,
    reviews_limit=REVIEWS_LIMIT_DEF,
)

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

IMMUTABLE_ON_INSERT = ("created_at",)

def _split_update(doc: dict) -> tuple[dict, dict]:
    """
    Возвращает (set_on_insert, set_always).
    Всё, что нельзя трогать при апдейте (created_at …), уходит
    **только** в $setOnInsert.
    """
    soi, sa = {}, {}
    for k, v in doc.items():
        if k in IMMUTABLE_ON_INSERT:
            soi[k] = v
        else:
            sa[k] = v
    # обновляем «дата последнего парсинга»
    sa["updated_at"] = datetime.utcnow()
    return soi, sa

def _now() -> datetime:
    return datetime.utcnow()


def _mk_candidate_doc(row: Dict[str, Any], first_seen_at: Optional[datetime]) -> Dict[str, Any]:
    """Convert raw row from ProductEnricher → structured candidate document."""
    img_array = row.get("images", "").split("|") if row.get("images") else []

    doc: Dict[str, Any] = {
        "sku": row["sku"],
        "created_at": first_seen_at or _now(),
        "updated_at": _now(),
        "basic": {
            "title": row.get("name"),
            "price_current": row.get("price_curr"),
            "price_old": row.get("price_old"),
            "rating_avg": row.get("rating"),
            "reviews_count": row.get("reviews"),
        },
        "stock": {
            "qty": row.get("qty"),  # placeholder; can be filled elsewhere
            "max_qty": row.get("max_qty"),
            "delivery_date": row.get("delivery_date"),
            "delivery_label_raw": row.get("delivery_day_raw"),
        },
        "media": {
            "images": img_array,
            "video": [],
        },
        "attributes": {},  # to be parsed later from charcs_json
        "raw": {
            "charcs_json": row.get("charcs_json"),
            "description": row.get("description"),
        },
        "reviews_agg": {
            "last_count": row.get("reviews"),
            "last_rating": row.get("rating"),
        },
    }
    return doc


async def _save_price_snapshot(candidate_id, price_curr, price_old):
    try:
        await db.prices.insert_one({
            "candidate_id": candidate_id,
            "captured_at": _now(),
            "price_current": price_curr,
            "price_old": price_old,
            "currency": CURRENCY,
        })
    except Exception as exc:
        logger.warning("price snapshot failed for %s: %s", candidate_id, exc)


# --------------------------------------------------------------------------
# Kafka worker
# --------------------------------------------------------------------------

async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_ENRICHER_CMD,
        bootstrap_servers=BOOT,
        group_id="enricher-worker",
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        value_serializer=lambda x: json.dumps(x).encode(),
        enable_idempotence=True,
    )
    await cons.start(); await prod.start()
    try:
        async for msg in cons:
            try:
                await _handle_message(msg.value, prod)
                await cons.commit()
            except Exception as exc:
                logger.exception("message failed → no commit: %s", exc)
    finally:
        await cons.stop(); await prod.stop()


async def _handle_message(cmd: Dict[str, Any], prod: AIOKafkaProducer):
    task_id: str = cmd["task_id"]
    skus: List[str] | None = cmd.get("skus")
    want_reviews: bool = bool(cmd.get("reviews", REVIEWS_DEF))
    reviews_limit: int = int(cmd.get("reviews_limit", REVIEWS_LIMIT_DEF))
    concurrency: int = int(cmd.get("concurrency", CONCURRENCY_DEF))

    enricher.concurrency = concurrency
    enricher.want_reviews = want_reviews
    enricher.reviews_limit = reviews_limit

    # ------------------------------------------------------------------
    # 1. Fetch rows to enrich
    # ------------------------------------------------------------------
    if skus:
        index_rows = await db.index.find({"sku": {"$in": skus}}).to_list(None)
    else:
        # все SKU у которых ещё нет candidate_id → никогда не обогащались
        index_rows = await db.index.find({"candidate_id": None}).to_list(None)

    if not index_rows:
        logger.info("task %s: nothing to enrich", task_id)
        await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
            "task_id": task_id, "status": "completed",
            "scraped_products": 0, "failed_products": 0, "total_products": 0,
        })
        return

    base_rows: List[Dict[str, Any]] = [
        {"sku": d["sku"], "first_seen_at": d.get("first_seen_at")} for d in index_rows
    ]

    total = len(base_rows)
    await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
        "task_id": task_id, "status": "running", "total_products": total,
    })
    logger.info("⚙️  task %s: enriching %s SKUs (reviews=%s)", task_id, total, want_reviews)

    # ------------------------------------------------------------------
    # 2. Grab PDP pages
    # ------------------------------------------------------------------
    # Re‑attach minimal data needed by ProductEnricher: at least sku & link
    for r in base_rows:
        r["link"] = f"/product/{r['sku']}/"  # slug‑less path works fine (302)

    enriched_rows, reviews_rows = await enricher.enrich(base_rows)

    # ------------------------------------------------------------------
    # 3. Persist to MongoDB
    # ------------------------------------------------------------------
    scraped = failed = 0
    for row in enriched_rows:
        sku = row["sku"]
        first_seen_at = next((it.get("first_seen_at") for it in base_rows if it["sku"] == sku), None)
        candidate_doc = _mk_candidate_doc(row, first_seen_at)
        soi, sa = _split_update(candidate_doc)
        try:
            res = await db.candidates.update_one(
                {"sku": candidate_doc["sku"]},
                {"$set": sa, "$setOnInsert": soi},
                upsert=True,
            )
            candidate_id = res.upserted_id or (await db.candidates.find_one({"sku": sku}, {"_id": 1}))['_id']
            # link back from index → candidates
            await db.index.update_one(
                {"sku": candidate_doc["sku"]},
                {"$set": {"candidate_id": candidate_id}},
            )
            # price history
            await _save_price_snapshot(candidate_id, candidate_doc["basic"]["price_current"], candidate_doc["basic"]["price_old"])
            scraped += 1
        except Exception as exc:
            logger.error("failed to save candidate for %s: %s", sku, exc)
            failed += 1

    # reviews
    saved_reviews = 0
    if want_reviews and reviews_rows:
        for rv in reviews_rows:
            try:
                await db.tea_reviews.update_one(
                    {"sku": rv["sku"], "author": rv["author"], "date": rv["date"]},
                    {"$set": rv},
                    upsert=True,
                )
                saved_reviews += 1
            except Exception:
                logger.warning("review upsert failed for %s", rv.get("sku"))

    # ------------------------------------------------------------------
    await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
        "task_id": task_id,
        "status": "batch_ready",
        "scraped_products": scraped,
        "failed_products": failed,
        "total_products": total,
        "saved_reviews": saved_reviews,
    })
    logger.info("✅ task %s done: saved=%s failed=%s reviews=%s", task_id, scraped, failed, saved_reviews)


if __name__ == "__main__":
    asyncio.run(main())
