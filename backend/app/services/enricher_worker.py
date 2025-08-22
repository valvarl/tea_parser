from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pymongo import ReturnDocument

from app.core.logging import configure_logging
from app.db.mongo import db
from app.services.enricher import ProductEnricher

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_ENRICHER_CMD = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")

CONCURRENCY_DEF = int(os.getenv("ENRICH_CONCURRENCY", 6))
REVIEWS_DEF = os.getenv("ENRICH_REVIEWS", "true").lower() == "true"
REVIEWS_LIMIT_DEF = int(os.getenv("ENRICH_REVIEWS_LIMIT", 20))
CURRENCY = os.getenv("CURRENCY", "RUB")
ENRICH_RETRY_MAX = int(os.getenv("ENRICH_RETRY_MAX", 3))
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

configure_logging()
logger = logging.getLogger("enricher-worker")

enricher = ProductEnricher(
    concurrency=CONCURRENCY_DEF,
    headless=True,
    reviews=REVIEWS_DEF,
    reviews_limit=REVIEWS_LIMIT_DEF,
    states=True,
    state_ids=[
        "webGallery",
        "webPrice",
        "webAspects",
        "webCollections",
        "webNutritionInfo",
        "webShortCharacteristics",
    ],
    state_regex=False,
    similar_offers=True,
)


# ===== time/json helpers =====

def _now_ts() -> int:
    return int(time.time())

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False).encode("utf-8")

def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))

def _digits_only(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val)
    n = "".join(ch for ch in s if ch.isdigit())
    return int(n) if n else None

def _batch_hash(task_id: str, skus: List[str]) -> str:
    payload = f"{task_id}::" + "|".join(sorted(skus))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


# ===== row parsing =====

def _pick_cover(row: Dict[str, Any]) -> Optional[str]:
    gallery = row.get("states", {}).get("gallery", {}) or {}
    images = gallery.get("images") or []
    first_img = images[0]["src"] if images and isinstance(images[0], dict) else None
    return row.get("cover_image") or gallery.get("coverImage") or first_img

def _pick_title(row: Dict[str, Any]) -> Optional[str]:
    return row.get("name") or row.get("states", {}).get("seo", {}).get("name")

def _extract_prices(row: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    pr = row.get("price") or {}
    card = _digits_only(pr.get("cardPrice"))
    orig = _digits_only(pr.get("originalPrice"))
    disc = _digits_only(pr.get("price"))
    return card, orig, disc

def _build_candidate(row: Dict[str, Any], first_seen_at: Optional[int]) -> Dict[str, Any]:
    return {
        "sku": row["sku"],
        "created_at": first_seen_at or _now_ts(),
        "updated_at": _now_ts(),
        "title": _pick_title(row),
        "cover_image": _pick_cover(row),
        "description": row.get("description"),
        "characteristics": {
            "full": row.get("characteristics"),
            "short": row.get("states", {}).get("shortCharacteristics"),
        },
        "gallery": row.get("states", {}).get("gallery", {}),
        "aspects": row.get("states", {}).get("aspects", {}),
        "collections": row.get("states", {}).get("collections", {}),
        "nutrition": row.get("states", {}).get("nutrition", {}),
        "seo": row.get("states", {}).get("seo", {}),
        "other_offers": row.get("other_offers"),
    }


# ===== persistence =====

async def _save_price_snapshot(candidate_id: Any, disc_price: Optional[int], orig_price: Optional[int]) -> None:
    await db.prices.insert_one(
        {
            "candidate_id": candidate_id,
            "captured_at": _now_ts(),
            "price_current": disc_price,
            "price_old": orig_price,
            "currency": CURRENCY,
        }
    )

async def _upsert_candidate(doc: Dict[str, Any]) -> Any:
    soi = {"created_at": doc["created_at"], "sku": doc["sku"]}
    sa = {k: v for k, v in doc.items() if k not in ("created_at", "sku")}
    sa["updated_at"] = _now_ts()

    res = await db.candidates.update_one({"sku": doc["sku"]}, {"$set": sa, "$setOnInsert": soi}, upsert=True)
    if res.upserted_id is not None:
        candidate_id = res.upserted_id
    else:
        got = await db.candidates.find_one({"sku": doc["sku"]}, {"_id": 1})
        candidate_id = got["_id"]

    await db.index.update_one({"sku": doc["sku"]}, {"$set": {"candidate_id": candidate_id}})
    return candidate_id

async def _bulk_upsert_reviews(reviews: List[Dict[str, Any]]) -> int:
    saved = 0
    for rv in reviews:
        for tkey in ("created_at", "published_at", "updated_at"):
            if isinstance(rv.get(tkey), str):
                rv[tkey] = _digits_only(rv[tkey])
        try:
            await db.reviews.update_one(
                {"uuid": rv.get("uuid") or f"{rv.get('sku')}::{rv.get('published_at')}"},
                {"$set": rv},
                upsert=True,
            )
            saved += 1
        except Exception as exc:
            logger.warning("review upsert failed sku=%s uuid=%s: %s", rv.get("sku"), rv.get("uuid"), exc)
    return saved


# ===== batch lifecycle =====

async def _acquire_or_create_batch(
    task_id: str,
    batch_id: Optional[int],
    skus: Optional[List[str]],
    trigger: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[List[str]]]:
    batch_doc = None

    if batch_id is not None:
        batch_doc = await db.enrich_batches.find_one_and_update(
            {"task_id": task_id, "batch_id": batch_id},
            {"$set": {"status": "in_progress", "updated_at": _now_ts(), "source": trigger}},
            return_document=ReturnDocument.AFTER,
        )
        if batch_doc and not skus:
            skus = batch_doc.get("skus", [])

    if skus and batch_doc is None:
        bh = _batch_hash(task_id, [str(s) for s in skus])
        batch_doc = await db.enrich_batches.find_one_and_update(
            {"task_id": task_id, "hash": bh},
            {
                "$setOnInsert": {
                    "task_id": task_id,
                    "hash": bh,
                    "skus": skus,
                    "created_at": _now_ts(),
                    "status": "in_progress",
                    "source": trigger,
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if batch_doc and "batch_id" not in batch_doc:
            await db.enrich_batches.update_one({"_id": batch_doc["_id"]}, {"$set": {"batch_id": int(time.time() % 1_000_000)}})
            batch_doc = await db.enrich_batches.find_one({"_id": batch_doc["_id"]})
        batch_id = batch_doc.get("batch_id")

    return batch_doc, batch_id, skus


# ===== kafka worker =====

async def _handle_message(cmd: Dict[str, Any], prod: AIOKafkaProducer) -> None:
    task_id: str = cmd["task_id"]
    batch_id: Optional[int] = cmd.get("batch_id")
    trigger: str = cmd.get("trigger") or cmd.get("source") or "ad-hoc"

    skus: Optional[List[str]] = cmd.get("skus")

    enricher.concurrency = int(cmd.get("concurrency", CONCURRENCY_DEF))
    enricher.want_reviews = bool(cmd.get("reviews", REVIEWS_DEF))
    enricher.reviews_limit = int(cmd.get("reviews_limit", REVIEWS_LIMIT_DEF))

    batch_doc, batch_id, skus = await _acquire_or_create_batch(task_id, batch_id, skus, trigger)

    # Finalize signal from background (no skus and no batch_id)
    if not skus and batch_id is None:
        await prod.send_and_wait(
            TOPIC_ENRICHER_STATUS,
            {
                "source": "enricher",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "status": "task_done",
                "done": True,
                "ts": _now_ts(),
            },
        )
        return

    # Legacy mode: enrich all items without candidate_id if no skus provided
    if not skus:
        index_rows = await db.index.find({"candidate_id": None}).to_list(None)
        skus = [d["sku"] for d in index_rows]

    if not skus:
        await prod.send_and_wait(
            TOPIC_ENRICHER_STATUS,
            {
                "source": "enricher",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "batch_id": batch_id,
                "status": "ok",
                "scraped_products": 0,
                "failed_products": 0,
                "total_products": 0,
                "ts": _now_ts(),
            },
        )
        return

    index_rows = await db.index.find({"sku": {"$in": skus}}).to_list(None)
    base_rows: List[Dict[str, Any]] = [{"sku": str(d["sku"]), "first_seen_at": d.get("first_seen_at")} for d in index_rows]
    for r in base_rows:
        # r["link"] = f"/product/{r['sku']}/?oos_search=false"
        r["link"] = f"/product/{r['sku']}/"

    await prod.send_and_wait(
        TOPIC_ENRICHER_STATUS,
        {
            "source": "enricher",
            "version": SERVICE_VERSION,
            "task_id": task_id,
            "batch_id": batch_id,
            "status": "running",
            "total_products": len(base_rows),
            "ts": _now_ts(),
        },
    )

    try:
        enriched_rows, reviews_rows, failed_rows = await enricher.enrich(base_rows)
    except Exception as exc:
        where = {"task_id": task_id, "batch_id": batch_id} if batch_id is not None else {"_id": batch_doc["_id"]}
        await db.enrich_batches.update_one(where, {"$set": {"status": "failed", "error": str(exc), "updated_at": _now_ts()}})
        await prod.send_and_wait(
            TOPIC_ENRICHER_STATUS,
            {
                "source": "enricher",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "batch_id": batch_id,
                "status": "failed",
                "error": str(exc),
                "error_message": str(exc),
                "ts": _now_ts(),
            },
        )
        raise

    fs_map = {r["sku"]: r.get("first_seen_at") for r in base_rows}

    scraped = failed = saved_reviews = 0
    for row in enriched_rows:
        try:
            sku = row["sku"]
            candidate_doc = _build_candidate(row, fs_map.get(sku))
            candidate_id = await _upsert_candidate(candidate_doc)
            card_price, orig_price, disc_price = _extract_prices(row)
            await _save_price_snapshot(candidate_id, disc_price or card_price, orig_price)
            scraped += 1
        except Exception as exc:
            failed += 1
            logger.error("candidate persist failed sku=%s: %s", row.get("sku"), exc, exc_info=True)

    if enricher.want_reviews and reviews_rows:
        saved_reviews = await _bulk_upsert_reviews(reviews_rows)

    dlq_saved = 0
    if failed_rows:
        docs = [
            {
                "task_id": task_id,
                "sku": r.get("sku"),
                "reason": "retry_exhausted",
                "attempts": ENRICH_RETRY_MAX,
                "created_at": _now_ts(),
                "updated_at": _now_ts(),
                "batch_id": batch_id,
                "source": trigger,
            }
            for r in failed_rows
        ]
        try:
            if docs:
                await db.enrich_dlq.insert_many(docs, ordered=False)
                dlq_saved = len(docs)
        except Exception as exc:
            logger.warning("DLQ insert failed: %s", exc)

    where = {"task_id": task_id, "batch_id": batch_id} if batch_id is not None else {"_id": batch_doc["_id"]}
    await db.enrich_batches.update_one(
        where,
        {
            "$set": {
                "status": "processed",
                "processed_at": _now_ts(),
                "scraped": scraped,
                "failed": failed + len(failed_rows),
                "updated_at": _now_ts(),
            }
        },
    )

    await prod.send_and_wait(
        TOPIC_ENRICHER_STATUS,
        {
            "source": "enricher",
            "version": SERVICE_VERSION,
            "task_id": task_id,
            "batch_id": batch_id,
            "status": "ok",
            "scraped_products": scraped,
            "failed_products": failed + len(failed_rows),
            "total_products": len(base_rows),
            "saved_reviews": saved_reviews,
            "dlq": dlq_saved,
            "ts": _now_ts(),
        },
    )

    logger.info("task %s batch %s done: saved=%s failed=%s reviews=%s", task_id, batch_id, scraped, failed + len(failed_rows), saved_reviews)


async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_ENRICHER_CMD,
        bootstrap_servers=BOOT,
        group_id="enricher-worker",
        value_deserializer=_loads,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    prod = AIOKafkaProducer(bootstrap_servers=BOOT, value_serializer=_dumps, enable_idempotence=True)

    await cons.start()
    await prod.start()
    try:
        async for msg in cons:
            try:
                await _handle_message(msg.value, prod)
                await cons.commit()
            except Exception as exc:
                logger.exception("message processing failed, not committing: %s", exc)
    finally:
        await cons.stop()
        await prod.stop()


if __name__ == "__main__":
    asyncio.run(main())
