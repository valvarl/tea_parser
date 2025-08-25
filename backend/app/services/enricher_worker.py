from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pymongo import ReturnDocument

from app.core.logging import configure_logging, get_logger
from app.db.mongo import db
from app.services.enricher import ProductEnricher

# ===== config =====

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_ENRICHER_CMD = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")

CONCURRENCY_DEF = int(os.getenv("ENRICH_CONCURRENCY", 6))
REVIEWS_DEF = os.getenv("ENRICH_REVIEWS", "true").lower() == "true"
REVIEWS_LIMIT_DEF = int(os.getenv("ENRICH_REVIEWS_LIMIT", 20))
CURRENCY = os.getenv("CURRENCY", "RUB")
ENRICH_RETRY_MAX = int(os.getenv("ENRICH_RETRY_MAX", 3))
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

# send heartbeat while a batch is processing (must be < coordinator STATUS_TIMEOUT_SEC)
HEARTBEAT_SEC = max(5, int(os.getenv("ENRICH_HEARTBEAT_SEC", "30")))

configure_logging(service="enricher", worker="enricher-worker")
logger = get_logger("enricher-worker")

enricher = ProductEnricher(
    concurrency=2,
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

# ===== time / json =====

def _now_ts() -> int:
    return int(time.time())

def _now_dt() -> datetime:
    return datetime.utcnow()

def _dt_from_ts(ts: Optional[int]) -> Optional[datetime]:
    return datetime.utcfromtimestamp(int(ts)) if ts is not None else None

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))

def _digits_only(val: Any) -> Optional[int]:
    if val is None:
        return None
    s = str(val)
    n = "".join(ch for ch in s if ch.isdigit())
    return int(n) if n else None

def _batch_hash(task_id: str, skus: List[str]) -> str:
    payload = f"{task_id}::" + "|".join(sorted(map(str, skus)))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()

# ===== status emitter =====

async def _emit_status(prod: AIOKafkaProducer, payload: Dict[str, Any]) -> None:
    base = {"source": "enricher", "version": SERVICE_VERSION, "ts": _now_ts()}
    base.update(payload)
    await prod.send_and_wait(TOPIC_ENRICHER_STATUS, base)

async def _heartbeat_loop(prod: AIOKafkaProducer, *, task_id: str, batch_id: Optional[int], trigger: str) -> None:
    """Periodic 'running' to keep coordinator heartbeat alive."""
    try:
        while True:
            await asyncio.sleep(HEARTBEAT_SEC)
            await _emit_status(
                prod,
                {
                    "task_id": task_id,
                    "batch_id": batch_id,
                    "status": "running",  # coordinator already understands this status
                    "trigger": trigger,
                    "heartbeat": True,   # hint flag; coordinator can ignore
                },
            )
    except asyncio.CancelledError:
        # silent stop when batch finishes or fails
        return

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

def _build_candidate(row: Dict[str, Any], first_seen_at_ts: Optional[int]) -> Dict[str, Any]:
    created_dt = _dt_from_ts(first_seen_at_ts) or _now_dt()
    return {
        "sku": row["sku"],
        "created_at": created_dt,
        "updated_at": _now_dt(),
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
            "captured_at": _now_dt(),  # datetime for time-series / TTL
            "price_current": disc_price,
            "price_old": orig_price,
            "currency": CURRENCY,
        }
    )

async def _upsert_candidate(doc: Dict[str, Any]) -> Tuple[Any, bool]:
    """Upsert candidate; return (candidate_id, is_insert)."""
    soi = {"created_at": doc["created_at"], "sku": doc["sku"]}
    sa = {k: v for k, v in doc.items() if k not in ("created_at", "sku")}
    sa["updated_at"] = _now_dt()

    res = await db.candidates.update_one({"sku": doc["sku"]}, {"$set": sa, "$setOnInsert": soi}, upsert=True)
    is_insert = res.upserted_id is not None

    if is_insert:
        candidate_id = res.upserted_id
    else:
        got = await db.candidates.find_one({"sku": doc["sku"]}, {"_id": 1})
        candidate_id = got["_id"]

    # link back into index
    await db.index.update_one({"sku": doc["sku"]}, {"$set": {"candidate_id": candidate_id}})
    return candidate_id, is_insert

async def _bulk_upsert_reviews(reviews: List[Dict[str, Any]]) -> int:
    saved = 0
    for rv in reviews:
        # keep upstream numeric timestamps as-is (analytics may rely on ints here)
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
        except Exception:
            logger.warning(
                "review upsert failed",
                extra={"event": "review_upsert_failed", "task_id": rv.get("task_id"), "sku": rv.get("sku")},
            )
    return saved

# ===== counters =====

async def _next_enrich_batch_id(task_id: str) -> int:
    doc = await db.counters.find_one_and_update(
        {"_id": f"enrich_batch::{task_id}"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return int(doc.get("seq", 1))

# ===== batch lifecycle =====

async def _acquire_or_create_batch(
    task_id: str,
    batch_id: Optional[int],
    skus: Optional[List[str]],
    trigger: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[int], Optional[List[str]]]:
    batch_doc = None

    if batch_id is not None:
        batch_doc = await db.enrich_batches.find_one_and_update(
            {"task_id": task_id, "batch_id": batch_id},
            {"$set": {"status": "in_progress", "updated_at": _now_dt(), "source": trigger}},
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
                    "created_at": _now_dt(),
                    "status": "in_progress",
                    "source": trigger,
                }
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if batch_doc and "batch_id" not in batch_doc:
            new_id = await _next_enrich_batch_id(task_id)
            await db.enrich_batches.update_one({"_id": batch_doc["_id"]}, {"$set": {"batch_id": new_id, "updated_at": _now_dt()}})
            batch_doc = await db.enrich_batches.find_one({"_id": batch_doc["_id"]})
            batch_id = int(batch_doc.get("batch_id"))

    return batch_doc, batch_id, skus

# ===== kafka worker =====

async def _handle_message(cmd: Dict[str, Any], prod: AIOKafkaProducer) -> None:
    task_id: str = cmd["task_id"]
    batch_id: Optional[int] = cmd.get("batch_id")
    trigger: str = cmd.get("trigger") or cmd.get("source") or "ad-hoc"
    skus: Optional[List[str]] = cmd.get("skus")
    is_finalize = str(trigger or "").lower().endswith("finalize")

    # dynamic settings
    enricher.concurrency = int(cmd.get("concurrency", CONCURRENCY_DEF))
    enricher.want_reviews = bool(cmd.get("reviews", REVIEWS_DEF))
    enricher.reviews_limit = int(cmd.get("reviews_limit", REVIEWS_LIMIT_DEF))

    batch_doc, batch_id, skus = await _acquire_or_create_batch(task_id, batch_id, skus, trigger)

    # finalize immediately (no work at all across the task)
    if is_finalize or (not skus and batch_id is None):
        await _emit_status(
            prod,
            {
                "task_id": task_id,
                "status": "task_done",
                "done": True,
                "trigger": trigger,
                "processed": 0,
                "inserted": 0,
                "updated": 0,
                "reviews_saved": 0,
                "dlq": 0,
            },
        )
        return

    # fallback: pull SKUs from index without candidate (first enrichment)
    if not skus:
        index_rows = await db.index.find({"candidate_id": None}).to_list(None)
        skus = [str(d["sku"]) for d in index_rows]

    # still nothing to do → batch OK with zeros (but emit 'ok' so coordinator progresses)
    if not skus:
        await _emit_status(
            prod,
            {
                "task_id": task_id,
                "batch_id": batch_id,
                "status": "ok",
                "trigger": trigger,
                "batch_data": {"processed": 0, "inserted": 0, "updated": 0, "reviews_saved": 0, "dlq": 0, "skus": []},
            },
        )
        return

    # base rows
    index_rows = await db.index.find({"sku": {"$in": skus}}).to_list(None)
    base_rows: List[Dict[str, Any]] = [{"sku": str(d["sku"]), "first_seen_at": d.get("first_seen_at")} for d in index_rows]
    for r in base_rows:
        r["link"] = f"/product/{r['sku']}/"

    # running signal + start heartbeat pinger
    await _emit_status(
        prod,
        {
            "task_id": task_id,
            "batch_id": batch_id,
            "status": "running",
            "trigger": trigger,
        },
    )
    hb_task = asyncio.create_task(_heartbeat_loop(prod, task_id=task_id, batch_id=batch_id, trigger=trigger))

    # enrich
    try:
        enriched_rows, reviews_rows, failed_rows = await enricher.enrich(base_rows)
    except Exception as exc:
        hb_task.cancel()
        where = {"task_id": task_id, "batch_id": batch_id} if batch_id is not None else {"_id": batch_doc["_id"]}
        await db.enrich_batches.update_one(
            where,
            {"$set": {"status": "failed", "error": str(exc), "updated_at": _now_dt()}},
        )
        await _emit_status(
            prod,
            {
                "task_id": task_id,
                "batch_id": batch_id,
                "status": "failed",
                "trigger": trigger,
                "reason_code": "unexpected_error",
                "error": str(exc),
                "error_message": str(exc),
            },
        )
        raise
    finally:
        # make sure heartbeat stops in all flows
        if not hb_task.cancelled():
            hb_task.cancel()
            try:
                await hb_task
            except asyncio.CancelledError:
                pass

    fs_map = {r["sku"]: r.get("first_seen_at") for r in base_rows}

    processed = 0
    inserted_cnt = 0
    updated_cnt = 0
    transient_failed = 0
    saved_reviews = 0

    # persist enriched candidates
    for row in enriched_rows:
        try:
            sku = row["sku"]
            candidate_doc = _build_candidate(row, fs_map.get(sku))
            candidate_id, is_insert = await _upsert_candidate(candidate_doc)
            card_price, orig_price, disc_price = _extract_prices(row)
            await _save_price_snapshot(candidate_id, disc_price or card_price, orig_price)
            processed += 1
            inserted_cnt += int(is_insert)
            updated_cnt += int(not is_insert)
        except Exception:
            transient_failed += 1
            logger.error(
                "candidate persist failed",
                extra={"event": "candidate_persist_failed", "task_id": task_id, "sku": row.get("sku")},
            )

    # reviews
    if enricher.want_reviews and reviews_rows:
        saved_reviews = await _bulk_upsert_reviews(reviews_rows)

    # DLQ for hard failures (from enricher.enrich failed_rows)
    dlq_saved = 0
    if failed_rows:
        docs = [
            {
                "task_id": task_id,
                "sku": r.get("sku"),
                "reason": "retry_exhausted",
                "attempts": ENRICH_RETRY_MAX,
                "created_at": _now_dt(),
                "updated_at": _now_dt(),
                "batch_id": batch_id,
                "source": trigger,
            }
            for r in failed_rows
        ]
        try:
            if docs:
                await db.enrich_dlq.insert_many(docs, ordered=False)
                dlq_saved = len(docs)
        except Exception:
            logger.warning("DLQ insert failed", extra={"event": "dlq_insert_failed", "task_id": task_id})

    # mark batch document (operational fields)
    where = {"task_id": task_id, "batch_id": batch_id} if batch_id is not None else {"_id": batch_doc["_id"]}
    try:
        await db.enrich_batches.update_one(
            where,
            {
                "$set": {
                    "status": "finished",
                    "processed": processed,
                    "inserted": inserted_cnt,
                    "updated": updated_cnt,
                    "failed": transient_failed + len(failed_rows),
                    "finished_at": _now_dt(),
                    "updated_at": _now_dt(),
                }
            },
        )
    except Exception:
        logger.warning("enrich_batches update failed", extra={"event": "batch_doc_update_failed", "task_id": task_id})

    # emit OK with only required metrics
    await _emit_status(
        prod,
        {
            "task_id": task_id,
            "batch_id": batch_id,
            "status": "ok",
            "trigger": trigger,
            "batch_data": {
                "processed": processed,
                "inserted": inserted_cnt,
                "updated": updated_cnt,
                "reviews_saved": saved_reviews,
                "dlq": dlq_saved,
                "skus": [r["sku"] for r in enriched_rows],
            },
        },
    )

    logger.info(
        "batch done",
        extra={
            "event": "batch_ok",
            "task_id": task_id,
            "batch_id": batch_id,
            "counts": {"processed": processed, "inserted": inserted_cnt, "updated": updated_cnt, "dlq": dlq_saved},
        },
    )

# ===== main =====

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
            except Exception:
                logger.exception("message processing failed", extra={"event": "msg_failed"})
    finally:
        await cons.stop()
        await prod.stop()

if __name__ == "__main__":
    asyncio.run(main())
