from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Set

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.logging import configure_logging, get_logger
from app.db.mongo import db
from app.services.indexer import ProductIndexer, CircuitOpen

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")

MAX_PAGES_DEF = int(os.getenv("INDEX_MAX_PAGES", 1))
CATEGORY_DEF = os.getenv("INDEX_CATEGORY_ID", "9373")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

INDEX_RETRY_MAX = int(os.getenv("INDEX_RETRY_MAX", "3"))
INDEX_RETRY_BASE_SEC = int(os.getenv("INDEX_RETRY_BASE_SEC", "60"))
INDEX_RETRY_MAX_SEC = int(os.getenv("INDEX_RETRY_MAX_SEC", "3600"))

configure_logging(service="indexer", worker="indexer-worker")
logger = get_logger("indexer-worker")
indexer = ProductIndexer()


# ---------- utils ----------

def _now_ts() -> int:
    return int(time.time())

def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

def _chunked(seq: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for it in seq:
        buf.append(it)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

async def _send_status(prod: AIOKafkaProducer, payload: Dict[str, Any]) -> None:
    base = {"source": "indexer", "version": SERVICE_VERSION, "ts": _now_ts()}
    base.update(payload)
    await prod.send_and_wait(TOPIC_INDEXER_STATUS, base)


# ---------- dispatcher ----------

async def _dispatch(payload: Dict[str, Any], prod: AIOKafkaProducer) -> None:
    cmd = (payload.get("cmd") or "").lower()
    task_id = payload.get("task_id")
    trigger = payload.get("trigger") or "ad-hoc"
    logger.info("dispatch", extra={"event": "dispatch", "cmd": cmd or "search", "task_id": task_id, "trigger": trigger})

    if cmd == "add_collection_members":
        await _handle_add_collection_members(
            task_id=task_id,
            batch_id=payload.get("batch_id"),
            skus=list(payload.get("skus") or []),
            trigger=trigger,
            prod=prod,
        )
        return

    await _handle_search(
        task_id=task_id,
        query=(payload.get("search_term") or "").strip(),
        category=str(payload.get("category_id") or CATEGORY_DEF),
        max_pages=int(payload.get("max_pages", MAX_PAGES_DEF)),
        trigger=trigger,
        prod=prod,
    )


# ---------- search flow ----------

async def _handle_search(
    task_id: str,
    query: str,
    category: str,
    max_pages: int,
    trigger: str,
    prod: AIOKafkaProducer,
) -> None:
    logger.info(
        "start search",
        extra={"event": "start", "task_id": task_id, "trigger": trigger, "params": {"query": query, "category": category, "max_pages": max_pages}},
    )
    try:
        await _send_status(prod, {"task_id": task_id, "status": "running", "trigger": trigger})

        totals_inserted = 0
        totals_updated = 0
        totals_pages = 0
        attempt = 0

        while True:
            attempt += 1
            try:
                page_no = 0
                async for batch in indexer.iter_products(
                    query=query,
                    category=category,
                    start_page=1,
                    max_pages=max_pages,
                    headless=True,
                ):
                    page_no += 1
                    totals_pages += 1

                    batch_skus: List[str] = []
                    batch_inserted = 0
                    batch_updated = 0

                    for p in batch:
                        sku = str(p.get("sku") or "").strip()
                        if not sku:
                            continue
                        try:
                            res = await db.index.update_one(
                                {"sku": sku},
                                {
                                    "$set": {
                                        "last_seen_at": _now_ts(),
                                        "is_active": True,
                                        "task_id": task_id,
                                    },
                                    "$setOnInsert": {
                                        "first_seen_at": _now_ts(),
                                        "candidate_id": None,
                                        "sku": sku,
                                        "status": "indexed_auto",
                                    },
                                },
                                upsert=True,
                            )
                            is_insert = res.upserted_id is not None
                            batch_inserted += int(is_insert)
                            batch_updated += int(not is_insert)
                            batch_skus.append(sku)
                        except Exception:
                            logger.error("index upsert failed", extra={"event": "index_upsert_failed", "task_id": task_id, "sku": sku})

                    totals_inserted += batch_inserted
                    totals_updated += batch_updated
                    batch_indexed = batch_inserted + batch_updated

                    await _send_status(
                        prod,
                        {
                            "task_id": task_id,
                            "status": "ok",
                            "batch_id": page_no,
                            "trigger": trigger,
                            "batch_data": {
                                "batch_id": page_no,
                                "skus": batch_skus,
                                "indexed": batch_indexed,
                                "inserted": batch_inserted,
                                "updated": batch_updated,
                                "pages": 1,
                            },
                        },
                    )
                    logger.info(
                        "page done",
                        extra={
                            "event": "batch_ok",
                            "task_id": task_id,
                            "batch_id": page_no,
                            "counts": {"indexed": batch_indexed, "inserted": batch_inserted, "updated": batch_updated, "pages": 1},
                        },
                    )

                break  # completed without CircuitOpen

            except CircuitOpen as co:
                if attempt >= INDEX_RETRY_MAX:
                    err = f"circuit_open_retry_exhausted: {co}"
                    await _send_status(
                        prod,
                        {
                            "task_id": task_id,
                            "status": "failed",
                            "error": err,
                            "error_message": err,
                            "reason_code": "circuit_open_retry_exhausted",
                            "trigger": trigger,
                        },
                    )
                    logger.error(
                        "circuit open retries exhausted",
                        extra={"event": "failed", "task_id": task_id, "attempts": attempt, "reason_code": "circuit_open_retry_exhausted"},
                    )
                    return
                delay = min(INDEX_RETRY_MAX_SEC, INDEX_RETRY_BASE_SEC * (2 ** (attempt - 1)))
                await _send_status(
                    prod,
                    {"task_id": task_id, "status": "retrying", "attempt": attempt, "next_retry_in_sec": delay, "trigger": trigger},
                )
                logger.warning(
                    "retrying after circuit open",
                    extra={"event": "retrying", "task_id": task_id, "attempt": attempt, "delay_sec": delay},
                )
                await asyncio.sleep(delay)
                continue

        await _send_status(prod, {"task_id": task_id, "status": "task_done", "done": True, "trigger": trigger})
        logger.info(
            "task done",
            extra={
                "event": "task_done",
                "task_id": task_id,
                "totals": {
                    "indexed": totals_inserted + totals_updated,
                    "inserted": totals_inserted,
                    "updated": totals_updated,
                    "pages": totals_pages,
                },
            },
        )

    except Exception as e:
        await _send_status(
            prod,
            {"task_id": task_id, "status": "failed", "error": str(e), "error_message": str(e), "reason_code": "unexpected_error", "trigger": trigger},
        )
        logger.exception("task failed", extra={"event": "failed", "task_id": task_id, "reason_code": "unexpected_error"})


# ---------- collections flow ----------

async def _handle_add_collection_members(
    task_id: str,
    batch_id: Optional[int],
    skus: List[str],
    trigger: str,
    prod: AIOKafkaProducer,
) -> None:
    is_finalize = str(trigger or "").lower().endswith("finalize")
    logger.info(
        "collections batch start",
        extra={"event": "collections_start", "task_id": task_id, "batch_id": batch_id, "skus_count": len(skus)},
    )
    try:
        if is_finalize or (not skus and batch_id is None):
            await _send_status(
                prod,
                {"task_id": task_id, "status": "task_done", "done": True, "trigger": trigger},
            )
            logger.info("collections finalize", extra={"event": "collections_finalize", "task_id": task_id, "trigger": trigger})
            return
        
        # empty batch -> OK with zeros
        if not skus:
            await _send_status(
                prod,
                {
                    "task_id": task_id,
                    "status": "ok",
                    "batch_id": batch_id,
                    "trigger": trigger,
                    "batch_data": {
                        "batch_id": batch_id, "pages": 0, "skus": [],
                        "inserted": 0, "updated": 0, "new_skus": []
                    },
                },
            )
            logger.info("collections batch empty", extra={"event": "collections_empty", "task_id": task_id, "batch_id": batch_id})
            return

        now_ts = _now_ts()
        inserted = 0
        updated = 0
        new_skus: List[str] = []

        coll_map = await _map_sku_to_collection_hashes(task_id=task_id, skus=skus)

        for sku in skus:
            hashes = sorted(coll_map.get(sku, set()))
            try:
                res = await db.index.update_one(
                    {"sku": sku},
                    {
                        "$set": {"last_seen_at": now_ts, "is_active": True, "task_id": task_id},
                        "$setOnInsert": {"first_seen_at": now_ts, "candidate_id": None, "sku": sku, "status": "pending_review"},
                        "$addToSet": {"collections": {"$each": hashes}},
                    },
                    upsert=True,
                )
                is_insert = res.upserted_id is not None
                inserted += int(is_insert)
                updated += int(not is_insert)
                if is_insert:
                    new_skus.append(sku)
            except Exception:
                logger.error("index upsert (collections) failed", extra={"event": "index_upsert_failed", "task_id": task_id, "sku": sku})

        await _send_status(
            prod,
            {
                "task_id": task_id,
                "status": "ok",
                "batch_id": batch_id,
                "trigger": trigger,
                "batch_data": {"batch_id": batch_id, "pages": 0, "skus": skus, "inserted": inserted, "updated": updated, "new_skus": new_skus},
            },
        )
        logger.info(
            "collections batch done",
            extra={"event": "collections_ok", "task_id": task_id, "batch_id": batch_id, "counts": {"inserted": inserted, "updated": updated, "new_skus": len(new_skus)}},
        )
    except Exception as e:
        await _send_status(
            prod,
            {"task_id": task_id, "status": "failed", "batch_id": batch_id, "error": str(e), "error_message": str(e), "reason_code": "unexpected_error", "trigger": trigger},
        )
        logger.exception("collections batch failed", extra={"event": "failed", "task_id": task_id, "batch_id": batch_id})


async def _map_sku_to_collection_hashes(task_id: str, skus: List[str]) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {s: set() for s in skus}
    cur = db.collection_members.find(
        {"task_id": task_id, "sku": {"$in": [str(s).strip() for s in skus]}},
        {"collection_hash": 1, "sku": 1, "_id": 0},
    )
    async for doc in cur:
        s = str(doc.get("sku") or "")
        ch = doc.get("collection_hash")
        if s and ch and s in mapping:
            mapping[s].add(ch)
    return mapping


# ---------- kafka main ----------

async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_INDEXER_CMD,
        bootstrap_servers=BOOT,
        group_id="indexer-worker",
        value_deserializer=_loads,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    prod = AIOKafkaProducer(bootstrap_servers=BOOT, value_serializer=_dumps, enable_idempotence=True)

    await cons.start()
    await prod.start()
    logger.info("consumer/producer started", extra={"event": "boot"})
    try:
        async for msg in cons:
            payload = msg.value or {}
            logger.info("message received", extra={"event": "msg", "topic": TOPIC_INDEXER_CMD, "has_task_id": bool(payload.get("task_id")), "cmd": payload.get("cmd")})
            try:
                await _dispatch(payload, prod)
                await cons.commit()
                logger.info("message committed", extra={"event": "commit_ok"})
            except Exception:
                logger.exception("message processing failed", extra={"event": "task_failed"})
    finally:
        await cons.stop()
        await prod.stop()
        logger.info("consumer/producer stopped", extra={"event": "shutdown"})


if __name__ == "__main__":
    asyncio.run(main())
