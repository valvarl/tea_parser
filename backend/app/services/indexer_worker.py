from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Set

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.logging import configure_logging
from app.db.mongo import db
from app.services.indexer import ProductIndexer, CircuitOpen

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
MAX_PAGES_DEF = int(os.getenv("INDEX_MAX_PAGES", 1))
CATEGORY_DEF = os.getenv("INDEX_CATEGORY_ID", "9373")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

configure_logging()
logger = logging.getLogger("indexer-worker")
indexer = ProductIndexer()


# ===== utils =====

def _now_ts() -> int:
    return int(time.time())

def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))

def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False).encode("utf-8")

def _chunked(seq: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for it in seq:
        buf.append(it)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# ===== kafka main =====

async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_INDEXER_CMD,
        bootstrap_servers=BOOT,
        group_id="indexer-worker",
        value_deserializer=_loads,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        value_serializer=_dumps,
        enable_idempotence=True,
    )
    await cons.start()
    await prod.start()
    try:
        async for msg in cons:
            payload = msg.value or {}
            try:
                await _dispatch(payload, prod)
                await cons.commit()
            except Exception as exc:
                logger.exception("Task failed: %s", exc)
    finally:
        await cons.stop()
        await prod.stop()


# ===== dispatcher =====

async def _dispatch(payload: Dict[str, Any], prod: AIOKafkaProducer) -> None:
    cmd = (payload.get("cmd") or "").lower()
    if cmd == "add_collection_members":
        await _handle_add_collection_members(
            task_id=payload["task_id"],
            batch_id=payload.get("batch_id"),
            skus=payload.get("skus", []),
            prod=prod,
        )
        return

    await _handle_search(
        task_id=payload.get("task_id"),
        query=payload.get("search_term", ""),
        category=payload.get("category_id", CATEGORY_DEF),
        max_pages=int(payload.get("max_pages", MAX_PAGES_DEF)),
        prod=prod,
    )


# ===== search flow =====

async def _handle_search(
    task_id: str,
    query: str,
    category: str,
    max_pages: int,
    prod: AIOKafkaProducer,
) -> None:
    try:
        await prod.send_and_wait(TOPIC_INDEXER_STATUS, {"task_id": task_id, "status": "running"})
        logger.info("index: query=%r category=%s pages=%s", query, category, max_pages)

        now_ts = _now_ts()
        scraped = failed = page_no = inserted = updated = 0

        try:
            async for batch in indexer.iter_products(
                query=query,
                category=category,
                start_page=1,
                max_pages=max_pages,
                headless=True,
            ):
                page_no += 1
                batch_skus: List[str] = []
                batch_ins = batch_upd = batch_fail = 0

                for p in batch:
                    sku = str(p.get("sku") or "")
                    if not sku:
                        continue
                    try:
                        res = await db.index.update_one(
                            {"sku": sku},
                            {
                                "$set": {
                                    "last_seen_at": now_ts,
                                    "is_active": True,
                                    "task_id": task_id,
                                },
                                "$setOnInsert": {
                                    "first_seen_at": now_ts,
                                    "candidate_id": None,
                                    "sku": sku,
                                    "status": "indexed_auto",
                                },
                            },
                            upsert=True,
                        )
                        is_insert = res.upserted_id is not None
                        inserted += int(is_insert)
                        updated  += int(not is_insert)
                        scraped += 1
                        batch_skus.append(sku)
                        batch_ins += int(is_insert)
                        batch_upd += int(not is_insert)
                    except Exception as exc:
                        failed += 1
                        batch_fail += 1
                        logger.error("index upsert failed sku=%s: %s", sku, exc, exc_info=True)

                await prod.send_and_wait(
                    TOPIC_INDEXER_STATUS,
                    {
                        "source": "indexer",
                        "version": SERVICE_VERSION,
                        "task_id": task_id,
                        "status": "ok",
                        "batch_id": page_no,
                        "batch_data": {
                            "batch_id": page_no,
                            "pages": 1, 
                            "skus": batch_skus,
                            "inserted": batch_ins,
                            "updated": batch_upd
                            },
                        "scraped_products": scraped,
                        "failed_products": failed,
                        "inserted_products": inserted,
                        "updated_products": updated,
                        "ts": _now_ts(),
                    },
                )
                logger.info("page %s done: saved=%s failed=%s total_saved=%s", page_no, len(batch_skus), failed, scraped)

        except CircuitOpen as co:
            next_retry_at = getattr(co, "next_retry_at", _now_ts() + 600)
            await prod.send_and_wait(
                TOPIC_INDEXER_STATUS,
                {
                    "source": "indexer",
                    "version": SERVICE_VERSION,
                    "task_id": task_id,
                    "status": "deferred",
                    "reason": str(co),
                    "next_retry_at": next_retry_at,
                    "scraped_products": scraped,
                    "failed_products": failed,
                    "inserted_products": inserted,
                    "updated_products": updated,
                    "ts": _now_ts(),
                },
            )
            logger.warning("task %s deferred until %s (%s)", task_id, next_retry_at, co)
            return

        total = scraped + failed
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "source": "indexer",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "status": "task_done",
                "done": True,
                "scraped_products": scraped,
                "failed_products": failed,
                "inserted_products": inserted,
                "updated_products": updated,
                "total_products": total,
                "ts": _now_ts(),
            },
        )
        logger.info("task %s done: total=%s scraped=%s failed=%s", task_id, total, scraped, failed)

    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {"task_id": task_id, "status": "failed", "error": str(e), "error_message": str(e), "ts": _now_ts()},
        )
        logger.exception("task %s failed: %s", task_id, e)


# ===== collections flow =====

async def _handle_add_collection_members(
    task_id: str,
    batch_id: Optional[int],
    skus: List[str],
    prod: AIOKafkaProducer,
) -> None:
    try:
        if not skus:
            await prod.send_and_wait(
                TOPIC_INDEXER_STATUS,
                {
                    "task_id": task_id,
                    "status": "ok",
                    "batch_id": batch_id,
                    "batch_data": {"batch_id": batch_id, "skus": []},
                    "scraped_products": 0,
                    "failed_products": 0,
                    "inserted_products": 0,
                    "updated_products": 0,
                    "ts": _now_ts(),
                },
            )
            return

        now_ts = _now_ts()
        inserted = updated = failed = 0

        coll_map = await _map_sku_to_collection_hashes(task_id=task_id, skus=skus)

        for sku in skus:
            hashes = sorted(coll_map.get(sku, set()))
            try:
                res = await db.index.update_one(
                    {"sku": sku},
                    {
                        "$set": {
                            "last_seen_at": now_ts,
                            "is_active": True,
                            "task_id": task_id,
                        },
                        "$setOnInsert": {
                            "first_seen_at": now_ts, 
                            "candidate_id": None, 
                            "sku": sku,
                            "status": "pending_review"
                        },
                        "$addToSet": {"collections": {"$each": hashes}},
                    },
                    upsert=True,
                )
                is_insert = res.upserted_id is not None
                inserted += int(is_insert)
                updated  += int(not is_insert)
            except Exception as exc:
                failed += 1
                logger.error("index upsert (collections) failed sku=%s: %s", sku, exc, exc_info=True)

        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "source": "indexer",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "status": "ok",
                "batch_id": batch_id,
                "batch_data": {
                    "batch_id": batch_id, 
                    "pages": 0,
                    "skus": skus,
                    "inserted": inserted,
                    "updated": updated,
                },
                "scraped_products": inserted + updated,
                "failed_products": failed,
                "inserted_products": inserted,
                "updated_products": updated,
                "ts": _now_ts(),
            },
        )
    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {"task_id": task_id, "status": "failed", "batch_id": batch_id, "error": str(e), "error_message": str(e), "ts": _now_ts()},
        )
        logger.exception("collections batch failed task=%s batch=%s: %s", task_id, batch_id, e)


async def _map_sku_to_collection_hashes(task_id: str, skus: List[str]) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {s: set() for s in skus}
    cur = db.collections.find(
        {"task_id": task_id, "skus.sku": {"$in": skus}},
        {"collection_hash": 1, "skus": 1},
    )
    async for col in cur:
        ch = col.get("collection_hash")
        if not ch:
            continue
        for sdoc in col.get("skus", []):
            s = str(sdoc.get("sku") or "")
            if s in mapping:
                mapping[s].add(ch)
    return mapping


if __name__ == "__main__":
    asyncio.run(main())
