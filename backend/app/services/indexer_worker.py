from __future__ import annotations

import asyncio
import json
import logging
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
            except Exception:
                logger.exception("task failed", extra={"event": "task_failed"})
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
            trigger=payload.get("trigger") or "collections_followup",
            prod=prod,
        )
        return

    await _handle_search(
        task_id=payload.get("task_id"),
        query=payload.get("search_term", ""),
        category=payload.get("category_id", CATEGORY_DEF),
        max_pages=int(payload.get("max_pages", MAX_PAGES_DEF)),
        trigger=payload.get("trigger") or "search",
        prod=prod,
    )


# ===== search flow =====

async def _handle_search(
    task_id: str,
    query: str,
    category: str,
    max_pages: int,
    trigger: str,
    prod: AIOKafkaProducer,
) -> None:
    async def _sleep_backoff(attempt: int) -> int:
        # exp backoff c капом
        delay = min(INDEX_RETRY_MAX_SEC, INDEX_RETRY_BASE_SEC * (2 ** (attempt - 1)))
        await asyncio.sleep(delay)
        return delay

    try:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "source": "indexer",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "status": "running",
                "ts": _now_ts(),
                "trigger": trigger,
            },
        )
        logger.info(
            "running",
            extra={
                "event": "running",
                "task_id": task_id,
                "trigger": trigger,
                "counts": {"max_pages": max_pages},
            },
        )

        now_ts = _now_ts()
        scraped = failed = page_no = inserted = updated = 0

        attempt = 0
        while True:
            attempt += 1
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
                            updated += int(not is_insert)
                            scraped += 1
                            batch_skus.append(sku)
                            batch_ins += int(is_insert)
                            batch_upd += int(not is_insert)
                        except Exception:
                            failed += 1
                            batch_fail += 1
                            logger.error(
                                "index upsert failed",
                                extra={"event": "index_upsert_failed", "task_id": task_id, "sku": sku},
                            )

                    await prod.send_and_wait(
                        TOPIC_INDEXER_STATUS,
                        {
                            "source": "indexer",
                            "version": SERVICE_VERSION,
                            "task_id": task_id,
                            "status": "ok",
                            "batch_id": page_no,
                            "ts": _now_ts(),
                            "trigger": trigger,
                            "batch_data": {
                                "batch_id": page_no,
                                "pages": 1,
                                "skus": batch_skus,
                                "inserted": batch_ins,
                                "updated": batch_upd,
                                "failed": batch_fail,
                            },
                            "scraped_products": scraped,
                            "failed_products": failed,
                            "inserted_products": inserted,
                            "updated_products": updated,
                        },
                    )
                    logger.info(
                        "page done",
                        extra={
                            "event": "batch_ok",
                            "task_id": task_id,
                            "batch_id": page_no,
                            "counts": {"inserted": batch_ins, "updated": batch_upd, "failed": batch_fail},
                        },
                    )

                # если итерация прошла без CircuitOpen — завершаем
                break

            except CircuitOpen as co:
                # ретраи внутри воркера, без участия координатора и БД
                if attempt >= INDEX_RETRY_MAX:
                    err = f"circuit_open_retry_exhausted: {co}"
                    await prod.send_and_wait(
                        TOPIC_INDEXER_STATUS,
                        {
                            "source": "indexer",
                            "version": SERVICE_VERSION,
                            "task_id": task_id,
                            "status": "failed",
                            "error": err,
                            "error_message": err,
                            "reason_code": "circuit_open_retry_exhausted",
                            "ts": _now_ts(),
                            "trigger": trigger,
                        },
                    )
                    logger.error("indexer failed after retries", extra={"event": "failed", "task_id": task_id, "reason_code": "circuit_open_retry_exhausted"})
                    return
                # информируем, что будет повтор
                delay = min(INDEX_RETRY_MAX_SEC, INDEX_RETRY_BASE_SEC * (2 ** (attempt - 1)))
                await prod.send_and_wait(
                    TOPIC_INDEXER_STATUS,
                    {
                        "source": "indexer",
                        "version": SERVICE_VERSION,
                        "task_id": task_id,
                        "status": "retrying",
                        "attempt": attempt,
                        "next_retry_in_sec": delay,
                        "ts": _now_ts(),
                        "trigger": trigger,
                    },
                )
                logger.warning(
                    "retrying due to circuit open",
                    extra={"event": "retrying", "task_id": task_id, "attempt": attempt, "delay_sec": delay},
                )
                await asyncio.sleep(delay)
                continue

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
                "trigger": trigger,
            },
        )
        logger.info(
            "task done",
            extra={"event": "task_done", "task_id": task_id, "counts": {"total": total, "failed": failed}},
        )

    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "source": "indexer",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "status": "failed",
                "error": str(e),
                "error_message": str(e),
                "reason_code": "unexpected_error",
                "ts": _now_ts(),
                "trigger": trigger,
            },
        )
        logger.exception("task failed", extra={"event": "failed", "task_id": task_id, "reason_code": "unexpected_error"})


# ===== collections flow =====

async def _handle_add_collection_members(
    task_id: str,
    batch_id: Optional[int],
    skus: List[str],
    trigger: str,
    prod: AIOKafkaProducer,
) -> None:
    try:
        if not skus:
            await prod.send_and_wait(
                TOPIC_INDEXER_STATUS,
                {
                    "source": "indexer",
                    "version": SERVICE_VERSION,
                    "task_id": task_id,
                    "status": "ok",
                    "batch_id": batch_id,
                    "ts": _now_ts(),
                    "trigger": trigger,
                    "batch_data": {"batch_id": batch_id, "pages": 0, "skus": [], "inserted": 0, "updated": 0, "failed": 0},
                    "scraped_products": 0,
                    "failed_products": 0,
                    "inserted_products": 0,
                    "updated_products": 0,
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
            except Exception:
                failed += 1
                logger.error("index upsert (collections) failed", extra={"event": "index_upsert_failed", "task_id": task_id, "sku": sku})

        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "source": "indexer",
                "version": SERVICE_VERSION,
                "task_id": task_id,
                "status": "ok",
                "batch_id": batch_id,
                "ts": _now_ts(),
                "trigger": trigger,
                "batch_data": {
                    "batch_id": batch_id,
                    "pages": 0,
                    "skus": skus,
                    "inserted": inserted,
                    "updated": updated,
                    "failed": failed,
                },
                "scraped_products": inserted + updated,
                "failed_products": failed,
                "inserted_products": inserted,
                "updated_products": updated,
            },
        )
    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {"source": "indexer", "version": SERVICE_VERSION, "task_id": task_id, "status": "failed", "batch_id": batch_id, "error": str(e), "error_message": str(e), "reason_code": "unexpected_error", "ts": _now_ts(), "trigger": trigger},
        )
        logger.exception("collections batch failed", extra={"event": "failed", "task_id": task_id, "batch_id": batch_id})


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
