from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.db.mongo import db

logger = logging.getLogger(__name__)

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
TOPIC_ENRICHER_CMD = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")

ENRICHER_BATCH_SIZE = max(1, int(os.getenv("ENRICHER_BATCH_SIZE", "50")))
STATUS_TIMEOUT_SEC = int(os.getenv("STATUS_TIMEOUT_SEC", "300"))
INDEXER_MAX_DEFERS = max(0, int(os.getenv("INDEXER_MAX_DEFERS", "3")))


# ---------- helpers ----------


def _dumps(x: Any) -> bytes:
    return json.dumps(x, ensure_ascii=False).encode("utf-8")


def _loads(x: bytes) -> Any:
    return json.loads(x.decode("utf-8"))


def _chunked(seq: Iterable[str], size: int) -> Iterable[List[str]]:
    buf: List[str] = []
    for s in seq:
        buf.append(s)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


async def _patch(task_id: str, patch: Dict[str, Any]) -> None:
    patch["updated_at"] = int(time.time())
    await db.scraping_tasks.update_one({"id": task_id}, {"$set": patch})


async def _new_producer() -> AIOKafkaProducer:
    prod = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        value_serializer=_dumps,
        enable_idempotence=True,
    )
    await prod.start()
    return prod


def _new_consumer(topic: str, group: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOT,
        group_id=group,
        value_deserializer=_loads,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


async def _schedule_indexer_retry(
    task_id: str,
    search_term: str,
    category_id: str,
    max_pages: int,
    next_retry_at: int,
    attempt_no: int,
) -> None:
    delay = max(0, next_retry_at - int(time.time()))
    if delay > 0:
        await asyncio.sleep(delay)

    prod = await _new_producer()
    try:
        await prod.send_and_wait(
            TOPIC_INDEXER_CMD,
            {
                "task_id": task_id,
                "search_term": search_term,
                "category_id": category_id,
                "max_pages": max_pages,
                "retry_attempt": attempt_no,
            },
        )
        await _patch(task_id, {"status": "requeued", "requeued_at": int(time.time()), "retry_attempt": attempt_no})
        logger.info("requeued indexer task=%s attempt=%s", task_id, attempt_no)
    finally:
        await prod.stop()


# ---------- public entrypoint ----------


async def scrape_tea_products_task(
    search_term: str,
    task_id: str,
    category_id: str = "9373",
    max_pages: int = 3,
) -> None:
    await _patch(
        task_id,
        {
            "status": "queued",
            "search_term": search_term,
            "category_id": category_id,
            "max_pages": max_pages,
            "retry_attempt": 0,
        },
    )

    prod_cmd = await _new_producer()
    await prod_cmd.send_and_wait(
        TOPIC_INDEXER_CMD,
        {"task_id": task_id, "search_term": search_term, "category_id": category_id, "max_pages": max_pages},
    )
    await prod_cmd.stop()
    logger.info("indexer command sent (task=%s, q=%r, cat=%s, pages=%s)", task_id, search_term, category_id, max_pages)

    c_idx = _new_consumer(TOPIC_INDEXER_STATUS, f"indexer-monitor-{uuid.uuid4()}")
    c_enr = _new_consumer(TOPIC_ENRICHER_STATUS, f"enricher-monitor-{uuid.uuid4()}")
    await asyncio.gather(c_idx.start(), c_enr.start())

    prod_enr = await _new_producer()

    idx_done = False
    enr_done = False
    deferred_retry_task: Optional[asyncio.Task] = None

    async def watch_indexer() -> None:
        nonlocal idx_done, deferred_retry_task
        while True:
            try:
                msg = await asyncio.wait_for(c_idx.getone(), timeout=STATUS_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.error("indexer status timeout (> %ss)", STATUS_TIMEOUT_SEC)
                break

            st: Dict[str, Any] = msg.value or {}
            if st.get("task_id") != task_id:
                continue

            await _patch(task_id, {"indexer": st})

            if st.get("status") == "batch_ready":
                skus = list(st.get("batch_data", {}).get("skus", []) or [])
                if skus:
                    for i, chunk in enumerate(_chunked(skus, ENRICHER_BATCH_SIZE), start=1):
                        await prod_enr.send_and_wait(TOPIC_ENRICHER_CMD, {"task_id": task_id, "skus": chunk})
                        logger.info(
                            "forwarded to enricher: page_batch=%s part=%s size=%s",
                            st.get("batch_id"),
                            i,
                            len(chunk),
                        )

            if st.get("status") == "deferred":
                attempt = int((await db.scraping_tasks.find_one({"id": task_id}, {"retry_attempt": 1})) or {}).get(
                    "retry_attempt", 0
                )
                attempt += 1

                next_retry_at = int(st.get("next_retry_at") or (int(time.time()) + 600))
                await _patch(
                    task_id,
                    {
                        "status": "deferred",
                        "next_retry_at": next_retry_at,
                        "retry_attempt": attempt,
                        "deferred_reason": st.get("reason"),
                    },
                )
                logger.warning(
                    "indexer deferred task=%s attempt=%s next_retry_at=%s reason=%s",
                    task_id,
                    attempt,
                    next_retry_at,
                    st.get("reason"),
                )

                if attempt <= INDEXER_MAX_DEFERS:
                    # schedule requeue (single task per deferral)
                    deferred_retry_task = asyncio.create_task(
                        _schedule_indexer_retry(
                            task_id=task_id,
                            search_term=search_term,
                            category_id=category_id,
                            max_pages=max_pages,
                            next_retry_at=next_retry_at,
                            attempt_no=attempt,
                        )
                    )
                else:
                    logger.error("indexer max defers exceeded for task=%s", task_id)

                # keep listening for enricher, but indexer stream is effectively done
                idx_done = True
                break

            if st.get("status") == "completed":
                await prod_enr.send_and_wait(TOPIC_ENRICHER_CMD, {"task_id": task_id})
                logger.info("finalize signal sent to enricher (task=%s)", task_id)
                idx_done = True
                break

            if st.get("status") == "failed":
                idx_done = True
                break

    async def watch_enricher() -> None:
        nonlocal enr_done
        while True:
            try:
                msg = await asyncio.wait_for(c_enr.getone(), timeout=STATUS_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.error("enricher status timeout (> %ss)", STATUS_TIMEOUT_SEC)
                break

            st: Dict[str, Any] = msg.value or {}
            if st.get("task_id") != task_id:
                continue

            await _patch(task_id, {"enricher": st})

            if st.get("status") in {"completed", "failed"}:
                enr_done = True
                break

    try:
        await asyncio.gather(watch_indexer(), watch_enricher())
        if deferred_retry_task is not None:
            # не блокируем здесь; оркестратор завершает текущий цикл,
            # а повторная попытка полетит сама по таймеру.
            pass
    finally:
        await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

    final_status = "completed" if idx_done and enr_done else "failed"
    await _patch(task_id, {"status": final_status, "completed_at": int(time.time())})
    logger.info("scraping task %s finished (%s)", task_id, final_status)
