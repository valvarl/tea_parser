from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List

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


# -------- helpers --------


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
    patch["updated_at"] = datetime.utcnow()
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


# -------- public entrypoint --------


async def scrape_tea_products_task(
    search_term: str,
    task_id: str,
    category_id: str = "9373",
    max_pages: int = 3,
) -> None:
    await _patch(task_id, {"status": "queued", "search_term": search_term})

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

    async def watch_indexer() -> None:
        nonlocal idx_done
        while True:
            try:
                msg = await asyncio.wait_for(c_idx.getone(), timeout=STATUS_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.error("indexer status timeout (>%ss)", STATUS_TIMEOUT_SEC)
                break

            st: Dict[str, Any] = msg.value or {}
            if st.get("task_id") != task_id:
                continue

            await _patch(task_id, {"indexer": st})

            if st.get("status") == "batch_ready":
                skus = list(st.get("batch_data", {}).get("skus", []) or [])
                if not skus:
                    logger.debug("empty batch from indexer; skipping")
                else:
                    for i, chunk in enumerate(_chunked(skus, ENRICHER_BATCH_SIZE), start=1):
                        await prod_enr.send_and_wait(TOPIC_ENRICHER_CMD, {"task_id": task_id, "skus": chunk})
                        logger.info(
                            "forwarded to enricher: page_batch=%s part=%s size=%s", st.get("batch_id"), i, len(chunk)
                        )

            if st.get("status") == "completed":
                # optional finalize signal for enricher
                await prod_enr.send_and_wait(TOPIC_ENRICHER_CMD, {"task_id": task_id})
                logger.info("finalize signal sent to enricher (task=%s)", task_id)

            if st.get("status") in {"completed", "failed"}:
                idx_done = True
                break

    async def watch_enricher() -> None:
        nonlocal enr_done
        while True:
            try:
                msg = await asyncio.wait_for(c_enr.getone(), timeout=STATUS_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                logger.error("enricher status timeout (>%ss)", STATUS_TIMEOUT_SEC)
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
    finally:
        await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

    final_status = "completed" if idx_done and enr_done else "failed"
    await _patch(task_id, {"status": final_status, "completed_at": datetime.utcnow()})
    logger.info("scraping task %s finished (%s)", task_id, final_status)
