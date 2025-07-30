from __future__ import annotations

import asyncio, json, logging, os, uuid
from datetime import datetime
from typing import Dict, Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.db.mongo import db

logger = logging.getLogger(__name__)

BOOT                    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD       = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS    = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
TOPIC_ENRICHER_CMD      = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS   = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")

# ────────────────────────── helpers ──────────────────────────
def dumps(x): return json.dumps(x).encode()
def loads(x): return json.loads(x.decode())


async def _patch(task_id: str, patch: Dict[str, Any]) -> None:
    patch["updated_at"] = datetime.utcnow()
    await db.scraping_tasks.update_one({"id": task_id}, {"$set": patch})


async def _new_producer() -> AIOKafkaProducer:
    prod = AIOKafkaProducer(bootstrap_servers=BOOT,
                            value_serializer=dumps,
                            enable_idempotence=True)
    await prod.start()
    return prod


def _new_consumer(topic: str, group: str) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        topic,
        bootstrap_servers=BOOT,
        group_id=group,
        value_deserializer=loads,
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


# ────────────────────── public entrypoint ────────────────────
async def scrape_tea_products_task(
    search_term: str, 
    task_id: str, 
    category_id: str = "9373", 
    max_pages: int = 3
) -> None:
    # create DB stub
    await _patch(task_id, {"status": "queued", "search_term": search_term})

    # ─── 1. посылаем команду indexer ───
    prod_cmd = await _new_producer()
    await prod_cmd.send_and_wait(TOPIC_INDEXER_CMD, {"task_id": task_id,
                                                     "search_term": search_term,
                                                     "category_id": category_id,
                                                     "max_pages": max_pages})
    await prod_cmd.stop()
    logger.info("📤 indexer command sent (task=%s, q='%s')", task_id, search_term)

    # ─── 2. поднимаем два consumer’а ───
    c_idx = _new_consumer(TOPIC_INDEXER_STATUS,  f"indexer-monitor-{uuid.uuid4()}")
    c_enr = _new_consumer(TOPIC_ENRICHER_STATUS,  f"enricher-monitor-{uuid.uuid4()}")
    await asyncio.gather(c_idx.start(), c_enr.start())

    # 2а. producer для команд к enricher
    prod_enr = await _new_producer()

    idx_done = enr_done = False

    async def watch_indexer():
        nonlocal idx_done
        while True:
            try:
                m = await asyncio.wait_for(c_idx.getone(), timeout=300)
            except asyncio.TimeoutError:
                logger.error("indexer status timeout")
                break

            st: dict = m.value
            if st.get("task_id") != task_id:
                continue

            await _patch(task_id, {"indexer": st})

            if st.get("status") == "batch_ready":
                batch = st["batch_data"]
                await prod_enr.send_and_wait(
                    TOPIC_ENRICHER_CMD,
                    {"task_id": task_id, "skus": batch.get("skus", [])},
                )
                logger.info("→ batch %s forwarded to enricher", batch.get("batch_id"))

            if st.get("status") == "completed":
                await prod_enr.send_and_wait(TOPIC_ENRICHER_CMD, {"task_id": task_id})
                logger.info("➡ enricher command sent for task %s", task_id)

            if st.get("status") in {"completed", "failed"}:
                idx_done = True
                break

    async def watch_enricher():
        nonlocal enr_done
        while True:
            try:
                m = await asyncio.wait_for(c_enr.getone(), timeout=300)
            except asyncio.TimeoutError:
                logger.error("enricher status timeout")
                break

            st: dict = m.value
            if st.get("task_id") != task_id:
                continue

            await _patch(task_id, {"enricher": st})

            if st.get("status") in {"completed", "failed"}:
                enr_done = True
                break

    # ─── 3. параллельно ждём оба стрима ───
    try:
        await asyncio.gather(watch_indexer(), watch_enricher())
    finally:
        await asyncio.gather(c_idx.stop(), c_enr.stop(), prod_enr.stop())

    # ─── 4. финал ───
    final_status = (
        "completed"
        if idx_done and enr_done
        else "failed"
    )
    await _patch(task_id, {"status": final_status,
                           "completed_at": datetime.utcnow()})

    logger.info("🏁 scraping task %s finished (%s)", task_id, final_status)
