"""
Listens topic <index_requests>, runs ProductIndexer, pushes rows to <product_raw>
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.logging import configure_logging
from app.db.mongo import db
from app.services.indexer import ProductIndexer  # ваш класс выше

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
MAX_PAGES_DEF = int(os.getenv("INDEX_MAX_PAGES", 3))
CATEGORY_DEF  = os.getenv("INDEX_CATEGORY_ID", "9373")

configure_logging()
logger = logging.getLogger("indexer-worker")
indexer = ProductIndexer()                               # один экземпляр


async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_INDEXER_CMD,
        bootstrap_servers=BOOT,
        group_id="indexer-worker",
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
            task_id = msg.value["task_id"]
            search  = msg.value["search_term"]
            category = msg.value.get("category_id", CATEGORY_DEF)
            max_pages = msg.value.get("max_pages", MAX_PAGES_DEF)
            try:
                await handle(task_id, search, category, max_pages, prod)
            except Exception as exc:          # не коммитим → ретрай
                logger.exception("task failed: %s", exc)
    finally:
        await cons.stop(); await prod.stop()


async def handle(task_id: str, query: str, category: str, max_pages: int, prod: AIOKafkaProducer) -> None:
    try:
        await prod.send_and_wait(TOPIC_INDEXER_STATUS, {"task_id": task_id, "status": "running"})
        logger.info("🔍 indexing '%s' (cat=%s, pages=%s)", query, category, max_pages)
        scraped = failed = page_no = 0
        async for batch in indexer.iter_products(
            query=query,
            category=category,
            start_page=1,
            max_pages=max_pages,
            headless=True,
        ):
            page_no += 1
            batch_skus: List[str] = []
            for p in batch:
                try:
                    p["task_id"] = task_id
                    await db.index.update_one(
                        {"sku": p.get("sku")}, {"$set": p}, upsert=True
                    )
                    scraped += 1
                    batch_skus.append(p.get("sku"))
                except Exception:
                    failed += 1

            await prod.send_and_wait(
                TOPIC_INDEXER_STATUS,
                {
                    "task_id": task_id,
                    "status": "batch_ready",
                    "batch_data": {"batch_id": page_no, "skus": batch_skus},
                    "scraped_products": scraped,
                    "failed_products": failed,
                },
            )

        total = scraped + failed
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "task_id": task_id,
                "status": "completed",
                "scraped_products": scraped,
                "failed_products": failed,
                "total_products": total,
            },
        )
    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {"task_id": task_id, "status": "failed", "error_message": str(e)},
        )


if __name__ == "__main__":
    asyncio.run(main())
