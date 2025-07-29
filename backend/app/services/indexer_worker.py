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
TOPIC_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_ST  = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
MAX_PAGES_DEF = int(os.getenv("INDEX_MAX_PAGES", 3))
CATEGORY_DEF  = os.getenv("INDEX_CATEGORY_ID", "9373")

configure_logging()
logger = logging.getLogger("indexer-worker")
indexer = ProductIndexer()                               # один экземпляр


async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_CMD,
        bootstrap_servers=BOOT,
        group_id="indexer-worker",
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset="earliest",
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
            try:
                await handle(task_id, search, prod)
            except Exception as exc:          # не коммитим → ретрай
                logger.exception("task failed: %s", exc)
    finally:
        await cons.stop(); await prod.stop()


async def handle(task_id: str, query: str, prod: AIOKafkaProducer) -> None:
    try:
        await prod.send_and_wait(TOPIC_ST, {"task_id": task_id, "status": "running"})
        logger.info("🔍 indexing '%s' (cat=%s, pages=%s)", query, CATEGORY_DEF, MAX_PAGES_DEF)
        products: List[Dict] = await indexer.search_products(
            query=query,
            category=CATEGORY_DEF,
            start_page=1,
            max_pages=MAX_PAGES_DEF,
            headless=True,
        )
        total = len(products)
        logger.info("→ %d products", total)

        scraped = failed = 0
        for idx, p in enumerate(products, 1):
            try:
                await db.tea_products.update_one(
                    {"ozon_id": p.get("ozon_id")}, {"$set": p}, upsert=True
                )
                scraped += 1
            except Exception:
                failed += 1

            # каждые 10 товаров отдаём прогресс
            if idx % 10 == 0 or idx == total:
                await prod.send_and_wait(
                    TOPIC_ST,
                    {
                        "task_id": task_id,
                        "status": "running",
                        "scraped_products": scraped,
                        "failed_products": failed,
                        "total_products": total,
                    },
                )

        await prod.send_and_wait(
            TOPIC_ST,
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
            TOPIC_ST,
            {"task_id": task_id, "status": "failed", "error_message": str(e)},
        )


if __name__ == "__main__":
    asyncio.run(main())
