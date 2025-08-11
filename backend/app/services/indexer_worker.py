from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.logging import configure_logging
from app.db.mongo import db
from app.services.indexer import ProductIndexer

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
MAX_PAGES_DEF = int(os.getenv("INDEX_MAX_PAGES", 3))
CATEGORY_DEF = os.getenv("INDEX_CATEGORY_ID", "9373")

configure_logging()
logger = logging.getLogger("indexer-worker")
indexer = ProductIndexer()


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
    await cons.start()
    await prod.start()
    try:
        async for msg in cons:
            payload = msg.value or {}
            task_id = payload.get("task_id")
            search = payload.get("search_term", "")
            category = payload.get("category_id", CATEGORY_DEF)
            max_pages = payload.get("max_pages", MAX_PAGES_DEF)
            try:
                await handle(task_id, search, category, max_pages, prod)
                await cons.commit()
            except Exception as exc:
                logger.exception("Task failed: %s", exc)
    finally:
        await cons.stop()
        await prod.stop()


async def handle(
    task_id: str,
    query: str,
    category: str,
    max_pages: int,
    prod: AIOKafkaProducer,
) -> None:
    try:
        await prod.send_and_wait(TOPIC_INDEXER_STATUS, {"task_id": task_id, "status": "running"})
        logger.info("Indexing query=%r category=%s pages=%s", query, category, max_pages)

        scraped = failed = page_no = 0
        inserted = updated = 0

        async for batch in indexer.iter_products(
            query=query,
            category=category,
            start_page=1,
            max_pages=max_pages,
            headless=True,
        ):
            page_no += 1
            now_ts = int(time.time())
            batch_skus: List[str] = []

            for p in batch:
                sku = p.get("sku")
                if not sku:
                    continue

                doc_now = {
                    "name": p.get("name"),
                    "rating": p.get("rating"),
                    "reviews": p.get("reviews"),
                    "cover_image": p.get("cover_image"),
                    "last_seen_at": now_ts,
                    "is_active": True,
                    "task_id": task_id,
                }

                start = time.perf_counter()
                try:
                    res = await db.index.update_one(
                        {"sku": sku},
                        {
                            "$set": doc_now,
                            "$setOnInsert": {
                                "first_seen_at": now_ts,
                                "candidate_id": None,
                                "sku": sku,
                            },
                        },
                        upsert=True,
                    )
                    if res.upserted_id is not None:
                        inserted += 1
                    else:
                        updated += 1
                    scraped += 1
                    batch_skus.append(sku)
                    logger.debug("Mongo upsert sku=%s OK (%.3fs)", sku, time.perf_counter() - start)
                except Exception as exc:
                    failed += 1
                    logger.error(
                        "Mongo upsert sku=%s failed after %.3fs: %s",
                        sku,
                        time.perf_counter() - start,
                        exc,
                        exc_info=True,
                    )

            await prod.send_and_wait(
                TOPIC_INDEXER_STATUS,
                {
                    "task_id": task_id,
                    "status": "batch_ready",
                    "batch_data": {"batch_id": page_no, "skus": batch_skus},
                    "scraped_products": scraped,
                    "failed_products": failed,
                    "inserted_products": inserted,
                    "updated_products": updated,
                },
            )
            logger.info(
                "Batch %s done: saved=%s failed=%s (total saved=%s)",
                page_no,
                len(batch_skus),
                failed,
                scraped,
            )

        total = scraped + failed
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {
                "task_id": task_id,
                "status": "completed",
                "scraped_products": scraped,
                "failed_products": failed,
                "inserted_products": inserted,
                "updated_products": updated,
                "total_products": total,
            },
        )
        logger.info("Task %s completed: total=%s scraped=%s failed=%s", task_id, total, scraped, failed)
    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {"task_id": task_id, "status": "failed", "error_message": str(e)},
        )
        logger.exception("Task %s failed: %s", task_id, e)


if __name__ == "__main__":
    asyncio.run(main())
