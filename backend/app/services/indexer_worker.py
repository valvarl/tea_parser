"""
Listens topic <index_requests>, runs ProductIndexer, pushes rows to <product_raw>
Now includes detailed debug-level logging around MongoDB operations and
conversion of unsupported Python types (e.g. datetime.date) before the upsert
so that they don't trigger encoding errors.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import date, datetime
from typing import Any, Dict, List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from app.core.logging import configure_logging
from app.db.mongo import db
from app.services.indexer import ProductIndexer  # ваш класс выше

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_INDEXER_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_INDEXER_STATUS = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")
MAX_PAGES_DEF = int(os.getenv("INDEX_MAX_PAGES", 3))
CATEGORY_DEF = os.getenv("INDEX_CATEGORY_ID", "9373")

configure_logging()
logger = logging.getLogger("indexer-worker")
indexer = ProductIndexer()  # один экземпляр


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _prepare_for_mongo(obj: Any) -> Any:
    """Recursively convert Python objects that PyMongo can't encode (e.g. ``date``)
    into types it understands (``datetime``).
    """
    if isinstance(obj, dict):
        return {k: _prepare_for_mongo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_prepare_for_mongo(v) for v in obj]
    # Convert ``datetime.date`` (not ``datetime``) to midnight UTC ``datetime``
    if isinstance(obj, date) and not isinstance(obj, datetime):
        return datetime(obj.year, obj.month, obj.day)
    return obj


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

async def main() -> None:
    """Entry point that blocks consuming Kafka until interrupted."""
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
            search = msg.value["search_term"]
            category = msg.value.get("category_id", CATEGORY_DEF)
            max_pages = msg.value.get("max_pages", MAX_PAGES_DEF)
            try:
                await handle(task_id, search, category, max_pages, prod)
                await cons.commit()  # зафиксируем offset, если всё без ошибок
            except Exception as exc:  # не коммитим → ретрай
                logger.exception("task failed: %s", exc)
    finally:
        await cons.stop(); await prod.stop()


async def handle(
    task_id: str,
    query: str,
    category: str,
    max_pages: int,
    prod: AIOKafkaProducer,
) -> None:
    """Orchestrates a single indexing task and pushes progress to Kafka."""
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
                p["task_id"] = task_id
                sku = p.get("sku")

                # --- MongoDB interaction with detailed timing & outcome logging ---
                prepared_doc: Dict[str, Any] = _prepare_for_mongo(p)
                start = time.perf_counter()
                try:
                    await db.index.update_one({"sku": sku}, {"$set": prepared_doc}, upsert=True)
                    duration = time.perf_counter() - start
                    logger.debug("Mongo upsert sku=%s OK (%.3fs)", sku, duration)
                    scraped += 1
                    batch_skus.append(sku)
                except Exception as exc:
                    duration = time.perf_counter() - start
                    logger.error(
                        "Mongo upsert sku=%s FAILED after %.3fs: %s", sku, duration, exc,
                        exc_info=True,
                    )
                    failed += 1
                # -----------------------------------------------------------------

            # send batch status – useful for downstream consumers
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
            logger.info(
                "Batch %s done: saved=%s failed=%s (total saved so far=%s)",
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
                "total_products": total,
            },
        )
        logger.info("✅ task %s completed: total=%s scraped=%s failed=%s", task_id, total, scraped, failed)
    except Exception as e:
        await prod.send_and_wait(
            TOPIC_INDEXER_STATUS,
            {"task_id": task_id, "status": "failed", "error_message": str(e)},
        )
        logger.exception("Task %s failed: %s", task_id, e)


if __name__ == "__main__":
    asyncio.run(main())
