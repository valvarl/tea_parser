"""
Listens to <enricher_cmd> Kafka topic, runs ProductEnricher on the specified
SKU‑list (or on all products lacking PDP‑fields), pushes the enriched data
into MongoDB collections <candidates> and <tea_reviews>, and emits progress
updates to <enricher_status>.

Message schema (JSON, UTF‑8):
{
    "task_id": "uuid",
    "skus": ["123456789", "987654321", ...],   # optional; if omitted → auto‑detect
    "reviews": true,            # optional, default: false
    "reviews_limit": 30,        # optional, default: 20
    "concurrency": 8            # optional, default via env
}

* If **skus** not provided, worker takes every product from index
  where charcs_json == null OR description == null.
* Enrichment progress is reported every 10 processed items (or at the end).
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
from app.services.enricher import ProductEnricher  # класс из enricher.py

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_ENRICHER_CMD    = os.getenv("TOPIC_ENRICHER_CMD", "enricher_cmd")
TOPIC_ENRICHER_STATUS = os.getenv("TOPIC_ENRICHER_STATUS", "enricher_status")
CONCURRENCY_DEF  = int(os.getenv("ENRICH_CONCURRENCY", 6))
REVIEWS_DEF      = os.getenv("ENRICH_REVIEWS", "false").lower() == "true"
REVIEWS_LIMIT_DEF = int(os.getenv("ENRICH_REVIEWS_LIMIT", 20))

configure_logging()
logger = logging.getLogger("enricher-worker")

# Один экземпляр на процесс ─ переиспользуем сессии Camoufox.
enricher = ProductEnricher(concurrency=CONCURRENCY_DEF,
                           headless=True,
                           reviews=REVIEWS_DEF,
                           reviews_limit=REVIEWS_LIMIT_DEF)


async def main() -> None:
    cons = AIOKafkaConsumer(
        TOPIC_ENRICHER_CMD,
        bootstrap_servers=BOOT,
        group_id="enricher-worker",
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
            cmd: Dict[str, Any] = msg.value
            task_id = cmd["task_id"]
            skus: List[str] | None = cmd.get("skus")
            want_reviews = bool(cmd.get("reviews", REVIEWS_DEF))
            reviews_limit = int(cmd.get("reviews_limit", REVIEWS_LIMIT_DEF))
            concurrency = int(cmd.get("concurrency", CONCURRENCY_DEF))
            try:
                await handle(task_id, skus, want_reviews, reviews_limit, concurrency, prod)
            except Exception as exc:
                logger.exception("task failed: %s", exc)
                # не коммитим → kafka ретрай
    finally:
        await cons.stop(); await prod.stop()


async def handle(task_id: str, skus: List[str] | None, want_reviews: bool,
                 reviews_limit: int, concurrency: int,
                 prod: AIOKafkaProducer) -> None:
    try:
        # 1. Выбираем товары для обогащения
        if skus:
            base_rows = await db.index.find(
                {"sku": {"$in": skus}}, {"_id": 0}
            ).to_list(None)
        else:
            base_rows = await db.index.find(
                {
                    "task_id": task_id,
                    "$or": [
                        {"charcs_json": {"$exists": False}},
                        {"description": {"$exists": False}},
                    ],
                },
                {"_id": 0},
            ).to_list(None)
        total = len(base_rows)
        if not total:
            await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
                "task_id": task_id,
                "status": "completed",
                "scraped_products": 0,
                "failed_products": 0,
                "total_products": 0,
            })
            logger.info("task %s — nothing to enrich", task_id)
            return

        # 2. Настраиваем enricher под параметры задачи (thread‑safe)
        enricher.concurrency   = concurrency
        enricher.want_reviews  = want_reviews
        enricher.reviews_limit = reviews_limit

        await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
            "task_id": task_id,
            "status": "running",
        })
        logger.info("⚙️  enriching %d products (reviews=%s)", total, want_reviews)

        # 3. Запускаем enrichment
        rows_plus, reviews = await enricher.enrich(base_rows)

        # 4. Сохранение в MongoDB с прогресс‑репортом
        scraped = failed = 0
        for idx, row in enumerate(rows_plus, 1):
            try:
                await db.candidates.update_one({"sku": row["sku"]}, {"$set": row}, upsert=True)
                scraped += 1
            except Exception:
                failed += 1

            if idx % 10 == 0 or idx == total:
                await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
                    "task_id": task_id,
                    "status": "running",
                    "scraped_products": scraped,
                    "failed_products": failed,
                    "total_products": total,
                })

        # 4b. Отзывы — в отдельную коллекцию
        if want_reviews and reviews:
            for rv in reviews:
                try:
                    await db.tea_reviews.update_one(
                        {"sku": rv["sku"], "author": rv["author"], "date": rv["date"]},
                        {"$set": rv}, upsert=True)
                except Exception:
                    logger.warning("failed to upsert review for %s", rv.get("sku"))

        # 5. Финальное сообщение
        await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
            "task_id": task_id,
            "status": "completed",
            "scraped_products": scraped,
            "failed_products": failed,
            "total_products": total,
            "saved_reviews": len(reviews) if want_reviews else 0,
        })
    except Exception as e:
        await prod.send_and_wait(TOPIC_ENRICHER_STATUS, {
            "task_id": task_id,
            "status": "failed",
            "error_message": str(e),
        })
        raise


if __name__ == "__main__":
    asyncio.run(main())
