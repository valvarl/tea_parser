from __future__ import annotations

import asyncio, json, logging, os, uuid
from datetime import datetime
from typing import Dict, Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from app.db.mongo import db
from app.models.task import ScrapingTask

logger = logging.getLogger(__name__)

BOOT      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_CMD = os.getenv("TOPIC_INDEXER_CMD", "indexer_cmd")
TOPIC_ST  = os.getenv("TOPIC_INDEXER_STATUS", "indexer_status")


async def scrape_tea_products_task(search_term: str, task_id: str) -> None:
    """
    Отправляем команду indexer-воркеру и слушаем статус-ивенты.
    MongoDB-insert сам воркер; здесь лишь обновление ScrapingTask.
    """

    async def patch(p: Dict[str, Any]) -> None:
        await db.scraping_tasks.update_one({"id": task_id}, {"$set": p})

    # 0. фиксируем факт задачи
    await patch({"status": "queued", "created_at": datetime.utcnow()})

    # 1. Producer → command
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOT,
        value_serializer=lambda x: json.dumps(x).encode(),
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    await producer.send_and_wait(
        TOPIC_CMD,
        {"task_id": task_id, "search_term": search_term},
    )
    await producer.stop()
    logger.info("📤 sent command for task %s (%s)", task_id, search_term)

    # 2. Consumer ← status
    consumer = AIOKafkaConsumer(
        TOPIC_ST,
        bootstrap_servers=BOOT,
        group_id=f"task-monitor-{uuid.uuid4()}",
        value_deserializer=lambda x: json.loads(x.decode()),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()

    try:
        async for msg in consumer:
            st: Dict[str, Any] = msg.value
            if st.get("task_id") != task_id:
                continue                              # другое задание

            # прогресс или финал — патчим документ
            await patch(st | {"updated_at": datetime.utcnow()})

            if st.get("status") in {"completed", "failed"}:
                logger.info("🏁 task %s finished (%s)", task_id, st["status"])
                break
    finally:
        await consumer.stop()

# async def scrape_tea_products_task(search_term: str, task_id: str) -> None:
    # """
    # Запускается в фоне из энд-пойнта `/scrape/start`.

    # Аргументы:
    #     search_term: строка для поиска на Ozon
    #     task_id:     идентификатор ScrapingTask в Mongo
    # """

    # async def _update_task(patch: Dict) -> None:
    #     """Удобный хелпер для патчей в коллекции `scraping_tasks`."""
    #     await db.scraping_tasks.update_one({"id": task_id}, {"$set": patch})

    # # ——— статус «running» ——— #
    # await _update_task({"status": "running", "started_at": datetime.utcnow()})
    # logger.info("🚀 Scraping task %s started (query='%s')", task_id, search_term)

    # scraped_count = failed_count = 0

    # try:
    #     # 1. Инициализируем браузер и ищем товары
    #     products = await indexer.search_products(search_term, max_pages=3)
    #     total_products = len(products)

    #     logger.info("📊 %d products found for query '%s'", total_products, search_term)

    #     if not products:
    #         await _update_task(
    #             {
    #                 "status": "completed",
    #                 "completed_at": datetime.utcnow(),
    #                 "total_products": 0,
    #                 "error_message": "No products found (geo-blocking or API change?)",
    #             }
    #         )
    #         return

    #     # 2. Обрабатываем каждый товар
    #     for idx, prod in enumerate(products, 1):
    #         try:
    #             if not prod.get("name"):
    #                 raise ValueError("product without name")

    #             tea = TeaProduct(**prod)

    #             # upsert по ozon_id (если есть) либо по id (fallback)
    #             query = {"ozon_id": tea.ozon_id} if tea.ozon_id else {"id": tea.id}
    #             exists = await db.tea_products.find_one(query)

    #             if exists:
    #                 tea.updated_at = datetime.utcnow()
    #                 await db.tea_products.update_one(query, {"$set": tea.dict()})
    #             else:
    #                 await db.tea_products.insert_one(tea.dict())

    #             scraped_count += 1
    #         except Exception as exc:  # pragma: no cover
    #             logger.exception("❌ error on product %d/%d: %s", idx, total_products, exc)
    #             failed_count += 1
    #         finally:
    #             # прогресс каждые 5 элементов или в самом конце
    #             if idx % 5 == 0 or idx == total_products:
    #                 await _update_task(
    #                     {
    #                         "scraped_products": scraped_count,
    #                         "failed_products": failed_count,
    #                         "total_products": total_products,
    #                     }
    #                 )

    #     # 3. Финальный статус
    #     await _update_task(
    #         {
    #             "status": "completed",
    #             "completed_at": datetime.utcnow(),
    #             "scraped_products": scraped_count,
    #             "failed_products": failed_count,
    #             "total_products": total_products,
    #         }
    #     )
    #     logger.info(
    #         "🎉 Task %s finished: %d scraped, %d failed",
    #         task_id,
    #         scraped_count,
    #         failed_count,
    #     )

    # except Exception as exc:  # pragma: no cover
    #     logger.exception("🔥 critical error in task %s: %s", task_id, exc)
    #     await _update_task(
    #         {
    #             "status": "failed",
    #             "completed_at": datetime.utcnow(),
    #             "error_message": str(exc),
    #         }
    #     )

    # finally:
    #     # 4. Обязательно закрываем браузер
    #     try:
    #         await indexer.close_browser()
    #     except Exception:  # pragma: no cover
    #         logger.warning("could not close browser in task %s", task_id)

    #     # небольшая пауза, чтобы фоновый поток FastAPI дошёл до конца
    #     await asyncio.sleep(0.1)
