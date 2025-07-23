"""
Фоновая корутина, которую запускает FastAPI `BackgroundTasks`.
Отвечает за полный цикл: создать задачу → запустить браузер →
спарсить продукты → сохранить / обновить в MongoDB → корректно
завершить и обновить статус.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Dict

from app.db.mongo import db
from app.models.task import ScrapingTask
from app.models.tea import TeaProduct
from app.services.indexer import indexer  # глобальный экземпляр ProductIndexer

logger = logging.getLogger(__name__)


async def scrape_tea_products_task(search_term: str, task_id: str) -> None:
    """
    Запускается в фоне из энд-пойнта `/scrape/start`.

    Аргументы:
        search_term: строка для поиска на Ozon
        task_id:     идентификатор ScrapingTask в Mongo
    """

    async def _update_task(patch: Dict) -> None:
        """Удобный хелпер для патчей в коллекции `scraping_tasks`."""
        await db.scraping_tasks.update_one({"id": task_id}, {"$set": patch})

    # ——— статус «running» ——— #
    await _update_task({"status": "running", "started_at": datetime.utcnow()})
    logger.info("🚀 Scraping task %s started (query='%s')", task_id, search_term)

    scraped_count = failed_count = 0

    try:
        # 1. Инициализируем браузер и ищем товары
        products = await indexer.search_products(search_term, max_pages=3)
        total_products = len(products)

        logger.info("📊 %d products found for query '%s'", total_products, search_term)

        if not products:
            await _update_task(
                {
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                    "total_products": 0,
                    "error_message": "No products found (geo-blocking or API change?)",
                }
            )
            return

        # 2. Обрабатываем каждый товар
        for idx, prod in enumerate(products, 1):
            try:
                if not prod.get("name"):
                    raise ValueError("product without name")

                tea = TeaProduct(**prod)

                # upsert по ozon_id (если есть) либо по id (fallback)
                query = {"ozon_id": tea.ozon_id} if tea.ozon_id else {"id": tea.id}
                exists = await db.tea_products.find_one(query)

                if exists:
                    tea.updated_at = datetime.utcnow()
                    await db.tea_products.update_one(query, {"$set": tea.dict()})
                else:
                    await db.tea_products.insert_one(tea.dict())

                scraped_count += 1
            except Exception as exc:  # pragma: no cover
                logger.exception("❌ error on product %d/%d: %s", idx, total_products, exc)
                failed_count += 1
            finally:
                # прогресс каждые 5 элементов или в самом конце
                if idx % 5 == 0 or idx == total_products:
                    await _update_task(
                        {
                            "scraped_products": scraped_count,
                            "failed_products": failed_count,
                            "total_products": total_products,
                        }
                    )

        # 3. Финальный статус
        await _update_task(
            {
                "status": "completed",
                "completed_at": datetime.utcnow(),
                "scraped_products": scraped_count,
                "failed_products": failed_count,
                "total_products": total_products,
            }
        )
        logger.info(
            "🎉 Task %s finished: %d scraped, %d failed",
            task_id,
            scraped_count,
            failed_count,
        )

    except Exception as exc:  # pragma: no cover
        logger.exception("🔥 critical error in task %s: %s", task_id, exc)
        await _update_task(
            {
                "status": "failed",
                "completed_at": datetime.utcnow(),
                "error_message": str(exc),
            }
        )

    finally:
        # 4. Обязательно закрываем браузер
        try:
            await indexer.close_browser()
        except Exception:  # pragma: no cover
            logger.warning("could not close browser in task %s", task_id)

        # небольшая пауза, чтобы фоновый поток FastAPI дошёл до конца
        await asyncio.sleep(0.1)
