"""Диагностические энд-пойнты."""

import asyncio

from fastapi import APIRouter

# from app.services.indexer import indexer
# from app.utils.proxy import proxy_pool
# from app.utils.captcha import captcha_solver
from app.utils.ozon_ping import ozon_ping

router = APIRouter(prefix="/debug", tags=["debug"])

@router.get("/test-ozon")
async def test_ozon_connection():
    """Проверяем, что страницы Ozon открываются (без парсинга)."""
    try:
        return await ozon_ping()
    except Exception as exc:               # pragma: no cover
        return {"status": "error", "error": str(exc)}

@router.get("/scraper-status")
async def get_scraper_status():
    """Текущая конфигурация и статистика скрейпера."""
    from app.db.mongo import db  # локальный импорт, чтобы избежать циклов

    running = await db.scraping_tasks.count_documents({"status": "running"})
    recent = await db.scraping_tasks.find().sort("created_at", -1).limit(5).to_list(5)
    for t in recent:
        t.pop("_id", None)

    # return {
    #     "status": "active",
    #     "running_tasks": running,
    #     "scraper_config": {
    #         "base_url": indexer.base_url,
    #         "search_url": indexer.search_url,
    #         "debug_mode": indexer.debug_mode,
    #         "request_count": indexer.request_count,
    #         "captcha_encounters": indexer.captcha_encounters,
    #         "captcha_solved": captcha_solver.solve_count,
    #         "proxy_pool": len(proxy_pool.proxies),
    #         "failed_proxies": len(proxy_pool.failed_proxies),
    #     },
    #     "recent_tasks": recent,
    # }

    return {
        "status": "active",
    }