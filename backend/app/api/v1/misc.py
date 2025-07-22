"""Прочие вспомогательные маршруты: приветствие, статистика, категории, экспорт."""

from typing import List

from fastapi import APIRouter

from app.db.mongo import db
from app.models.task import ScrapingStats
from app.utils.search import generate_search_queries
from app.utils.captcha import captcha_solver

router = APIRouter(tags=["misc"], prefix="/v1")


@router.get("/")
async def root():
    return {"message": "Chinese Tea Scraper API", "version": "1.0.0"}


# ---------- search / queries ----------


@router.get("/search/queries")
async def get_search_queries(limit: int = 50):
    return {"queries": generate_search_queries(limit)}


# ---------- stats & categories ----------


@router.get("/stats", response_model=ScrapingStats)
async def get_scraping_stats():
    total = await db.scraping_tasks.count_documents({})
    running = await db.scraping_tasks.count_documents({"status": "running"})
    completed = await db.scraping_tasks.count_documents({"status": "completed"})
    failed = await db.scraping_tasks.count_documents({"status": "failed"})

    products_total = await db.tea_products.count_documents({})
    error_rate = (failed / total * 100) if total else 0

    return ScrapingStats(
        total_tasks=total,
        running_tasks=running,
        completed_tasks=completed,
        failed_tasks=failed,
        total_products=products_total,
        captcha_solves=captcha_solver.solve_count,
        error_rate=error_rate,
    )


@router.get("/categories")
async def get_tea_categories():
    pipeline = [
        {"$group": {"_id": "$tea_type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    cats = await db.tea_products.aggregate(pipeline).to_list(100)
    return {"categories": cats, "total_types": len(cats)}


# ---------- export ----------


@router.get("/export/csv")
async def export_products_csv():
    products = await db.tea_products.find().to_list(10_000)
    for p in products:
        p.pop("_id", None)
    return {"data": products, "count": len(products)}
