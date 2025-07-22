"""Маршруты, связанные со скрейпинг-тасками."""

from typing import List

from fastapi import APIRouter, BackgroundTasks, HTTPException

from app.db.mongo import db
from app.models.task import ScrapingTask
from app.services.background import scrape_tea_products_task

router = APIRouter(prefix="/v1/scrape", tags=["scraping"])


@router.post("/start")
async def start_scraping(
    background_tasks: BackgroundTasks,
    search_term: str = "пуэр",
):
    """Запустить новую задачу скрейпинга."""
    task = ScrapingTask(search_term=search_term)
    await db.scraping_tasks.insert_one(task.dict())

    background_tasks.add_task(
        scrape_tea_products_task,
        search_term,
        task.id,
    )
    return {"task_id": task.id, "status": "started", "search_term": search_term}


@router.get("/status/{task_id}")
async def get_scraping_status(task_id: str):
    """Статус конкретной задачи."""
    task = await db.scraping_tasks.find_one({"id": task_id})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task.pop("_id", None)
    return task


@router.get("/tasks", response_model=List[ScrapingTask])
async def get_scraping_tasks():
    """Последние 100 задач."""
    tasks = (
        await db.scraping_tasks.find()
        .sort("created_at", -1)
        .to_list(100)
    )
    for t in tasks:
        t.pop("_id", None)
    return tasks