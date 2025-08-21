"""Маршруты, связанные со скрейпинг-тасками."""

from datetime import datetime
from typing import List, Optional

from app.db.mongo import db
from app.models.task import BaseTask, TaskParams, TaskStatus
from app.services.background import scrape_tea_products_task
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

router = APIRouter(prefix="/v1/scrape", tags=["scraping"])


@router.post("/start")
async def start_scraping(
    background_tasks: BackgroundTasks,
    search_term: str = "пуэр",
    category_id: Optional[str] = Query("9373"),
    max_pages: int = Query(3, ge=1, le=50),
):
    """Запустить новую задачу скрейпинга (indexing → enricher → collections follow-up)."""
    task = BaseTask(
        task_type="indexing",
        status=TaskStatus.pending,
        params=TaskParams(
            search_term=search_term,
            category_id=category_id,
            max_pages=max_pages,
            trigger="manual",
            priority=5,
        ),
    )
    # pipeline_id = первый корневой task.id
    task.pipeline_id = task.id
    task.status_history.append(
        {
            "from_status": None,
            "to_status": TaskStatus.pending,
            "at": datetime.utcnow(),
            "reason": "created"
        }
    )

    doc = task.model_dump(exclude_none=True)
    # Храним UUID как _id для единственности
    doc["_id"] = task.id
    await db.scraping_tasks.insert_one(doc)

    background_tasks.add_task(
        scrape_tea_products_task,
        search_term,
        task.id,
        category_id=category_id,
        max_pages=max_pages,
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


@router.get("/tasks")  # response_model убираем, т.к. схема расширена
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
