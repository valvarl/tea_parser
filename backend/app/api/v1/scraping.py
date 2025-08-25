from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from aiokafka import AIOKafkaProducer

from app.db.mongo import db
from app.models.task import BaseTask, TaskParams, TaskStatus

router = APIRouter(prefix="/v1/scrape", tags=["scraping"])

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_COORDINATOR_CMD = os.getenv("TOPIC_COORDINATOR_CMD", "coordinator_cmd")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

_producer: Optional[AIOKafkaProducer] = None
_producer_lock = asyncio.Lock()


def _now_ts() -> int:
    return int(time.time())


async def _ensure_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is not None:
        return _producer
    async with _producer_lock:
        if _producer is None:
            _producer = AIOKafkaProducer(
                bootstrap_servers=BOOT,
                value_serializer=lambda x: (  # small inline JSON encoder
                    __import__("json").dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                ),
                enable_idempotence=True,
            )
            await _producer.start()
    return _producer


@router.on_event("shutdown")
async def _shutdown_kafka() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


@router.post("/start")
async def start_scraping(
    search_term: str = "пуэр",
    category_id: Optional[str] = Query("9373"),
    max_pages: int = Query(3, ge=1, le=50),
):
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
    task.pipeline_id = task.id
    task.status_history.append(
        {"from_status": None, "to_status": TaskStatus.pending, "at": datetime.utcnow(), "reason": "created"}
    )

    doc = task.model_dump(exclude_none=True)
    doc["_id"] = task.id
    await db.scraping_tasks.insert_one(doc)

    prod = await _ensure_producer()
    await prod.send_and_wait(
        TOPIC_COORDINATOR_CMD,
        {
            "cmd": "start_task",
            "task_id": task.id,
            "source": "api",
            "version": SERVICE_VERSION,
            "ts": _now_ts(),
        },
    )
    return {"task_id": task.id, "status": "queued", "search_term": search_term}


@router.post("/resume")
async def resume_inflight():
    prod = await _ensure_producer()
    await prod.send_and_wait(
        TOPIC_COORDINATOR_CMD,
        {"cmd": "resume_inflight", "source": "api", "version": SERVICE_VERSION, "ts": _now_ts()},
    )
    return {"status": "ok"}


@router.get("/status/{task_id}")
async def get_scraping_status(task_id: str):
    task = await db.scraping_tasks.find_one({"id": task_id})
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    task.pop("_id", None)
    return task


@router.get("/tasks")
async def get_scraping_tasks():
    tasks = await db.scraping_tasks.find().sort("created_at", -1).to_list(100)
    for t in tasks:
        t.pop("_id", None)
    return tasks
