# app/api/v1/tasks.py
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field
from bson import ObjectId

from app.db.mongo import db
from app.models.task import BaseTask, TaskStatus
from app.models.product import Product

router = APIRouter(prefix="/v1", tags=["tasks"])

BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_COORDINATOR_CMD = os.getenv("TOPIC_COORDINATOR_CMD", "coordinator_cmd")
SERVICE_VERSION = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

INDEX_COLLECTION = os.getenv("INDEX_COLLECTION", "index")
CANDIDATES_COLLECTION = os.getenv("CANDIDATES_COLLECTION", "candidates")

_producer: Optional[AIOKafkaProducer] = None
_producer_lock = asyncio.Lock()


# ───────── helpers

def _now_ts() -> int:
    return int(time.time())


def _make_task_title(t: BaseTask) -> str:
    tt = (t.task_type or "task").strip().lower()
    if tt == "indexing":
        term = (t.params.search_term or "").strip() or "—"
        cat = (t.params.category_id or "—").strip()
        pages = t.params.max_pages or 0
        return f"Индексирование: «{term}» • категория {cat} • {pages} стр."
    if tt == "collections":
        base = (t.params.source_task_id or "—").strip()
        return f"Коллекции (из задачи {base})"
    if tt == "enriching":
        return "Обогащение SKU (пакетная обработка)"
    return f"{tt.capitalize()} task"


async def _ensure_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is not None:
        return _producer
    async with _producer_lock:
        if _producer is None:
            _producer = AIOKafkaProducer(
                bootstrap_servers=BOOT,
                value_serializer=lambda x: (
                    __import__("json").dumps(x, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                ),
                enable_idempotence=True,
            )
            await _producer.start()
    return _producer


def _task_public_view(tdoc: Dict[str, Any]) -> Dict[str, Any]:
    tdoc = dict(tdoc)
    tdoc.pop("_id", None)

    if not tdoc.get("title"):
        bt = BaseTask(**tdoc)
        tdoc["title"] = _make_task_title(bt)

    try:
        bt = BaseTask(**tdoc)
        tdoc["enrich_progress"] = bt.stats.enriched_summary
    except Exception:
        tdoc["enrich_progress"] = {"enriched": 0, "to_enrich": 0, "failed": 0}

    return tdoc


async def _pipeline_task_ids(pipeline_id: str) -> List[str]:
    cursor = db.scraping_tasks.find({"pipeline_id": pipeline_id}, {"id": 1})
    return [doc["id"] for doc in await cursor.to_list(length=10000)]


# ───────── tasks: list / details / children

@router.get("/tasks")
async def get_tasks(
    parent_task_id: Optional[str] = Query(None),
    pipeline_id: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    task_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    skip: int = Query(0, ge=0),
    sort: Optional[str] = Query("-created_at"),
):
    q: Dict[str, Any] = {}
    if parent_task_id:
        q["parent_task_id"] = parent_task_id
    if pipeline_id:
        q["pipeline_id"] = pipeline_id
    if status:
        q["status"] = status
    if task_type:
        q["task_type"] = task_type

    sort_spec: List[Tuple[str, int]] = []
    if sort:
        for part in sort.split(","):
            part = part.strip()
            if not part:
                continue
            sort_spec.append((part[1:], -1) if part.startswith("-") else (part, 1))
    sort_spec = sort_spec or [("created_at", -1)]

    cursor = db.scraping_tasks.find(q).sort(sort_spec).skip(skip).limit(limit)
    items = [_task_public_view(t) for t in await cursor.to_list(length=limit)]
    total = await db.scraping_tasks.count_documents(q)
    return {"items": items, "total": total}


@router.get("/tasks/{task_id}")
async def get_task(task_id: str):
    t = await db.scraping_tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")
    return _task_public_view(t)


@router.get("/tasks/{task_id}/details")
async def get_task_details(task_id: str):
    t = await db.scraping_tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")

    pub = _task_public_view(t)
    return {
        "task": {
            "id": pub["id"],
            "title": pub.get("title"),
            "status": pub.get("status"),
            "created_at": pub.get("created_at"),
            "started_at": pub.get("started_at"),
            "finished_at": pub.get("finished_at"),
            "enrich_progress": pub.get("enrich_progress"),
        },
        "workers": pub.get("workers", {}),
        "outputs": pub.get("outputs", {}),
        "errors": pub.get("errors", []),
        "error_message": pub.get("error_message"),
        "result": pub.get("result"),
        "stats": pub.get("stats"),
    }


@router.get("/tasks/{task_id}/children")
async def get_children_tasks(task_id: str, limit: int = Query(100, ge=1, le=500), skip: int = 0):
    q = {"parent_task_id": task_id}
    cursor = db.scraping_tasks.find(q).sort("created_at", -1).skip(skip).limit(limit)
    items = [_task_public_view(t) for t in await cursor.to_list(length=limit)]
    total = await db.scraping_tasks.count_documents(q)
    return {"items": items, "total": total}


# ───────── products from index (join candidates)

@router.get("/tasks/{task_id}/products")
async def get_task_products(
    task_id: str,
    scope: str = Query("task", pattern="^(task|pipeline)$"),
    status: Optional[str] = Query(None, description="Фильтр по status в index: pending_review/indexed_auto/..."),
    limit: int = Query(24, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """
    Возвращает список продуктов из candidates, отфильтрованных по задачам (через index).
    Формат ответа совпадает с /v1/products: items (List[Product]), total, skip, limit.
    """
    # 1) Находим задачу и набор task_id для фильтрации в index
    t = await db.scraping_tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")

    if scope == "pipeline" and t.get("pipeline_id"):
        task_ids = await _pipeline_task_ids(t["pipeline_id"])
        if not task_ids:
            task_ids = [task_id]
    else:
        task_ids = [task_id]

    # 2) Собираем фильтр по index
    match: Dict[str, Any] = {"task_id": {"$in": task_ids}}
    if status:
        match["status"] = status

    # 3) Получаем уникальные candidate_id из index
    cand_ids_raw = await db[INDEX_COLLECTION].distinct("candidate_id", match)
    # отфильтруем None и приведём к ObjectId, если нужно
    cand_ids: List[ObjectId] = []
    for v in cand_ids_raw:
        if v is None:
            continue
        if isinstance(v, ObjectId):
            cand_ids.append(v)
        else:
            try:
                cand_ids.append(ObjectId(str(v)))
            except Exception:
                # если id не ObjectId, пропустим (в candidates _id — ObjectId)
                continue

    total = len(cand_ids)
    if total == 0:
        return {"items": [], "total": 0, "skip": skip, "limit": limit}

    # 4) Выбираем продукты из candidates, сортировка как в products.py
    cur = (
        db[CANDIDATES_COLLECTION]
        .find({"_id": {"$in": cand_ids}})
        .sort([("updated_at", -1), ("created_at", -1)])
        .skip(int(skip))
        .limit(int(limit))
    )
    products = await cur.to_list(length=limit)

    for p in products:
        p.pop("_id", None)

    items = [Product.model_validate(p) for p in products]

    return {
        "items": items,
        "total": total,
        "skip": skip,
        "limit": limit,
    }

# ───────── fix errors

class FixRequest(BaseModel):
    mode: str = Field(default="retry_failed", pattern="^(retry_failed|re_enrich|reindex)$")
    limit: Optional[int] = Field(default=500, ge=1, le=5000)

@router.post("/tasks/{task_id}/fix_errors")
async def fix_task_errors(task_id: str, req: FixRequest):
    t = await db.scraping_tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")

    prod = await _ensure_producer()
    payload = {
        "cmd": "fix_task_errors",
        "task_id": task_id,
        "mode": req.mode,
        "limit": req.limit,
        "source": "api",
        "version": SERVICE_VERSION,
        "ts": _now_ts(),
    }
    await prod.send_and_wait(TOPIC_COORDINATOR_CMD, payload)
    return {"status": "queued", "task_id": task_id, "mode": req.mode}
