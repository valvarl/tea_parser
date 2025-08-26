# app/api/v1/tasks.py
from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request
from aiokafka import AIOKafkaProducer
from pydantic import BaseModel, Field
from bson import ObjectId

from app.db.mongo import db
from app.models.task import BaseTask, TaskStatus
from app.models.product import Product, CharacteristicItem

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


# ----- search/sort helpers -----

SORTABLE_FIELDS = {
    "updated_at": "updated_at",
    "created_at": "created_at",
    "title": "title",
    "sku": "sku",
    "rating": "seo.aggregateRating.ratingValue",  # строка
    "price": "seo.offers.price",                  # строка
}
NUMERIC_CAST_REQUIRED = {"price", "rating"}

def _parse_sort(sort_by: str | None, sort_dir: str | None):
    key = (sort_by or "updated_at").strip()
    field = SORTABLE_FIELDS.get(key, "updated_at")
    direction = -1 if (sort_dir or "desc").lower() in ("desc", "down", "-1", "-") else 1
    needs_cast = key in NUMERIC_CAST_REQUIRED
    return field, direction, needs_cast

def _build_search_query(q: str | None):
    if not q:
        return None
    rx = {"$regex": re.escape(q), "$options": "i"}
    return {"$or": [
        {"title": rx},
        {"seo.description": rx},
        {"characteristics.full.values": rx},
        {"sku": rx},
    ]}

def _build_characteristics_filters(params) -> list[dict]:
    """
    ?char_TeaType=пуэр&char_TeaType=черный&char_TeaGrade=шу%20пуэр
    """
    items = []
    for key in params.keys():
        if not key.startswith("char_"):
            continue
        cid = key[5:]
        vals = [v for v in params.getlist(key) if v]
        if not vals:
            continue
        items.append({
            "characteristics.full": {
                "$elemMatch": {
                    "id": cid,
                    "values": {"$in": vals}
                }
            }
        })
    return items


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
    request: Request,
    task_id: str,
    scope: str = Query("task", pattern="^(task|pipeline)$"),
    status: Optional[str] = Query(None, description="Фильтр по status в index: pending_review/indexed_auto/..."),
    # новое:
    q: Optional[str] = Query(None, description="Поиск по слову"),
    sort_by: Optional[str] = Query("updated_at", description=f"Поле: {', '.join(SORTABLE_FIELDS.keys())}"),
    sort_dir: Optional[str] = Query("desc", description="asc|desc"),
    limit: int = Query(24, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """
    Возвращает продукты из candidates, привязанные к задаче (через index),
    с поддержкой поиска (?q=), фильтров по характеристикам (?char_*), сортировки (?sort_by, ?sort_dir).
    """
    # 1) получаем ids задач для scope
    t = await db.scraping_tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")

    if scope == "pipeline" and t.get("pipeline_id"):
        task_ids = await _pipeline_task_ids(t["pipeline_id"]) or [task_id]
    else:
        task_ids = [task_id]

    # 2) match по index
    idx_match: Dict[str, Any] = {"task_id": {"$in": task_ids}}
    if status:
        idx_match["status"] = status

    # 3) список candidate _id
    cand_ids_raw = await db[INDEX_COLLECTION].distinct("candidate_id", idx_match)
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
                continue

    if not cand_ids:
        return {"items": [], "total": 0, "skip": skip, "limit": limit}

    # 4) соберём фильтр по candidates
    base_match: Dict[str, Any] = {"_id": {"$in": cand_ids}}

    search_q = _build_search_query(q)
    if search_q:
        base_match.update(search_q)

    char_filters = _build_characteristics_filters(request.query_params)
    if char_filters:
        base_match.setdefault("$and", []).extend(char_filters)

    # 5) total ПОСЛЕ фильтров
    total = await db[CANDIDATES_COLLECTION].count_documents(base_match)

    if total == 0:
        return {"items": [], "total": 0, "skip": skip, "limit": limit}

    # 6) сортировка
    sort_field, sort_dir_int, needs_cast = _parse_sort(sort_by, sort_dir)

    # 7) выдача
    if needs_cast:
        pipeline = [
            {"$match": base_match},
            {"$addFields": {
                "__sort_key": {
                    "$convert": {"input": f"${sort_field}", "to": "double", "onError": None, "onNull": None}
                }
            }},
            {"$sort": {"__sort_key": sort_dir_int, "_id": 1}},
            {"$skip": int(skip)},
            {"$limit": int(limit)},
            {"$project": {"__sort_key": 0}},
        ]
        docs = await db[CANDIDATES_COLLECTION].aggregate(pipeline).to_list(length=limit)
    else:
        cursor = (
            db[CANDIDATES_COLLECTION]
            .find(base_match)
            .sort([(sort_field, sort_dir_int), ("updated_at", -1)])
            .skip(int(skip))
            .limit(int(limit))
        )
        docs = await cursor.to_list(length=limit)

    for d in docs:
        d.pop("_id", None)

    return {
        "items": [Product.model_validate(d) for d in docs],
        "total": total,
        "skip": skip,
        "limit": limit,
    }

@router.get("/tasks/{task_id}/products/characteristics", response_model=List[CharacteristicItem])
async def get_task_products_characteristics(
    request: Request,
    task_id: str,
    scope: str = Query("task", pattern="^(task|pipeline)$"),
    limit_values_per_char: int = Query(500, ge=1, le=10_000),
):
    t = await db.scraping_tasks.find_one({"id": task_id})
    if not t:
        raise HTTPException(status_code=404, detail="Task not found")

    if scope == "pipeline" and t.get("pipeline_id"):
        task_ids = await _pipeline_task_ids(t["pipeline_id"]) or [task_id]
    else:
        task_ids = [task_id]

    idx_match = {"task_id": {"$in": task_ids}}
    cand_ids_raw = await db[INDEX_COLLECTION].distinct("candidate_id", idx_match)

    cand_ids: List[ObjectId] = []
    for v in cand_ids_raw:
        if isinstance(v, ObjectId):
            cand_ids.append(v)
        else:
            try:
                cand_ids.append(ObjectId(str(v)))
            except Exception:
                continue

    if not cand_ids:
        return []

    # база под текущие фильтры/поиск
    base_match: Dict[str, Any] = {"_id": {"$in": cand_ids}}
    q = request.query_params.get("q")
    search_q = _build_search_query(q)
    if search_q:
        base_match.update(search_q)
    char_filters = _build_characteristics_filters(request.query_params)
    if char_filters:
        base_match.setdefault("$and", []).extend(char_filters)

    pipeline = [
        {"$match": base_match},
        {"$unwind": {"path": "$characteristics.full", "preserveNullAndEmptyArrays": False}},
        {"$unwind": {"path": "$characteristics.full.values", "preserveNullAndEmptyArrays": False}},
        {"$group": {
            "_id": {"id": "$characteristics.full.id", "title": "$characteristics.full.title"},
            "values": {"$addToSet": "$characteristics.full.values"},
        }},
        {"$project": {
            "_id": 0,
            "id": "$_id.id",
            "title": "$_id.title",
            "values": {"$slice": ["$values", limit_values_per_char]},
        }},
        {"$sort": {"title": 1}},
    ]
    rows = await db[CANDIDATES_COLLECTION].aggregate(pipeline).to_list(length=10_000)
    return [CharacteristicItem.model_validate(r) for r in rows]

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
