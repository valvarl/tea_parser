"""Маршруты, работающие с коллекциями товаров."""

from __future__ import annotations

from typing import List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime

from app.db.mongo import db

router = APIRouter(prefix="/v1/collections", tags=["collections"])


# ---------- Pydantic модели ----------

class CollectionSku(BaseModel):
    sku: str
    status: Optional[str] = None


class CollectionOut(BaseModel):
    collection_hash: str
    type: Literal["collection", "aspect", "other_offer"] = Field(..., description="Тип коллекции")
    come_from: Optional[str] = Field(None, description="SKU источника (строка)")
    aspect_key: Optional[str] = None
    aspect_name: Optional[str] = None
    status: Optional[str] = None
    task_id: Optional[str] = None
    skus: List[CollectionSku] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CollectionPage(BaseModel):
    items: List[CollectionOut]
    total: int
    skip: int
    limit: int


# ---------- Вспомогательные функции ----------

def _to_int_maybe(value: str | int | None):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except Exception:
        return value  # на случай, если в БД хранится строкой


def _normalize_collection(doc: dict) -> dict:
    """Убираем _id, приводим sku и come_from к строкам для фронта."""
    if not doc:
        return doc
    d = dict(doc)
    d.pop("_id", None)

    # come_from -> str
    if "come_from" in d and d["come_from"] is not None:
        d["come_from"] = str(d["come_from"])

    # skus[].sku -> str
    skus = d.get("skus") or []
    norm_skus: List[dict] = []
    for it in skus:
        if not isinstance(it, dict):
            continue
        sku_val = it.get("sku")
        norm_skus.append({
            "sku": str(sku_val) if sku_val is not None else "",
            "status": it.get("status"),
        })
    d["skus"] = norm_skus
    return d


def _normalize_type(t: Optional[str]) -> Optional[str]:
    if not t:
        return None
    t = t.strip().lower()
    if t in ("other_offer", "other_offers"):
        return "other_offer"
    if t in ("aspect", "collection"):
        return t
    return None


# ---------- Эндпоинты ----------

@router.get("/", response_model=CollectionPage)
async def list_collections(
    skip: int = 0,
    limit: int = 50,
    type: Optional[str] = Query(None, description="collection | aspect | other_offer"),
    status: Optional[str] = None,
    task_id: Optional[str] = None,
    come_from: Optional[str] = Query(None, description="SKU источника (строка или число)"),
    sku: Optional[str] = Query(None, description="SKU, входящий в коллекцию"),
    collection_hash: Optional[str] = None,
):
    q: dict = {}

    t_norm = _normalize_type(type)
    if t_norm:
        q["type"] = t_norm

    if status:
        q["status"] = status

    if task_id:
        q["task_id"] = task_id

    if collection_hash:
        q["collection_hash"] = collection_hash

    if come_from:
        # Ищем и по int, и по строке, т.к. в БД могло сохраниться по-разному
        cf_int = _to_int_maybe(come_from)
        q["come_from"] = {"$in": [cf_int, str(come_from)]}

    if sku:
        s_int = _to_int_maybe(sku)
        q["skus.sku"] = {"$in": [s_int, str(sku)]}

    total = await db.collections.count_documents(q)

    rows = (
        await db.collections.find(q)
        .sort([("updated_at", -1), ("created_at", -1)])
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )

    items = [CollectionOut.model_validate(_normalize_collection(r)) for r in rows]

    return CollectionPage(items=items, total=total, skip=skip, limit=limit)


@router.get("/{collection_hash}", response_model=CollectionOut)
async def get_collection(collection_hash: str):
    doc = await db.collections.find_one({"collection_hash": collection_hash})
    if not doc:
        raise HTTPException(status_code=404, detail="Collection not found")
    return CollectionOut.model_validate(_normalize_collection(doc))


@router.get("/by-sku/{sku}", response_model=CollectionPage)
async def list_collections_by_sku(
    sku: str,
    skip: int = 0,
    limit: int = 50,
    type: Optional[str] = Query(None, description="Опциональный фильтр по типу"),
):
    t_norm = _normalize_type(type)
    q: dict = {"skus.sku": {"$in": [_to_int_maybe(sku), str(sku)]}}
    if t_norm:
        q["type"] = t_norm

    total = await db.collections.count_documents(q)

    rows = (
        await db.collections.find(q)
        .sort([("updated_at", -1), ("created_at", -1)])
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )
    items = [CollectionOut.model_validate(_normalize_collection(r)) for r in rows]
    return CollectionPage(items=items, total=total, skip=skip, limit=limit)


@router.delete("/{collection_hash}")
async def delete_collection(collection_hash: str):
    res = await db.collections.delete_one({"collection_hash": collection_hash})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Collection not found")
    return {"message": "Collection deleted successfully"}


@router.post("/bulk-delete")
async def bulk_delete_collections(collection_hashes: List[str]):
    if not collection_hashes:
        return {"deleted_count": 0, "message": "Nothing to delete"}
    res = await db.collections.delete_many({"collection_hash": {"$in": list(set(collection_hashes))}})
    return {
        "deleted_count": res.deleted_count,
        "message": f"Deleted {res.deleted_count} collections",
    }
