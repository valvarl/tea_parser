# products.py

"""Маршруты, работающие с объектами чая."""

from fastapi import APIRouter, HTTPException, Request, Query
from typing import Any, Dict, List, Optional, Tuple
import re

from app.db.mongo import db
from app.models.product import Product, CharacteristicItem
from pydantic import BaseModel

router = APIRouter(prefix="/v1/products", tags=["products"])


# -------------------- Models --------------------

class ProductPage(BaseModel):
    items: List[Product]
    total: int
    skip: int
    limit: int


# -------------------- Helpers --------------------

SORTABLE_FIELDS: Dict[str, str] = {
    # простые поля
    "updated_at": "updated_at",
    "created_at": "created_at",
    "title": "title",
    "sku": "sku",
    # составные (могут храниться строками)
    "rating": "seo.aggregateRating.ratingValue",  # строка "4.8"
    "price": "seo.offers.price",                 # строка "769"
}
NUMERIC_CAST_REQUIRED = {"price", "rating"}


def _parse_sort(sort_by: Optional[str], sort_dir: Optional[str]) -> Tuple[str, int, bool]:
    field_key = (sort_by or "updated_at").strip()
    field = SORTABLE_FIELDS.get(field_key, SORTABLE_FIELDS["updated_at"])

    dir_s = (sort_dir or "desc").lower()
    direction = -1 if dir_s in ("desc", "down", "-1", "-") else 1

    needs_cast = field_key in NUMERIC_CAST_REQUIRED
    return field, direction, needs_cast


def _build_search_query(q: Optional[str]) -> Optional[Dict[str, Any]]:
    if not q:
        return None
    # безопасный regex (экранируем ввод)
    rx = {"$regex": re.escape(q), "$options": "i"}
    # минимально полезные поля для поиска
    return {
        "$or": [
            {"title": rx},
            {"seo.description": rx},
            {"characteristics.full.values": rx},
        ]
    }


def _build_characteristics_filters(query_params) -> List[Dict[str, Any]]:
    """
    Собирает фильтры по характеристикам из параметров запроса вида:
      ?char_TeaType=пуэр&char_TeaType=черный&char_TeaGrade=шу%20пуэр
    Для каждого char_<id> строится $elemMatch по characteristics.full.
    """
    out: List[Dict[str, Any]] = []
    for key in query_params.keys():
        if not key.startswith("char_"):
            continue
        char_id = key[5:]  # срез после 'char_'
        values = [v for v in query_params.getlist(key) if v is not None and v != ""]
        if not values:
            continue
        out.append({
            "characteristics.full": {
                "$elemMatch": {
                    "id": char_id,
                    "values": {"$in": values}  # любой из указанных
                }
            }
        })
    return out


async def _query_total(query: Dict[str, Any]) -> int:
    return await db.candidates.count_documents(query)


# -------------------- Routes --------------------

@router.get("/", response_model=ProductPage)
async def get_tea_products(
    request: Request,
    skip: int = 0,
    limit: int = 50,
    # поиск
    q: Optional[str] = Query(None, description="Поиск по слову (title, описание, значения характеристик)"),
    # сортировка
    sort_by: Optional[str] = Query("updated_at", description=f"Поле сортировки: {', '.join(SORTABLE_FIELDS.keys())}"),
    sort_dir: Optional[str] = Query("desc", description="Направление: asc|desc"),
    # legacy-фильтр (оставлен для совместимости, при желании можно убрать)
    tea_type: Optional[str] = None,
):
    """
    Дополненный список продуктов с:
      - поиском (?q=слово),
      - сортировкой (?sort_by=price&sort_dir=asc),
      - фильтрами по характеристикам (?char_TeaType=пуэр&char_TeaGrade=шу%20пуэр ...).
    """
    query: Dict[str, Any] = {}

    # поиск
    search_q = _build_search_query(q)
    if search_q:
        query.update(search_q)

    # legacy-фильтр по полю tea_type (если есть такие документы)
    if tea_type:
        query["tea_type"] = tea_type

    # фильтры характеристик
    char_filters = _build_characteristics_filters(request.query_params)
    if char_filters:
        query.setdefault("$and", []).extend(char_filters)

    total = await _query_total(query)

    # сортировка
    sort_field, sort_dir_int, needs_cast = _parse_sort(sort_by, sort_dir)

    items: List[Dict[str, Any]]
    if needs_cast:
        # Для price/rating приводим к числу через агрегирование, чтобы сортировка была числовой
        pipeline = [
            {"$match": query} if query else {"$match": {}},
            {"$addFields": {
                "__sort_key": {
                    "$convert": {
                        "input": f"${sort_field}",
                        "to": "double",
                        "onError": None,
                        "onNull": None
                    }
                }
            }},
            {"$sort": {"__sort_key": sort_dir_int, "_id": 1}},
            {"$skip": max(0, int(skip))},
            {"$limit": max(0, int(limit))},
        ]
        items = await db.candidates.aggregate(pipeline).to_list(length=limit)
        for p in items:
            p.pop("__sort_key", None)
    else:
        # Обычная сортировка по полю
        cursor = (
            db.candidates
            .find(query)
            .sort([(sort_field, sort_dir_int), ("updated_at", -1)])
            .skip(skip)
            .limit(limit)
        )
        items = await cursor.to_list(length=limit)

    # очистка _id и валидация
    for p in items:
        p.pop("_id", None)

    return ProductPage(
        items=[Product.model_validate(p) for p in items],
        total=total,
        skip=skip,
        limit=limit,
    )


@router.get("/characteristics", response_model=List[CharacteristicItem])
async def get_available_characteristics(
    limit_values_per_char: int = Query(500, ge=1, le=10_000, description="Ограничение на кол-во значений в одной характеристике")
):
    """
    Возвращает список доступных характеристик и уникальные значения по каждому id.
    Используется фронтендом для построения фильтров.
    """
    pipeline = [
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
    rows = await db.candidates.aggregate(pipeline).to_list(length=10_000)

    # Pydantic валидация к единому формату
    return [CharacteristicItem.model_validate(r) for r in rows]


@router.get("/{product_id}", response_model=Product)
async def get_tea_product(product_id: str):
    product = await db.candidates.find_one({"sku": product_id})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    product.pop("_id", None)
    return Product.model_validate(product)


@router.delete("/{product_id}")
async def delete_tea_product(product_id: str):
    res = await db.candidates.delete_one({"sku": product_id})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product deleted successfully"}


@router.post("/bulk-delete")
async def bulk_delete_products(product_ids: List[str]):
    res = await db.candidates.delete_many({"sku": {"$in": product_ids}})
    return {
        "deleted_count": res.deleted_count,
        "message": f"Deleted {res.deleted_count} products",
    }
