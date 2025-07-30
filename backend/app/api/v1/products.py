"""Маршруты, работающие с объектами чая."""

from typing import List, Optional

from fastapi import APIRouter, HTTPException

from app.db.mongo import db
from app.models.tea import TeaProduct
from app.utils.search import generate_search_queries  # пригодится bulk-delete

router = APIRouter(prefix="/v1/products", tags=["products"])


@router.get("/", response_model=List[TeaProduct])
async def get_tea_products(
    skip: int = 0,
    limit: int = 50,
    tea_type: Optional[str] = None,
):
    query = {"tea_type": tea_type} if tea_type else {}
    products = (
        await db.candidates.find(query)
        .skip(skip)
        .limit(limit)
        .sort("scraped_at", -1)
        .to_list(limit)
    )
    for p in products:
        p.pop("_id", None)
    return [TeaProduct(**p) for p in products]


@router.get("/{product_id}", response_model=TeaProduct)
async def get_tea_product(product_id: str):
    product = await db.candidates.find_one({"id": product_id})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    product.pop("_id", None)
    return TeaProduct(**product)


@router.delete("/{product_id}")
async def delete_tea_product(product_id: str):
    res = await db.candidates.delete_one({"id": product_id})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Product not found")
    return {"message": "Product deleted successfully"}


@router.post("/bulk-delete")
async def bulk_delete_products(product_ids: List[str]):
    res = await db.candidates.delete_many({"id": {"$in": product_ids}})
    return {
        "deleted_count": res.deleted_count,
        "message": f"Deleted {res.deleted_count} products",
    }