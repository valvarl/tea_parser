"""Маршруты, работающие с объектами чая."""

from fastapi import APIRouter, HTTPException
from typing import List, Optional
from pydantic import BaseModel

from app.db.mongo import db
from app.models.product import Product
from app.utils.search import generate_search_queries  # пригодится bulk-delete

router = APIRouter(prefix="/v1/products", tags=["products"])


class ProductPage(BaseModel):
    items: List[Product]
    total: int
    skip: int
    limit: int


@router.get("/", response_model=ProductPage)
async def get_tea_products(
    skip: int = 0,
    limit: int = 50,
    tea_type: Optional[str] = None,
):
    query = {"tea_type": tea_type} if tea_type else {}

    total = await db.candidates.count_documents(query)

    products = (
        await db.candidates.find(query)
        .sort([("updated_at", -1), ("created_at", -1)])
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )

    for p in products:
        p.pop("_id", None)

    return ProductPage(
        items=[Product.model_validate(p) for p in products],
        total=total,
        skip=skip,
        limit=limit,
    )


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