
from pydantic import BaseModel, Field, field_validator  
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime

class TeaProduct(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ozon_id: Optional[str] = None
    sku_id: Optional[str] = None
    name: str
    price: Optional[float] = None
    original_price: Optional[float] = None
    discount: Optional[int] = None
    rating: Optional[float] = None
    reviews_count: Optional[int] = None
    description: Optional[str] = None
    characteristics: Optional[Dict[str, Any]] = None
    images: List[str] = Field(default_factory=list)
    category_path: Optional[str] = None
    category_id: Optional[str] = None
    brand: Optional[str] = None
    seller: Optional[str] = None
    availability: Optional[str] = None
    delivery_info: Optional[str] = None
    weight: Optional[str] = None
    tea_type: Optional[str] = None
    tea_region: Optional[str] = None
    tea_year: Optional[str] = None
    tea_grade: Optional[str] = None
    is_pressed: Optional[bool] = None
    raw_data: Optional[Dict[str, Any]] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    @field_validator("images", mode="before")
    @classmethod
    def _images_to_list(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            return [url for url in v.split("|") if url]
        if isinstance(v, list):
            return v
        raise TypeError("images must be list or '|'-separated string")

class TeaProductCreate(BaseModel):
    name: str
    price: Optional[float] = None
    description: Optional[str] = None

class TeaReview(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    product_id: str
    ozon_review_id: Optional[str] = None
    author: Optional[str] = None
    author_id: Optional[str] = None
    rating: Optional[int] = None
    title: Optional[str] = None
    text: Optional[str] = None
    pros: Optional[str] = None
    cons: Optional[str] = None
    photos: List[str] = []
    helpful_count: Optional[int] = None
    review_date: Optional[datetime] = None
    verified_purchase: Optional[bool] = None
    seller_response: Optional[str] = None
    scraped_at: datetime = Field(default_factory=datetime.utcnow)