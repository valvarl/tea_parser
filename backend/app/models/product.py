from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import (
    BaseModel, Field, AliasChoices,
    field_validator, model_validator, ConfigDict
)

# ---------- helpers ----------

def _clean_join(items: List[Any]) -> str:
    items = [str(x) for x in items if x is not None]
    s = " ".join(items).replace("\xa0", " ")
    return " ".join(s.split())

def _to_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        return _clean_join(v)
    return str(v)

def _to_str_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [str(x) for x in v if x is not None]
    return [str(v)]

def _list_to_mapping(v: Any, *, key="items") -> Dict[str, Any]:
    if isinstance(v, list):
        # Частный случай для aspects вида [{'aspectKey':..., 'aspectValue':...}, ...]
        if all(isinstance(x, dict) for x in v) and any(("aspectKey" in x or "key" in x) for x in v):
            out: Dict[str, Any] = {}
            for i, x in enumerate(v):
                k = x.get("aspectKey") or x.get("key") or str(i)
                val = x.get("aspectValue") if "aspectValue" in x else {**x}
                out[str(k)] = val
            return out
        return {key: v}
    if isinstance(v, dict):
        return v
    if v is None:
        return {}
    return {key: v}

# ---------- Description / Content blocks ----------

class MediaImage(BaseModel):
    alt: str = ""
    src: str = ""

class SpecItem(BaseModel):
    title: str = ""
    content: str = ""

class ContentBlock(BaseModel):
    img: Optional[MediaImage] = None
    video: List[Dict[str, Any]] = Field(default_factory=list)

    text: str = ""
    text_items: List[str] = Field(default_factory=list)
    title: str = ""
    title_items: List[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, v):
        # Нормализация всего блока до проверки типов полей
        if isinstance(v, dict):
            v = dict(v)
            v["text"] = _to_str(v.get("text"))
            v["title"] = _to_str(v.get("title"))
            v["text_items"] = _to_str_list(v.get("text_items"))
            v["title_items"] = _to_str_list(v.get("title_items"))
            img = v.get("img")
            # Пустую картинку убираем
            if isinstance(img, dict) and not (img.get("src") or img.get("alt")):
                v["img"] = None
        return v

    @model_validator(mode="after")
    def _fold_items(self):
        # Если text/title пустые — соберём их из *_items
        if not self.title and self.title_items:
            self.title = _clean_join(self.title_items)
        if not self.text and self.text_items:
            self.text = _clean_join(self.text_items)
        return self

class Description(BaseModel):
    content_blocks: List[ContentBlock] = Field(default_factory=list)
    specs: List[SpecItem] = Field(default_factory=list)

# ---------- Characteristics ----------

class CharacteristicItem(BaseModel):
    id: str
    title: str
    values: List[str] = Field(default_factory=list)

class Characteristics(BaseModel):
    full: List[CharacteristicItem] = Field(default_factory=list)
    short: Optional[List[CharacteristicItem]] = None

# ---------- Product ----------

class Product(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    sku: Optional[str] = Field(default=None, validation_alias=AliasChoices("sku", "sku_id"))
    title: Optional[str] = Field(default=None, validation_alias=AliasChoices("title", "name"))

    aspects: Dict[str, Any] = Field(default_factory=dict)
    collections: Dict[str, Any] = Field(default_factory=dict)

    characteristics: Characteristics = Field(default_factory=Characteristics)
    description: Optional[Description] = None

    cover_image: Optional[str] = None
    gallery: Dict[str, Any] = Field(default_factory=dict)
    nutrition: Dict[str, Any] = Field(default_factory=dict)
    other_offers: Optional[Any] = None
    seo: Dict[str, Any] = Field(default_factory=dict)

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # ------ validators ------

    @field_validator("aspects", mode="before")
    @classmethod
    def _coerce_aspects(cls, v):
        return _list_to_mapping(v, key="items")

    @field_validator("collections", mode="before")
    @classmethod
    def _coerce_collections(cls, v):
        return _list_to_mapping(v, key="items")

    @field_validator("created_at", "updated_at", mode="before")
    @classmethod
    def _parse_dt(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return datetime.utcfromtimestamp(v)
        if isinstance(v, str) and v.strip().isdigit():
            return datetime.utcfromtimestamp(int(v.strip()))
        return v

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