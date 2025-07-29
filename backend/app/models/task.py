from pydantic import BaseModel, Field
from typing import Optional
import uuid
from datetime import datetime


class BaseTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = "indexing"  # indexing, enriching, scraping
    status: str = "pending"  # pending, running, completed, failed
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class IndexingTask(BaseTask):
    search_term: str
    category_id: Optional[str] = None
    max_pages: int = 3
    indexed_pages: int = 0
    indexed_products: int = 0
    new_products: int = 0


class EnrichingTask(BaseTask):
    enriched_products: int = 0


class ScrapingTask(IndexingTask, EnrichingTask):
    pass


class ScrapingStats(BaseModel):
    total_tasks: int = 0
    running_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_products: int = 0
    captcha_solves: int = 0
    proxy_switches: int = 0
    error_rate: float = 0.0