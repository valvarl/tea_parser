from pydantic import BaseModel, Field
from typing import Optional
import uuid
from datetime import datetime


class ScrapingTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    search_term: str
    status: str = "pending"  # pending, running, completed, failed
    total_products: int = 0
    scraped_products: int = 0
    failed_products: int = 0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

class ScrapingStats(BaseModel):
    total_tasks: int = 0
    running_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    total_products: int = 0
    captcha_solves: int = 0
    proxy_switches: int = 0
    error_rate: float = 0.0