import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from pydantic import BaseModel, Field, field_validator


# --- Enums

class TaskStatus(str, Enum):
    pending = "pending"
    queued = "queued"
    running = "running"
    deferred = "deferred"
    finished = "finished"
    failed = "failed"
    canceled = "canceled"


# --- Value objects

class TaskParams(BaseModel):
    search_term: Optional[str] = None
    category_id: Optional[str] = None
    max_pages: Optional[int] = 3
    trigger: Optional[str] = "manual"
    priority: Optional[int] = 5
    source_task_id: Optional[str] = None  # для collections-задач

class WorkerState(BaseModel):
    status: Optional[str] = None
    version: Optional[str] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    raw: Dict[str, Any] = Field(default_factory=dict)  # временно — до унификации формата indexer/enricher

class TaskStats(BaseModel):
    pages_indexed: int = 0
    products_indexed: int = 0
    new_products: int = 0
    created_collections: int = 0
    skus_to_process: int = 0

class TaskOutputs(BaseModel):
    next_tasks: List[Dict[str, str]] = Field(default_factory=list)
    collection_ids: List[str] = Field(default_factory=list)
    reports: List[Dict[str, Any]] = Field(default_factory=list)

class StatusChange(BaseModel):
    from_status: Optional[str] = None
    to_status: TaskStatus
    at: datetime = Field(default_factory=datetime.utcnow)
    reason: Optional[str] = None

class AttemptInfo(BaseModel):
    current: int = 0
    max: int = 3
    backoff: str = "exp"

class LeaseInfo(BaseModel):
    owner: Optional[str] = None
    leased_at: Optional[datetime] = None
    ttl_sec: Optional[int] = None


# --- Base task

class BaseTask(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = "indexing"  # indexing | collections | enriching | scraping (общее имя)
    status: TaskStatus = TaskStatus.pending
    status_history: List[StatusChange] = Field(default_factory=list)

    params: TaskParams = Field(default_factory=TaskParams)
    workers: Dict[str, WorkerState] = Field(default_factory=dict)
    stats: TaskStats = Field(default_factory=TaskStats)
    outputs: TaskOutputs = Field(default_factory=TaskOutputs)

    pipeline_id: Optional[str] = None
    parent_task_id: Optional[str] = None

    error_message: Optional[str] = None
    errors: List[Dict[str, Any]] = Field(default_factory=list)

    attempt: AttemptInfo = Field(default_factory=AttemptInfo)
    lease: Optional[LeaseInfo] = None
    timings: Dict[str, Any] = Field(default_factory=dict)

    started_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)

    version: Optional[str] = None  # commit/docker tag

    @field_validator("created_at", "started_at", "updated_at", "finished_at", mode="before")
    @classmethod
    def _parse_dt(cls, v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return datetime.utcfromtimestamp(v)
        if isinstance(v, str) and v.strip().isdigit():
            return datetime.utcfromtimestamp(int(v.strip()))
        return v


class ScrapingStats(BaseModel):
    total_tasks: int = 0
    running_tasks: int = 0
    finished_tasks: int = 0
    failed_tasks: int = 0
    total_products: int = 0
    captcha_solves: int = 0
    proxy_switches: int = 0
    error_rate: float = 0.0