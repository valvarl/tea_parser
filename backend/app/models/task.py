from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

from pydantic import BaseModel, Field, field_validator, ConfigDict


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательная функция парсинга дат, поддерживает:
# - datetime как есть
# - число/строку с числом (unix seconds)
# - ISO-строку
# - Mongo Extended JSON: {"$date": "..."} или {"$date": <millis>}
def _parse_dt_like(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    # Mongo Extended JSON
    if isinstance(v, dict) and "$date" in v:
        inner = v["$date"]
        # "$date" может быть ISO-строкой или миллисекундами
        if isinstance(inner, (int, float)):
            # миллисекунды → секунды
            return datetime.utcfromtimestamp(float(inner) / 1000.0)
        try:
            # ISO-строка
            return datetime.fromisoformat(str(inner).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            pass
    # Числа (unix seconds)
    if isinstance(v, (int, float)):
        return datetime.utcfromtimestamp(float(v))
    # Строки: число или ISO
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            return datetime.utcfromtimestamp(int(s))
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Enums

class TaskStatus(str, Enum):
    pending = "pending"
    queued = "queued"
    running = "running"
    deferred = "deferred"
    finished = "finished"
    failed = "failed"
    canceled = "canceled"


# ─────────────────────────────────────────────────────────────────────────────
# Value objects

class TaskParams(BaseModel):
    model_config = ConfigDict(extra="allow")

    search_term: Optional[str] = None
    category_id: Optional[str] = None
    max_pages: Optional[int] = 3
    trigger: Optional[str] = "manual"
    priority: Optional[int] = 5
    source_task_id: Optional[str] = None  # для collections-задач


class WorkerState(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: Optional[str] = None
    version: Optional[str] = None
    stats: Dict[str, Any] = Field(default_factory=dict)
    raw: Dict[str, Any] = Field(default_factory=dict)  # до унификации форматов
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @field_validator("started_at", "finished_at", mode="before")
    @classmethod
    def _parse_dt_fields(cls, v):
        return _parse_dt_like(v)


# ── Детальные выходы воркеров (опционально, если вы решите их заполнять в outputs)

class IndexerOutput(BaseModel):
    model_config = ConfigDict(extra="allow")

    pages_scanned: int = 0
    skus_found: List[str] = Field(default_factory=list)                 # uniq
    new_skus: List[str] = Field(default_factory=list)                   # не было в индексе
    collection_ids: List[str] = Field(default_factory=list)
    sample_products: List[Dict[str, Any]] = Field(default_factory=list) # небольшой сэмпл
    notes: Optional[str] = None


class EnricherOutput(BaseModel):
    model_config = ConfigDict(extra="allow")

    enriched_ok: List[str] = Field(default_factory=list)
    enriched_failed: List[Dict[str, Any]] = Field(default_factory=list)  # [{sku, reason}]
    retries_scheduled: List[str] = Field(default_factory=list)
    notes: Optional[str] = None


class TaskOutputs(BaseModel):
    model_config = ConfigDict(extra="allow")

    indexer: Optional[IndexerOutput] = None
    enricher: Optional[EnricherOutput] = None
    next_tasks: List[Dict[str, str]] = Field(default_factory=list)
    collection_ids: List[str] = Field(default_factory=list)
    reports: List[Dict[str, Any]] = Field(default_factory=list)


# ── История статусов

class StatusChange(BaseModel):
    model_config = ConfigDict(extra="allow")

    from_status: Optional[str] = None
    to_status: TaskStatus
    at: datetime = Field(default_factory=datetime.utcnow)
    reason: Optional[str] = None

    @field_validator("at", mode="before")
    @classmethod
    def _parse_dt_fields(cls, v):
        return _parse_dt_like(v) or datetime.utcnow()


# ── Попытки/аренда

class AttemptInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    current: int = 0
    max: int = 3
    backoff: str = "exp"


class LeaseInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    owner: Optional[str] = None
    leased_at: Optional[datetime] = None
    ttl_sec: Optional[int] = None

    @field_validator("leased_at", mode="before")
    @classmethod
    def _parse_dt_fields(cls, v):
        return _parse_dt_like(v)


# ── Блоки статистики в формате, который реально хранится в Mongo

class StatsIndex(BaseModel):
    model_config = ConfigDict(extra="allow")

    indexed: int = 0
    inserted: int = 0
    updated: int = 0
    pages: int = 0


class StatsEnrich(BaseModel):
    model_config = ConfigDict(extra="allow")

    processed: int = 0
    inserted: int = 0
    updated: int = 0
    reviews_saved: int = 0
    dlq: int = 0


class StatsCollections(BaseModel):
    model_config = ConfigDict(extra="allow")

    created_collections: int = 0
    skus_to_process: int = 0


class StatsForwarded(BaseModel):
    model_config = ConfigDict(extra="allow")

    to_enricher: int = 0
    from_collections: int = 0


class TaskStats(BaseModel):
    """
    Полностью совместим со структурой в примерах:
    {
      "index": {...},
      "enrich": {...},
      "collections": {...},
      "forwarded": {...}
    }
    Дополнительно содержит агрегирующие свойства, полезные для UI.
    """
    model_config = ConfigDict(extra="allow")

    index: StatsIndex = Field(default_factory=StatsIndex)
    enrich: StatsEnrich = Field(default_factory=StatsEnrich)
    collections: StatsCollections = Field(default_factory=StatsCollections)
    forwarded: StatsForwarded = Field(default_factory=StatsForwarded)

    # ── Агрегирующие удобные методы (не сохраняются отдельно)
    @property
    def enriched_ok(self) -> int:
        """
        Считаем «обогащено успешно».
        Предпочтение: enrich.inserted; если 0, берём processed - dlq.
        """
        if self.enrich.inserted:
            return int(self.enrich.inserted)
        return max(0, int(self.enrich.processed) - int(self.enrich.dlq))

    @property
    def to_enrich(self) -> int:
        """
        Считаем «требуют обогащения».
        Предпочтение: forwarded.to_enricher; если 0 — collections.skus_to_process;
        если и там 0 — пробуем index.inserted (для простых кейсов).
        """
        if self.forwarded.to_enricher:
            return int(self.forwarded.to_enricher)
        if self.collections.skus_to_process:
            return int(self.collections.skus_to_process)
        if self.index.inserted:
            return int(self.index.inserted)
        return 0

    @property
    def failed(self) -> int:
        """Считаем «ошибки обогащения» по dlq (dead letter)."""
        return int(self.enrich.dlq)

    @property
    def enriched_summary(self) -> Dict[str, int]:
        """Короткая сводка для фронта: {enriched, to_enrich, failed}."""
        return {
            "enriched": self.enriched_ok,
            "to_enrich": self.to_enrich,
            "failed": self.failed,
        }


# ── Блок result в формате из примеров

class ResultIndexer(BaseModel):
    model_config = ConfigDict(extra="allow")

    indexed: int = 0
    inserted: int = 0
    updated: int = 0
    pages: int = 0


class ResultEnricher(BaseModel):
    model_config = ConfigDict(extra="allow")

    processed: int = 0
    inserted: int = 0
    updated: int = 0
    reviews_saved: int = 0
    dlq: int = 0


class TaskResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    indexer: Optional[ResultIndexer] = None
    enricher: Optional[ResultEnricher] = None
    forwarded_to_enricher: int = 0
    collections_followup_total: int = 0
    duration_total_sec: Optional[int] = None

    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @field_validator("started_at", "finished_at", mode="before")
    @classmethod
    def _parse_dt_fields(cls, v):
        return _parse_dt_like(v)


# ─────────────────────────────────────────────────────────────────────────────
# Base task

class BaseTask(BaseModel):
    """
    Базовая модель задачи. Совместима с уже сохранёнными документами:
    - поддерживает вложенные stats.*
    - имеет поле result (как в примерах)
    - умеет парсить Mongo-таймстемпы {"$date": ...}
    - пропускает лишние поля (extra=allow), чтобы не ломаться на расширениях
    """
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = "indexing"  # indexing | collections | enriching | scraping
    title: Optional[str] = None
    status: TaskStatus = TaskStatus.pending
    status_history: List[StatusChange] = Field(default_factory=list)

    params: TaskParams = Field(default_factory=TaskParams)
    workers: Dict[str, WorkerState] = Field(default_factory=dict)
    stats: TaskStats = Field(default_factory=TaskStats)
    outputs: Dict[str, Any] = Field(default_factory=dict)  # хранится как в БД (next_tasks, collection_ids, reports)
    result: Optional[TaskResult] = None

    pipeline_id: Optional[str] = None
    parent_task_id: Optional[str] = None

    error_message: Optional[str] = None
    errors: List[Dict[str, Any]] = Field(default_factory=list)

    attempt: AttemptInfo = Field(default_factory=AttemptInfo)
    lease: Optional[LeaseInfo] = None
    timings: Dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    last_event_ts: Optional[int] = None  # как в примере

    version: Optional[str] = None  # commit/docker tag

    @field_validator("created_at", "started_at", "updated_at", "finished_at", mode="before")
    @classmethod
    def _parse_dt_fields(cls, v):
        return _parse_dt_like(v) or (datetime.utcnow() if v is None and "created_at" else None)


# ─────────────────────────────────────────────────────────────────────────────
# Агрегированная статистика по сервису (для дашборда)

class ScrapingStats(BaseModel):
    model_config = ConfigDict(extra="allow")

    total_tasks: int = 0
    running_tasks: int = 0
    finished_tasks: int = 0
    failed_tasks: int = 0
    total_products: int = 0
    captcha_solves: int = 0
    proxy_switches: int = 0
    error_rate: float = 0.0
