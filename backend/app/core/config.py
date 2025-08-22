from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR: Path = Path(__file__).resolve().parent.parent.parent  # корень репо
load_dotenv(ROOT_DIR / ".env")  # подхватываем .env ещё до инициализации Settings


class Settings(BaseSettings):
    # Mongo
    mongo_url: str = Field(default="mongodb://localhost:27017/", alias="MONGO_URL")
    db_name: str = Field(default="teadb", alias="DB_NAME")

    # Kafka
    kafka_bootstrap_servers: str = Field(default="kafka:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    topic_coordinator_cmd: str = Field(default="coordinator_cmd", alias="TOPIC_COORDINATOR_CMD")
    topic_indexer_cmd: str = Field(default="indexer_cmd", alias="TOPIC_INDEXER_CMD")
    topic_indexer_status: str = Field(default="indexer_status", alias="TOPIC_INDEXER_STATUS")
    topic_enricher_cmd: str = Field(default="enricher_cmd", alias="TOPIC_ENRICHER_CMD")
    topic_enricher_status: str = Field(default="enricher_status", alias="TOPIC_ENRICHER_STATUS")

    # Coordinator
    enricher_batch_size: int = Field(default=50, alias="ENRICHER_BATCH_SIZE")
    status_timeout_sec: int = Field(default=300, alias="STATUS_TIMEOUT_SEC")
    heartbeat_deadline_sec: int = Field(default=900, alias="HEARTBEAT_DEADLINE_SEC")
    heartbeat_check_interval: int = Field(default=60, alias="HEARTBEAT_CHECK_INTERVAL")
    coordinator_poll_interval: int = Field(default=15, alias="COORDINATOR_POLL_INTERVAL")
    indexer_max_defers: int = Field(default=3, alias="INDEXER_MAX_DEFERS")
    retry_backoff_base: int = Field(default=60, alias="RETRY_BACKOFF_BASE")
    retry_backoff_max: int = Field(default=3600, alias="RETRY_BACKOFF_MAX")
    forward_throttle_delay_ms: int = Field(default=0, alias="FORWARD_THROTTLE_DELAY_MS")
    max_running_tasks: int = Field(default=0, alias="MAX_RUNNING_TASKS")
    coordinator_health_port: int = Field(default=8081, alias="COORDINATOR_HEALTH_PORT")

    # Indexer defaults
    index_max_pages: int = Field(default=3, alias="INDEX_MAX_PAGES")
    index_category_id: str = Field(default="9373", alias="INDEX_CATEGORY_ID")

    # Enricher defaults
    enrich_concurrency: int = Field(default=6, alias="ENRICH_CONCURRENCY")
    enrich_reviews: bool = Field(default=True, alias="ENRICH_REVIEWS")
    enrich_reviews_limit: int = Field(default=20, alias="ENRICH_REVIEWS_LIMIT")
    enrich_retry_max: int = Field(default=3, alias="ENRICH_RETRY_MAX")
    currency: str = Field(default="RUB", alias="CURRENCY")

    # Logging / versioning
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    service_version: str = Field(default="dev", alias="SERVICE_VERSION")
    git_sha: str = Field(default="dev", alias="GIT_SHA")

    # Главное: лишние env — игнорируем; регистр — не важен
    model_config = SettingsConfigDict(
        extra="ignore",
        case_sensitive=False,
        env_prefix="",  # читаем «как есть»
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # ВАЖНО: не передавать os.environ внутрь
    return Settings()
