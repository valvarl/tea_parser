from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, Optional


class _ContextFilter(logging.Filter):
    def __init__(self, service: str, worker: Optional[str], version: str) -> None:
        super().__init__()
        self.service = service
        self.worker = worker
        self.version = version

    def filter(self, record: logging.LogRecord) -> bool:
        # Inject stable fields if missing
        if not hasattr(record, "service"):
            record.service = self.service
        if not hasattr(record, "worker"):
            record.worker = self.worker
        if not hasattr(record, "version"):
            record.version = self.version
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "service": getattr(record, "service", None),
            "worker": getattr(record, "worker", None),
            "version": getattr(record, "version", None),
            "event": getattr(record, "event", None),
            "task_id": getattr(record, "task_id", None),
            "batch_id": getattr(record, "batch_id", None),
            "status": getattr(record, "status", None),
            "trigger": getattr(record, "trigger", None),
            "reason_code": getattr(record, "reason_code", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "message": record.getMessage(),
        }
        counts = getattr(record, "counts", None)
        if counts is not None:
            payload["counts"] = counts
        # Include extra keys safely
        for k, v in getattr(record, "__dict__", {}).items():
            if k.startswith("_") or k in payload or k in ("args", "msg", "levelno", "pathname", "filename",
                                                          "module", "exc_info", "exc_text", "stack_info",
                                                          "lineno", "funcName", "created", "msecs", "relativeCreated",
                                                          "thread", "threadName", "processName", "process", "name"):
                continue
            try:
                json.dumps({k: v})
                payload[k] = v
            except Exception:
                payload[k] = str(v)
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

def _as_level(level: str | int | None) -> int:
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        return getattr(logging, level.upper(), logging.INFO)
    return logging.INFO

def configure_logging(*, service: str, worker: Optional[str] = None, level: str | int | None = None) -> None:
    lvl = _as_level(level or os.getenv("LOG_LEVEL", "INFO"))
    version = os.getenv("SERVICE_VERSION", os.getenv("GIT_SHA", "dev"))

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.setLevel(lvl)
    root.handlers.clear()
    root.addHandler(handler)
    root.addFilter(_ContextFilter(service=service, worker=worker, version=version))


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
