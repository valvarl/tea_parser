"""
Простая настройка логгирования + вывод публичного IP для отладки.
Вызываем configure_logging() один раз в create_app().
"""

from __future__ import annotations

import logging
import os
from typing import Literal

import requests


def _get_public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=3).text
    except Exception:  # pragma: no cover
        return "unknown"


def configure_logging(level: str | int | None = None) -> None:
    """
    Настраивает root-логгер. Можно переопределить уровень через
    env-переменную LOG_LEVEL или аргументом.

    Пример:
        configure_logging()            # INFO по умолчанию
        configure_logging("DEBUG")     # явный уровень
    """
    # приоритет: аргумент-функции → env → INFO
    level = level or os.getenv("LOG_LEVEL", "INFO")
    level = level.upper() if isinstance(level, str) else level  # type: ignore[arg-type]

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # полезно сразу подсветить IP-адрес контейнера/хоста
    logging.getLogger(__name__).warning("⚠️  Current public IP: %s", _get_public_ip())
