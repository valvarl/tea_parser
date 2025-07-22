"""
Глобальные настройки проекта: секреты, пути, переменные окружения,
а также справочные константы вроде TEA_KEYWORDS.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List, Dict

from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import Field

# ──────────────────────────────────────────────────────────────────────────────
# Базовые пути / переменные окружения
# ──────────────────────────────────────────────────────────────────────────────

ROOT_DIR: Path = Path(__file__).resolve().parent.parent.parent  # корень репо
load_dotenv(ROOT_DIR / ".env")  # подхватываем .env ещё до инициализации Settings


# ──────────────────────────────────────────────────────────────────────────────
# Pydantic-класс с переменными окружения
# ──────────────────────────────────────────────────────────────────────────────

class Settings(BaseSettings):
    # Основное
    APP_NAME: str = "Chinese Tea Scraper API"
    VERSION: str = "1.0.0"
    SECRET: str = Field("Tz178Fksyu4oAs14", env="SECRET")

    # MongoDB
    MONGO_URL: str = Field(..., env="MONGO_URL")
    DB_NAME: str = Field(..., env="DB_NAME")

    # Captcha-сервисы
    CAPSOLVER_API_KEY: str | None = None
    CAPSOLVER_CLIENT_KEY: str | None = None
    TWOCAPTCHA_API_KEY: str | None = None

    # Прокси-пул (строка «ip:port,ip:port,…»)
    PROXY_POOL: str | None = None

    # внутренние параметры — чтобы не тащить через env
    REQUEST_TIMEOUT: int = 30

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    # удобный helper
    @property
    def proxy_list(self) -> List[str]:
        return [p.strip() for p in (self.PROXY_POOL or "").split(",") if p.strip()]


# ──────────────────────────────────────────────────────────────────────────────
# Единый синглтон настроек
# ──────────────────────────────────────────────────────────────────────────────

@lru_cache
def get_settings() -> Settings:  # FastAPI сможет заинжектить Depends(get_settings)
    return Settings()


# ──────────────────────────────────────────────────────────────────────────────
# Справочник чайных ключевых слов (используется классификатором и генератором
# поисковых запросов). Никаких env-переменных не нужно, поэтому оставляем
# обычной константой.
# ──────────────────────────────────────────────────────────────────────────────

TEA_KEYWORDS: Dict[str, List[str]] = {
    "base_terms": [
        "пуэр", "пуер", "pu-erh", "pu erh", "puer",
        "китайский чай", "chinese tea", "чай китай",
        "чёрный чай", "черный чай", "black tea",
        "зелёный чай", "зеленый чай", "green tea",
        "улун", "оолонг", "oolong",
        "белый чай", "white tea",
        "дахунпао", "да хун пао", "da hong pao",
        "лунцзин", "longjing", "dragon well",
        "те гуань инь", "tie guan yin", "tieguanyin",
    ],
    "forms": [
        "блин", "блинчик", "плитка", "таблетка",
        "прессованный", "рассыпной", "листовой",
        "cake", "brick", "тuo", "то ча",
    ],
    "regions": [
        "юннань", "yunnan", "фуцзянь", "fujian",
        "аньхой", "anhui", "чжэцзян", "zhejiang",
        "гуанси", "guangxi", "гуандун", "guangdong",
    ],
    "grades": [
        "шэн", "шен", "sheng", "сырой",
        "шу", "shu", "shou", "готовый",
        "молодой", "выдержанный", "aged",
    ],
    "years": [str(year) for year in range(2010, 2026)],
}