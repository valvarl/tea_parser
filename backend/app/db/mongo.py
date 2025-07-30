"""
Подключение к MongoDB и удобные хелперы.

Импортируйте:
    from app.db.mongo import db        # объект AsyncIOMotorDatabase
или
    from app.db.mongo import get_db    # Depends-функция для FastAPI
"""

from functools import lru_cache
from typing import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import get_settings

# ──────────────────────────────────────────────────────────────────────────────
# Client / Database singletons
# ──────────────────────────────────────────────────────────────────────────────


@lru_cache
def _get_client() -> AsyncIOMotorClient:
    """Создаёт (и кэширует) единственный экземпляр Mongo-клиента."""
    settings = get_settings()
    return AsyncIOMotorClient(settings.MONGO_URL)

client: AsyncIOMotorClient = _get_client()
db: AsyncIOMotorDatabase = client[get_settings().DB_NAME]

# ──────────────────────────────────────────────────────────────────────────────
# FastAPI dependencies
# ──────────────────────────────────────────────────────────────────────────────


async def get_db() -> AsyncGenerator[AsyncIOMotorDatabase, None]:
    """
    Зависимость для роутеров / сервисов:

    ```python
    from fastapi import Depends
    from app.db.mongo import get_db

    async def my_endpoint(db = Depends(get_db)):
        ...
    ```
    """
    yield db


# ──────────────────────────────────────────────────────────────────────────────
# Shutdown helper
# ──────────────────────────────────────────────────────────────────────────────


def close_mongo_client() -> None:
    """Вызывайте из события shutdown (`app.add_event_handler`)."""
    client.close()
