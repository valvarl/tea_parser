from __future__ import annotations

from functools import lru_cache
from typing import AsyncGenerator

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core.config import get_settings


@lru_cache(maxsize=1)
def get_client() -> AsyncIOMotorClient:
    settings = get_settings()
    return AsyncIOMotorClient(
        settings.mongo_url,
        uuidRepresentation="standard",
        tz_aware=True,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=20_000,
        maxPoolSize=100,
        retryWrites=True,
    )


def get_db_handle() -> AsyncIOMotorDatabase:
    return get_client()[get_settings().db_name]


# Module-level singletons (safe to reuse across the app)
client: AsyncIOMotorClient = get_client()
db: AsyncIOMotorDatabase = get_db_handle()


async def get_db() -> AsyncGenerator[AsyncIOMotorDatabase, None]:
    yield db


def close_mongo_client() -> None:
    client.close()


async def ping_mongo() -> bool:
    try:
        await db.command("ping")
        return True
    except Exception:
        return False
