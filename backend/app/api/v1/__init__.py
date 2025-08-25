"""Сборщик v1-роутеров."""

from fastapi import APIRouter

from .collections import router as collections_router
from .debug import router as debug_router
from .misc import router as misc_router
from .products import router as products_router
from .scraping import router as scraping_router
from .tasks import router as tasks_router

router = APIRouter()
router.include_router(collections_router)  # /collections/*
router.include_router(debug_router)      # /debug/*
router.include_router(misc_router)       # /, /search, /stats, ...
router.include_router(products_router)   # /products/*
router.include_router(scraping_router)   # /scrape/*
router.include_router(tasks_router)      # /tasks/*
