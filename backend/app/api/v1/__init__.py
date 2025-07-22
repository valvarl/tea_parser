"""Сборщик v1-роутеров."""

from fastapi import APIRouter

from .scraping import router as scraping_router
from .products import router as products_router
from .debug import router as debug_router
from .misc import router as misc_router

router = APIRouter()
router.include_router(misc_router)       # /, /search, /stats, ...
router.include_router(scraping_router)   # /scrape/*
router.include_router(products_router)   # /products/*
router.include_router(debug_router)      # /debug/*