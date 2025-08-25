from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.v1 import router as api_router
from app.core.logging import configure_logging
from app.db.mongo import close_mongo_client

def create_app() -> FastAPI:
    configure_logging(service="backend")
    app = FastAPI(title="Chinese Tea Scraper API", version="1.0.0")
    app.include_router(api_router, prefix="/api")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        allow_credentials=True,
    )
    app.add_event_handler("shutdown", close_mongo_client)
    return app
