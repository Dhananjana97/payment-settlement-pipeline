"""Read API entry point: FastAPI + uvicorn on API_PORT (8080)."""
from __future__ import annotations

from fastapi import FastAPI

from services.api.adapters.pg_read_store import PgReadStore
from services.api.ports import ReadStore
from services.api.routes import build_router, timing_middleware
from services.common.config import Settings
from services.common.db import async_engine
from services.common.log import configure, get_logger


def create_app(store: ReadStore) -> FastAPI:
    app = FastAPI(title="Settlement Stream read API", docs_url=None, redoc_url=None)
    app.middleware("http")(timing_middleware)
    app.include_router(build_router(store))
    return app


def create_default_app() -> FastAPI:
    settings = Settings.from_env("api")
    configure(settings.log_level)
    get_logger("api", instance=settings.instance_id).info("api starting", port=settings.api_port)
    return create_app(PgReadStore(async_engine(settings)))


if __name__ == "__main__":
    import uvicorn

    s = Settings.from_env("api")
    uvicorn.run("services.api.main:create_default_app", factory=True, host="0.0.0.0", port=s.api_port,
                log_level="warning", access_log=False)
