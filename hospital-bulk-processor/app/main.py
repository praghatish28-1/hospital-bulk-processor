from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

import structlog
from fastapi import Depends, FastAPI

from app.core.redis import close_redis, get_async_redis, init_redis
from app.routers.bulk import router as bulk_router
from app.services.bulk_service import health_check


def configure_logging() -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),
        cache_logger_on_first_use=True,
    )


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    await init_redis()
    try:
        yield
    finally:
        await close_redis()


app = FastAPI(
    title="Hospital Bulk Processing API",
    description=(
        "Upload a CSV to receive an immediate 202 Accepted response with a job ID. "
        "Clients can poll the status endpoint or connect to the WebSocket endpoint for real-time progress while "
        "Celery creates hospitals in parallel and activates the batch when all rows succeed."
    ),
    lifespan=lifespan,
)
app.include_router(bulk_router, prefix="/hospitals")


@app.get("/health")
async def health(redis: Annotated[Any, Depends(get_async_redis)]) -> dict[str, str]:
    """Report Redis and Celery health for deployment probes."""

    return await health_check(redis)

