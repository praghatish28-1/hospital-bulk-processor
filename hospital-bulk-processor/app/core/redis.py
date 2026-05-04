from collections.abc import AsyncIterator

import redis
import redis.asyncio as redis_async

from app.core.config import settings

async_redis_client: redis_async.Redis | None = None
sync_redis_client: redis.Redis | None = None


async def init_redis() -> None:
    """Initialize Redis clients used by FastAPI routes and Celery tasks."""

    global async_redis_client, sync_redis_client
    if async_redis_client is None:
        async_redis_client = redis_async.from_url(settings.REDIS_URL, decode_responses=True)
    if sync_redis_client is None:
        sync_redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)


async def close_redis() -> None:
    """Close Redis connections on application shutdown."""

    global async_redis_client, sync_redis_client
    if async_redis_client is not None:
        await async_redis_client.aclose()
        async_redis_client = None
    if sync_redis_client is not None:
        sync_redis_client.close()
        sync_redis_client = None


async def get_async_redis() -> AsyncIterator[redis_async.Redis]:
    """FastAPI dependency that provides the async Redis client."""

    if async_redis_client is None:
        await init_redis()
    if async_redis_client is None:
        raise RuntimeError("Redis client was not initialized")
    yield async_redis_client


def get_sync_redis() -> redis.Redis:
    """Return the synchronous Redis client used by Celery tasks."""

    global sync_redis_client
    if sync_redis_client is None:
        sync_redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    return sync_redis_client

