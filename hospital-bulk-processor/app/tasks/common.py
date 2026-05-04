from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import redis

JOB_STATE_TTL_SECONDS = 86_400
TERMINAL_STATUSES = {"complete", "partial_failure", "failed"}


def job_state_key(job_id: str) -> str:
    return f"bulk:job:{job_id}"


def job_events_channel(job_id: str) -> str:
    return f"job:{job_id}:events"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def load_state(redis_client: redis.Redis, job_id: str) -> dict[str, Any] | None:
    raw = redis_client.get(job_state_key(job_id))
    return json.loads(raw) if raw else None


def save_state(redis_client: redis.Redis, job_id: str, state: dict[str, Any]) -> None:
    redis_client.set(job_state_key(job_id), json.dumps(state), ex=JOB_STATE_TTL_SECONDS)


def mutate_state(
    redis_client: redis.Redis,
    job_id: str,
    mutator: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    lock_key = f"bulk:job:{job_id}:lock"
    token = str(uuid.uuid4())
    acquired = False
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        acquired = bool(redis_client.set(lock_key, token, nx=True, ex=10))
        if acquired:
            break
        time.sleep(0.05)

    try:
        state = load_state(redis_client, job_id) or {}
        state = mutator(state)
        save_state(redis_client, job_id, state)
        return state
    finally:
        if acquired and redis_client.get(lock_key) == token:
            redis_client.delete(lock_key)


def publish_event(redis_client: redis.Redis, job_id: str, event: dict[str, Any]) -> None:
    redis_client.publish(job_events_channel(job_id), json.dumps(event))


def processing_time_seconds(state: dict[str, Any]) -> float:
    started_at = datetime.fromisoformat(state["started_at"])
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)
    return max(round((datetime.now(UTC) - started_at).total_seconds(), 3), 0.001)


def build_batch_summary(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch_id": state["job_id"],
        "total_hospitals": state.get("total", 0),
        "processed_hospitals": state.get("done", 0),
        "failed_hospitals": state.get("failed_hospitals", 0),
        "processing_time_seconds": state.get("processing_time_seconds") or 0.0,
        "batch_activated": state.get("batch_activated", False),
        "hospitals": state.get("hospitals", []),
    }
