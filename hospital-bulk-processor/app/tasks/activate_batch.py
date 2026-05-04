from __future__ import annotations

import time
from typing import Any

import httpx
import structlog

from app.core.config import settings
from app.core.redis import get_sync_redis
from app.tasks.common import build_batch_summary, mutate_state, processing_time_seconds, publish_event
from app.worker import celery_app

logger = structlog.get_logger(__name__)


@celery_app.task(name="tasks.activate_batch", bind=True)
def activate_batch(self: Any, results: list[dict[str, Any]], job_id: str) -> dict[str, Any]:
    """Activate a batch after all create tasks finish, or finalize partial failure."""

    started = time.perf_counter()
    redis_client = get_sync_redis()
    has_failures = any(result.get("status") == "failed" for result in results)
    activation_error: str | None = None

    if not has_failures:
        activation_error = _activate_with_retries(job_id)

    def finalize(state: dict[str, Any]) -> dict[str, Any]:
        state.setdefault("job_id", job_id)
        state.setdefault("hospitals", [])
        state.setdefault("failed_rows", [])
        state["processing_time_seconds"] = processing_time_seconds(state)

        if has_failures:
            state["status"] = "partial_failure"
            state["batch_activated"] = False
        elif activation_error:
            state["status"] = "failed"
            state["batch_activated"] = False
            state["activation_error"] = activation_error
        else:
            state["status"] = "complete"
            state["batch_activated"] = True
            state["failed_rows"] = []
            for hospital in state["hospitals"]:
                if hospital.get("status") != "failed":
                    hospital["status"] = "created_and_activated"
                    hospital["error"] = None

        state["failed_hospitals"] = len(state.get("failed_rows", []))
        return state

    state = mutate_state(redis_client, job_id, finalize)
    event = {
        "status": state["status"],
        "batch_activated": state.get("batch_activated", False),
        "done": state.get("done", 0),
        "total": state.get("total", 0),
        "processing_time_seconds": state.get("processing_time_seconds"),
    }
    publish_event(redis_client, job_id, event)

    duration_ms = int((time.perf_counter() - started) * 1000)
    logger.info("activate_batch_finished", job_id=job_id, row=None, status=state["status"], duration_ms=duration_ms)
    return build_batch_summary(state)


def _activate_with_retries(job_id: str) -> str | None:
    with httpx.Client(base_url=str(settings.HOSPITAL_API_URL).rstrip("/"), timeout=settings.HTTP_TIMEOUT) as client:
        for attempt in range(settings.MAX_RETRIES + 1):
            try:
                response = client.patch(f"/hospitals/batch/{job_id}/activate")
            except httpx.TimeoutException:
                if attempt < settings.MAX_RETRIES:
                    time.sleep(2**attempt)
                    continue
                return "Hospital API timeout during activation"
            except httpx.HTTPError as exc:
                if attempt < settings.MAX_RETRIES:
                    time.sleep(2**attempt)
                    continue
                return f"Hospital API activation request failed: {exc}"

            if 500 <= response.status_code:
                if attempt < settings.MAX_RETRIES:
                    time.sleep(2**attempt)
                    continue
                return f"Hospital API returned {response.status_code} during activation"
            if 400 <= response.status_code:
                return f"Hospital API returned {response.status_code} during activation: {response.text}"
            return None
    return "Hospital API activation request failed"

