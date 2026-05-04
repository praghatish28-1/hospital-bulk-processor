from __future__ import annotations

from typing import Any

import structlog
from celery import chord, group

from app.core.redis import get_sync_redis
from app.tasks.activate_batch import activate_batch
from app.tasks.common import load_state, mutate_state
from app.tasks.create_hospital import create_hospital
from app.worker import celery_app

logger = structlog.get_logger(__name__)


@celery_app.task(name="tasks.resume_batch", bind=True)
def resume_batch(self: Any, job_id: str) -> dict[str, Any]:
    """Resume a partial failure by processing only failed rows."""

    redis_client = get_sync_redis()
    state = load_state(redis_client, job_id)
    if not state:
        return {"job_id": job_id, "status": "not_found"}
    if state.get("status") == "complete":
        return {"job_id": job_id, "status": "complete", "message": "Job is already complete"}

    failed_rows = state.get("failed_rows", [])
    if not failed_rows:
        return {"job_id": job_id, "status": state.get("status"), "message": "No failed rows to resume"}

    def mark_processing(current: dict[str, Any]) -> dict[str, Any]:
        current["status"] = "processing"
        current["batch_activated"] = False
        current["processing_time_seconds"] = None
        return current

    mutate_state(redis_client, job_id, mark_processing)
    rows = [{**failed["data"], "row": failed["row"]} for failed in failed_rows]
    workflow = chord(
        group(create_hospital.s(job_id, row, int(row["row"])) for row in rows),
        activate_batch.s(job_id),
    )
    workflow.apply_async()
    logger.info("resume_batch_enqueued", job_id=job_id, row=None, total=len(rows), duration_ms=0)
    return {"job_id": job_id, "status": "enqueued", "total": len(rows)}

