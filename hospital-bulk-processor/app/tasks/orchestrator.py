from __future__ import annotations

from typing import Any

import structlog
from celery import chord, group

from app.core.redis import get_sync_redis
from app.tasks.activate_batch import activate_batch
from app.tasks.common import mutate_state, utc_now_iso
from app.tasks.create_hospital import create_hospital
from app.worker import celery_app

logger = structlog.get_logger(__name__)


@celery_app.task(name="tasks.orchestrator", bind=True)
def process_bulk_orchestrator(self: Any, job_id: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Create a chord of parallel hospital creation tasks followed by activation."""

    redis_client = get_sync_redis()

    def initialize(state: dict[str, Any]) -> dict[str, Any]:
        return {
            **state,
            "job_id": job_id,
            "status": "processing",
            "total": len(rows),
            "done": state.get("done", 0),
            "started_at": state.get("started_at") or utc_now_iso(),
            "hospitals": state.get("hospitals", []),
            "failed_rows": state.get("failed_rows", []),
            "failed_hospitals": state.get("failed_hospitals", 0),
            "batch_activated": state.get("batch_activated", False),
        }

    mutate_state(redis_client, job_id, initialize)

    workflow = chord(
        group(create_hospital.s(job_id, row, int(row.get("row", idx))) for idx, row in enumerate(rows, start=1)),
        activate_batch.s(job_id),
    )
    workflow.apply_async()
    logger.info("orchestrator_enqueued", job_id=job_id, row=None, total=len(rows), duration_ms=0)
    return {"job_id": job_id, "status": "enqueued", "total": len(rows)}

