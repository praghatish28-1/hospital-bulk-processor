from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from celery.app.control import Inspect
from fastapi import HTTPException, UploadFile, WebSocket, WebSocketDisconnect, WebSocketException, status

from app.core.redis import get_async_redis
from app.models.schemas import BulkJobAccepted, BulkJobStatus, CSVValidationResult, ValidationError
from app.services.csv_validator import CSVValidator
from app.tasks.common import JOB_STATE_TTL_SECONDS, TERMINAL_STATUSES, job_events_channel, job_state_key
from app.tasks.orchestrator import process_bulk_orchestrator
from app.tasks.resume import resume_batch
from app.worker import celery_app

HASH_TTL_SECONDS = 3_600
CSV_CONTENT_TYPES = {
    "text/csv",
    "application/csv",
    "application/vnd.ms-excel",
    "application/octet-stream",
    "text/plain",
}


async def start_bulk_job(file: UploadFile, redis: Any) -> BulkJobAccepted:
    """Validate a CSV, create an idempotent job state, and enqueue processing."""

    content = await _read_csv_file(file)
    rows = CSVValidator().validate(content)
    digest = hashlib.sha256(content).hexdigest()
    hash_key = f"bulk:hash:{digest}"
    existing_job_id = await redis.get(hash_key)
    if existing_job_id:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "DUPLICATE_CSV", "message": f"CSV is already being processed as {existing_job_id}"},
        )

    job_id = str(uuid4())
    stored = await redis.set(hash_key, job_id, ex=HASH_TTL_SECONDS, nx=True)
    if not stored:
        existing_job_id = await redis.get(hash_key)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "DUPLICATE_CSV", "message": f"CSV is already being processed as {existing_job_id}"},
        )

    row_payloads = [
        {"row": index, **row.model_dump()}
        for index, row in enumerate(rows, start=1)
    ]
    await _save_initial_state(redis, job_id, len(row_payloads))
    process_bulk_orchestrator.delay(job_id, row_payloads)
    return _accepted(job_id)


async def get_job_status(job_id: str, redis: Any) -> BulkJobStatus:
    """Return the current state of a bulk processing job."""

    state = await _load_state(redis, job_id)
    if not state:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return BulkJobStatus(
        job_id=state["job_id"],
        status=state["status"],
        total=state["total"],
        done=state["done"],
        batch_activated=state.get("batch_activated", False),
        hospitals=state.get("hospitals", []),
        failed_hospitals=state.get("failed_hospitals", 0),
        processing_time_seconds=state.get("processing_time_seconds"),
        started_at=state["started_at"],
    )


async def stream_progress(websocket: WebSocket, job_id: str, redis: Any | None = None) -> None:
    """Stream Redis pub/sub progress events over a WebSocket connection."""

    if redis is None:
        dependency = get_async_redis()
        redis = await anext(dependency)

    state = await _load_state(redis, job_id)
    if not state:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason="Job not found")

    await websocket.accept()

    try:
        for hospital in state.get("hospitals", []):
            await websocket.send_json(
                {
                    "done": state.get("done", 0),
                    "total": state.get("total", 0),
                    "row": hospital.get("row"),
                    "hospital_id": hospital.get("hospital_id"),
                    "status": hospital.get("status"),
                }
            )

        if state.get("status") in TERMINAL_STATUSES:
            await websocket.send_json(_final_event(state))
            await websocket.close()
            return

        pubsub = redis.pubsub()
        await pubsub.subscribe(job_events_channel(job_id))
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if not message:
                    continue
                payload = message["data"]
                event = json.loads(payload if isinstance(payload, str) else payload.decode("utf-8"))
                await websocket.send_json(event)
                if event.get("status") in TERMINAL_STATUSES:
                    await websocket.close()
                    return
        finally:
            await pubsub.unsubscribe(job_events_channel(job_id))
            await pubsub.aclose()
    except WebSocketDisconnect:
        return


async def validate_only(file: UploadFile) -> CSVValidationResult:
    """Validate a CSV without creating a job or touching Redis."""

    content = await _read_csv_file(file)
    rows = CSVValidator().validate(content)
    return CSVValidationResult(valid=True, row_count=len(rows), errors=[])


async def resume_job(job_id: str, redis: Any) -> BulkJobAccepted:
    """Enqueue a resume task for a partial failure job."""

    state = await _load_state(redis, job_id)
    if not state:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    if state.get("status") != "partial_failure":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "JOB_NOT_RESUMABLE", "message": "Only partial_failure jobs can be resumed"},
        )
    state["status"] = "processing"
    state["batch_activated"] = False
    state["processing_time_seconds"] = None
    await redis.set(job_state_key(job_id), json.dumps(state), ex=JOB_STATE_TTL_SECONDS)
    resume_batch.delay(job_id)
    return _accepted(job_id)


async def health_check(redis: Any) -> dict[str, str]:
    """Check Redis connectivity and Celery worker availability."""

    redis_status = "ok"
    celery_status = "ok"
    try:
        await redis.ping()
    except Exception:  # noqa: BLE001 - health endpoint should report, not raise
        redis_status = "error"

    if not celery_app.conf.task_always_eager:
        try:
            inspector: Inspect = celery_app.control.inspect(timeout=1.0)
            celery_status = "ok" if inspector.ping() else "error"
        except Exception:  # noqa: BLE001
            celery_status = "error"

    return {
        "status": "ok" if redis_status == "ok" and celery_status == "ok" else "degraded",
        "redis": redis_status,
        "celery": celery_status,
    }


async def _read_csv_file(file: UploadFile) -> bytes:
    filename = (file.filename or "").lower()
    content_type = (file.content_type or "").lower()
    if filename and not filename.endswith(".csv"):
        _raise_file_validation("file", filename, "Uploaded file must be a .csv file")
    if content_type and content_type not in CSV_CONTENT_TYPES:
        _raise_file_validation("file", content_type, "Uploaded file must be CSV content")
    return await file.read()


def _raise_file_validation(field: str, value: str | None, message: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail={
            "code": "CSV_VALIDATION_FAILED",
            "message": "CSV validation failed. No hospitals were created.",
            "errors": [ValidationError(row=0, field=field, value=value, message=message).model_dump()],
        },
    )


async def _save_initial_state(redis: Any, job_id: str, total: int) -> None:
    state = {
        "job_id": job_id,
        "status": "pending",
        "total": total,
        "done": 0,
        "started_at": datetime.now(UTC).isoformat(),
        "hospitals": [],
        "failed_rows": [],
        "failed_hospitals": 0,
        "batch_activated": False,
        "processing_time_seconds": None,
    }
    await redis.set(job_state_key(job_id), json.dumps(state), ex=JOB_STATE_TTL_SECONDS)


async def _load_state(redis: Any, job_id: str) -> dict[str, Any] | None:
    raw = await redis.get(job_state_key(job_id))
    return json.loads(raw) if raw else None


def _accepted(job_id: str) -> BulkJobAccepted:
    return BulkJobAccepted(
        job_id=job_id,
        status_url=f"/hospitals/bulk/{job_id}/status",
        ws_url=f"/hospitals/bulk/{job_id}/ws",
    )


def _final_event(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": state.get("status"),
        "batch_activated": state.get("batch_activated", False),
        "done": state.get("done", 0),
        "total": state.get("total", 0),
        "processing_time_seconds": state.get("processing_time_seconds"),
    }
