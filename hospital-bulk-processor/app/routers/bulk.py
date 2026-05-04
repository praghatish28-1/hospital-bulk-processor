from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, UploadFile, WebSocket, status

from app.core.redis import get_async_redis
from app.models.schemas import BulkJobAccepted, BulkJobStatus, CSVValidationResult
from app.services import bulk_service

router = APIRouter(tags=["Hospital bulk processing"])


@router.post("/bulk", response_model=BulkJobAccepted, status_code=status.HTTP_202_ACCEPTED)
async def upload_bulk_hospitals(
    file: Annotated[UploadFile, File(description="CSV file containing up to 20 hospitals")],
    redis: Annotated[Any, Depends(get_async_redis)],
) -> BulkJobAccepted:
    """Upload a CSV and start asynchronous hospital creation and activation."""

    return await bulk_service.start_bulk_job(file, redis)


@router.post("/bulk/validate", response_model=CSVValidationResult)
async def validate_bulk_hospitals(
    file: Annotated[UploadFile, File(description="CSV file to validate without side effects")],
) -> CSVValidationResult:
    """Dry-run CSV validation without creating a Redis job or calling the Hospital Directory API."""

    return await bulk_service.validate_only(file)


@router.get("/bulk/{job_id}/status", response_model=BulkJobStatus)
async def bulk_job_status(
    job_id: str,
    redis: Annotated[Any, Depends(get_async_redis)],
) -> BulkJobStatus:
    """Poll progress for a previously accepted bulk hospital job."""

    return await bulk_service.get_job_status(job_id, redis)


@router.websocket("/bulk/{job_id}/ws")
async def bulk_job_progress_ws(
    websocket: WebSocket,
    job_id: str,
    redis: Annotated[Any, Depends(get_async_redis)],
) -> None:
    """Stream hospital creation progress and the final activation event over WebSocket."""

    await bulk_service.stream_progress(websocket, job_id, redis)


@router.post("/bulk/{job_id}/resume", response_model=BulkJobAccepted, status_code=status.HTTP_202_ACCEPTED)
async def resume_bulk_job(
    job_id: str,
    redis: Annotated[Any, Depends(get_async_redis)],
) -> BulkJobAccepted:
    """Resume a partial failure job by submitting only failed rows again."""

    return await bulk_service.resume_job(job_id, redis)

