from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class HospitalRow(BaseModel):
    name: str
    address: str
    phone: str | None = None

    @field_validator("name", "address")
    @classmethod
    def required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Field must not be empty")
        return cleaned

    @field_validator("phone")
    @classmethod
    def optional_phone(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class HospitalResult(BaseModel):
    row: int
    hospital_id: int | None = None
    name: str
    status: Literal["created_and_activated", "created", "failed"]
    error: str | None = None


class BulkJobAccepted(BaseModel):
    job_id: str
    status_url: str
    ws_url: str


class BulkJobStatus(BaseModel):
    job_id: str
    status: str
    total: int
    done: int
    batch_activated: bool
    hospitals: list[HospitalResult] = Field(default_factory=list)
    failed_hospitals: int = 0
    processing_time_seconds: float | None = None
    started_at: datetime


class ValidationError(BaseModel):
    row: int
    field: str
    value: str | None = None
    message: str


class CSVValidationResult(BaseModel):
    valid: bool
    row_count: int
    errors: list[ValidationError] = Field(default_factory=list)


class BatchProcessingSummary(BaseModel):
    """The completed/partial batch response shape required by the assignment."""

    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    hospitals: list[HospitalResult]

    model_config = ConfigDict(json_schema_extra={
        "example": {
            "batch_id": "4c1a48ca-2f5b-40b4-9f5c-2f3c8f572d65",
            "total_hospitals": 20,
            "processed_hospitals": 20,
            "failed_hospitals": 0,
            "processing_time_seconds": 2.4,
            "batch_activated": True,
            "hospitals": [
                {
                    "row": 1,
                    "hospital_id": 101,
                    "name": "General Hospital",
                    "status": "created_and_activated",
                    "error": None,
                }
            ],
        }
    })

