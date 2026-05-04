from __future__ import annotations

import time
from typing import Any

import httpx
import structlog

from app.core.config import settings
from app.core.redis import get_sync_redis
from app.tasks.common import mutate_state, publish_event
from app.worker import celery_app

logger = structlog.get_logger(__name__)


@celery_app.task(name="tasks.create_hospital", bind=True)
def create_hospital(self: Any, job_id: str, row: dict[str, Any], idx: int) -> dict[str, Any]:
    """Create one hospital safely and record progress without raising."""

    started = time.perf_counter()
    name = (row.get("name") or "").strip()
    result: dict[str, Any]

    try:
        result = _create_or_skip(job_id, row, idx)
    except Exception as exc:  # noqa: BLE001 - task must never raise into chord
        result = {
            "row": idx,
            "hospital_id": None,
            "name": name,
            "status": "failed",
            "error": str(exc),
        }

    state = _record_result(job_id, row, result)
    publish_event(
        get_sync_redis(),
        job_id,
        {
            "done": state.get("done", 0),
            "total": state.get("total", 0),
            "row": idx,
            "hospital_id": result.get("hospital_id"),
            "status": result["status"],
        },
    )
    duration_ms = int((time.perf_counter() - started) * 1000)
    logger.info(
        "create_hospital_finished",
        job_id=job_id,
        row=idx,
        status=result["status"],
        duration_ms=duration_ms,
    )
    return result


def _create_or_skip(job_id: str, row: dict[str, Any], idx: int) -> dict[str, Any]:
    name = (row.get("name") or "").strip()
    address = (row.get("address") or "").strip()
    phone = (row.get("phone") or None) or None

    with httpx.Client(base_url=str(settings.HOSPITAL_API_URL).rstrip("/"), timeout=settings.HTTP_TIMEOUT) as client:
        existing, existing_error = _find_existing_hospital(client, job_id, name)
        if existing_error:
            return _failed(idx, name, existing_error)
        if existing:
            return {
                "row": idx,
                "hospital_id": existing.get("id"),
                "name": existing.get("name") or name,
                "status": "created_and_activated" if existing.get("active") else "created",
                "error": None,
            }

        response, error = _request_with_retries(
            client,
            "POST",
            "/hospitals/",
            json={
                "name": name,
                "address": address,
                "phone": phone,
                "creation_batch_id": job_id,
            },
        )
        if error or response is None:
            return _failed(idx, name, error or "Hospital API request failed")

        payload = response.json()
        return {
            "row": idx,
            "hospital_id": payload.get("id"),
            "name": payload.get("name") or name,
            "status": "created_and_activated" if payload.get("active") else "created",
            "error": None,
        }


def _find_existing_hospital(
    client: httpx.Client,
    job_id: str,
    name: str,
) -> tuple[dict[str, Any] | None, str | None]:
    response, error = _request_with_retries(client, "GET", f"/hospitals/batch/{job_id}", allow_404=True)
    if error or response is None:
        return None, error
    if response.status_code == 404:
        return None, None

    payload = response.json()
    hospitals = payload.get("hospitals", payload) if isinstance(payload, dict) else payload
    if not isinstance(hospitals, list):
        return None, "Hospital API returned an invalid batch response"

    normalized_name = name.casefold()
    for hospital in hospitals:
        if str(hospital.get("name", "")).strip().casefold() == normalized_name:
            return hospital, None
    return None, None


def _request_with_retries(
    client: httpx.Client,
    method: str,
    path: str,
    *,
    json: dict[str, Any] | None = None,
    allow_404: bool = False,
) -> tuple[httpx.Response | None, str | None]:
    max_retries = settings.MAX_RETRIES
    for attempt in range(max_retries + 1):
        try:
            response = client.request(method, path, json=json)
        except httpx.TimeoutException:
            if attempt < max_retries:
                time.sleep(2**attempt)
                continue
            return None, "Hospital API timeout"
        except httpx.HTTPError as exc:
            if attempt < max_retries:
                time.sleep(2**attempt)
                continue
            return None, f"Hospital API request failed: {exc}"

        if allow_404 and response.status_code == 404:
            return response, None
        if 500 <= response.status_code:
            if attempt < max_retries:
                time.sleep(2**attempt)
                continue
            return None, f"Hospital API returned {response.status_code}"
        if 400 <= response.status_code:
            return None, f"Hospital API returned {response.status_code}: {response.text}"
        return response, None

    return None, "Hospital API request failed"


def _failed(idx: int, name: str, error: str) -> dict[str, Any]:
    return {
        "row": idx,
        "hospital_id": None,
        "name": name,
        "status": "failed",
        "error": error,
    }


def _record_result(job_id: str, row: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    redis_client = get_sync_redis()

    def update(state: dict[str, Any]) -> dict[str, Any]:
        state.setdefault("job_id", job_id)
        state.setdefault("status", "processing")
        state.setdefault("total", 0)
        state.setdefault("done", 0)
        state.setdefault("hospitals", [])
        state.setdefault("failed_rows", [])

        prior_rows = {hospital["row"] for hospital in state["hospitals"]}
        hospital_result = {
            "row": result["row"],
            "hospital_id": result.get("hospital_id"),
            "name": result["name"],
            "status": result["status"],
            "error": result.get("error"),
        }
        state["hospitals"] = [
            hospital for hospital in state["hospitals"] if hospital.get("row") != result["row"]
        ]
        state["hospitals"].append(hospital_result)
        state["hospitals"].sort(key=lambda hospital: hospital["row"])

        if result["row"] not in prior_rows:
            state["done"] = min(state.get("total", 0), state.get("done", 0) + 1)

        state["failed_rows"] = [
            failed for failed in state.get("failed_rows", []) if failed.get("row") != result["row"]
        ]
        if result["status"] == "failed":
            state["failed_rows"].append(
                {
                    "row": result["row"],
                    "data": {
                        "name": row.get("name"),
                        "address": row.get("address"),
                        "phone": row.get("phone"),
                    },
                    "error": result.get("error"),
                }
            )
            state["failed_rows"].sort(key=lambda failed: failed["row"])

        state["failed_hospitals"] = len(state.get("failed_rows", []))
        if state.get("status") not in {"complete", "partial_failure", "failed"}:
            state["status"] = "processing"
        return state

    return mutate_state(redis_client, job_id, update)

