from __future__ import annotations

from uuid import UUID

from app.tasks.common import build_batch_summary
from tests.conftest import post_bulk


async def test_returns_202_with_job_id(async_client, make_csv, mock_hospital_api):
    response = await post_bulk(async_client, make_csv(2))
    assert response.status_code == 202
    assert "job_id" in response.json()


async def test_job_id_is_valid_uuid(async_client, make_csv, mock_hospital_api):
    response = await post_bulk(async_client, make_csv(1))
    UUID(response.json()["job_id"])


async def test_happy_path_full_response_shape_matches_spec_exactly(
    async_client,
    make_csv,
    wait_for_completion,
    fake_redis,
    mock_hospital_api,
):
    response = await post_bulk(async_client, make_csv(3))
    job_id = response.json()["job_id"]
    status_payload = await wait_for_completion(job_id)
    state = {
        "job_id": job_id,
        "total": status_payload["total"],
        "done": status_payload["done"],
        "failed_hospitals": status_payload["failed_hospitals"],
        "processing_time_seconds": status_payload["processing_time_seconds"],
        "batch_activated": status_payload["batch_activated"],
        "hospitals": status_payload["hospitals"],
    }
    summary = build_batch_summary(state)
    assert set(summary) == {
        "batch_id",
        "total_hospitals",
        "processed_hospitals",
        "failed_hospitals",
        "processing_time_seconds",
        "batch_activated",
        "hospitals",
    }
    assert summary["batch_id"] == job_id
    assert summary["total_hospitals"] == 3
    assert summary["processed_hospitals"] == 3
    assert summary["failed_hospitals"] == 0
    assert summary["batch_activated"] is True
    assert summary["hospitals"][0]["status"] == "created_and_activated"


async def test_batch_activate_called_only_after_all_hospitals_created(
    async_client,
    make_csv,
    wait_for_completion,
    mock_hospital_api,
):
    response = await post_bulk(async_client, make_csv(4))
    await wait_for_completion(response.json()["job_id"])
    patch_index = next(index for index, event in enumerate(mock_hospital_api.events) if event[0] == "patch")
    assert all(event[0] == "post" for event in mock_hospital_api.events[:patch_index])
    assert len([event for event in mock_hospital_api.events[:patch_index] if event[0] == "post"]) == 4


async def test_processing_time_seconds_greater_than_zero(
    async_client,
    make_csv,
    wait_for_completion,
    mock_hospital_api,
):
    response = await post_bulk(async_client, make_csv(1))
    payload = await wait_for_completion(response.json()["job_id"])
    assert payload["processing_time_seconds"] > 0


async def test_duplicate_csv_returns_409(async_client, make_csv, mock_hospital_api):
    csv_text = make_csv(2)
    first = await post_bulk(async_client, csv_text)
    second = await post_bulk(async_client, csv_text)
    assert first.status_code == 202
    assert second.status_code == 409
