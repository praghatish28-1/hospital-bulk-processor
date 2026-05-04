from __future__ import annotations

from tests.conftest import post_bulk


async def test_only_failed_rows_resubmitted(async_client, make_csv, wait_for_completion, mock_hospital_api):
    mock_hospital_api.client_error_names.update({"Hospital 2", "Hospital 4"})
    response = await post_bulk(async_client, make_csv(5))
    job_id = response.json()["job_id"]
    await wait_for_completion(job_id)
    before_resume = len(mock_hospital_api.post_calls)

    mock_hospital_api.client_error_names.clear()
    resume_response = await async_client.post(f"/hospitals/bulk/{job_id}/resume")
    assert resume_response.status_code == 202
    await wait_for_completion(job_id)
    assert mock_hospital_api.post_names_after(before_resume) == ["Hospital 2", "Hospital 4"]


async def test_after_resume_failed_hospitals_zero(async_client, make_csv, wait_for_completion, mock_hospital_api):
    mock_hospital_api.client_error_names.add("Hospital 1")
    response = await post_bulk(async_client, make_csv(3))
    job_id = response.json()["job_id"]
    await wait_for_completion(job_id)
    mock_hospital_api.client_error_names.clear()
    await async_client.post(f"/hospitals/bulk/{job_id}/resume")
    payload = await wait_for_completion(job_id)
    assert payload["failed_hospitals"] == 0
    assert payload["batch_activated"] is True


async def test_resume_on_complete_returns_409(async_client, make_csv, wait_for_completion, mock_hospital_api):
    response = await post_bulk(async_client, make_csv(1))
    job_id = response.json()["job_id"]
    await wait_for_completion(job_id)
    resume_response = await async_client.post(f"/hospitals/bulk/{job_id}/resume")
    assert resume_response.status_code == 409


async def test_resume_unknown_job_returns_404(async_client):
    response = await async_client.post("/hospitals/bulk/unknown/resume")
    assert response.status_code == 404

