from __future__ import annotations

from tests.conftest import post_bulk


async def test_two_of_five_fail(async_client, make_csv, wait_for_completion, mock_hospital_api):
    mock_hospital_api.client_error_names.update({"Hospital 2", "Hospital 4"})
    response = await post_bulk(async_client, make_csv(5))
    payload = await wait_for_completion(response.json()["job_id"])
    assert payload["failed_hospitals"] == 2
    assert payload["batch_activated"] is False
    assert payload["status"] == "partial_failure"


async def test_failed_hospital_has_failed_status_and_error(
    async_client,
    make_csv,
    wait_for_completion,
    mock_hospital_api,
):
    mock_hospital_api.client_error_names.add("Hospital 3")
    response = await post_bulk(async_client, make_csv(5))
    payload = await wait_for_completion(response.json()["job_id"])
    failed = [hospital for hospital in payload["hospitals"] if hospital["status"] == "failed"]
    assert failed
    assert failed[0]["error"]


async def test_hospital_api_timeout_marked_failed(async_client, make_csv, wait_for_completion, mock_hospital_api):
    mock_hospital_api.timeout_names.add("Hospital 1")
    response = await post_bulk(async_client, make_csv(2))
    payload = await wait_for_completion(response.json()["job_id"])
    assert payload["failed_hospitals"] == 1
    assert "timeout" in payload["hospitals"][0]["error"].lower()


async def test_hospital_api_500_marked_failed(async_client, make_csv, wait_for_completion, mock_hospital_api):
    mock_hospital_api.server_error_names.add("Hospital 1")
    response = await post_bulk(async_client, make_csv(2))
    payload = await wait_for_completion(response.json()["job_id"])
    assert payload["failed_hospitals"] == 1
    assert "500" in payload["hospitals"][0]["error"]


async def test_all_fail_batch_not_activated(async_client, make_csv, wait_for_completion, mock_hospital_api):
    mock_hospital_api.client_error_names.update({"Hospital 1", "Hospital 2", "Hospital 3"})
    response = await post_bulk(async_client, make_csv(3))
    payload = await wait_for_completion(response.json()["job_id"])
    assert payload["failed_hospitals"] == 3
    assert payload["batch_activated"] is False
    assert mock_hospital_api.patch_calls == []
