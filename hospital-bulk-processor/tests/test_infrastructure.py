from __future__ import annotations

import asyncio

from tests.conftest import post_bulk


async def test_health_check_returns_ok(async_client, fake_redis):
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "redis": "ok", "celery": "ok"}


async def test_celery_worker_reachable(async_client):
    response = await async_client.get("/health")
    assert response.json()["celery"] == "ok"


async def test_two_simultaneous_jobs_have_separate_states(
    async_client,
    make_csv,
    wait_for_completion,
    mock_hospital_api,
):
    csv_a = make_csv([{"name": "A Hospital", "address": "1 A St", "phone": "555-0101"}])
    csv_b = make_csv([{"name": "B Hospital", "address": "1 B St", "phone": "555-0102"}])
    first, second = await asyncio.gather(post_bulk(async_client, csv_a), post_bulk(async_client, csv_b))
    first_payload = await wait_for_completion(first.json()["job_id"])
    second_payload = await wait_for_completion(second.json()["job_id"])
    assert first_payload["job_id"] != second_payload["job_id"]
    assert first_payload["hospitals"][0]["name"] == "A Hospital"
    assert second_payload["hospitals"][0]["name"] == "B Hospital"


async def test_two_simultaneous_jobs_produce_correct_counts(
    async_client,
    make_csv,
    wait_for_completion,
    mock_hospital_api,
):
    first, second = await asyncio.gather(post_bulk(async_client, make_csv(2)), post_bulk(async_client, make_csv(3)))
    first_payload = await wait_for_completion(first.json()["job_id"])
    second_payload = await wait_for_completion(second.json()["job_id"])
    assert first_payload["total"] == 2
    assert first_payload["done"] == 2
    assert second_payload["total"] == 3
    assert second_payload["done"] == 3
