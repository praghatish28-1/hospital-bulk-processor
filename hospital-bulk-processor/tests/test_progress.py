from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from app.services import bulk_service
from tests.conftest import post_bulk


async def test_status_returns_pending_or_processing_during_job(async_client, make_csv, monkeypatch, mock_hospital_api):
    monkeypatch.setattr(bulk_service.process_bulk_orchestrator, "delay", lambda *_: None)
    response = await post_bulk(async_client, make_csv(2))
    status_response = await async_client.get(f"/hospitals/bulk/{response.json()['job_id']}/status")
    assert status_response.json()["status"] in {"pending", "processing"}


async def test_status_returns_complete_after_job(async_client, make_csv, wait_for_completion, mock_hospital_api):
    response = await post_bulk(async_client, make_csv(2))
    payload = await wait_for_completion(response.json()["job_id"])
    assert payload["status"] == "complete"


async def test_404_for_unknown_job(async_client):
    response = await async_client.get("/hospitals/bulk/unknown/status")
    assert response.status_code == 404


async def test_done_count_only_increases(async_client, make_csv, wait_for_completion, mock_hospital_api):
    response = await post_bulk(async_client, make_csv(4))
    payload = await wait_for_completion(response.json()["job_id"])
    done_values = [index for index, _ in enumerate(payload["hospitals"], start=1)]
    assert done_values == sorted(done_values)
    assert payload["done"] == 4


def test_websocket_connects(test_client, make_csv, mock_hospital_api):
    response = test_client.post(
        "/hospitals/bulk",
        files={"file": ("hospitals.csv", make_csv(1).encode(), "text/csv")},
    )
    with test_client.websocket_connect(f"/hospitals/bulk/{response.json()['job_id']}/ws") as websocket:
        assert websocket.receive_json()["status"] == "created_and_activated"


def test_websocket_receives_one_event_per_hospital(test_client, make_csv, mock_hospital_api):
    response = test_client.post(
        "/hospitals/bulk",
        files={"file": ("hospitals.csv", make_csv(3).encode(), "text/csv")},
    )
    events = []
    with test_client.websocket_connect(f"/hospitals/bulk/{response.json()['job_id']}/ws") as websocket:
        for _ in range(4):
            events.append(websocket.receive_json())
    hospital_events = [event for event in events if "row" in event]
    assert len(hospital_events) == 3


def test_websocket_closes_cleanly_on_completion(test_client, make_csv, mock_hospital_api):
    response = test_client.post(
        "/hospitals/bulk",
        files={"file": ("hospitals.csv", make_csv(1).encode(), "text/csv")},
    )
    with pytest.raises(WebSocketDisconnect):
        with test_client.websocket_connect(f"/hospitals/bulk/{response.json()['job_id']}/ws") as websocket:
            while True:
                websocket.receive_json()


def test_websocket_404_for_unknown_job(test_client):
    with pytest.raises(WebSocketDisconnect):
        with test_client.websocket_connect("/hospitals/bulk/unknown/ws"):
            pass

