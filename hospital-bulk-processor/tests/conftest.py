from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import fakeredis
import httpx
import pytest
import pytest_asyncio
import respx
from asgi_lifespan import LifespanManager
from fastapi.testclient import TestClient

from app.core import redis as redis_module
from app.core.redis import get_async_redis
from app.main import app
from app.worker import celery_app


@dataclass
class MockHospitalAPI:
    store: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    events: list[tuple[str, str]] = field(default_factory=list)
    post_calls: list[dict[str, Any]] = field(default_factory=list)
    patch_calls: list[str] = field(default_factory=list)
    client_error_names: set[str] = field(default_factory=set)
    server_error_names: set[str] = field(default_factory=set)
    timeout_names: set[str] = field(default_factory=set)
    next_id: int = 101

    def get_batch(self, request: httpx.Request) -> httpx.Response:
        batch_id = request.url.path.split("/")[-1]
        return httpx.Response(200, json=self.store.get(batch_id, []))

    def post_hospital(self, request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content.decode("utf-8"))
        self.events.append(("post", data["name"]))
        self.post_calls.append(data)

        if data["name"] in self.timeout_names:
            raise httpx.TimeoutException("timeout", request=request)
        if data["name"] in self.server_error_names:
            return httpx.Response(500, json={"detail": "server error"})
        if data["name"] in self.client_error_names:
            return httpx.Response(400, json={"detail": "bad data"})

        hospital = {
            "id": self.next_id,
            "name": data["name"],
            "address": data["address"],
            "phone": data.get("phone"),
            "creation_batch_id": data["creation_batch_id"],
            "active": False,
            "created_at": "2025-09-19T10:30:00Z",
        }
        self.next_id += 1
        self.store.setdefault(data["creation_batch_id"], []).append(hospital)
        return httpx.Response(200, json=hospital)

    def activate_batch(self, request: httpx.Request) -> httpx.Response:
        batch_id = request.url.path.split("/")[-2]
        self.events.append(("patch", batch_id))
        self.patch_calls.append(batch_id)
        for hospital in self.store.get(batch_id, []):
            hospital["active"] = True
        return httpx.Response(200, json={"ok": True})

    def post_names_after(self, index: int) -> list[str]:
        return [payload["name"] for payload in self.post_calls[index:]]


@pytest.fixture(autouse=True)
def celery_eager(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    celery_app.conf.update(task_always_eager=True, task_eager_propagates=True, task_store_eager_result=True)
    monkeypatch.setattr("app.tasks.create_hospital.time.sleep", lambda _: None)
    monkeypatch.setattr("app.tasks.activate_batch.time.sleep", lambda _: None)
    yield


@pytest.fixture
def redis_pair(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[Any, Any]]:
    server = fakeredis.FakeServer()
    async_client = fakeredis.aioredis.FakeRedis(server=server, decode_responses=True)
    sync_client = fakeredis.FakeRedis(server=server, decode_responses=True)
    monkeypatch.setattr(redis_module, "async_redis_client", async_client)
    monkeypatch.setattr(redis_module, "sync_redis_client", sync_client)
    yield async_client, sync_client
    try:
        asyncio.run(async_client.aclose())
    except RuntimeError:
        pass
    sync_client.close()


@pytest.fixture
def fake_redis(redis_pair: tuple[Any, Any]) -> Any:
    return redis_pair[0]


@pytest.fixture
def mock_hospital_api() -> Iterator[MockHospitalAPI]:
    api = MockHospitalAPI()
    base_pattern = r"https://hospital-directory\.onrender\.com"
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        router.get(re.compile(base_pattern + r"/hospitals/batch/[^/]+$")).mock(side_effect=api.get_batch)
        router.post("https://hospital-directory.onrender.com/hospitals/").mock(side_effect=api.post_hospital)
        router.patch(re.compile(base_pattern + r"/hospitals/batch/[^/]+/activate$")).mock(
            side_effect=api.activate_batch
        )
        yield api


@pytest_asyncio.fixture
async def async_client(fake_redis: Any) -> AsyncIterator[httpx.AsyncClient]:
    async def override_redis() -> AsyncIterator[Any]:
        yield fake_redis

    app.dependency_overrides[get_async_redis] = override_redis
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client
    app.dependency_overrides.clear()


@pytest.fixture
def test_client(fake_redis: Any) -> Iterator[TestClient]:
    async def override_redis() -> AsyncIterator[Any]:
        yield fake_redis

    app.dependency_overrides[get_async_redis] = override_redis
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def make_csv() -> Callable[[int | list[dict[str, str | None]], bool], str]:
    def _make_csv(rows: int | list[dict[str, str | None]] = 5, include_phone: bool = True) -> str:
        if isinstance(rows, int):
            row_data = [
                {
                    "name": f"Hospital {index}",
                    "address": f"{index} Main St",
                    "phone": f"555-01{index:02d}",
                }
                for index in range(1, rows + 1)
            ]
        else:
            row_data = rows

        header = "name,address,phone" if include_phone else "name,address"
        lines = [header]
        for row in row_data:
            if include_phone:
                lines.append(f"{row.get('name', '')},{row.get('address', '')},{row.get('phone') or ''}")
            else:
                lines.append(f"{row.get('name', '')},{row.get('address', '')}")
        return "\n".join(lines) + "\n"

    return _make_csv


@pytest.fixture
def wait_for_completion(async_client: httpx.AsyncClient) -> Callable[[str, float], Any]:
    async def _wait(job_id: str, deadline_seconds: float = 30) -> dict[str, Any]:
        deadline = asyncio.get_running_loop().time() + deadline_seconds
        while asyncio.get_running_loop().time() < deadline:
            response = await async_client.get(f"/hospitals/bulk/{job_id}/status")
            if response.status_code == 200:
                payload = response.json()
                if payload["status"] in {"complete", "partial_failure", "failed"}:
                    return payload
            await asyncio.sleep(0.01)
        raise AssertionError(f"job {job_id} did not finish")

    return _wait


async def post_bulk(client: httpx.AsyncClient, csv_text: str) -> httpx.Response:
    return await client.post(
        "/hospitals/bulk",
        files={"file": ("hospitals.csv", csv_text.encode("utf-8"), "text/csv")},
    )
