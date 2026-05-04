from __future__ import annotations

import pytest


async def test_valid_csv_with_phone(async_client, make_csv):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", make_csv(3).encode(), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json() == {"valid": True, "row_count": 3, "errors": []}


async def test_valid_csv_without_phone(async_client, make_csv):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", make_csv(3, include_phone=False).encode(), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json()["row_count"] == 3


@pytest.mark.parametrize("csv_text, missing", [("address\n123 Main St\n", "name"), ("name\nGeneral\n", "address")])
async def test_missing_required_columns(async_client, csv_text, missing):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", csv_text.encode(), "text/csv")},
    )
    assert response.status_code == 422
    assert missing in str(response.json()["detail"]["errors"])


async def test_exceeds_20_rows(async_client, make_csv):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", make_csv(21).encode(), "text/csv")},
    )
    assert response.status_code == 422


async def test_exactly_20_rows(async_client, make_csv):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", make_csv(20).encode(), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json()["row_count"] == 20


async def test_empty_file(async_client):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", b"", "text/csv")},
    )
    assert response.status_code == 422


async def test_wrong_file_type(async_client, make_csv):
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.txt", make_csv(1).encode(), "text/plain")},
    )
    assert response.status_code == 422


async def test_extra_columns_ignored(async_client):
    csv_text = "name,address,phone,notes\nGeneral,123 Main,555-0101,ignored\n"
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", csv_text.encode(), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json()["row_count"] == 1


async def test_empty_name_reports_row(async_client):
    csv_text = "name,address,phone\n,123 Main,555-0101\n"
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", csv_text.encode(), "text/csv")},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["errors"][0]["row"] == 2


async def test_empty_address_reports_row(async_client):
    csv_text = "name,address,phone\nGeneral,,555-0101\n"
    response = await async_client.post(
        "/hospitals/bulk/validate",
        files={"file": ("hospitals.csv", csv_text.encode(), "text/csv")},
    )
    assert response.status_code == 422
    assert response.json()["detail"]["errors"][0]["row"] == 2

