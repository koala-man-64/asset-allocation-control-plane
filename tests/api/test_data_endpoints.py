import pytest

from api.endpoints import data as data_endpoints
from api.service.app import create_app
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_data_endpoint_calls_service(monkeypatch):
    calls = []

    def fake_get_data(
        layer: str,
        domain: str,
        ticker: str | None = None,
        *,
        limit: int | None = None,
        sort_by_date: str | None = None,
    ):
        calls.append((layer, domain, ticker, limit, sort_by_date))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/market?ticker=AAPL")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("silver", "market", "AAPL", None, None)]


@pytest.mark.asyncio
async def test_data_endpoint_rejects_unknown_layer():
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/platinum/market?ticker=AAPL")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_bronze_endpoint_allows_missing_ticker_for_generic_domains(monkeypatch):
    calls = []

    def fake_get_data(
        layer: str,
        domain: str,
        ticker: str | None = None,
        *,
        limit: int | None = None,
        sort_by_date: str | None = None,
    ):
        calls.append((layer, domain, ticker, limit, sort_by_date))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/bronze/market")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("bronze", "market", None, None, None)]


@pytest.mark.asyncio
async def test_finance_endpoint_calls_service(monkeypatch):
    calls = []

    def fake_get_finance_data(layer: str, sub_domain: str, ticker: str | None = None):
        calls.append((layer, sub_domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_finance_data", fake_get_finance_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/finance/balance_sheet?ticker=AAPL")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("silver", "balance_sheet", "AAPL")]


@pytest.mark.asyncio
async def test_bronze_finance_endpoint_allows_missing_ticker(monkeypatch):
    calls = []

    def fake_get_finance_data(layer: str, sub_domain: str, ticker: str | None = None):
        calls.append((layer, sub_domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_finance_data", fake_get_finance_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/bronze/finance/balance_sheet")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("bronze", "balance_sheet", None)]


@pytest.mark.asyncio
async def test_data_endpoint_rejects_invalid_ticker():
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/market?ticker=BAD/TICKER")

    assert resp.status_code == 400
    assert "Invalid ticker format" in resp.text


@pytest.mark.asyncio
async def test_data_endpoint_normalizes_ticker_to_uppercase(monkeypatch):
    calls = []

    def fake_get_data(
        layer: str,
        domain: str,
        ticker: str | None = None,
        *,
        limit: int | None = None,
        sort_by_date: str | None = None,
    ):
        calls.append((layer, domain, ticker, limit, sort_by_date))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/market?ticker=aapl")

    assert resp.status_code == 200
    assert calls == [("silver", "market", "AAPL", None, None)]


@pytest.mark.asyncio
async def test_data_endpoint_forwards_date_sort_and_limit(monkeypatch):
    calls = []

    def fake_get_data(
        layer: str,
        domain: str,
        ticker: str | None = None,
        *,
        limit: int | None = None,
        sort_by_date: str | None = None,
    ):
        calls.append((layer, domain, ticker, limit, sort_by_date))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/market?ticker=AAPL&limit=25&date_sort=desc")

    assert resp.status_code == 200
    assert calls == [("silver", "market", "AAPL", 25, "desc")]


@pytest.mark.asyncio
async def test_data_endpoint_rejects_invalid_date_sort():
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/market?date_sort=latest")

    assert resp.status_code == 400
    assert "date_sort must be 'asc' or 'desc'" in resp.text


@pytest.mark.asyncio
async def test_validation_endpoint_calls_service_with_normalized_ticker(monkeypatch):
    calls = []

    def fake_get_validation_report(layer: str, domain: str, ticker: str | None = None):
        calls.append((layer, domain, ticker))
        return {
            "layer": layer,
            "domain": domain,
            "status": "healthy",
            "rowCount": 1,
            "columns": [],
            "timestamp": "2026-02-08T00:00:00Z",
        }

    monkeypatch.setattr(
        data_endpoints.ValidationService, "get_validation_report", fake_get_validation_report
    )

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/quality/silver/market/validation?ticker=msft")

    assert resp.status_code == 200
    assert calls == [("silver", "market", "MSFT")]


@pytest.mark.asyncio
async def test_validation_endpoint_rejects_invalid_ticker():
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/quality/silver/market/validation?ticker=BAD/TICKER")

    assert resp.status_code == 400
    assert "Invalid ticker format" in resp.text


@pytest.mark.asyncio
async def test_adls_file_preview_endpoint_forwards_max_delta_files(monkeypatch):
    calls = []

    def fake_get_adls_file_preview(*, layer: str, path: str, max_bytes: int | None = None, max_delta_files: int | None = None):
        calls.append((layer, path, max_bytes, max_delta_files))
        return {
            "layer": layer,
            "container": "gold",
            "path": path,
            "isPlainText": False,
            "encoding": None,
            "truncated": False,
            "maxBytes": max_bytes or 262144,
            "contentType": "application/x-delta-table-preview",
            "contentPreview": None,
            "previewMode": "delta-table",
            "processedDeltaFiles": None,
            "maxDeltaFiles": max_delta_files,
            "deltaLogPath": "market/buckets/A/_delta_log/",
            "tableColumns": ["symbol", "close"],
            "tableRows": [{"symbol": "AAPL", "close": 101.25}],
            "tableRowCount": 1,
            "tablePreviewLimit": 100,
            "tableTruncated": False,
            "resolvedTablePath": "market/buckets/A",
            "tableVersion": None,
        }

    monkeypatch.setattr(data_endpoints.DataService, "get_adls_file_preview", fake_get_adls_file_preview)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/data/adls/file-preview",
            params={
                "layer": "gold",
                "path": "market/buckets/A/part-00000.snappy.parquet",
                "max_bytes": 262144,
                "max_delta_files": 9,
            },
        )

    assert resp.status_code == 200
    assert calls == [("gold", "market/buckets/A/part-00000.snappy.parquet", 262144, 9)]
