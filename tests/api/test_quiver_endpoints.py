from __future__ import annotations

import logging

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from api.service.quiver_gateway import QuiverGateway
from quiver_provider.errors import (
    QuiverAuthError,
    QuiverEntitlementError,
    QuiverInvalidRequestError,
    QuiverNotConfiguredError,
    QuiverNotFoundError,
    QuiverProtocolError,
    QuiverRateLimitError,
    QuiverTimeoutError,
    QuiverUnavailableError,
)
from tests.api._client import get_test_client


def _configure_quiver_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")


_ROUTE_CASES = [
    pytest.param(
        "/api/providers/quiver/live/congress-trading?normalized=true&representative=Pelosi",
        "get_live_congress_trading",
        {"normalized": True, "representative": "Pelosi"},
        id="live-congress-trading",
    ),
    pytest.param(
        "/api/providers/quiver/historical/congress-trading/aapl?analyst=Jane%20Doe",
        "get_historical_congress_trading",
        {"ticker": "AAPL", "analyst": "Jane Doe"},
        id="historical-congress-trading",
    ),
    pytest.param(
        "/api/providers/quiver/live/senate-trading?name=Smith&options=false",
        "get_live_senate_trading",
        {"name": "Smith", "options": False},
        id="live-senate-trading",
    ),
    pytest.param(
        "/api/providers/quiver/historical/senate-trading/msft",
        "get_historical_senate_trading",
        {"ticker": "MSFT"},
        id="historical-senate-trading",
    ),
    pytest.param(
        "/api/providers/quiver/live/house-trading?name=Pelosi&options=true",
        "get_live_house_trading",
        {"name": "Pelosi", "options": True},
        id="live-house-trading",
    ),
    pytest.param(
        "/api/providers/quiver/historical/house-trading/nvda",
        "get_historical_house_trading",
        {"ticker": "NVDA"},
        id="historical-house-trading",
    ),
    pytest.param(
        "/api/providers/quiver/live/gov-contracts",
        "get_live_gov_contracts",
        {},
        id="live-gov-contracts",
    ),
    pytest.param(
        "/api/providers/quiver/historical/gov-contracts/pltr",
        "get_historical_gov_contracts",
        {"ticker": "PLTR"},
        id="historical-gov-contracts",
    ),
    pytest.param(
        "/api/providers/quiver/live/gov-contracts-all?date=2026-03-31&page=2&page_size=50",
        "get_live_gov_contracts_all",
        {"date": "2026-03-31", "page": 2, "page_size": 50},
        id="live-gov-contracts-all",
    ),
    pytest.param(
        "/api/providers/quiver/historical/gov-contracts-all/lmt",
        "get_historical_gov_contracts_all",
        {"ticker": "LMT"},
        id="historical-gov-contracts-all",
    ),
    pytest.param(
        "/api/providers/quiver/live/insiders"
        "?ticker=aapl&date=2026-03-31&uploaded=2026-04-01&limit_codes=true&page=3&page_size=25",
        "get_live_insiders",
        {
            "ticker": "AAPL",
            "date": "2026-03-31",
            "uploaded": "2026-04-01",
            "limit_codes": True,
            "page": 3,
            "page_size": 25,
        },
        id="live-insiders",
    ),
    pytest.param(
        "/api/providers/quiver/live/sec13f"
        "?ticker=nvda&owner=Vanguard&date=2026-03-31&period=2025-12-31&today=false&page=2&page_size=100",
        "get_live_sec13f",
        {
            "ticker": "NVDA",
            "owner": "Vanguard",
            "date": "2026-03-31",
            "period": "2025-12-31",
            "today": False,
            "page": 2,
            "page_size": 100,
        },
        id="live-sec13f",
    ),
    pytest.param(
        "/api/providers/quiver/live/sec13f-changes"
        "?ticker=nvda&owner=Vanguard&date=2026-03-31&period=2025-12-31"
        "&today=false&most_recent=true&show_new_funds=false&mobile=true&page=2&page_size=100",
        "get_live_sec13f_changes",
        {
            "ticker": "NVDA",
            "owner": "Vanguard",
            "date": "2026-03-31",
            "period": "2025-12-31",
            "today": False,
            "most_recent": True,
            "show_new_funds": False,
            "mobile": True,
            "page": 2,
            "page_size": 100,
        },
        id="live-sec13f-changes",
    ),
    pytest.param(
        "/api/providers/quiver/live/lobbying?all=true&date_from=2026-01-01&date_to=2026-03-31&page=2&page_size=30",
        "get_live_lobbying",
        {
            "all_records": True,
            "date_from": "2026-01-01",
            "date_to": "2026-03-31",
            "page": 2,
            "page_size": 30,
        },
        id="live-lobbying",
    ),
    pytest.param(
        "/api/providers/quiver/historical/lobbying/aapl?page=2&page_size=25&query=chips&queryTicker=msft",
        "get_historical_lobbying",
        {
            "ticker": "AAPL",
            "page": 2,
            "page_size": 25,
            "query": "chips",
            "query_ticker": "MSFT",
        },
        id="historical-lobbying",
    ),
    pytest.param(
        "/api/providers/quiver/live/etf-holdings?etf=spy&ticker=nvda",
        "get_live_etf_holdings",
        {"etf": "SPY", "ticker": "NVDA"},
        id="live-etf-holdings",
    ),
    pytest.param(
        "/api/providers/quiver/live/congress-holdings",
        "get_live_congress_holdings",
        {},
        id="live-congress-holdings",
    ),
]

_ERROR_CASES = [
    pytest.param(QuiverNotConfiguredError("not configured"), 503, "not configured", id="not-configured"),
    pytest.param(QuiverInvalidRequestError("bad request"), 400, "bad request", id="invalid-request"),
    pytest.param(QuiverRateLimitError("rate limited"), 429, "rate limited", id="rate-limit"),
    pytest.param(QuiverNotFoundError("missing"), 404, "missing", id="not-found"),
    pytest.param(QuiverTimeoutError("timed out"), 504, "timed out", id="timeout"),
    pytest.param(QuiverAuthError("auth failed"), 502, "auth failed", id="auth"),
    pytest.param(QuiverEntitlementError("entitlement failed"), 502, "entitlement failed", id="entitlement"),
    pytest.param(QuiverProtocolError("protocol failed"), 502, "protocol failed", id="protocol"),
    pytest.param(QuiverUnavailableError("upstream unavailable"), 503, "upstream unavailable", id="unavailable"),
    pytest.param(RuntimeError("boom"), 500, "Unexpected error: RuntimeError: boom", id="unexpected"),
]

_VALIDATION_CASES = [
    pytest.param(
        "/api/providers/quiver/live/senate-trading?options=yes",
        400,
        "options must be 'true' or 'false'",
        id="invalid-boolean",
    ),
    pytest.param(
        "/api/providers/quiver/live/gov-contracts-all?page_size=999",
        400,
        "page_size must be between 1 and 500",
        id="invalid-page-size",
    ),
    pytest.param(
        "/api/providers/quiver/live/lobbying?date_from=2026-04-01&date_to=2026-03-01",
        400,
        "'date_from' must be <= 'date_to'.",
        id="invalid-date-range",
    ),
    pytest.param(
        "/api/providers/quiver/live/sec13f?date=2026/03/31",
        400,
        "Invalid date='2026/03/31'",
        id="invalid-date-format",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("path", "method_name", "expected_kwargs"), _ROUTE_CASES)
async def test_quiver_routes_map_to_gateway_methods(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    method_name: str,
    expected_kwargs: dict[str, object],
) -> None:
    observed: list[dict[str, object]] = []

    def fake_gateway_method(self, **kwargs):
        observed.append(dict(kwargs))
        return [{"method": method_name}]

    monkeypatch.setattr(QuiverGateway, method_name, fake_gateway_method)
    _configure_quiver_env(monkeypatch)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(path)

    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "no-store"
    assert resp.json() == [{"method": method_name}]
    assert observed == [expected_kwargs]


@pytest.mark.asyncio
@pytest.mark.parametrize(("path", "expected_status", "expected_detail"), _VALIDATION_CASES)
async def test_quiver_routes_reject_invalid_inputs(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    expected_status: int,
    expected_detail: str,
) -> None:
    _configure_quiver_env(monkeypatch)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(path)

    assert resp.status_code == expected_status
    assert expected_detail in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize(("error", "expected_status", "expected_detail"), _ERROR_CASES)
async def test_quiver_routes_map_provider_errors_to_http_status(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_status: int,
    expected_detail: str,
) -> None:
    def fake_gateway_method(self):
        raise error

    monkeypatch.setattr(QuiverGateway, "get_live_congress_holdings", fake_gateway_method)
    _configure_quiver_env(monkeypatch)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/quiver/live/congress-holdings")

    assert resp.status_code == expected_status
    assert expected_detail in resp.json()["detail"]


@pytest.mark.asyncio
async def test_quiver_gateway_logs_caller_context_from_route_headers(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FakeClient:
        def get_json(self, path, params=None):
            assert path == "/beta/live/congressholdings"
            assert params is None
            return [{"Politician": "Test User"}]

    monkeypatch.setattr(QuiverGateway, "get_client", lambda self: _FakeClient())
    _configure_quiver_env(monkeypatch)

    app = create_app()
    with caplog.at_level(logging.INFO, logger="asset-allocation.api.quiver"):
        async with get_test_client(app) as client:
            resp = await client.get(
                "/api/providers/quiver/live/congress-holdings",
                headers={"X-Caller-Job": "bronze-quiver-job", "X-Caller-Execution": "exec-123"},
            )

    assert resp.status_code == 200
    assert "caller_job=bronze-quiver-job" in caplog.text
    assert "caller_execution=exec-123" in caplog.text
    assert "path=/beta/live/congressholdings" in caplog.text
    assert "status=success" in caplog.text


def test_quiver_gateway_uses_seeded_service_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_quiver_env(monkeypatch)
    monkeypatch.setenv("QUIVER_TIMEOUT_SECONDS", "45")
    app = create_app()

    monkeypatch.setenv("QUIVER_TIMEOUT_SECONDS", "not-a-number")
    snapshot, config = app.state.quiver_gateway._build_snapshot()

    assert config.timeout_seconds == pytest.approx(45.0)
    assert snapshot.timeout_seconds == pytest.approx(45.0)


@pytest.mark.asyncio
async def test_quiver_missing_required_roles_maps_to_403(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_quiver_env(monkeypatch)
    monkeypatch.setenv("QUIVER_REQUIRED_ROLES", "AssetAllocation.Quiver.Read")

    class _FakeAuth:
        def authenticate_headers(self, _headers):
            return AuthContext(mode="oidc", subject="user-1", claims={"roles": []})

    app = create_app()
    app.state.auth = _FakeAuth()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/providers/quiver/live/congress-holdings",
            headers={"Authorization": "Bearer ignored"},
        )

    assert resp.status_code == 403
    assert "Missing required roles" in resp.json()["detail"]
