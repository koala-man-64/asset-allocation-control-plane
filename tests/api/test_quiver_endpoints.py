from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from api.service.quiver_gateway import QuiverGateway, get_current_caller_context
from quiver_provider.errors import QuiverRateLimitError
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_quiver_live_congress_trading_returns_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_live(self, *, normalized=None, representative=None):
        assert normalized is True
        assert representative == "Pelosi"
        return [{"Ticker": "AAPL"}]

    monkeypatch.setattr(QuiverGateway, "get_live_congress_trading", fake_live)
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/quiver/live/congress-trading?normalized=true&representative=Pelosi")

    assert resp.status_code == 200
    assert resp.json() == [{"Ticker": "AAPL"}]


@pytest.mark.asyncio
async def test_quiver_invalid_boolean_maps_to_400(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/quiver/live/senate-trading?options=yes")

    assert resp.status_code == 400
    assert "options must be 'true' or 'false'" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_quiver_historical_gov_contracts_upcases_ticker(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_historical(self, *, ticker):
        assert ticker == "PLTR"
        return [{"Ticker": ticker}]

    monkeypatch.setattr(QuiverGateway, "get_historical_gov_contracts", fake_historical)
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/quiver/historical/gov-contracts/pltr")

    assert resp.status_code == 200
    assert resp.json()[0]["Ticker"] == "PLTR"


@pytest.mark.asyncio
async def test_quiver_sec13f_changes_passes_strict_params(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_live(
        self,
        *,
        ticker=None,
        owner=None,
        date=None,
        period=None,
        today=None,
        most_recent=None,
        show_new_funds=None,
        mobile=None,
        page=None,
        page_size=None,
    ):
        assert ticker == "NVDA"
        assert owner == "Vanguard"
        assert date == "2026-03-31"
        assert period == "2025-12-31"
        assert today is False
        assert most_recent is True
        assert show_new_funds is False
        assert mobile is True
        assert page == 2
        assert page_size == 100
        return [{"Ticker": ticker}]

    monkeypatch.setattr(QuiverGateway, "get_live_sec13f_changes", fake_live)
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/providers/quiver/live/sec13f-changes"
            "?ticker=nvda&owner=Vanguard&date=2026-03-31&period=2025-12-31"
            "&today=false&most_recent=true&show_new_funds=false&mobile=true&page=2&page_size=100"
        )

    assert resp.status_code == 200
    assert resp.json()[0]["Ticker"] == "NVDA"


@pytest.mark.asyncio
async def test_quiver_insiders_maps_rate_limit_to_429(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_live(self, **_kwargs):
        raise QuiverRateLimitError("rate limited")

    monkeypatch.setattr(QuiverGateway, "get_live_insiders", fake_live)
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/quiver/live/insiders?ticker=AAPL")

    assert resp.status_code == 429


@pytest.mark.asyncio
async def test_quiver_routes_set_caller_context(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: dict[str, str] = {}

    def fake_live(self):
        caller_job, caller_execution = get_current_caller_context()
        observed["job"] = caller_job
        observed["execution"] = caller_execution
        return [{"Politician": "Test User"}]

    monkeypatch.setattr(QuiverGateway, "get_live_congress_holdings", fake_live)
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/providers/quiver/live/congress-holdings",
            headers={"X-Caller-Job": "bronze-quiver-job", "X-Caller-Execution": "exec-123"},
        )

    assert resp.status_code == 200
    assert observed["job"] == "bronze-quiver-job"
    assert observed["execution"] == "exec-123"


@pytest.mark.asyncio
async def test_quiver_missing_required_roles_maps_to_403(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("QUIVER_ENABLED", "true")
    monkeypatch.setenv("QUIVER_API_KEY", "quiver-key")
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
