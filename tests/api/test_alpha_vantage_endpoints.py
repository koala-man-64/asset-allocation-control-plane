import pytest

from alpha_vantage import AlphaVantageInvalidSymbolError, AlphaVantageThrottleError
from api.service.alpha_vantage_gateway import AlphaVantageGateway, get_current_caller_context
from api.service.app import create_app
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_alpha_vantage_listing_status_returns_csv(monkeypatch):
    def fake_listing(self, *, state="active", date=None):
        assert state == "active"
        assert date is None
        return "symbol,name,exchange,assetType,ipoDate,delistingDate,status\nAAPL,Apple,NASDAQ,Stock,1980-12-12,null,Active\n"

    monkeypatch.setattr(AlphaVantageGateway, "get_listing_status_csv", fake_listing)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/alpha-vantage/listing-status?state=active")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "AAPL" in resp.text


@pytest.mark.asyncio
async def test_alpha_vantage_daily_time_series_returns_csv(monkeypatch):
    def fake_daily(self, *, symbol, outputsize="compact", adjusted=False):
        assert symbol == "AAPL"
        assert outputsize == "compact"
        assert adjusted is False
        return "timestamp,open,high,low,close,volume\n2024-01-02,10,11,9,10.5,100\n"

    monkeypatch.setattr(AlphaVantageGateway, "get_daily_time_series_csv", fake_daily)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/alpha-vantage/time-series/daily?symbol=AAPL&outputsize=compact")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "timestamp" in resp.text


@pytest.mark.asyncio
async def test_alpha_vantage_daily_time_series_maps_throttle_to_429(monkeypatch):
    def fake_daily(self, *, symbol, outputsize="compact", adjusted=False):
        raise AlphaVantageThrottleError("throttled")

    monkeypatch.setattr(AlphaVantageGateway, "get_daily_time_series_csv", fake_daily)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/alpha-vantage/time-series/daily?symbol=AAPL")

    assert resp.status_code == 429


@pytest.mark.asyncio
async def test_alpha_vantage_earnings_returns_json(monkeypatch):
    def fake_earnings(self, *, symbol):
        assert symbol == "AAPL"
        return {"symbol": symbol, "quarterlyEarnings": [{"fiscalDateEnding": "2024-01-01"}]}

    monkeypatch.setattr(AlphaVantageGateway, "get_earnings", fake_earnings)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/alpha-vantage/earnings?symbol=AAPL")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_alpha_vantage_earnings_calendar_returns_csv(monkeypatch):
    def fake_calendar(self, *, symbol=None, horizon="12month"):
        assert symbol == "AAPL"
        assert horizon == "6month"
        return "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\nAAPL,Apple,2026-05-01,2026-03-31,1.5,USD,post-market\n"

    monkeypatch.setattr(AlphaVantageGateway, "get_earnings_calendar_csv", fake_calendar)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/alpha-vantage/earnings-calendar?symbol=AAPL&horizon=6month")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "reportDate" in resp.text

@pytest.mark.asyncio
async def test_alpha_vantage_invalid_symbol_maps_to_404(monkeypatch):
    def fake_earnings(self, *, symbol):
        raise AlphaVantageInvalidSymbolError("invalid")

    monkeypatch.setattr(AlphaVantageGateway, "get_earnings", fake_earnings)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/alpha-vantage/earnings?symbol=BAD")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_alpha_vantage_routes_set_caller_context(monkeypatch):
    observed: dict[str, str] = {}

    def fake_earnings(self, *, symbol):
        caller_job, caller_execution = get_current_caller_context()
        observed["job"] = caller_job
        observed["execution"] = caller_execution
        return {"symbol": symbol}

    monkeypatch.setattr(AlphaVantageGateway, "get_earnings", fake_earnings)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/providers/alpha-vantage/earnings?symbol=AAPL",
            headers={"X-Caller-Job": "bronze-market-job", "X-Caller-Execution": "exec-123"},
        )

    assert resp.status_code == 200
    assert observed["job"] == "bronze-market-job"
    assert observed["execution"] == "exec-123"
