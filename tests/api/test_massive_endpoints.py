import pytest

from api.service.app import create_app
from api.service.massive_gateway import MassiveError, MassiveGateway, MassiveNotConfiguredError, get_current_caller_context
from massive_provider.errors import MassiveNotFoundError, MassiveRateLimitError
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_massive_daily_time_series_returns_csv(monkeypatch):
    def fake_daily(self, *, symbol, from_date=None, to_date=None, adjusted=True):
        assert symbol == "AAPL"
        assert from_date == "2025-01-01"
        assert to_date == "2025-01-10"
        assert adjusted is True
        return "Date,Open,High,Low,Close,Volume\n2025-01-02,10,11,9,10.5,100\n"

    monkeypatch.setattr(MassiveGateway, "get_daily_time_series_csv", fake_daily)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/time-series/daily?symbol=AAPL&from=2025-01-01&to=2025-01-10")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert resp.text.splitlines()[0] == "Date,Open,High,Low,Close,Volume"


@pytest.mark.asyncio
async def test_massive_daily_time_series_maps_rate_limit_to_429(monkeypatch):
    def fake_daily(self, *, symbol, from_date=None, to_date=None, adjusted=True):
        raise MassiveRateLimitError("rate limited")

    monkeypatch.setattr(MassiveGateway, "get_daily_time_series_csv", fake_daily)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/time-series/daily?symbol=AAPL")

    assert resp.status_code == 429


@pytest.mark.asyncio
async def test_massive_market_history_returns_json(monkeypatch):
    def fake_market_history(self, *, symbol, from_date=None, to_date=None):
        assert symbol == "AAPL"
        assert from_date == "1970-01-01"
        assert to_date == "2025-01-10"
        return {"symbol": symbol, "status": "ok", "rows": [{"date": "2025-01-10"}]}

    monkeypatch.setattr(MassiveGateway, "get_market_history", fake_market_history)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/market-history?symbol=AAPL&from=1970-01-01&to=2025-01-10")

    assert resp.status_code == 200
    assert resp.json() == {"symbol": "AAPL", "status": "ok", "rows": [{"date": "2025-01-10"}]}


@pytest.mark.asyncio
async def test_massive_short_interest_returns_json(monkeypatch):
    def fake_short_interest(self, *, symbol):
        assert symbol == "AAPL"
        return {"results": [{"ticker": symbol}]}

    monkeypatch.setattr(MassiveGateway, "get_short_interest", fake_short_interest)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/fundamentals/short-interest?symbol=AAPL")

    assert resp.status_code == 200
    assert resp.json()["results"][0]["ticker"] == "AAPL"


@pytest.mark.asyncio
async def test_massive_unified_snapshot_returns_json(monkeypatch):
    def fake_snapshot(self, *, symbols, asset_type="stocks"):
        assert symbols == ["AAPL", "MSFT", "TSLA"]
        assert asset_type == "stocks"
        return {"results": [{"ticker": "AAPL"}, {"ticker": "MSFT"}, {"ticker": "TSLA"}]}

    monkeypatch.setattr(MassiveGateway, "get_unified_snapshot", fake_snapshot)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/snapshot?symbols=AAPL,msft,TSLA&type=stocks")

    assert resp.status_code == 200
    assert [row["ticker"] for row in resp.json()["results"]] == ["AAPL", "MSFT", "TSLA"]


@pytest.mark.asyncio
async def test_massive_tickers_returns_json(monkeypatch):
    def fake_tickers(self, *, market="stocks", locale="us", active=True):
        assert market == "stocks"
        assert locale == "us"
        assert active is True
        return [{"Symbol": "AAPL"}, {"Symbol": "^VIX"}]

    monkeypatch.setattr(MassiveGateway, "get_tickers", fake_tickers)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/tickers?market=stocks&locale=us&active=true")

    assert resp.status_code == 200
    assert resp.json()["results"] == [{"Symbol": "AAPL"}, {"Symbol": "^VIX"}]


@pytest.mark.asyncio
async def test_massive_financials_returns_json(monkeypatch):
    def fake_financials(self, *, symbol, report, timeframe=None, sort=None, limit=None, pagination=True):
        assert symbol == "AAPL"
        assert report == "balance_sheet"
        assert timeframe is None
        assert sort is None
        assert limit is None
        assert pagination is True
        return {"results": [{"ticker": symbol}]}

    monkeypatch.setattr(MassiveGateway, "get_finance_report", fake_financials)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/financials/balance_sheet?symbol=AAPL")

    assert resp.status_code == 200
    assert resp.json()["results"][0]["ticker"] == "AAPL"


@pytest.mark.asyncio
async def test_massive_ratios_returns_json(monkeypatch):
    def fake_financials(self, *, symbol, report, timeframe=None, sort=None, limit=None, pagination=True):
        assert symbol == "AAPL"
        assert report == "valuation"
        assert timeframe is None
        assert sort == "market_cap.desc"
        assert limit == 1
        assert pagination is False
        return {"results": [{"ticker": symbol}]}

    monkeypatch.setattr(MassiveGateway, "get_finance_report", fake_financials)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/providers/massive/fundamentals/ratios?symbol=AAPL&sort=market_cap.desc&limit=1&pagination=false"
        )

    assert resp.status_code == 200
    assert resp.json()["results"][0]["ticker"] == "AAPL"


@pytest.mark.asyncio
async def test_massive_missing_symbol_maps_to_404(monkeypatch):
    def fake_financials(self, *, symbol, report, timeframe=None, sort=None, limit=None, pagination=True):
        del symbol, report, timeframe, sort, limit, pagination
        raise MassiveNotFoundError("not found")

    monkeypatch.setattr(MassiveGateway, "get_finance_report", fake_financials)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/financials/balance_sheet?symbol=BAD")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_massive_provider_bad_request_maps_to_400(monkeypatch):
    def fake_financials(self, *, symbol, report, timeframe=None, sort=None, limit=None, pagination=True):
        del symbol, report, timeframe, sort, limit, pagination
        raise MassiveError("invalid query parameter", status_code=400, detail="invalid query parameter")

    monkeypatch.setattr(MassiveGateway, "get_finance_report", fake_financials)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/financials/balance_sheet?symbol=AAPL")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_massive_not_configured_maps_to_503(monkeypatch):
    def fake_short_interest(self, *, symbol):
        raise MassiveNotConfiguredError("MASSIVE_API_KEY is missing.")

    monkeypatch.setattr(MassiveGateway, "get_short_interest", fake_short_interest)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/providers/massive/fundamentals/short-interest?symbol=AAPL")

    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_massive_routes_set_caller_context(monkeypatch):
    observed: dict[str, str] = {}

    def fake_short_volume(self, *, symbol):
        caller_job, caller_execution = get_current_caller_context()
        observed["job"] = caller_job
        observed["execution"] = caller_execution
        return {"results": [{"ticker": symbol}]}

    monkeypatch.setattr(MassiveGateway, "get_short_volume", fake_short_volume)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/providers/massive/fundamentals/short-volume?symbol=AAPL",
            headers={"X-Caller-Job": "bronze-market-job", "X-Caller-Execution": "exec-123"},
        )

    assert resp.status_code == 200
    assert observed["job"] == "bronze-market-job"
    assert observed["execution"] == "exec-123"
