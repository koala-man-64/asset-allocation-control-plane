from api.service.massive_gateway import MassiveGateway
from core.market_history_contract import MARKET_HISTORY_START_DATE, MARKET_HISTORY_STATUS_NO_HISTORY, MARKET_HISTORY_STATUS_OK
from massive_provider.errors import MassiveNotFoundError


def test_daily_time_series_uses_open_close_for_single_day() -> None:
    calls = {"summary": 0, "aggs": 0}

    class _FakeClient:
        def get_daily_ticker_summary(self, *, ticker, date, adjusted=True):
            calls["summary"] += 1
            assert ticker == "AAPL"
            assert date == "2026-02-09"
            assert adjusted is False
            return {
                "symbol": "AAPL",
                "from": "2026-02-09",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1234,
            }

        def list_ohlcv(self, **kwargs):
            calls["aggs"] += 1
            return []

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(
        symbol="AAPL",
        from_date="2026-02-09",
        to_date="2026-02-09",
        adjusted=False,
    )

    lines = csv_text.strip().splitlines()
    assert lines[0] == "Date,Open,High,Low,Close,Volume"
    assert lines[1] == "2026-02-09,10.0,11.0,9.0,10.5,1234.0"
    assert calls["summary"] == 1
    assert calls["aggs"] == 0


def test_daily_time_series_falls_back_to_aggs_when_open_close_not_found() -> None:
    calls = {"summary": 0, "aggs": 0}

    class _FakeClient:
        def get_daily_ticker_summary(self, *, ticker, date, adjusted=True):
            calls["summary"] += 1
            raise MassiveNotFoundError("not found")

        def list_ohlcv(self, **kwargs):
            calls["aggs"] += 1
            return [{"t": 1735776000000, "o": 10.0, "h": 11.0, "l": 9.0, "c": 10.5, "v": 1234}]

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(
        symbol="AAPL",
        from_date="2025-01-02",
        to_date="2025-01-02",
        adjusted=True,
    )

    lines = csv_text.strip().splitlines()
    assert lines[0] == "Date,Open,High,Low,Close,Volume"
    assert lines[1] == "2025-01-02,10.0,11.0,9.0,10.5,1234.0"
    assert calls["summary"] == 1
    assert calls["aggs"] == 1


def test_gateway_fundamentals_request_historical_defaults() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        def get_short_interest(self, **kwargs):
            self.calls.append(("short_interest", kwargs))
            return {"results": []}

        def get_short_volume(self, **kwargs):
            self.calls.append(("short_volume", kwargs))
            return {"results": []}

        def get_float(self, **kwargs):
            self.calls.append(("float", kwargs))
            return {"results": []}

    fake = _FakeClient()
    gateway = MassiveGateway()
    gateway.get_client = lambda: fake  # type: ignore[method-assign]

    gateway.get_short_interest(symbol="AAPL")
    gateway.get_short_volume(symbol="AAPL")
    gateway.get_float(symbol="AAPL")

    by_name = {name: kwargs for name, kwargs in fake.calls}
    assert by_name["short_interest"]["ticker"] == "AAPL"
    assert by_name["short_interest"]["params"]["sort"] == "settlement_date.asc"
    assert by_name["short_interest"]["params"]["limit"] == 50000
    assert by_name["short_interest"]["pagination"] is True

    assert by_name["short_volume"]["ticker"] == "AAPL"
    assert by_name["short_volume"]["params"]["sort"] == "date.asc"
    assert by_name["short_volume"]["params"]["limit"] == 50000
    assert by_name["short_volume"]["pagination"] is True

    assert by_name["float"]["ticker"] == "AAPL"
    assert by_name["float"]["params"]["sort"] == "effective_date.asc"
    assert by_name["float"]["params"]["limit"] == 5000
    assert by_name["float"]["pagination"] is True


def test_daily_time_series_defaults_to_full_history_window() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def list_ohlcv(self, **kwargs):
            captured.update(kwargs)
            return []

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(symbol="AAPL")
    assert csv_text.splitlines()[0] == "Date,Open,High,Low,Close,Volume"
    assert captured["from_"] == "1970-01-01"
    assert captured["ticker"] == "AAPL"


def test_market_history_defaults_to_2016_floor_and_merges_supplementals() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def list_ohlcv(self, **kwargs):
            captured["ohlcv"] = kwargs
            return [
                {"t": 1704153600000, "o": 10.0, "h": 11.0, "l": 9.0, "c": 10.5, "v": 100.0},
                {"t": 1704240000000, "o": 11.0, "h": 12.0, "l": 10.0, "c": 11.5, "v": 110.0},
            ]

        def get_short_interest(self, **kwargs):
            captured["short_interest"] = kwargs
            return {"results": [{"settlement_date": "2024-01-02", "short_interest": 1000.0}]}

        def get_short_volume(self, **kwargs):
            captured["short_volume"] = kwargs
            return {"results": [{"date": "2024-01-03", "short_volume": 500.0}]}

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_market_history(symbol="AAPL")

    assert payload["status"] == MARKET_HISTORY_STATUS_OK
    assert captured["ohlcv"]["from_"] == MARKET_HISTORY_START_DATE
    assert captured["short_interest"]["params"]["settlement_date.gte"] == MARKET_HISTORY_START_DATE
    assert captured["short_volume"]["params"]["date.gte"] == MARKET_HISTORY_START_DATE
    assert payload["rows"][0]["short_interest"] == 1000.0
    assert payload["rows"][0]["short_volume"] is None
    assert payload["rows"][1]["short_interest"] == 1000.0
    assert payload["rows"][1]["short_volume"] == 500.0


def test_market_history_clamps_earlier_from_date_to_2016_floor() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def list_ohlcv(self, **kwargs):
            captured["ohlcv"] = kwargs
            return []

        def get_short_interest(self, **kwargs):
            raise AssertionError("short interest should not be fetched when there are no daily rows")

        def get_short_volume(self, **kwargs):
            raise AssertionError("short volume should not be fetched when there are no daily rows")

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_market_history(symbol="AAPL", from_date="1970-01-01", to_date="2024-01-10")

    assert payload["status"] == MARKET_HISTORY_STATUS_NO_HISTORY
    assert captured["ohlcv"]["from_"] == MARKET_HISTORY_START_DATE


def test_market_history_preserves_later_from_date() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def list_ohlcv(self, **kwargs):
            captured["ohlcv"] = kwargs
            return []

        def get_short_interest(self, **kwargs):
            raise AssertionError("short interest should not be fetched when there are no daily rows")

        def get_short_volume(self, **kwargs):
            raise AssertionError("short volume should not be fetched when there are no daily rows")

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_market_history(symbol="AAPL", from_date="2020-01-01", to_date="2024-01-10")

    assert payload["status"] == MARKET_HISTORY_STATUS_NO_HISTORY
    assert captured["ohlcv"]["from_"] == "2020-01-01"


def test_market_history_returns_no_history_without_provider_calls_when_window_precedes_floor() -> None:
    class _FakeClient:
        def list_ohlcv(self, **kwargs):
            raise AssertionError("ohlcv should not be fetched when the clamped window is empty")

        def get_short_interest(self, **kwargs):
            raise AssertionError("short interest should not be fetched when the clamped window is empty")

        def get_short_volume(self, **kwargs):
            raise AssertionError("short volume should not be fetched when the clamped window is empty")

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_market_history(symbol="AAPL", from_date="2010-01-01", to_date="2015-12-31")

    assert payload == {"symbol": "AAPL", "status": MARKET_HISTORY_STATUS_NO_HISTORY, "rows": []}


def test_gateway_unified_snapshot_batches_symbols() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def get_unified_snapshot(self, **kwargs):
            captured.update(kwargs)
            return {"results": [{"ticker": "AAPL"}]}

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_unified_snapshot(symbols=["aapl", "MSFT", "AAPL"], asset_type="stocks")
    assert payload["results"][0]["ticker"] == "AAPL"
    assert captured["tickers"] == ["AAPL", "MSFT", "AAPL"]
    assert captured["asset_type"] == "stocks"
    assert captured["limit"] == 250


def test_daily_time_series_maps_regime_index_symbols_to_provider_aliases() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def get_daily_ticker_summary(self, *, ticker, date, adjusted=True):
            captured["ticker"] = ticker
            captured["date"] = date
            captured["adjusted"] = adjusted
            return {
                "symbol": ticker,
                "from": date,
                "open": 20.0,
                "high": 21.0,
                "low": 19.0,
                "close": 20.5,
                "volume": 100.0,
            }

        def list_ohlcv(self, **kwargs):
            raise AssertionError("list_ohlcv should not be called for the single-day summary path")

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(
        symbol="^VIX",
        from_date="2026-03-09",
        to_date="2026-03-09",
        adjusted=True,
    )

    assert "2026-03-09,20.0,21.0,19.0,20.5,100.0" in csv_text
    assert captured["ticker"] == "I:VIX"
    assert captured["date"] == "2026-03-09"
    assert captured["adjusted"] is True


def test_gateway_unified_snapshot_rewrites_provider_aliases_to_canonical_symbols() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def get_unified_snapshot(self, **kwargs):
            captured.update(kwargs)
            return {
                "results": [
                    {"ticker": "I:VIX", "value": 21.5},
                    {"ticker": "I:VIX3M", "value": 22.1},
                    {"ticker": "SPY", "value": 580.0},
                ]
            }

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_unified_snapshot(symbols=["^VIX", "^VIX3M", "SPY"], asset_type="stocks")

    assert captured["tickers"] == ["I:VIX", "I:VIX3M", "SPY"]
    assert [row["ticker"] for row in payload["results"]] == ["^VIX", "^VIX3M", "SPY"]


def test_gateway_finance_report_passes_docs_aligned_statement_params() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def get_balance_sheet(self, **kwargs):
            captured.update(kwargs)
            return {"results": []}

        def get_cash_flow_statement(self, **kwargs):
            raise AssertionError("unexpected cash flow call")

        def get_income_statement(self, **kwargs):
            raise AssertionError("unexpected income statement call")

        def get_ratios(self, **kwargs):
            raise AssertionError("unexpected ratios call")

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_finance_report(
        symbol="AAPL",
        report="balance_sheet",
        timeframe="quarterly",
        sort="period_end.asc",
        limit=100,
        pagination=True,
    )

    assert payload == {"results": []}
    assert captured["ticker"] == "AAPL"
    assert captured["params"] == {"timeframe": "quarterly", "sort": "period_end.asc", "limit": 100}
    assert captured["pagination"] is True


def test_gateway_finance_report_maps_valuation_to_ratios() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def get_balance_sheet(self, **kwargs):
            raise AssertionError("unexpected balance sheet call")

        def get_cash_flow_statement(self, **kwargs):
            raise AssertionError("unexpected cash flow call")

        def get_income_statement(self, **kwargs):
            raise AssertionError("unexpected income statement call")

        def get_ratios(self, **kwargs):
            captured.update(kwargs)
            return {"results": []}

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    gateway.get_finance_report(symbol="AAPL", report="valuation", sort="market_cap.desc", limit=1, pagination=False)

    assert captured["ticker"] == "AAPL"
    assert captured["params"] == {"sort": "market_cap.desc", "limit": 1}
    assert captured["pagination"] is False


def test_gateway_finance_report_logs_empty_payload_anomaly(caplog) -> None:
    class _FakeClient:
        def get_balance_sheet(self, **kwargs):
            return {"status": "OK", "request_id": "req-empty", "results": []}

        def get_cash_flow_statement(self, **kwargs):
            raise AssertionError("unexpected cash flow call")

        def get_income_statement(self, **kwargs):
            raise AssertionError("unexpected income statement call")

        def get_ratios(self, **kwargs):
            raise AssertionError("unexpected ratios call")

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    with caplog.at_level("WARNING", logger="asset-allocation.api.massive"):
        payload = gateway.get_finance_report(
            symbol="AAPL",
            report="balance_sheet",
            timeframe="quarterly",
            sort="period_end.asc",
            limit=100,
            pagination=True,
        )

    assert payload["results"] == []
    assert "Massive finance provider anomaly" in caplog.text
    assert "symbol=AAPL" in caplog.text
    assert "report=balance_sheet" in caplog.text
