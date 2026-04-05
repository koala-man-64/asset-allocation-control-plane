import httpx

from alpha_vantage import AlphaVantageClient, AlphaVantageConfig


def test_get_daily_time_series_csv_does_not_double_pass_symbol(monkeypatch):
    captured = {}

    def fake_fetch_csv(self, function, symbol=None, **params):
        captured["function"] = function
        captured["symbol"] = symbol
        captured["params"] = params
        return "timestamp,open\n2024-01-02,10\n"

    monkeypatch.setattr(AlphaVantageClient, "fetch_csv", fake_fetch_csv)

    cfg = AlphaVantageConfig(api_key="test", rate_limit_per_min=10_000, max_workers=1, max_retries=0)
    av = AlphaVantageClient(cfg, http_client=httpx.Client(transport=httpx.MockTransport(lambda _r: httpx.Response(500))))

    out = av.get_daily_time_series("AAPL", outputsize="compact", datatype="csv")
    assert "timestamp,open" in out
    assert captured["function"] == "TIME_SERIES_DAILY"
    assert captured["symbol"] == "AAPL"
    assert "symbol" not in captured["params"]
    assert captured["params"]["outputsize"] == "compact"


def test_get_intraday_time_series_csv_does_not_double_pass_symbol(monkeypatch):
    captured = {}

    def fake_fetch_csv(self, function, symbol=None, **params):
        captured["function"] = function
        captured["symbol"] = symbol
        captured["params"] = params
        return "timestamp,open\n2024-01-02 09:30,10\n"

    monkeypatch.setattr(AlphaVantageClient, "fetch_csv", fake_fetch_csv)

    cfg = AlphaVantageConfig(api_key="test", rate_limit_per_min=10_000, max_workers=1, max_retries=0)
    av = AlphaVantageClient(cfg, http_client=httpx.Client(transport=httpx.MockTransport(lambda _r: httpx.Response(500))))

    out = av.get_intraday_time_series("AAPL", interval="5min", outputsize="full", month="2024-01", datatype="csv")
    assert "timestamp,open" in out
    assert captured["function"] == "TIME_SERIES_INTRADAY"
    assert captured["symbol"] == "AAPL"
    assert "symbol" not in captured["params"]
    assert captured["params"]["interval"] == "5min"
    assert captured["params"]["outputsize"] == "full"
    assert captured["params"]["month"] == "2024-01"


def test_get_technical_indicator_csv_does_not_double_pass_symbol(monkeypatch):
    captured = {}

    def fake_fetch_csv(self, function, symbol=None, **params):
        captured["function"] = function
        captured["symbol"] = symbol
        captured["params"] = params
        return "time,real\n2024-01-02,10\n"

    monkeypatch.setattr(AlphaVantageClient, "fetch_csv", fake_fetch_csv)

    cfg = AlphaVantageConfig(api_key="test", rate_limit_per_min=10_000, max_workers=1, max_retries=0)
    av = AlphaVantageClient(cfg, http_client=httpx.Client(transport=httpx.MockTransport(lambda _r: httpx.Response(500))))

    out = av.get_technical_indicator("SMA", "AAPL", interval="daily", time_period=10, datatype="csv")
    assert "time,real" in out
    assert captured["function"] == "SMA"
    assert captured["symbol"] == "AAPL"
    assert "symbol" not in captured["params"]
    assert captured["params"]["interval"] == "daily"
    assert captured["params"]["series_type"] == "close"
    assert captured["params"]["time_period"] == 10

