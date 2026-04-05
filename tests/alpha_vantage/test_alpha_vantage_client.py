import httpx
import pytest

from alpha_vantage import AlphaVantageClient, AlphaVantageConfig
from alpha_vantage.errors import AlphaVantageThrottleError


def test_fetch_many_accepts_generators():
    def handler(request: httpx.Request) -> httpx.Response:
        params = dict(request.url.params)
        symbol = params.get("symbol") or "UNKNOWN"
        return httpx.Response(200, json={"symbol": symbol, "ok": True})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    cfg = AlphaVantageConfig(api_key="test", rate_limit_per_min=10_000, max_workers=2, max_retries=0)
    av = AlphaVantageClient(cfg, http_client=http_client)

    def gen():
        yield {"function": "TIME_SERIES_DAILY", "symbol": "AAPL"}
        yield {"function": "TIME_SERIES_DAILY", "symbol": "MSFT"}

    results = av.fetch_many(gen())
    assert len(results) == 2
    assert results[0]["symbol"] == "AAPL"
    assert results[1]["symbol"] == "MSFT"


def test_fetch_csv_retries_on_throttle_note(monkeypatch):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(200, json={"Note": "Thank you for using Alpha Vantage!"})
        return httpx.Response(200, text="timestamp,open\n2024-01-02,10\n")

    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    cfg = AlphaVantageConfig(
        api_key="test",
        rate_limit_per_min=10_000,
        max_workers=1,
        max_retries=1,
        backoff_base_seconds=0.0,
    )
    av = AlphaVantageClient(cfg, http_client=http_client)

    csv_text = av.fetch_csv("TIME_SERIES_DAILY", "AAPL")
    assert "timestamp,open" in csv_text
    assert calls["count"] == 2


def test_fetch_raises_on_throttle_when_retries_exhausted(monkeypatch):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"Note": "Throttle"})

    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    cfg = AlphaVantageConfig(
        api_key="test",
        rate_limit_per_min=10_000,
        max_workers=1,
        max_retries=0,
        backoff_base_seconds=0.0,
    )
    av = AlphaVantageClient(cfg, http_client=http_client)

    with pytest.raises(AlphaVantageThrottleError):
        av.fetch("TIME_SERIES_DAILY", "AAPL")


def test_fetch_raises_throttle_when_rate_wait_timeout(monkeypatch):
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    cfg = AlphaVantageConfig(
        api_key="test",
        rate_limit_per_min=10_000,
        max_workers=1,
        max_retries=0,
        backoff_base_seconds=0.0,
        rate_wait_timeout_seconds=0.01,
    )
    av = AlphaVantageClient(cfg, http_client=http_client)

    def _raise_timeout(*, caller=None, timeout_seconds=None):
        raise TimeoutError("simulated wait timeout")

    monkeypatch.setattr(av._rate_limiter, "wait", _raise_timeout)

    with pytest.raises(AlphaVantageThrottleError):
        av.fetch("TIME_SERIES_DAILY", "AAPL")


def test_throttle_cooldown_applies_to_next_request(monkeypatch):
    calls = {"count": 0}
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(200, json={"Note": "Thank you for using Alpha Vantage!"})
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr("time.sleep", lambda seconds: sleeps.append(float(seconds)))

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    cfg = AlphaVantageConfig(
        api_key="test",
        rate_limit_per_min=10_000,
        max_workers=1,
        max_retries=0,
        backoff_base_seconds=0.0,
        throttle_cooldown_seconds=60.0,
    )
    av = AlphaVantageClient(cfg, http_client=http_client)

    with pytest.raises(AlphaVantageThrottleError):
        av.fetch("TIME_SERIES_DAILY", "AAPL")

    payload = av.fetch("TIME_SERIES_DAILY", "AAPL")
    assert payload["ok"] is True
    assert calls["count"] == 2
    assert any(seconds >= 59.0 for seconds in sleeps)


def test_fetch_retries_http_429_with_60_second_sleep(monkeypatch):
    calls = {"count": 0}
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(429, json={"error": "throttle"})
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr("time.sleep", lambda seconds: sleeps.append(float(seconds)))

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)

    cfg = AlphaVantageConfig(
        api_key="test",
        rate_limit_per_min=10_000,
        max_workers=1,
        max_retries=1,
        backoff_base_seconds=0.0,
        throttle_cooldown_seconds=60.0,
    )
    av = AlphaVantageClient(cfg, http_client=http_client)

    payload = av.fetch("TIME_SERIES_DAILY", "AAPL")
    assert payload["ok"] is True
    assert calls["count"] == 2
    assert any(seconds >= 59.0 for seconds in sleeps)
