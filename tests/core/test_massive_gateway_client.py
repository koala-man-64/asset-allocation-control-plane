import httpx

import asset_allocation_runtime_common.providers.massive_gateway_client as massive_gateway_client_module
from asset_allocation_runtime_common.providers.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayClientConfig,
)


def test_warmup_probe_retries_before_first_request(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            if counters["warmup"] < 3:
                return httpx.Response(503, text="warming")
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/massive/time-series/daily":
            counters["data"] += 1
            return httpx.Response(200, text="Date,Open,High,Low,Close,Volume\n2026-01-01,1,1,1,1,1\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(massive_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=True,
            warmup_max_attempts=3,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        first = client.get_daily_time_series_csv(symbol="AAPL")
        second = client.get_daily_time_series_csv(symbol="MSFT")
    finally:
        http_client.close()

    assert "Date,Open,High,Low,Close,Volume" in first
    assert "Date,Open,High,Low,Close,Volume" in second
    assert counters["warmup"] == 3
    assert counters["data"] == 2


def test_warmup_can_be_disabled(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/massive/time-series/daily":
            counters["data"] += 1
            return httpx.Response(200, text="Date,Open,High,Low,Close,Volume\n2026-01-01,1,1,1,1,1\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(massive_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        csv = client.get_daily_time_series_csv(symbol="AAPL")
    finally:
        http_client.close()

    assert "Date,Open,High,Low,Close,Volume" in csv
    assert counters["warmup"] == 0
    assert counters["data"] == 1


def test_unified_snapshot_uses_batch_api_route() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/snapshot":
            return httpx.Response(200, json={"results": [{"ticker": "AAPL"}]})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        payload = client.get_unified_snapshot(symbols=["aapl", "MSFT", "AAPL"], asset_type="stocks")
    finally:
        http_client.close()

    assert payload["results"][0]["ticker"] == "AAPL"
    assert seen[0][0] == "/api/providers/massive/snapshot"
    assert seen[0][1].get("symbols") == "AAPL,MSFT"
    assert seen[0][1].get("type") == "stocks"


def test_market_history_uses_aggregated_route() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/market-history":
            return httpx.Response(200, json={"symbol": "AAPL", "status": "ok", "rows": []})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        payload = client.get_market_history(symbol="AAPL", from_date="2016-01-01", to_date="2025-01-10")
    finally:
        http_client.close()

    assert payload == {"symbol": "AAPL", "status": "ok", "rows": []}
    assert seen[0][0] == "/api/providers/massive/market-history"
    assert seen[0][1].get("symbol") == "AAPL"
    assert seen[0][1].get("from") == "2016-01-01"
    assert seen[0][1].get("to") == "2025-01-10"


def test_get_tickers_uses_reference_ticker_route() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/tickers":
            return httpx.Response(200, json={"results": [{"Symbol": "AAPL"}]})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        payload = client.get_tickers(market="stocks", locale="us", active=True)
    finally:
        http_client.close()

    assert payload == [{"Symbol": "AAPL"}]
    assert seen[0][0] == "/api/providers/massive/tickers"
    assert seen[0][1].get("market") == "stocks"
    assert seen[0][1].get("locale") == "us"
    assert seen[0][1].get("active") == "true"


def test_short_interest_uses_underscore_date_filters() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/fundamentals/short-interest":
            return httpx.Response(200, json={"results": []})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        client.get_short_interest(
            symbol="AAPL",
            settlement_date_gte="2024-01-01",
            settlement_date_lte="2024-01-31",
        )
    finally:
        http_client.close()

    assert seen[0][0] == "/api/providers/massive/fundamentals/short-interest"
    assert seen[0][1].get("symbol") == "AAPL"
    assert seen[0][1].get("settlement_date_gte") == "2024-01-01"
    assert seen[0][1].get("settlement_date_lte") == "2024-01-31"
    assert "settlement_date.gte" not in seen[0][1]
    assert "settlement_date.lte" not in seen[0][1]


def test_short_volume_uses_underscore_date_filters() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/fundamentals/short-volume":
            return httpx.Response(200, json={"results": []})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        client.get_short_volume(
            symbol="AAPL",
            date_gte="2024-01-01",
            date_lte="2024-01-31",
        )
    finally:
        http_client.close()

    assert seen[0][0] == "/api/providers/massive/fundamentals/short-volume"
    assert seen[0][1].get("symbol") == "AAPL"
    assert seen[0][1].get("date_gte") == "2024-01-01"
    assert seen[0][1].get("date_lte") == "2024-01-31"
    assert "date.gte" not in seen[0][1]
    assert "date.lte" not in seen[0][1]


def test_finance_valuation_uses_ratios_route() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/fundamentals/ratios":
            return httpx.Response(200, json={"results": []})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        client.get_finance_report(
            symbol="AAPL",
            report="valuation",
            sort="market_cap.desc",
            limit=1,
            pagination=False,
        )
    finally:
        http_client.close()

    assert seen[0][0] == "/api/providers/massive/fundamentals/ratios"
    assert seen[0][1].get("symbol") == "AAPL"
    assert seen[0][1].get("sort") == "market_cap.desc"
    assert seen[0][1].get("limit") == "1"
    assert seen[0][1].get("pagination") == "false"
