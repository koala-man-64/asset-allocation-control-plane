import httpx

import asset_allocation_runtime_common.providers.alpha_vantage_gateway_client as alpha_vantage_gateway_client_module
from asset_allocation_runtime_common.providers.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayClientConfig,
    AlphaVantageGatewayError,
)


def test_warmup_probe_retries_before_first_request(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            if counters["warmup"] < 2:
                return httpx.Response(503, text="warming")
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/alpha-vantage/listing-status":
            counters["data"] += 1
            return httpx.Response(200, text="symbol,name\nAAPL,Apple\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(alpha_vantage_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=600.0,
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
        first = client.get_listing_status_csv()
        second = client.get_listing_status_csv()
    finally:
        http_client.close()

    assert "symbol,name" in first
    assert "symbol,name" in second
    assert counters["warmup"] == 2
    assert counters["data"] == 2


def test_warmup_can_be_disabled(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/alpha-vantage/listing-status":
            counters["data"] += 1
            return httpx.Response(200, text="symbol,name\nAAPL,Apple\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(alpha_vantage_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=600.0,
            warmup_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        csv = client.get_listing_status_csv()
    finally:
        http_client.close()

    assert "symbol,name" in csv
    assert counters["warmup"] == 0
    assert counters["data"] == 1


def test_get_earnings_calendar_csv_calls_calendar_route(monkeypatch):
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["path"] = request.url.path
        observed["query"] = str(request.url.query)
        return httpx.Response(
            200,
            text="symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\nAAPL,Apple,2026-05-01,2026-03-31,1.5,USD,post-market\n",
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=600.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        csv = client.get_earnings_calendar_csv(symbol="AAPL", horizon="6month")
    finally:
        http_client.close()

    assert "reportDate" in csv
    assert observed["path"] == "/api/providers/alpha-vantage/earnings-calendar"
    assert "symbol=AAPL" in str(observed["query"])
    assert "horizon=6month" in str(observed["query"])


def test_request_retries_after_504_with_extended_backoff(monkeypatch):
    counters = {"warmup": 0, "data": 0}
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/alpha-vantage/listing-status":
            counters["data"] += 1
            if counters["data"] == 1:
                return httpx.Response(504, text="gateway timeout")
            return httpx.Response(200, text="symbol,name\nAAPL,Apple\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(alpha_vantage_gateway_client_module.time, "sleep", sleep_calls.append)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=600.0,
            warmup_enabled=True,
            warmup_max_attempts=1,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
            readiness_enabled=False,
            request_retry_attempts=3,
            request_retry_base_delay_seconds=120.0,
            request_retry_max_delay_seconds=300.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        csv = client.get_listing_status_csv()
    finally:
        http_client.close()

    assert "symbol,name" in csv
    assert counters["warmup"] == 2
    assert counters["data"] == 2
    assert len(sleep_calls) == 1
    assert 120.0 <= sleep_calls[0] <= 150.0


def test_request_raises_after_exhausting_504_retry_budget(monkeypatch):
    counters = {"warmup": 0, "data": 0}
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/alpha-vantage/listing-status":
            counters["data"] += 1
            return httpx.Response(504, text="gateway timeout")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(alpha_vantage_gateway_client_module.time, "sleep", sleep_calls.append)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=600.0,
            warmup_enabled=True,
            warmup_max_attempts=1,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
            readiness_enabled=False,
            request_retry_attempts=3,
            request_retry_base_delay_seconds=120.0,
            request_retry_max_delay_seconds=240.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        try:
            client.get_listing_status_csv()
        except AlphaVantageGatewayError as exc:
            assert exc.status_code == 504
            assert exc.payload == {
                "path": "/api/providers/alpha-vantage/listing-status",
                "status_code": 504,
                "detail": "gateway timeout",
            }
        else:
            raise AssertionError("Expected AlphaVantageGatewayError after retries are exhausted.")
    finally:
        http_client.close()

    assert counters["warmup"] == 3
    assert counters["data"] == 3
    assert len(sleep_calls) == 2
    assert 120.0 <= sleep_calls[0] <= 150.0
    assert sleep_calls[1] == 240.0
