from __future__ import annotations

from typing import Any, Callable

import httpx
import pytest

import core.alpha_vantage_gateway_client as alpha_vantage_gateway_client_module
import core.massive_gateway_client as massive_gateway_client_module
from core.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayClientConfig,
    AlphaVantageGatewayUnavailableError,
)
from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayClientConfig,
    MassiveGatewayUnavailableError,
)


GATEWAY_CASES: list[dict[str, Any]] = [
    {
        "id": "alpha-vantage",
        "module": alpha_vantage_gateway_client_module,
        "client_cls": AlphaVantageGatewayClient,
        "config_cls": AlphaVantageGatewayClientConfig,
        "unavailable_error": AlphaVantageGatewayUnavailableError,
        "timeout_floor": 600.0,
        "timeout_env": "120",
        "request_path": "/api/providers/alpha-vantage/listing-status",
        "request_call": lambda client: client.get_listing_status_csv(),
        "success_text": "symbol,name\nAAPL,Apple\n",
    },
    {
        "id": "massive",
        "module": massive_gateway_client_module,
        "client_cls": MassiveGatewayClient,
        "config_cls": MassiveGatewayClientConfig,
        "unavailable_error": MassiveGatewayUnavailableError,
        "timeout_floor": 60.0,
        "timeout_env": "5",
        "request_path": "/api/providers/massive/time-series/daily",
        "request_call": lambda client: client.get_daily_time_series_csv(symbol="AAPL"),
        "success_text": "Date,Open,High,Low,Close,Volume\n2026-01-01,1,1,1,1,1\n",
    },
]


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_build_headers_include_bearer_token_and_caller_context(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client_cls = case["client_cls"]
    config_cls = case["config_cls"]

    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-market-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-market-job-abc123")

    client = client_cls(
        config_cls(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=10.0,
        ),
        access_token_provider=lambda: "oidc-token",
    )

    headers = client._build_headers()
    assert headers["Authorization"] == "Bearer oidc-token"
    assert headers["X-Caller-Job"] == "bronze-market-job"
    assert headers["X-Caller-Execution"] == "bronze-market-job-abc123"


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_build_headers_build_provider_from_scope_when_needed(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = case["module"]
    client_cls = case["client_cls"]
    config_cls = case["config_cls"]

    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-market-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-market-job-abc123")

    client = client_cls(
        config_cls(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=10.0,
        ),
    )
    monkeypatch.setattr(module, "build_access_token_provider", lambda scope: (lambda: f"{scope}-token"))

    headers = client._build_headers()
    assert headers["Authorization"] == "Bearer api://asset-allocation/.default-token"
    assert headers["X-Caller-Job"] == "bronze-market-job"
    assert headers["X-Caller-Execution"] == "bronze-market-job-abc123"


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_from_env_enforces_timeout_floor(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    client_cls = case["client_cls"]

    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://asset-allocation/.default")
    monkeypatch.setenv("ASSET_ALLOCATION_API_TIMEOUT_SECONDS", case["timeout_env"])

    client = client_cls.from_env()
    try:
        assert client.config.timeout_seconds >= case["timeout_floor"]
    finally:
        client.close()


def test_massive_gateway_timeout_floor_warning_emits_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://asset-allocation/.default")
    monkeypatch.setenv("ASSET_ALLOCATION_API_TIMEOUT_SECONDS", "5")
    monkeypatch.setattr(massive_gateway_client_module, "_TIMEOUT_FLOOR_WARNING_EMITTED", False)

    with caplog.at_level("WARNING"):
        client_one = MassiveGatewayClient.from_env()
        client_two = MassiveGatewayClient.from_env()

    try:
        warnings = [
            record.message
            for record in caplog.records
            if "ASSET_ALLOCATION_API_TIMEOUT_SECONDS=5.0 is too low" in record.message
        ]
        assert len(warnings) == 1
    finally:
        client_one.close()
        client_two.close()


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_from_env_reads_api_scope(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    client_cls = case["client_cls"]

    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("ASSET_ALLOCATION_API_SCOPE", "api://asset-allocation/.default")

    client = client_cls.from_env()
    try:
        assert client.config.api_scope == "api://asset-allocation/.default"
    finally:
        client.close()


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_from_env_requires_api_scope(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    client_cls = case["client_cls"]

    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.delenv("ASSET_ALLOCATION_API_SCOPE", raising=False)

    with pytest.raises(ValueError, match="ASSET_ALLOCATION_API_SCOPE is required"):
        client_cls.from_env()


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_public_warmup_reports_failure(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    module = case["module"]
    client_cls = case["client_cls"]
    config_cls = case["config_cls"]
    counters = {"warmup": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(503, text="warming")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    client = client_cls(
        config_cls(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=case["timeout_floor"],
            warmup_enabled=True,
            warmup_max_attempts=2,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        assert client.warm_up_gateway() is False
    finally:
        http_client.close()

    assert counters["warmup"] == 2


@pytest.mark.parametrize("case", GATEWAY_CASES, ids=[c["id"] for c in GATEWAY_CASES])
def test_gateway_request_fails_fast_when_readiness_never_recovers(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = case["module"]
    client_cls = case["client_cls"]
    config_cls = case["config_cls"]
    unavailable_error = case["unavailable_error"]
    request_call: Callable[[Any], Any] = case["request_call"]
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(503, text="warming")
        if request.url.path == case["request_path"]:
            counters["data"] += 1
            return httpx.Response(200, text=case["success_text"])
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    client = client_cls(
        config_cls(
            base_url="http://asset-allocation-api",
            api_scope="api://asset-allocation/.default",
            timeout_seconds=case["timeout_floor"],
            warmup_enabled=True,
            warmup_max_attempts=1,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
            readiness_enabled=True,
            readiness_max_attempts=2,
            readiness_sleep_seconds=0.0,
        ),
        http_client=http_client,
        access_token_provider=lambda: "oidc-token",
    )
    try:
        with pytest.raises(unavailable_error):
            request_call(client)
    finally:
        http_client.close()

    assert counters["warmup"] == 2
    assert counters["data"] == 0
