from __future__ import annotations

import logging

import httpx
import pytest

from quiver_provider import (
    QuiverClient,
    QuiverConfig,
    QuiverEntitlementError,
    QuiverProtocolError,
    QuiverTimeoutError,
    QuiverUnavailableError,
)


def test_quiver_client_sets_bearer_auth_and_retries_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer test-key"
        calls["count"] += 1
        if calls["count"] == 1:
            return httpx.Response(429, json={"detail": "rate limited"}, headers={"Retry-After": "7"})
        return httpx.Response(200, json=[{"Ticker": "AAPL"}])

    monkeypatch.setattr("time.sleep", lambda seconds: sleeps.append(float(seconds)))
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    client = QuiverClient(
        QuiverConfig(api_key="test-key", rate_limit_per_min=10_000, max_retries=1),
        http_client=http_client,
    )

    payload = client.get_json("/beta/live/insiders", params={"ticker": "AAPL"})

    assert payload == [{"Ticker": "AAPL"}]
    assert calls["count"] == 2
    assert sleeps == [7.0]


def test_quiver_client_maps_subscription_denial_to_entitlement_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, json={"detail": "Tier 1 subscription required"})

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    client = QuiverClient(
        QuiverConfig(api_key="test-key", rate_limit_per_min=10_000, max_retries=0),
        http_client=http_client,
    )

    with pytest.raises(QuiverEntitlementError):
        client.get_json("/beta/live/congresstrading")


def test_quiver_client_maps_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    client = QuiverClient(
        QuiverConfig(api_key="test-key", rate_limit_per_min=10_000, max_retries=0),
        http_client=http_client,
    )

    with pytest.raises(QuiverTimeoutError):
        client.get_json("/beta/live/congresstrading")


def test_quiver_client_rejects_non_json_response() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not json")

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    client = QuiverClient(
        QuiverConfig(api_key="test-key", rate_limit_per_min=10_000, max_retries=0),
        http_client=http_client,
    )

    with pytest.raises(QuiverProtocolError):
        client.get_json("/beta/live/congresstrading")


def test_quiver_client_redacts_api_key_in_retry_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "unavailable"})

    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, timeout=httpx.Timeout(5.0), trust_env=False)
    client = QuiverClient(
        QuiverConfig(api_key="super-secret-key", rate_limit_per_min=10_000, max_retries=1),
        http_client=http_client,
    )

    with caplog.at_level(logging.WARNING):
        with pytest.raises(QuiverUnavailableError):
            client.get_json("/beta/live/insiders?token=super-secret-key", params={"ticker": "AAPL"})

    assert "super-secret-key" not in caplog.text
    assert "[REDACTED]" in caplog.text
