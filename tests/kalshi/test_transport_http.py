from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from kalshi.config import HttpConfig, KalshiEnvironmentConfig
from kalshi.errors import KalshiAmbiguousWriteError, KalshiInvalidResponseError, KalshiNotConfiguredError
from kalshi.transport_http import KalshiHttpTransport


@pytest.fixture
def provider_config() -> KalshiEnvironmentConfig:
    return KalshiEnvironmentConfig(
        environment="demo",
        api_key_id="demo-key",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
        http=HttpConfig(timeout_s=5.0, read_retry_attempts=2, read_retry_base_s=0.0),
    )


def test_public_get_retries_retryable_status_then_succeeds(
    provider_config: KalshiEnvironmentConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kalshi.transport_http.load_private_key", lambda _pem: object())
    monkeypatch.setattr("kalshi.transport_http.build_auth_headers", lambda *args, **kwargs: {})
    transport = KalshiHttpTransport(provider_config)
    monkeypatch.setattr("kalshi.transport_http.time.sleep", lambda _seconds: None)

    request = httpx.Request("GET", "https://demo-api.kalshi.co/trade-api/v2/markets")
    transport._client.request = MagicMock(
        side_effect=[
            httpx.Response(503, json={"message": "temporary failure"}, request=request),
            httpx.Response(200, json={"markets": []}, request=request),
        ]
    )

    payload = transport.get("/markets", authenticated=False)

    assert payload == {"markets": []}
    assert transport._client.request.call_count == 2


def test_authenticated_get_requires_credentials() -> None:
    config = KalshiEnvironmentConfig(
        environment="demo",
        http=HttpConfig(timeout_s=5.0, read_retry_attempts=0, read_retry_base_s=0.0),
    )
    transport = KalshiHttpTransport(config)

    with pytest.raises(KalshiNotConfiguredError, match="credentials are not configured"):
        transport.get("/portfolio/balance", authenticated=True)


def test_write_timeout_raises_ambiguous_write_without_retry(
    provider_config: KalshiEnvironmentConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kalshi.transport_http.load_private_key", lambda _pem: object())
    monkeypatch.setattr("kalshi.transport_http.build_auth_headers", lambda *args, **kwargs: {"KALSHI-ACCESS-KEY": "demo-key"})
    transport = KalshiHttpTransport(provider_config)

    request = httpx.Request("POST", "https://demo-api.kalshi.co/trade-api/v2/portfolio/orders")
    transport._client.request = MagicMock(side_effect=httpx.ReadTimeout("timed out", request=request))

    with pytest.raises(KalshiAmbiguousWriteError, match="submission state is unknown"):
        transport.post("/portfolio/orders", json_data={"ticker": "TEST-1"}, authenticated=True)

    assert transport._client.request.call_count == 1


def test_non_json_success_response_raises_invalid_response(
    provider_config: KalshiEnvironmentConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("kalshi.transport_http.load_private_key", lambda _pem: object())
    monkeypatch.setattr("kalshi.transport_http.build_auth_headers", lambda *args, **kwargs: {})
    transport = KalshiHttpTransport(provider_config)

    request = httpx.Request("GET", "https://demo-api.kalshi.co/trade-api/v2/markets")
    transport._client.request = MagicMock(return_value=httpx.Response(200, text="ok", request=request))

    with pytest.raises(KalshiInvalidResponseError, match="non-JSON success response"):
        transport.get("/markets", authenticated=False)
