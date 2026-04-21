from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from alpaca.config import AlpacaEnvironmentConfig, HttpConfig
from alpaca.errors import AlpacaAmbiguousWriteError, AlpacaInvalidResponseError
from alpaca.transport_http import AlpacaHttpTransport


@pytest.fixture
def provider_config() -> AlpacaEnvironmentConfig:
    return AlpacaEnvironmentConfig(
        environment="paper",
        api_key="paper-key",
        api_secret="paper-secret",
        http=HttpConfig(timeout_s=5.0, max_retries=2, backoff_base_s=0.0),
    )


def test_get_retries_retryable_status_then_succeeds(
    provider_config: AlpacaEnvironmentConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = AlpacaHttpTransport(provider_config)
    monkeypatch.setattr("alpaca.transport_http.time.sleep", lambda _seconds: None)

    request = httpx.Request("GET", "https://paper-api.alpaca.markets/v2/account")
    transport._client.request = MagicMock(
        side_effect=[
            httpx.Response(503, json={"message": "temporary failure"}, request=request),
            httpx.Response(200, json={"id": "acct-1"}, request=request),
        ]
    )

    payload = transport.get("/v2/account")

    assert payload == {"id": "acct-1"}
    assert transport._client.request.call_count == 2


def test_write_timeout_raises_ambiguous_write_without_retry(
    provider_config: AlpacaEnvironmentConfig,
) -> None:
    transport = AlpacaHttpTransport(provider_config)

    request = httpx.Request("POST", "https://paper-api.alpaca.markets/v2/orders")
    transport._client.request = MagicMock(
        side_effect=httpx.ReadTimeout("timed out", request=request)
    )

    with pytest.raises(AlpacaAmbiguousWriteError, match="submission state is unknown"):
        transport.post("/v2/orders", json_data={"symbol": "AAPL"})

    assert transport._client.request.call_count == 1


def test_non_json_success_response_raises_invalid_response(
    provider_config: AlpacaEnvironmentConfig,
) -> None:
    transport = AlpacaHttpTransport(provider_config)

    request = httpx.Request("GET", "https://paper-api.alpaca.markets/v2/account")
    transport._client.request = MagicMock(
        return_value=httpx.Response(200, text="ok", request=request)
    )

    with pytest.raises(AlpacaInvalidResponseError, match="non-JSON success response"):
        transport.get("/v2/account")
