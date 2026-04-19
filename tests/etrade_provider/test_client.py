from __future__ import annotations

import requests
from requests import Response
from requests_oauthlib import OAuth1

import pytest

from etrade_provider.client import ETradeClient, ETradeOAuth1Client
from etrade_provider.config import ETradeEnvironmentConfig
from etrade_provider.errors import ETradeAmbiguousWriteError, ETradeApiError


def _config() -> ETradeEnvironmentConfig:
    return ETradeEnvironmentConfig(
        environment="sandbox",
        consumer_key="sandbox-key",
        consumer_secret="sandbox-secret",
        api_base_url="https://apisb.etrade.com",
    )


def _response(*, status_code: int, text: str, reason: str = "OK") -> Response:
    response = Response()
    response.status_code = status_code
    response._content = text.encode("utf-8")
    response.reason = reason
    response.url = "https://apisb.etrade.com/v1/accounts/list.json"
    return response


def test_oauth_signature_matches_etrade_reference_sample() -> None:
    auth = OAuth1(
        client_key="c5bb4dcb7bd6826c7c4340df3f791188",
        client_secret="7d30246211192cda43ede3abd9b393b9",
        resource_owner_key="VbiNYl63EejjlKdQM6FeENzcnrLACrZ2JYD6NQROfVI=",
        resource_owner_secret="XCF9RzyQr4UEPloA+WlC06BnTfYC1P0Fwr3GUw/B0Es=",
        signature_method="HMAC-SHA1",
        signature_type="AUTH_HEADER",
        nonce="0bba225a40d1bbac2430aa0c6163ce44",
        timestamp="1344885636",
        client_class=ETradeOAuth1Client,
    )
    prepared = requests.Request("GET", "https://api.etrade.com/v1/accounts/list").prepare()
    prepared = auth(prepared)

    header = str(prepared.headers["Authorization"])

    assert 'oauth_signature="UOnPVdzExTAgHkcGWLLfeTaaMSM%3D"' in header
    assert 'oauth_nonce="0bba225a40d1bbac2430aa0c6163ce44"' in header
    assert 'oauth_timestamp="1344885636"' in header
    assert 'oauth_consumer_key="c5bb4dcb7bd6826c7c4340df3f791188"' in header
    assert 'oauth_token="VbiNYl63EejjlKdQM6FeENzcnrLACrZ2JYD6NQROfVI%3D"' in header


def test_read_request_retries_retryable_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=3, read_retry_base_delay_seconds=0.0)
    calls = {"count": 0}

    class _FakeSession:
        def request(self, **kwargs):
            del kwargs
            calls["count"] += 1
            if calls["count"] < 3:
                return _response(status_code=503, text='{"error":"retry later"}', reason="Service Unavailable")
            return _response(status_code=200, text='{"AccountListResponse":{"accounts":[]}}')

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    payload = client.list_accounts(access_token="access-token", access_token_secret="access-secret")

    assert calls["count"] == 3
    assert payload == {"AccountListResponse": {"accounts": []}}


def test_place_order_timeout_is_ambiguous_and_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=3, read_retry_base_delay_seconds=0.0)
    calls = {"count": 0}

    class _FakeSession:
        def request(self, **kwargs):
            del kwargs
            calls["count"] += 1
            raise requests.Timeout("timed out")

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    with pytest.raises(ETradeAmbiguousWriteError, match="unknown submission state"):
        client.place_order(
            access_token="access-token",
            access_token_secret="access-secret",
            account_key="acct-key",
            payload={"PlaceOrderRequest": {"orderType": "EQ", "Order": []}},
        )

    assert calls["count"] == 1


def test_read_timeout_uses_retry_budget_before_failing(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)
    calls = {"count": 0}

    class _FakeSession:
        def request(self, **kwargs):
            del kwargs
            calls["count"] += 1
            raise requests.Timeout("timed out")

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    with pytest.raises(ETradeApiError, match="failed after 2 attempt"):
        client.list_accounts(access_token="access-token", access_token_secret="access-secret")

    assert calls["count"] == 2
