from __future__ import annotations

from typing import Any

import requests
from requests import Response
from requests_oauthlib import OAuth1
from requests_oauthlib.oauth1_session import TokenRequestDenied

import pytest

from etrade_provider.client import ETradeClient, ETradeOAuth1Client
from etrade_provider.config import ETradeEnvironmentConfig
from etrade_provider.errors import ETradeAmbiguousWriteError, ETradeApiError, ETradeBrokerAuthError


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


def test_fetch_request_token_denied_maps_to_broker_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)

    class _FakeSession:
        def fetch_request_token(self, *args, **kwargs):
            del args, kwargs
            raise TokenRequestDenied(
                "request denied",
                _response(
                    status_code=401,
                    text="<html><body>oauth_problem=consumer_key_rejected</body></html>",
                    reason="Unauthorized",
                ),
            )

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    with pytest.raises(ETradeBrokerAuthError, match="consumer_key_rejected") as exc_info:
        client.fetch_request_token(callback_uri="oob")

    assert exc_info.value.status_code == 401


def test_fetch_request_token_always_uses_oob_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)
    captured: dict[str, Any] = {}

    class _FakeSession:
        def fetch_request_token(self, *args, **kwargs):
            del args, kwargs
            return {
                "oauth_token": "request-token",
                "oauth_token_secret": "request-secret",
                "oauth_callback_confirmed": "true",
            }

    def _oauth_session(**kwargs):
        captured.update(kwargs)
        return _FakeSession()

    monkeypatch.setattr(client, "_oauth_session", _oauth_session)

    payload = client.fetch_request_token(callback_uri="https://example.com/callback")

    assert payload["oauth_token"] == "request-token"
    assert captured["callback_uri"] == "oob"


def test_fetch_access_token_denied_maps_to_broker_auth_error(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)

    class _FakeSession:
        def fetch_access_token(self, *args, **kwargs):
            del args, kwargs
            raise TokenRequestDenied(
                "request denied",
                _response(status_code=403, text='{"error":"invalid verifier"}', reason="Forbidden"),
            )

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    with pytest.raises(ETradeBrokerAuthError, match="invalid verifier") as exc_info:
        client.fetch_access_token(
            request_token="request-token",
            request_token_secret="request-secret",
            verifier="bad-verifier",
        )

    assert exc_info.value.status_code == 403


def test_list_transactions_uses_group_path_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)
    captured: dict[str, Any] = {}

    class _FakeSession:
        def request(self, **kwargs):
            captured.update(kwargs)
            return _response(status_code=200, text='{"TransactionDetailsResponse":{"Transaction":[]}}')

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    payload = client.list_transactions(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
        transaction_group="Trades",
        params={"count": 25},
    )

    assert payload == {"TransactionDetailsResponse": {"Transaction": []}}
    assert captured["url"] == "https://apisb.etrade.com/v1/accounts/acct-key/transactions/Trades.json"
    assert captured["params"] == {"count": 25}


def test_list_accounts_returns_none_on_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)

    class _FakeSession:
        def request(self, **kwargs):
            del kwargs
            return _response(status_code=204, text="", reason="No Content")

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    payload = client.list_accounts(access_token="access-token", access_token_secret="access-secret")

    assert payload is None


def test_get_balance_defaults_real_time_nav_to_false(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)
    captured: dict[str, Any] = {}

    class _FakeSession:
        def request(self, **kwargs):
            captured.update(kwargs)
            return _response(status_code=200, text='{"BalanceResponse":{"accountId":"1"}}')

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    payload = client.get_balance(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
        inst_type="BROKERAGE",
    )

    assert payload == {"BalanceResponse": {"accountId": "1"}}
    assert captured["params"]["realTimeNAV"] == "false"


def test_get_balance_allows_real_time_nav_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)
    captured: dict[str, Any] = {}

    class _FakeSession:
        def request(self, **kwargs):
            captured.update(kwargs)
            return _response(status_code=200, text='{"BalanceResponse":{"accountId":"1"}}')

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    client.get_balance(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
        inst_type="BROKERAGE",
        real_time_nav=True,
    )

    assert captured["params"]["realTimeNAV"] == "true"


def test_get_portfolio_returns_none_on_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)

    class _FakeSession:
        def request(self, **kwargs):
            del kwargs
            return _response(status_code=204, text="", reason="No Content")

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    payload = client.get_portfolio(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
    )

    assert payload is None


def test_list_transactions_returns_none_on_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)

    class _FakeSession:
        def request(self, **kwargs):
            del kwargs
            return _response(status_code=204, text="", reason="No Content")

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    payload = client.list_transactions(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
    )

    assert payload is None


def test_get_transaction_details_forwards_store_id_and_handles_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ETradeClient(_config(), timeout_seconds=1.0, read_retry_attempts=2, read_retry_base_delay_seconds=0.0)
    calls = {"count": 0}
    captured: dict[str, Any] = {}

    class _FakeSession:
        def request(self, **kwargs):
            calls["count"] += 1
            captured.update(kwargs)
            if calls["count"] == 1:
                return _response(status_code=204, text="", reason="No Content")
            return _response(status_code=200, text='{"TransactionDetailsResponse":{"transactionId":"123"}}')

    monkeypatch.setattr(client, "_oauth_session", lambda **kwargs: _FakeSession())

    first = client.get_transaction_details(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
        transaction_id="123",
        store_id="987",
    )
    second = client.get_transaction_details(
        access_token="access-token",
        access_token_secret="access-secret",
        account_key="acct-key",
        transaction_id="123",
        store_id="987",
    )

    assert first is None
    assert second == {"TransactionDetailsResponse": {"transactionId": "123"}}
    assert captured["url"] == "https://apisb.etrade.com/v1/accounts/acct-key/transactions/123.json"
    assert captured["params"] == {"storeId": "987"}


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
