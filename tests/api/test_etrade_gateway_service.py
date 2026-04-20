from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import api.service.etrade_gateway as etrade_gateway_module
from api.service.etrade_gateway import ETradeGateway, _BrokerSessionState
from api.service.settings import ETradeSettings
from etrade_provider.errors import ETradeInactiveSessionError, ETradeSessionExpiredError, ETradeValidationError


class _FakeClient:
    def __init__(self) -> None:
        self.config = type("Config", (), {"is_configured": True})()
        self.preview_payload = None
        self.place_payload = None
        self.request_token_callback_uri = None
        self.balance_request = None
        self.portfolio_request = None
        self.transactions_request = None
        self.transaction_detail_request = None
        self.accounts_response = {"AccountListResponse": {"accounts": []}}
        self.balance_response = {"BalanceResponse": {"accountId": "1"}}
        self.portfolio_response = {"PortfolioResponse": {"AccountPortfolio": []}}
        self.transactions_response = {"TransactionDetailsResponse": {"Transaction": []}}
        self.transaction_detail_response = {"TransactionDetailsResponse": {"transactionId": "123"}}
        self.renew_calls = 0

    def fetch_request_token(self, *, callback_uri: str | None = None):
        self.request_token_callback_uri = callback_uri
        return {
            "oauth_token": "request-token",
            "oauth_token_secret": "request-secret",
            "oauth_callback_confirmed": "true" if callback_uri else "false",
        }

    def build_authorize_url(self, *, request_token: str) -> str:
        return f"https://us.etrade.com/e/t/etws/authorize?key=test-key&token={request_token}"

    def fetch_access_token(self, *, request_token: str, request_token_secret: str, verifier: str):
        assert request_token == "request-token"
        assert request_token_secret == "request-secret"
        assert verifier
        return {"oauth_token": "access-token", "oauth_token_secret": "access-secret"}

    def renew_access_token(self, *, access_token: str, access_token_secret: str) -> str:
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        self.renew_calls += 1
        return "Access Token has been renewed"

    def revoke_access_token(self, *, access_token: str, access_token_secret: str) -> str:
        return "Revoked"

    def list_accounts(self, *, access_token: str, access_token_secret: str):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        return self.accounts_response

    def get_balance(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        inst_type: str,
        real_time_nav: bool = False,
        account_type: str | None = None,
    ):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        self.balance_request = {
            "account_key": account_key,
            "inst_type": inst_type,
            "real_time_nav": real_time_nav,
            "account_type": account_type,
        }
        return self.balance_response

    def get_portfolio(self, *, access_token: str, access_token_secret: str, account_key: str, params=None):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        self.portfolio_request = {"account_key": account_key, "params": params}
        return self.portfolio_response

    def list_transactions(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        transaction_group: str | None = None,
        params=None,
    ):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        self.transactions_request = {
            "account_key": account_key,
            "transaction_group": transaction_group,
            "params": params,
        }
        return self.transactions_response

    def get_transaction_details(
        self,
        *,
        access_token: str,
        access_token_secret: str,
        account_key: str,
        transaction_id: str,
        store_id: str | None = None,
    ):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        self.transaction_detail_request = {
            "account_key": account_key,
            "transaction_id": transaction_id,
            "store_id": store_id,
        }
        return self.transaction_detail_response

    def preview_order(self, *, access_token: str, access_token_secret: str, account_key: str, payload):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        assert account_key == "acct-key"
        self.preview_payload = payload
        return {"PreviewOrderResponse": {"PreviewIds": {"previewId": "2785277279"}}}

    def place_order(self, *, access_token: str, access_token_secret: str, account_key: str, payload):
        assert access_token == "access-token"
        assert access_token_secret == "access-secret"
        assert account_key == "acct-key"
        self.place_payload = payload
        return {"PlaceOrderResponse": {"OrderIds": [{"orderId": 485}]}}

    def cancel_order(self, *, access_token: str, access_token_secret: str, account_key: str, order_id: int):
        return {"CancelOrderResponse": {"orderId": order_id}}


def _settings(**overrides) -> ETradeSettings:
    values = {
        "enabled": True,
        "trading_enabled": True,
        "callback_url": "http://localhost:8000/api/providers/etrade/connect/callback",
        "sandbox_consumer_key": "sandbox-key",
        "sandbox_consumer_secret": "sandbox-secret",
        "live_consumer_key": "live-key",
        "live_consumer_secret": "live-secret",
    }
    values.update(overrides)
    return ETradeSettings(**values)


def _gateway(fake_client: _FakeClient | None = None, **settings_overrides) -> tuple[ETradeGateway, _FakeClient]:
    gateway = ETradeGateway(_settings(**settings_overrides))
    client = fake_client or _FakeClient()
    gateway._clients["sandbox"] = client
    gateway._clients["live"] = _FakeClient()
    return gateway, client


def _seed_session(gateway: ETradeGateway, *, now: datetime, expires_at: datetime, last_activity_at: datetime) -> None:
    gateway._sessions["sandbox"] = _BrokerSessionState(
        environment="sandbox",
        access_token="access-token",
        access_token_secret="access-secret",
        created_at=now,
        expires_at=expires_at,
        last_activity_at=last_activity_at,
    )


def test_gateway_connect_flow_sets_request_ttl_and_eastern_midnight_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 4, 19, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: fixed_now)

    gateway, _client = _gateway()

    start = gateway.start_connect(environment="sandbox")
    complete = gateway.complete_connect(environment="sandbox", verifier="verifier-123")
    session = gateway.get_session_state(environment="sandbox")

    assert start["request_token_expires_at"] == "2026-04-19T15:05:00Z"
    assert start["callback_confirmed"] is True
    assert start["callback_url"] == "http://localhost:8000/api/providers/etrade/connect/callback"
    assert gateway._clients["sandbox"].request_token_callback_uri == "http://localhost:8000/api/providers/etrade/connect/callback"
    assert complete["expires_at"] == "2026-04-20T04:00:00Z"
    assert session["connected"] is True
    assert session["token_expires_at"] == "2026-04-20T04:00:00Z"


def test_gateway_connect_flow_uses_oob_when_callback_url_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 4, 19, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: fixed_now)

    gateway, client = _gateway(callback_url=None)

    start = gateway.start_connect(environment="sandbox")

    assert "callback_url" not in start
    assert client.request_token_callback_uri == "oob"


def test_gateway_callback_completion_matches_pending_request_token(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 4, 19, 15, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: fixed_now)

    gateway, _client = _gateway()
    gateway.start_connect(environment="sandbox")

    response = gateway.complete_connect_from_callback(request_token="request-token", verifier="callback-verifier")

    assert response["environment"] == "sandbox"
    assert response["connected"] is True


def test_gateway_read_renews_idle_session(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(seconds=7201),
    )

    payload = gateway.list_accounts(environment="sandbox", subject="user-123")

    assert payload == {"AccountListResponse": {"accounts": []}}
    assert client.renew_calls == 1
    assert gateway._sessions["sandbox"].last_activity_at == now
    assert gateway._sessions["sandbox"].renewed_at == now


def test_gateway_list_accounts_returns_none_when_broker_has_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    client.accounts_response = None
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    payload = gateway.list_accounts(environment="sandbox", subject="user-123")

    assert payload is None
    assert gateway._sessions["sandbox"].last_activity_at == now


def test_gateway_balance_defaults_real_time_nav_to_false(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    payload = gateway.get_balance(environment="sandbox", account_key="acct-key", subject="user-123")

    assert payload == {"BalanceResponse": {"accountId": "1"}}
    assert client.balance_request == {
        "account_key": "acct-key",
        "inst_type": "BROKERAGE",
        "real_time_nav": False,
        "account_type": None,
    }


def test_gateway_portfolio_returns_none_when_broker_has_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    client.portfolio_response = None
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    payload = gateway.get_portfolio(environment="sandbox", account_key="acct-key", subject="user-123")

    assert payload is None
    assert client.portfolio_request == {"account_key": "acct-key", "params": None}


def test_gateway_list_transactions_formats_filters_and_group(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    payload = gateway.list_transactions(
        environment="sandbox",
        account_key="acct-key",
        subject="user-123",
        start_date="2026-04-01",
        end_date="2026-04-19",
        sort_order="desc",
        marker="12345",
        count=25,
        transaction_group="trades",
    )

    assert payload == {"TransactionDetailsResponse": {"Transaction": []}}
    assert client.transactions_request == {
        "account_key": "acct-key",
        "transaction_group": "Trades",
        "params": {
            "count": 25,
            "marker": "12345",
            "sortOrder": "DESC",
            "startDate": "04012026",
            "endDate": "04192026",
        },
    }
    assert gateway._sessions["sandbox"].last_activity_at == now


def test_gateway_list_transactions_requires_paired_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, _client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    with pytest.raises(ETradeValidationError, match="must be provided together"):
        gateway.list_transactions(
            environment="sandbox",
            account_key="acct-key",
            subject="user-123",
            start_date="2026-04-01",
        )


def test_gateway_list_transactions_rejects_inverted_dates(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, _client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    with pytest.raises(ETradeValidationError, match="on or after"):
        gateway.list_transactions(
            environment="sandbox",
            account_key="acct-key",
            subject="user-123",
            start_date="2026-04-19",
            end_date="2026-04-01",
        )


def test_gateway_transaction_detail_returns_none_when_broker_has_no_content(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    client.transaction_detail_response = None
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    payload = gateway.get_transaction_details(
        environment="sandbox",
        account_key="acct-key",
        transaction_id="123",
        subject="user-123",
        store_id="store-1",
    )

    assert payload is None
    assert client.transaction_detail_request == {
        "account_key": "acct-key",
        "transaction_id": "123",
        "store_id": "store-1",
    }


def test_gateway_preview_and_place_reuse_cached_request(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(minutes=5),
    )

    preview = gateway.preview_order(
        environment="sandbox",
        subject="user-123",
        order={
            "account_key": "acct-key",
            "asset_type": "equity",
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 1,
            "price_type": "LIMIT",
            "limit_price": "180",
            "term": "GOOD_FOR_DAY",
            "session": "REGULAR",
            "all_or_none": False,
        },
    )
    placed = gateway.place_order(environment="sandbox", preview_id=preview["preview_id"], subject="user-123")

    assert preview["preview_id"] == "2785277279"
    assert placed["order_id"] == "485"
    assert client.preview_payload["PreviewOrderRequest"]["clientOrderId"] == client.place_payload["PlaceOrderRequest"][
        "clientOrderId"
    ]
    assert client.place_payload["PlaceOrderRequest"]["PreviewIds"] == [{"previewId": "2785277279"}]
    with pytest.raises(ETradeValidationError, match="missing, expired, or belongs to a different environment"):
        gateway.place_order(environment="sandbox", preview_id="2785277279", subject="user-123")


def test_gateway_blocks_preview_when_session_is_idle(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 19, 18, 0, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, _client = _gateway()
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(hours=3),
        last_activity_at=now - timedelta(seconds=7201),
    )

    with pytest.raises(ETradeInactiveSessionError, match="idle for over two hours"):
        gateway.preview_order(
            environment="sandbox",
            subject="user-123",
            order={
                "account_key": "acct-key",
                "asset_type": "equity",
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 1,
                "price_type": "MARKET",
                "term": "GOOD_FOR_DAY",
                "session": "REGULAR",
                "all_or_none": False,
            },
        )


def test_gateway_blocks_preview_when_session_is_near_midnight_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 4, 20, 3, 56, tzinfo=UTC)
    monkeypatch.setattr(etrade_gateway_module, "_utc_now", lambda: now)

    gateway, _client = _gateway(session_expiry_guard_seconds=300)
    _seed_session(
        gateway,
        now=now,
        expires_at=now + timedelta(seconds=240),
        last_activity_at=now - timedelta(minutes=5),
    )

    with pytest.raises(ETradeSessionExpiredError, match="too close to midnight Eastern expiry"):
        gateway.preview_order(
            environment="sandbox",
            subject="user-123",
            order={
                "account_key": "acct-key",
                "asset_type": "equity",
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 1,
                "price_type": "MARKET",
                "term": "GOOD_FOR_DAY",
                "session": "REGULAR",
                "all_or_none": False,
            },
        )
