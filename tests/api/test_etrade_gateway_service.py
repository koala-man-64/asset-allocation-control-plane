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
        self.renew_calls = 0

    def fetch_request_token(self, *, callback_uri: str | None = None):
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
        return {"AccountListResponse": {"accounts": []}}

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
    return ETradeSettings(
        enabled=True,
        trading_enabled=True,
        callback_url="http://localhost:8000/api/providers/etrade/connect/callback",
        sandbox_consumer_key="sandbox-key",
        sandbox_consumer_secret="sandbox-secret",
        live_consumer_key="live-key",
        live_consumer_secret="live-secret",
        **overrides,
    )


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
    assert complete["expires_at"] == "2026-04-20T04:00:00Z"
    assert session["connected"] is True
    assert session["token_expires_at"] == "2026-04-20T04:00:00Z"


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
