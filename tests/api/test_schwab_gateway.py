from __future__ import annotations

import httpx
import pytest

from api.service.schwab_gateway import (
    SchwabGateway,
    SchwabGatewayAmbiguousWriteError,
    SchwabGatewaySessionExpiredError,
    SchwabGatewayValidationError,
)
from api.service.settings import SchwabSettings
from schwab import SchwabHTTPResponse, SchwabOAuthTokens
from schwab.errors import SchwabAuthError, SchwabError


class _FakeSchwabClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.account_number_attempts = 0
        self.place_error: Exception | None = None

    def close(self) -> None:
        self.calls.append(("close", None))

    def build_authorization_url(self, *, state: str | None = None) -> str:
        self.calls.append(("build_authorization_url", state))
        return f"https://schwab.example/authorize?state={state}"

    def exchange_authorization_code(self, authorization_code: str) -> SchwabOAuthTokens:
        self.calls.append(("exchange_authorization_code", authorization_code))
        return SchwabOAuthTokens(
            access_token="access-from-code",
            refresh_token="refresh-from-code",
            id_token="id-token",
            token_type="Bearer",
            scope="api",
            expires_in=1800,
            raw={},
        )

    def refresh_access_token(self, refresh_token: str | None = None) -> SchwabOAuthTokens:
        self.calls.append(("refresh_access_token", refresh_token))
        return SchwabOAuthTokens(
            access_token="refreshed-access",
            refresh_token=refresh_token or "",
            id_token="id-token",
            token_type="Bearer",
            scope="api",
            expires_in=1800,
            raw={},
        )

    def get_account_numbers(self, *, access_token: str | None = None):
        self.account_number_attempts += 1
        self.calls.append(("get_account_numbers", access_token))
        if self.account_number_attempts == 1:
            raise SchwabAuthError("expired")
        return [{"accountNumber": "123456789"}]

    def place_order(self, account_number: str, order, *, access_token: str | None = None) -> SchwabHTTPResponse:
        self.calls.append(("place_order", {"account": account_number, "order": order, "access_token": access_token}))
        if self.place_error is not None:
            raise self.place_error
        return SchwabHTTPResponse(
            status_code=201,
            url="https://api.schwabapi.com/trader/v1/accounts/123456789/orders",
            payload=None,
            headers=httpx.Headers({"Location": "/accounts/123456789/orders/456"}),
        )


def _settings(**overrides) -> SchwabSettings:
    values = {
        "enabled": True,
        "trading_enabled": True,
        "callback_url": "https://api.example.com/api/providers/schwab/connect/callback",
        "client_id": "client-id",
        "client_secret": "client-secret",
        "access_token": "access-token",
        "refresh_token": "refresh-token",
    }
    values.update(overrides)
    return SchwabSettings(**values)


def test_start_and_complete_connect_validates_pending_state() -> None:
    client = _FakeSchwabClient()
    gateway = SchwabGateway(_settings(access_token=None, refresh_token=None), client=client)  # type: ignore[arg-type]

    start = gateway.start_connect(subject="user-123")

    assert start["authorize_url"].startswith("https://schwab.example/authorize?state=")
    complete = gateway.complete_connect(code="auth-code", state=start["state"], subject="user-123")
    assert complete["connected"] is True
    assert complete["has_refresh_token"] is True
    assert ("exchange_authorization_code", "auth-code") in client.calls


def test_complete_connect_rejects_missing_pending_state() -> None:
    gateway = SchwabGateway(_settings(), client=_FakeSchwabClient())  # type: ignore[arg-type]

    with pytest.raises(SchwabGatewayValidationError, match="active Schwab authorization request"):
        gateway.complete_connect(code="auth-code", state="missing-state", subject="user-123")


def test_read_retries_once_after_auth_failure_when_refresh_token_exists() -> None:
    client = _FakeSchwabClient()
    gateway = SchwabGateway(_settings(), client=client)  # type: ignore[arg-type]

    response = gateway.get_account_numbers(subject="user-123")

    assert response == [{"accountNumber": "123456789"}]
    assert client.calls == [
        ("get_account_numbers", "access-token"),
        ("refresh_access_token", "refresh-token"),
        ("get_account_numbers", "refreshed-access"),
    ]


def test_write_maps_network_unknowns_to_ambiguous_outcome() -> None:
    client = _FakeSchwabClient()
    client.place_error = SchwabError("Schwab timeout calling orders")
    gateway = SchwabGateway(_settings(), client=client)  # type: ignore[arg-type]

    with pytest.raises(SchwabGatewayAmbiguousWriteError, match="outcome is unknown"):
        gateway.place_order(
            account_number="123456789",
            order={"orderType": "MARKET"},
            subject="user-123",
        )


def test_write_does_not_retry_broker_auth_failure() -> None:
    client = _FakeSchwabClient()
    client.place_error = SchwabAuthError("expired")
    gateway = SchwabGateway(_settings(), client=client)  # type: ignore[arg-type]

    with pytest.raises(SchwabGatewaySessionExpiredError, match="Reconnect before previewing or trading"):
        gateway.place_order(
            account_number="123456789",
            order={"orderType": "MARKET"},
            subject="user-123",
        )

    assert [call[0] for call in client.calls] == ["place_order"]


def test_place_order_returns_provider_write_metadata() -> None:
    gateway = SchwabGateway(_settings(), client=_FakeSchwabClient())  # type: ignore[arg-type]

    response = gateway.place_order(
        account_number="123456789",
        order={"orderType": "MARKET"},
        subject="user-123",
    )

    assert response == {
        "status_code": 201,
        "location": "/accounts/123456789/orders/456",
        "response": None,
    }
