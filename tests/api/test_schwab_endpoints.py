from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from api.service.schwab_gateway import SchwabGatewaySessionExpiredError
from tests.api._auth import install_auth_stub
from tests.api._client import get_test_client


def _configure_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")


def _install_auth(monkeypatch: pytest.MonkeyPatch, app, roles: list[str] | None = None) -> None:
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-123",
            claims={"roles": roles or ["AssetAllocation.Access"]},
        ),
    )


@pytest.mark.asyncio
async def test_schwab_callback_route_is_unauthenticated(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)

    app = create_app()
    app.state.schwab_gateway.complete_connect_from_callback = lambda **kwargs: {  # type: ignore[method-assign]
        "connected": True,
        "state": kwargs["state"],
    }

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback",
            params={"code": "auth-code-123", "state": "opaque", "session": "session-1"},
        )

    assert response.status_code == 200
    assert response.json() == {"connected": True, "state": "opaque"}
    assert "auth-code-123" not in response.text


@pytest.mark.asyncio
async def test_schwab_callback_route_rejects_unmatched_state(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback",
            params={"code": "auth-code-123", "state": "opaque"},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "The callback did not match an active Schwab authorization request."


@pytest.mark.asyncio
async def test_schwab_callback_url_route_requires_auth_when_deployed(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/providers/schwab/connect/callback-url")

    assert response.status_code == 401
    assert response.headers.get("www-authenticate") == "Bearer"


@pytest.mark.asyncio
async def test_schwab_callback_url_route_returns_resolved_url(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("API_PUBLIC_BASE_URL", "https://api.example.com")

    app = create_app()
    _install_auth(monkeypatch, app)

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback-url",
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"callback_url": "https://api.example.com/api/providers/schwab/connect/callback"}


@pytest.mark.asyncio
async def test_schwab_callback_url_route_returns_503_when_unresolved(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)

    app = create_app()
    _install_auth(monkeypatch, app)

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/connect/callback-url",
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Schwab callback URL is not configured. Set SCHWAB_APP_CALLBACK_URL or API_PUBLIC_BASE_URL."


@pytest.mark.asyncio
async def test_schwab_read_routes_return_503_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHWAB_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/providers/schwab/account-numbers")

    assert response.status_code == 503
    assert response.json()["detail"] == "Schwab integration is disabled."


@pytest.mark.asyncio
async def test_schwab_account_numbers_route_calls_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("SCHWAB_ENABLED", "true")

    app = create_app()
    _install_auth(monkeypatch, app)
    app.state.schwab_gateway.get_account_numbers = lambda **kwargs: [  # type: ignore[method-assign]
        {"accountNumber": "123456789", "hashValue": "hash-1", "subject": kwargs["subject"]}
    ]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/account-numbers",
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == [{"accountNumber": "123456789", "hashValue": "hash-1", "subject": "user-123"}]


@pytest.mark.asyncio
async def test_schwab_account_numbers_route_returns_reconnect_payload_when_session_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("SCHWAB_ENABLED", "true")

    app = create_app()
    _install_auth(monkeypatch, app)

    def _missing_session(**_kwargs):
        raise SchwabGatewaySessionExpiredError(
            "No active Schwab broker session exists. Connect first.",
            payload={
                "connect_required": True,
                "authorize_url": "https://schwab.example/authorize?state=opaque",
                "state": "opaque",
                "state_expires_at": "2026-04-24T16:00:00Z",
            },
        )

    app.state.schwab_gateway.get_account_numbers = _missing_session  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/account-numbers",
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "message": "No active Schwab broker session exists. Connect first.",
        "connect_required": True,
        "authorize_url": "https://schwab.example/authorize?state=opaque",
        "state": "opaque",
        "state_expires_at": "2026-04-24T16:00:00Z",
    }


@pytest.mark.asyncio
async def test_schwab_preview_route_requires_trading_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCHWAB_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/schwab/accounts/123456789/orders/preview",
            json={"order": {"orderType": "MARKET", "session": "NORMAL"}},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Schwab trading is disabled."


@pytest.mark.asyncio
async def test_schwab_preview_route_requires_trade_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("SCHWAB_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_REQUIRED_ROLES", "AssetAllocation.Schwab.Trade")

    app = create_app()
    _install_auth(monkeypatch, app)

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/schwab/accounts/123456789/orders/preview",
            json={"order": {"orderType": "MARKET", "session": "NORMAL"}},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing required roles: AssetAllocation.Schwab.Trade."


@pytest.mark.asyncio
async def test_schwab_preview_route_calls_gateway_when_role_present(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("SCHWAB_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_REQUIRED_ROLES", "AssetAllocation.Schwab.Trade")

    app = create_app()
    _install_auth(monkeypatch, app, ["AssetAllocation.Access", "AssetAllocation.Schwab.Trade"])
    captured: dict[str, object] = {}

    def _preview_order(**kwargs):
        captured.update(kwargs)
        return {"previewId": "preview-1"}

    app.state.schwab_gateway.preview_order = _preview_order  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/schwab/accounts/123456789/orders/preview",
            json={"order": {"orderType": "MARKET", "session": "NORMAL"}},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"previewId": "preview-1"}
    assert captured == {
        "account_number": "123456789",
        "order": {"orderType": "MARKET", "session": "NORMAL"},
        "subject": "user-123",
    }


@pytest.mark.asyncio
async def test_schwab_transactions_route_uses_read_access_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("SCHWAB_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_ENABLED", "true")
    monkeypatch.setenv("SCHWAB_TRADING_REQUIRED_ROLES", "AssetAllocation.Schwab.Trade")

    app = create_app()
    _install_auth(monkeypatch, app)
    captured: dict[str, object] = {}

    def _list_transactions(**kwargs):
        captured.update(kwargs)
        return [{"activityId": 123, "type": "TRADE"}]

    app.state.schwab_gateway.list_transactions = _list_transactions  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/schwab/accounts/123456789/transactions",
            params={
                "startDate": "2026-04-01T00:00:00Z",
                "endDate": "2026-04-19T23:59:59Z",
                "types": "TRADE",
                "symbol": "AAPL",
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == [{"activityId": 123, "type": "TRADE"}]
    assert captured == {
        "account_number": "123456789",
        "subject": "user-123",
        "start_date": "2026-04-01T00:00:00Z",
        "end_date": "2026-04-19T23:59:59Z",
        "types": "TRADE",
        "symbol": "AAPL",
    }
