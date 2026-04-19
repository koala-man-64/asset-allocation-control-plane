from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from tests.api._client import get_test_client


def _configure_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")


@pytest.mark.asyncio
async def test_etrade_read_routes_return_503_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETRADE_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/providers/etrade/accounts?environment=sandbox")

    assert response.status_code == 503
    assert response.json()["detail"] == "E*TRADE integration is disabled."


@pytest.mark.asyncio
async def test_etrade_preview_route_requires_trading_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETRADE_ENABLED", "true")
    monkeypatch.setenv("ETRADE_TRADING_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/etrade/orders/preview",
            json={
                "environment": "sandbox",
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

    assert response.status_code == 503
    assert response.json()["detail"] == "E*TRADE trading is disabled."


@pytest.mark.asyncio
async def test_etrade_preview_route_requires_trade_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")
    monkeypatch.setenv("ETRADE_TRADING_ENABLED", "true")
    monkeypatch.setenv("ETRADE_TRADING_REQUIRED_ROLES", "AssetAllocation.ETrade.Trade")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/etrade/orders/preview",
            json={
                "environment": "sandbox",
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
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing required roles: AssetAllocation.ETrade.Trade."


@pytest.mark.asyncio
async def test_etrade_callback_route_completes_without_outer_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ETRADE_ENABLED", "true")

    app = create_app()
    app.state.etrade_gateway.complete_connect_from_callback = lambda **kwargs: {  # type: ignore[method-assign]
        "environment": "sandbox",
        "connected": True,
        "oauth_token": kwargs["request_token"],
    }

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/connect/callback?oauth_token=request-token&oauth_verifier=verifier-123"
        )

    assert response.status_code == 200
    assert response.json()["connected"] is True
    assert response.json()["oauth_token"] == "request-token"


@pytest.mark.asyncio
async def test_etrade_preview_route_calls_gateway_when_role_present(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")
    monkeypatch.setenv("ETRADE_TRADING_ENABLED", "true")
    monkeypatch.setenv("ETRADE_TRADING_REQUIRED_ROLES", "AssetAllocation.ETrade.Trade")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access", "AssetAllocation.ETrade.Trade"]},
    )
    app.state.etrade_gateway.preview_order = lambda **kwargs: {  # type: ignore[method-assign]
        "environment": kwargs["environment"],
        "preview_id": "2785277279",
        "response": {"ok": True},
    }

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/etrade/orders/preview",
            json={
                "environment": "sandbox",
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
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json()["preview_id"] == "2785277279"
