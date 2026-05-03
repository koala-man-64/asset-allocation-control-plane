from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from kalshi import KalshiAccountLimits, KalshiInvalidResponseError
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
async def test_kalshi_read_routes_return_503_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/providers/kalshi/markets", params={"environment": "demo"})

    assert response.status_code == 503
    assert response.json()["detail"] == "Kalshi integration is disabled."


@pytest.mark.asyncio
async def test_kalshi_market_route_calls_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("KALSHI_ENABLED", "true")

    app = create_app()
    _install_auth(monkeypatch, app)
    captured: dict[str, object] = {}

    def _get_market(**kwargs):
        captured.update(kwargs)
        return {"ticker": "KXTEST-1", "event_ticker": "KXTEST", "status": "open"}

    app.state.kalshi_gateway.get_market = _get_market  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/kalshi/markets/KXTEST-1",
            params={"environment": "demo"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"ticker": "KXTEST-1", "event_ticker": "KXTEST", "status": "open"}
    assert captured == {"environment": "demo", "ticker": "KXTEST-1", "subject": "user-123"}


@pytest.mark.asyncio
async def test_kalshi_account_limits_route_calls_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("KALSHI_ENABLED", "true")

    app = create_app()
    _install_auth(monkeypatch, app)
    captured: dict[str, object] = {}

    def _get_account_limits(**kwargs):
        captured.update(kwargs)
        return KalshiAccountLimits(usage_tier="basic", read_limit=200, write_limit=100)

    app.state.kalshi_gateway.get_account_limits = _get_account_limits  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/kalshi/account/limits",
            params={"environment": "live"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"usage_tier": "basic", "read_limit": 200, "write_limit": 100}
    assert response.headers["Cache-Control"] == "no-store"
    assert captured == {"environment": "live", "subject": "user-123"}


@pytest.mark.asyncio
async def test_kalshi_account_limits_route_returns_503_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/kalshi/account/limits",
            params={"environment": "live"},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Kalshi integration is disabled."


@pytest.mark.asyncio
async def test_kalshi_account_limits_invalid_provider_response_returns_502(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("KALSHI_ENABLED", "true")

    app = create_app()
    _install_auth(monkeypatch, app)

    def _get_account_limits(**kwargs):
        raise KalshiInvalidResponseError("Kalshi account limits response was invalid.")

    app.state.kalshi_gateway.get_account_limits = _get_account_limits  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/kalshi/account/limits",
            params={"environment": "live"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 502
    assert response.json()["detail"] == "Kalshi account limits response was invalid."


@pytest.mark.asyncio
async def test_kalshi_create_order_requires_trading_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KALSHI_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TRADING_ENABLED", "false")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/kalshi/orders",
            json={
                "environment": "demo",
                "ticker": "KXTEST-1",
                "side": "yes",
                "action": "buy",
                "count": 1,
                "yes_price": 1,
            },
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Kalshi trading is disabled."


@pytest.mark.asyncio
async def test_kalshi_create_order_requires_trade_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("KALSHI_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TRADING_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TRADING_REQUIRED_ROLES", "AssetAllocation.Kalshi.Trade")

    app = create_app()
    _install_auth(monkeypatch, app)

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/kalshi/orders",
            json={
                "environment": "demo",
                "ticker": "KXTEST-1",
                "side": "yes",
                "action": "buy",
                "count": 1,
                "yes_price": 1,
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing required roles: AssetAllocation.Kalshi.Trade."


@pytest.mark.asyncio
async def test_kalshi_create_order_normalizes_fixed_point_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("KALSHI_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TRADING_ENABLED", "true")
    monkeypatch.setenv("KALSHI_TRADING_REQUIRED_ROLES", "AssetAllocation.Kalshi.Trade")

    app = create_app()
    _install_auth(monkeypatch, app, ["AssetAllocation.Access", "AssetAllocation.Kalshi.Trade"])
    captured: dict[str, object] = {}

    def _create_order(**kwargs):
        captured.update(kwargs)
        return {
            "order_id": "order-1",
            "ticker": kwargs["order"]["ticker"],
            "side": kwargs["order"]["side"],
            "action": kwargs["order"]["action"],
            "status": "resting",
            "yes_price_dollars": kwargs["order"]["yes_price_dollars"],
            "initial_count_fp": kwargs["order"]["count_fp"],
        }

    app.state.kalshi_gateway.create_order = _create_order  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/kalshi/orders",
            json={
                "environment": "demo",
                "ticker": "kxtest-1",
                "side": "yes",
                "action": "buy",
                "count_fp": "1.5",
                "yes_price_dollars": "0.56",
                "client_order_id": " client-1 ",
                "post_only": True,
                "reduce_only": True,
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "order_id": "order-1",
        "ticker": "KXTEST-1",
        "side": "yes",
        "action": "buy",
        "status": "resting",
        "yes_price_dollars": "0.5600",
        "initial_count_fp": "1.50",
    }
    assert captured == {
        "environment": "demo",
        "order": {
            "ticker": "KXTEST-1",
            "side": "yes",
            "action": "buy",
            "type": "limit",
            "client_order_id": "client-1",
            "count_fp": "1.50",
            "yes_price_dollars": "0.5600",
            "subaccount": 0,
            "post_only": True,
            "reduce_only": True,
        },
        "subject": "user-123",
    }
