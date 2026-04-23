from __future__ import annotations

from datetime import datetime, timezone

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from tests.api._auth import install_auth_stub
from tests.api._client import get_test_client


def _configure_oidc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")


def _configure_paper_alpaca(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPACA_PAPER_API_KEY_ID", "paper-key")
    monkeypatch.setenv("ALPACA_PAPER_SECRET_KEY", "paper-secret")


def _sample_order_payload() -> dict[str, object]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": "order-1",
        "client_order_id": "client-1",
        "symbol": "AAPL",
        "created_at": now,
        "updated_at": now,
        "submitted_at": now,
        "filled_at": None,
        "expired_at": None,
        "canceled_at": None,
        "failed_at": None,
        "asset_id": "asset-1",
        "asset_class": "us_equity",
        "qty": 5,
        "filled_qty": 0,
        "type": "market",
        "side": "buy",
        "time_in_force": "day",
        "limit_price": None,
        "stop_price": None,
        "status": "new",
    }


@pytest.mark.asyncio
async def test_alpaca_live_route_returns_503_when_live_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    _configure_paper_alpaca(monkeypatch)

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-123",
            claims={"roles": ["AssetAllocation.Access"]},
        ),
    )

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/alpaca/account",
            params={"environment": "live"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 503
    assert response.json()["detail"] == "Alpaca live credentials are not configured."


@pytest.mark.asyncio
async def test_alpaca_account_route_calls_gateway(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    _configure_paper_alpaca(monkeypatch)

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-123",
            claims={"roles": ["AssetAllocation.Access"]},
        ),
    )
    app.state.alpaca_gateway.get_account = lambda **kwargs: {  # type: ignore[method-assign]
        "id": "acct-1",
        "account_number": "ACC-1",
        "status": "ACTIVE",
        "currency": "USD",
        "cash": 1000.0,
        "equity": 1250.0,
        "buying_power": 2500.0,
        "daytrade_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/alpaca/account",
            params={"environment": "paper"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json()["id"] == "acct-1"


@pytest.mark.asyncio
async def test_alpaca_submit_order_requires_trade_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    _configure_paper_alpaca(monkeypatch)
    monkeypatch.setenv("ALPACA_TRADING_REQUIRED_ROLES", "AssetAllocation.Alpaca.Trade")

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-123",
            claims={"roles": ["AssetAllocation.Access"]},
        ),
    )

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/alpaca/orders",
            json={
                "environment": "paper",
                "symbol": "AAPL",
                "qty": 1,
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing required roles: AssetAllocation.Alpaca.Trade."


@pytest.mark.asyncio
async def test_alpaca_submit_order_calls_gateway_when_role_present(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    _configure_paper_alpaca(monkeypatch)
    monkeypatch.setenv("ALPACA_TRADING_REQUIRED_ROLES", "AssetAllocation.Alpaca.Trade")

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-123",
            claims={"roles": ["AssetAllocation.Access", "AssetAllocation.Alpaca.Trade"]},
        ),
    )
    captured: dict[str, object] = {}

    def _submit_order(**kwargs):
        captured.update(kwargs)
        return _sample_order_payload()

    app.state.alpaca_gateway.submit_order = _submit_order  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.post(
            "/api/providers/alpaca/orders",
            json={
                "environment": "paper",
                "symbol": "aapl",
                "qty": 1,
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
                "client_order_id": " client-1 ",
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json()["id"] == "order-1"
    assert captured == {
        "environment": "paper",
        "order": {
            "symbol": "AAPL",
            "qty": 1.0,
            "side": "buy",
            "type": "market",
            "time_in_force": "day",
            "client_order_id": "client-1",
        },
        "subject": "user-123",
    }


@pytest.mark.asyncio
async def test_alpaca_orders_route_parses_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    _configure_paper_alpaca(monkeypatch)

    app = create_app()
    install_auth_stub(
        monkeypatch,
        app.state.auth,
        auth_context=AuthContext(
            mode="oidc",
            subject="user-123",
            claims={"roles": ["AssetAllocation.Access"]},
        ),
    )
    captured: dict[str, object] = {}

    def _list_orders(**kwargs):
        captured.update(kwargs)
        return [_sample_order_payload()]

    app.state.alpaca_gateway.list_orders = _list_orders  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/alpaca/orders",
            params={
                "environment": "paper",
                "status": "all",
                "limit": 10,
                "nested": "true",
                "symbols": "aapl,msft,aapl",
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json()[0]["id"] == "order-1"
    assert captured == {
        "environment": "paper",
        "subject": "user-123",
        "status": "all",
        "limit": 10,
        "after": None,
        "until": None,
        "nested": True,
        "symbols": ["AAPL", "MSFT"],
    }
