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


@pytest.mark.asyncio
async def test_etrade_accounts_route_returns_204(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )
    app.state.etrade_gateway.list_accounts = lambda **kwargs: None  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/accounts",
            params={"environment": "sandbox"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 204
    assert response.text == ""


@pytest.mark.asyncio
async def test_etrade_balance_route_defaults_real_time_nav_to_false(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )
    captured: dict[str, object] = {}

    def _get_balance(**kwargs):
        captured.update(kwargs)
        return {"BalanceResponse": {"accountId": "1"}}

    app.state.etrade_gateway.get_balance = _get_balance  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/accounts/acct-key/balance",
            params={"environment": "sandbox"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"BalanceResponse": {"accountId": "1"}}
    assert captured["real_time_nav"] is False


@pytest.mark.asyncio
async def test_etrade_balance_route_allows_real_time_nav_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )
    captured: dict[str, object] = {}

    def _get_balance(**kwargs):
        captured.update(kwargs)
        return {"BalanceResponse": {"accountId": "1"}}

    app.state.etrade_gateway.get_balance = _get_balance  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/accounts/acct-key/balance",
            params={"environment": "sandbox", "real_time_nav": "true"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert captured["real_time_nav"] is True


@pytest.mark.asyncio
async def test_etrade_portfolio_route_returns_204(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )
    app.state.etrade_gateway.get_portfolio = lambda **kwargs: None  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/accounts/acct-key/portfolio",
            params={"environment": "sandbox"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 204
    assert response.text == ""


@pytest.mark.asyncio
async def test_etrade_transactions_route_uses_read_access_only(monkeypatch: pytest.MonkeyPatch) -> None:
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

    captured: dict[str, object] = {}

    def _list_transactions(**kwargs):
        captured.update(kwargs)
        return {"TransactionDetailsResponse": {"Transaction": []}}

    app.state.etrade_gateway.list_transactions = _list_transactions  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/accounts/acct-key/transactions",
            params={
                "environment": "sandbox",
                "startDate": "2026-04-01",
                "endDate": "2026-04-19",
                "sortOrder": "desc",
                "marker": "12345",
                "count": 25,
                "transactionGroup": "trades",
            },
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 200
    assert response.json() == {"TransactionDetailsResponse": {"Transaction": []}}
    assert captured == {
        "environment": "sandbox",
        "account_key": "acct-key",
        "subject": "user-123",
        "start_date": "2026-04-01",
        "end_date": "2026-04-19",
        "sort_order": "desc",
        "marker": "12345",
        "count": 25,
        "transaction_group": "trades",
    }


@pytest.mark.asyncio
async def test_etrade_transaction_details_route_returns_204(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_oidc(monkeypatch)
    monkeypatch.setenv("ETRADE_ENABLED", "true")

    app = create_app()
    app.state.auth.authenticate_headers = lambda _headers: AuthContext(  # type: ignore[method-assign]
        mode="oidc",
        subject="user-123",
        claims={"roles": ["AssetAllocation.Access"]},
    )

    captured: dict[str, object] = {}

    def _get_transaction_details(**kwargs):
        captured.update(kwargs)
        return None

    app.state.etrade_gateway.get_transaction_details = _get_transaction_details  # type: ignore[method-assign]

    async with get_test_client(app) as client:
        response = await client.get(
            "/api/providers/etrade/accounts/acct-key/transactions/123",
            params={"environment": "sandbox", "storeId": "store-1"},
            headers={"Authorization": "Bearer placeholder"},
        )

    assert response.status_code == 204
    assert response.text == ""
    assert captured == {
        "environment": "sandbox",
        "account_key": "acct-key",
        "transaction_id": "123",
        "subject": "user-123",
        "store_id": "store-1",
    }
