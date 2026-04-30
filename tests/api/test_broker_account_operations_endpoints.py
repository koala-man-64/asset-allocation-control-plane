from __future__ import annotations

from datetime import datetime, timezone

import pytest
from asset_allocation_contracts.broker_accounts import (
    BrokerAccountConfiguration,
    BrokerAccountDetail,
    BrokerAccountListResponse,
    BrokerAccountSummary,
    BrokerCapabilityFlags,
    BrokerConnectionHealth,
)

from api.service.app import create_app
from api.service.broker_account_operations_service import (
    BrokerAccountOperationsError,
    BrokerAccountOperationsService,
)
from tests.api._client import get_test_client


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _summary() -> BrokerAccountSummary:
    return BrokerAccountSummary(
        accountId="acct-paper",
        broker="alpaca",
        name="Core Paper",
        accountNumberMasked="****1234",
        baseCurrency="USD",
        overallStatus="healthy",
        tradeReadiness="ready",
        connectionHealth=BrokerConnectionHealth(
            overallStatus="healthy",
            authStatus="authenticated",
            connectionState="connected",
            syncStatus="fresh",
            lastCheckedAt=_now(),
            lastSuccessfulSyncAt=_now(),
        ),
        equity=125_000,
        cash=100_000,
        buyingPower=100_000,
        openPositionCount=3,
        openOrderCount=1,
        lastSyncedAt=_now(),
        snapshotAsOf=_now(),
        configurationVersion=2,
        alertCount=0,
    )


def _configuration() -> BrokerAccountConfiguration:
    return BrokerAccountConfiguration(
        accountId="acct-paper",
        accountName="Core Paper",
        baseCurrency="USD",
        configurationVersion=2,
        capabilities=BrokerCapabilityFlags(
            canReadBalances=True,
            canReadPositions=True,
            canReadOrders=True,
            canReadTradingPolicy=True,
            canWriteTradingPolicy=True,
            canReadAllocation=True,
            canWriteAllocation=True,
        ),
    )


def _detail() -> BrokerAccountDetail:
    return BrokerAccountDetail(
        account=_summary(),
        capabilities=BrokerCapabilityFlags(
            canReadBalances=True,
            canReadPositions=True,
            canReadOrders=True,
            canReadTradingPolicy=True,
            canWriteTradingPolicy=True,
            canReadAllocation=True,
            canWriteAllocation=True,
        ),
        accountType="paper",
        tradingBlocked=False,
        alerts=[],
        syncRuns=[],
        recentActivity=[],
        configuration=_configuration(),
    )


@pytest.mark.asyncio
async def test_broker_account_read_endpoints_return_contract_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(
        BrokerAccountOperationsService,
        "list_accounts",
        lambda self: BrokerAccountListResponse(accounts=[_summary()], generatedAt=_now()),
    )
    monkeypatch.setattr(BrokerAccountOperationsService, "get_account", lambda self, account_id: _detail())

    app = create_app()
    async with get_test_client(app) as client:
        accounts_response = await client.get("/api/broker-accounts")
        detail_response = await client.get("/api/broker-accounts/acct-paper")

    assert accounts_response.status_code == 200
    assert accounts_response.headers["cache-control"] == "no-store"
    accounts_payload = accounts_response.json()
    assert accounts_payload["accounts"][0]["accountId"] == "acct-paper"
    assert accounts_payload["accounts"][0]["broker"] == "alpaca"
    assert accounts_payload["accounts"][0]["configurationVersion"] == 2
    assert accounts_payload["generatedAt"] is not None

    assert detail_response.status_code == 200
    assert detail_response.headers["cache-control"] == "no-store"
    detail_payload = detail_response.json()
    assert detail_payload["account"]["accountId"] == "acct-paper"
    assert detail_payload["configuration"]["configurationVersion"] == 2


@pytest.mark.asyncio
async def test_broker_account_detail_unknown_account_returns_404(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def get_account(self, account_id):
        raise BrokerAccountOperationsError(404, f"Trade account '{account_id}' not found.")

    monkeypatch.setattr(BrokerAccountOperationsService, "get_account", get_account)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/broker-accounts/missing")

    assert response.status_code == 404
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["detail"] == "Trade account 'missing' not found."


@pytest.mark.asyncio
async def test_broker_account_actions_return_501_not_route_404(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    def unsupported(self, *args, **kwargs):
        raise BrokerAccountOperationsError(501, "Action is not implemented in Account Operations v1.")

    monkeypatch.setattr(BrokerAccountOperationsService, "reconnect_account", unsupported)
    monkeypatch.setattr(BrokerAccountOperationsService, "set_sync_paused", unsupported)
    monkeypatch.setattr(BrokerAccountOperationsService, "refresh_account", unsupported)
    monkeypatch.setattr(BrokerAccountOperationsService, "acknowledge_alert", unsupported)

    app = create_app()
    async with get_test_client(app) as client:
        responses = [
            await client.post("/api/broker-accounts/acct-paper/reconnect", json={"reason": "operator request"}),
            await client.post(
                "/api/broker-accounts/acct-paper/sync/pause",
                json={"paused": True, "reason": "operator request"},
            ),
            await client.post(
                "/api/broker-accounts/acct-paper/sync/resume",
                json={"paused": False, "reason": "operator request"},
            ),
            await client.post(
                "/api/broker-accounts/acct-paper/refresh",
                json={"scope": "full", "force": True, "reason": "operator request"},
            ),
            await client.post(
                "/api/broker-accounts/acct-paper/alerts/alert-1/acknowledge",
                json={"note": "reviewed"},
            ),
        ]

    for response in responses:
        assert response.status_code == 501
        assert response.status_code != 404
        assert response.headers["cache-control"] == "no-store"
        assert "not implemented" in response.json()["detail"]


@pytest.mark.asyncio
async def test_broker_account_routes_exist_before_auth_and_return_401(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://login.example.test/tenant")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "api://asset-allocation")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    app = create_app()
    async with get_test_client(app) as client:
        responses = [
            await client.get("/api/broker-accounts"),
            await client.get("/api/broker-accounts/acct-paper"),
            await client.post(
                "/api/broker-accounts/acct-paper/refresh",
                json={"scope": "full", "force": True, "reason": "operator request"},
            ),
        ]

    for response in responses:
        assert response.status_code == 401


def test_broker_account_openapi_includes_operations_routes() -> None:
    schema = create_app().openapi()
    paths = schema["paths"]

    for path in (
        "/api/broker-accounts",
        "/api/broker-accounts/{account_id}",
        "/api/broker-accounts/{account_id}/configuration",
        "/api/broker-accounts/{account_id}/trading-policy",
        "/api/broker-accounts/{account_id}/allocation",
        "/api/broker-accounts/{account_id}/reconnect",
        "/api/broker-accounts/{account_id}/sync/pause",
        "/api/broker-accounts/{account_id}/sync/resume",
        "/api/broker-accounts/{account_id}/refresh",
        "/api/broker-accounts/{account_id}/alerts/{alert_id}/acknowledge",
    ):
        assert path in paths
