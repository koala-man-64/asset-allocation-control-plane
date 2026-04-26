from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountListResponse,
    TradeAccountSummary,
    TradeCapabilityFlags,
    TradeDataFreshness,
    TradeOrder,
    TradeOrderCancelResponse,
    TradeOrderHistoryResponse,
    TradeOrderPlaceResponse,
    TradeOrderPreviewResponse,
    TradePosition,
    TradePositionListResponse,
)

from api.service.app import create_app
from api.service.trade_desk_service import TradeDeskService
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _account() -> TradeAccountSummary:
    return TradeAccountSummary(
        accountId="acct-paper",
        name="Core Paper",
        provider="alpaca",
        environment="paper",
        readiness="ready",
        capabilities=TradeCapabilityFlags(
            canReadAccount=True,
            canReadPositions=True,
            canReadOrders=True,
            canReadHistory=True,
            canPreview=True,
            canSubmitPaper=True,
            canCancel=True,
            supportsMarketOrders=True,
            supportsLimitOrders=True,
            supportsEquities=True,
            readOnly=False,
        ),
        cash=100_000,
        buyingPower=100_000,
        freshness=TradeDataFreshness(
            balancesState="fresh",
            positionsState="fresh",
            ordersState="fresh",
            balancesAsOf=_now(),
            positionsAsOf=_now(),
            ordersAsOf=_now(),
        ),
    )


def _order(status: str = "accepted") -> TradeOrder:
    now = _now()
    return TradeOrder(
        orderId="order-1",
        accountId="acct-paper",
        provider="alpaca",
        environment="paper",
        status=status,
        symbol="MSFT",
        side="buy",
        orderType="limit",
        timeInForce="day",
        quantity=10,
        limitPrice=100,
        estimatedNotional=1000,
        createdAt=now,
        updatedAt=now,
    )


async def test_trade_desk_read_endpoints_return_contract_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    account = _account()
    position = TradePosition(accountId="acct-paper", symbol="msft", quantity=10, marketValue=1_000)

    monkeypatch.setattr(TradeDeskService, "list_accounts", lambda self: TradeAccountListResponse(accounts=[account]))
    monkeypatch.setattr(TradeDeskService, "get_account", lambda self, account_id: TradeAccountDetail(account=account))
    monkeypatch.setattr(
        TradeDeskService,
        "list_positions",
        lambda self, account_id: TradePositionListResponse(accountId=account_id, positions=[position]),
    )
    monkeypatch.setattr(
        TradeDeskService,
        "list_orders",
        lambda self, account_id: TradeOrderHistoryResponse(accountId=account_id, orders=[_order()]),
    )
    monkeypatch.setattr(
        TradeDeskService,
        "list_history",
        lambda self, account_id: TradeOrderHistoryResponse(accountId=account_id, orders=[_order("filled")]),
    )

    app = create_app()
    async with get_test_client(app) as client:
        accounts = await client.get("/api/trade-accounts")
        detail = await client.get("/api/trade-accounts/acct-paper")
        positions = await client.get("/api/trade-accounts/acct-paper/positions")
        orders = await client.get("/api/trade-accounts/acct-paper/orders")
        history = await client.get("/api/trade-accounts/acct-paper/history")

    assert accounts.status_code == 200
    assert accounts.json()["accounts"][0]["accountId"] == "acct-paper"
    assert detail.json()["account"]["provider"] == "alpaca"
    assert positions.json()["positions"][0]["symbol"] == "MSFT"
    assert orders.json()["orders"][0]["status"] == "accepted"
    assert history.json()["orders"][0]["status"] == "filled"


async def test_trade_desk_mutations_preserve_idempotency_and_contract_shapes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    now = _now()

    def preview(self, account_id, payload, *, actor, granted_roles=None):
        order = _order("previewed")
        return TradeOrderPreviewResponse(
            previewId="preview-1",
            accountId=account_id,
            provider="alpaca",
            environment=payload.environment,
            order=order,
            generatedAt=now,
            expiresAt=now + timedelta(minutes=5),
            estimatedCost=1000,
        )

    def place(self, account_id, payload, *, actor, granted_roles=None):
        return TradeOrderPlaceResponse(order=_order(), submitted=True, replayed=False, message="accepted")

    def cancel(self, account_id, order_id, payload, *, actor, granted_roles=None):
        return TradeOrderCancelResponse(order=_order("cancel_pending"), cancelAccepted=True, replayed=False)

    monkeypatch.setattr(TradeDeskService, "preview_order", preview)
    monkeypatch.setattr(TradeDeskService, "place_order", place)
    monkeypatch.setattr(TradeDeskService, "cancel_order", cancel)

    app = create_app()
    async with get_test_client(app) as client:
        preview_response = await client.post(
            "/api/trade-accounts/acct-paper/orders/preview",
            json={
                "accountId": "acct-paper",
                "environment": "paper",
                "clientRequestId": "client-1",
                "symbol": "msft",
                "side": "buy",
                "orderType": "limit",
                "timeInForce": "day",
                "quantity": 10,
                "limitPrice": 100,
            },
        )
        place_response = await client.post(
            "/api/trade-accounts/acct-paper/orders",
            json={
                "accountId": "acct-paper",
                "environment": "paper",
                "clientRequestId": "client-2",
                "idempotencyKey": "idem-000000000002",
                "previewId": "preview-1",
                "confirmedAt": now.isoformat(),
                "symbol": "msft",
                "side": "buy",
                "orderType": "limit",
                "timeInForce": "day",
                "quantity": 10,
                "limitPrice": 100,
            },
        )
        cancel_response = await client.post(
            "/api/trade-accounts/acct-paper/orders/order-1/cancel",
            json={
                "accountId": "acct-paper",
                "orderId": "order-1",
                "clientRequestId": "client-3",
                "idempotencyKey": "idem-000000000003",
            },
        )

    assert preview_response.status_code == 200
    assert preview_response.json()["order"]["status"] == "previewed"
    assert preview_response.json()["order"]["symbol"] == "MSFT"
    assert place_response.status_code == 200
    assert place_response.json()["submitted"] is True
    assert cancel_response.status_code == 200
    assert cancel_response.json()["cancelAccepted"] is True
