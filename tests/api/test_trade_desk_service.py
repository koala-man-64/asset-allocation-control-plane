from __future__ import annotations

from datetime import datetime, timezone

import pytest
from asset_allocation_contracts.broker_accounts import BrokerAccountConfiguration, BrokerTradingPolicy
from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountSummary,
    TradeCapabilityFlags,
    TradeDataFreshness,
    TradeOrderPlaceRequest,
    TradeOrderPreviewRequest,
)

from api.service.settings import TradeDeskSettings
from api.service.trade_desk_service import TradeDeskError, TradeDeskService
from core.trade_desk_repository import IdempotencyRecord, TradeAccountRecord, stable_hash


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _account(*, readiness: str = "ready", freshness: TradeDataFreshness | None = None) -> TradeAccountSummary:
    return TradeAccountSummary(
        accountId="acct-paper",
        name="Core Paper",
        provider="alpaca",
        environment="paper",
        readiness=readiness,
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
        freshness=freshness
        or TradeDataFreshness(
            balancesState="fresh",
            positionsState="fresh",
            ordersState="fresh",
            balancesAsOf=_now(),
            positionsAsOf=_now(),
            ordersAsOf=_now(),
        ),
    )


def _configuration(policy: BrokerTradingPolicy, *, version: int = 3) -> BrokerAccountConfiguration:
    return BrokerAccountConfiguration(
        accountId="acct-paper",
        configurationVersion=version,
        requestedPolicy=policy,
        effectivePolicy=policy,
        allocation={"allocatableCapital": 100000.0},
    )


def _risk_codes(checks) -> set[str]:
    return {check.code for check in checks}


class FakeTradeDeskRepository:
    def __init__(self, account: TradeAccountSummary) -> None:
        self.account = account
        self.saved_orders = []
        self.saved_audit_events = []
        self.idempotency: IdempotencyRecord | None = None

    def get_account_record(self, account_id: str):
        if account_id != self.account.accountId:
            return None
        return TradeAccountRecord(
            account=self.account,
            detail=TradeAccountDetail(account=self.account),
            providerAccountKey=None,
        )

    def list_audit_events(self, account_id: str, *, limit: int = 100):
        raise AssertionError("not used")

    def save_order(self, order, *, request_payload, response_payload, request_hash):
        self.saved_orders.append(
            {
                "order": order,
                "request_payload": request_payload,
                "response_payload": response_payload,
                "request_hash": request_hash,
            }
        )

    def get_idempotency(self, account_id: str, action: str, idempotency_key: str):
        return self.idempotency

    def save_idempotency(
        self,
        *,
        account_id: str,
        action: str,
        idempotency_key: str,
        request_hash: str,
        actor: str | None,
        response_payload,
        provider_order_id: str | None,
    ):
        self.idempotency = IdempotencyRecord(requestHash=request_hash, responsePayload=response_payload)

    def save_audit_event(self, event):
        self.saved_audit_events.append(event)

    def get_order(self, account_id: str, order_id: str):
        return self.saved_orders[-1]["order"] if self.saved_orders else None


class FakeConfigurationRepository:
    def __init__(self, configuration: BrokerAccountConfiguration | None) -> None:
        self.configuration = configuration

    def get_configuration(self, account_id: str) -> BrokerAccountConfiguration | None:
        return self.configuration


def test_trade_desk_preview_returns_blocking_risk_checks_for_stale_data() -> None:
    stale_account = _account(
        freshness=TradeDataFreshness(
            balancesState="stale",
            positionsState="fresh",
            ordersState="fresh",
            staleReason="Balances are older than policy.",
        )
    )
    service = TradeDeskService(FakeTradeDeskRepository(stale_account), TradeDeskSettings())

    response = service.preview_order(
        "acct-paper",
        TradeOrderPreviewRequest(
            accountId="acct-paper",
            environment="paper",
            clientRequestId="client-1",
            symbol="MSFT",
            side="buy",
            orderType="market",
            quantity=1,
        ),
        actor="desk@example.com",
    )

    assert response.blocked is True
    assert response.blockReason == "Balances are older than policy."
    assert any(check.code == "balancesState" and check.blocking for check in response.riskChecks)


def test_trade_desk_place_persists_and_replays_idempotent_response() -> None:
    repo = FakeTradeDeskRepository(_account())
    service = TradeDeskService(
        repo,
        TradeDeskSettings(paper_execution_enabled=True, simulated_execution_enabled=True),
    )
    payload = TradeOrderPlaceRequest(
        accountId="acct-paper",
        environment="paper",
        clientRequestId="client-1",
        idempotencyKey="idem-000000000001",
        previewId="preview-1",
        confirmedAt=_now(),
        symbol="MSFT",
        side="buy",
        orderType="market",
        quantity=1,
    )

    first = service.place_order("acct-paper", payload, actor="desk@example.com")
    second = service.place_order("acct-paper", payload, actor="desk@example.com")

    assert first.submitted is True
    assert second.replayed is True
    assert repo.idempotency is not None
    assert repo.idempotency.requestHash == stable_hash(payload.model_dump(mode="json"))

    conflicting_payload = payload.model_copy(update={"quantity": 2})
    with pytest.raises(TradeDeskError, match="Idempotency key"):
        service.place_order("acct-paper", conflicting_payload, actor="desk@example.com")


def test_trade_desk_place_blocks_when_confirmation_policy_is_not_satisfied() -> None:
    repo = FakeTradeDeskRepository(_account())
    configuration = _configuration(BrokerTradingPolicy(requireOrderConfirmation=True, allowedSides=["long"]))
    service = TradeDeskService(
        repo,
        TradeDeskSettings(paper_execution_enabled=True, simulated_execution_enabled=True),
        confirmation_release_required_roles=["AssetAllocation.TradeConfirmation.Release"],
        configuration_repository=FakeConfigurationRepository(configuration),
    )
    payload = TradeOrderPlaceRequest(
        accountId="acct-paper",
        environment="paper",
        clientRequestId="client-1",
        idempotencyKey="idem-000000000001",
        previewId="preview-1",
        confirmedAt=_now(),
        symbol="MSFT",
        side="buy",
        orderType="market",
        quantity=1,
        policyVersion=2,
        orderHash="stale-hash",
        confirmationToken="stale-token",
    )

    response = service.place_order(
        "acct-paper",
        payload,
        actor="desk@example.com",
        granted_roles=[],
    )

    assert response.submitted is False
    assert response.confirmationRequired is True
    assert response.policyVersion == 3
    assert response.message == "The account policy changed after preview; re-preview before submitting."


def test_trade_desk_preview_enforces_trading_policy_opening_order_limits() -> None:
    account = _account().model_copy(update={"positionCount": 1, "openOrderCount": 0})
    repo = FakeTradeDeskRepository(account)
    configuration = _configuration(
        BrokerTradingPolicy(
            maxOpenPositions=1,
            maxSinglePositionExposure={"mode": "notional_base_ccy", "value": 1000.0},
            allowedSides=["long"],
            allowedAssetClasses=["equity"],
        )
    )
    service = TradeDeskService(
        repo,
        TradeDeskSettings(paper_execution_enabled=True, simulated_execution_enabled=True),
        configuration_repository=FakeConfigurationRepository(configuration),
    )

    response = service.preview_order(
        "acct-paper",
        TradeOrderPreviewRequest(
            accountId="acct-paper",
            environment="paper",
            clientRequestId="client-policy-limits",
            symbol="MSFT",
            side="sell",
            orderType="limit",
            assetClass="option",
            quantity=10,
            limitPrice=200,
        ),
        actor="desk@example.com",
    )

    assert response.blocked is True
    assert {
        "account_policy_side",
        "account_policy_asset_class",
        "account_policy_max_open_positions",
        "account_policy_max_single_position_exposure",
    }.issubset(_risk_codes(response.riskChecks))


def test_trade_desk_place_blocks_stale_confirmation_token() -> None:
    repo = FakeTradeDeskRepository(_account())
    configuration = _configuration(BrokerTradingPolicy(requireOrderConfirmation=True, allowedSides=["long"]))
    service = TradeDeskService(
        repo,
        TradeDeskSettings(paper_execution_enabled=True, simulated_execution_enabled=True),
        confirmation_release_required_roles=["AssetAllocation.TradeConfirmation.Release"],
        configuration_repository=FakeConfigurationRepository(configuration),
    )
    preview_payload = TradeOrderPreviewRequest(
        accountId="acct-paper",
        environment="paper",
        clientRequestId="client-confirm-token",
        symbol="MSFT",
        side="buy",
        orderType="market",
        quantity=1,
    )
    preview = service.preview_order("acct-paper", preview_payload, actor="desk@example.com")

    response = service.place_order(
        "acct-paper",
        TradeOrderPlaceRequest(
            **preview_payload.model_dump(mode="python"),
            idempotencyKey="idem-000000000001",
            previewId=preview.previewId,
            confirmedAt=_now(),
            policyVersion=preview.policyVersion,
            orderHash=preview.orderHash,
            confirmationToken="stale-token",
        ),
        actor="desk@example.com",
        granted_roles=["AssetAllocation.TradeConfirmation.Release"],
    )

    assert response.submitted is False
    assert response.confirmationRequired is True
    assert response.message == "Order confirmation token is missing or stale; re-preview before submitting."
    assert "stale_confirmation_token" in _risk_codes(response.order.riskChecks)
