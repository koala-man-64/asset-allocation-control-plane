from __future__ import annotations

from datetime import datetime, timezone

import pytest
from asset_allocation_contracts.broker_accounts import (
    BrokerAccountConfiguration,
    BrokerCapabilityFlags,
    BrokerStrategyAllocationItem,
    BrokerStrategyAllocationSummary,
)
from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountListResponse,
    TradeAccountSummary,
    TradeCapabilityFlags,
    TradeDataFreshness,
)

from api.service.broker_account_operations_service import (
    BrokerAccountOperationsError,
    BrokerAccountOperationsService,
)
from api.service.settings import TradeDeskSettings
from core.trade_desk_repository import TradeAccountRecord


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _trade_account() -> TradeAccountSummary:
    timestamp = _now()
    return TradeAccountSummary(
        accountId="acct-paper",
        name="Core Paper",
        provider="alpaca",
        environment="paper",
        accountNumberMasked="****1234",
        baseCurrency="USD",
        readiness="ready",
        capabilities=TradeCapabilityFlags(
            canReadAccount=True,
            canReadPositions=True,
            canReadOrders=True,
            canPreview=True,
            canSubmitPaper=True,
            supportsMarketOrders=True,
            supportsLimitOrders=True,
            supportsEquities=True,
            readOnly=False,
        ),
        cash=100_000,
        buyingPower=100_000,
        equity=125_000,
        openOrderCount=2,
        positionCount=4,
        lastSyncedAt=timestamp,
        snapshotAsOf=timestamp,
        freshness=TradeDataFreshness(
            balancesState="fresh",
            positionsState="fresh",
            ordersState="fresh",
            balancesAsOf=timestamp,
            positionsAsOf=timestamp,
            ordersAsOf=timestamp,
        ),
    )


def _configuration() -> BrokerAccountConfiguration:
    return BrokerAccountConfiguration(
        accountId="acct-paper",
        accountName="Core Paper",
        baseCurrency="USD",
        configurationVersion=5,
        capabilities=BrokerCapabilityFlags(
            canReadBalances=True,
            canReadPositions=True,
            canReadOrders=True,
            canTrade=True,
            canReconnect=True,
            canPauseSync=True,
            canRefresh=True,
            canAcknowledgeAlerts=True,
            canReadTradingPolicy=True,
            canWriteTradingPolicy=True,
            canReadAllocation=True,
            canWriteAllocation=True,
            canReleaseTradeConfirmation=True,
        ),
        allocation=BrokerStrategyAllocationSummary(
            portfolioName="core-balanced",
            portfolioVersion=2,
            allocationMode="percent",
            allocatableCapital=100_000,
            allocatedPercent=100,
            remainingPercent=0,
            items=[
                BrokerStrategyAllocationItem(
                    sleeveId="core",
                    sleeveName="Core",
                    strategy={"strategyName": "quality-trend", "strategyVersion": 4},
                    allocationMode="percent",
                    targetWeightPct=100,
                )
            ],
        ),
    )


class FakeTradeRepo:
    def __init__(self, account: TradeAccountSummary | None) -> None:
        self._account = account

    def list_accounts(self) -> TradeAccountListResponse:
        return TradeAccountListResponse(accounts=[] if self._account is None else [self._account], generatedAt=_now())

    def get_account_record(self, account_id: str) -> TradeAccountRecord | None:
        if self._account is None or account_id != self._account.accountId:
            return None
        return TradeAccountRecord(
            account=self._account,
            detail=TradeAccountDetail(account=self._account),
            providerAccountKey="provider-key",
        )


class FakeConfigurationService:
    def __init__(self, configuration: BrokerAccountConfiguration | None) -> None:
        self._configuration = configuration

    def get_configuration(self, account_id: str) -> BrokerAccountConfiguration | None:
        return self._configuration


def _service(
    account: TradeAccountSummary | None = None,
    configuration: BrokerAccountConfiguration | None = None,
) -> BrokerAccountOperationsService:
    return BrokerAccountOperationsService(
        FakeTradeRepo(account or _trade_account()),
        TradeDeskSettings(),
        FakeConfigurationService(configuration or _configuration()),
    )


def test_broker_account_operations_maps_trade_account_summary_into_broker_contracts() -> None:
    response = _service().list_accounts()

    assert len(response.accounts) == 1
    account = response.accounts[0]
    assert account.accountId == "acct-paper"
    assert account.broker == "alpaca"
    assert account.tradeReadiness == "ready"
    assert account.connectionHealth.overallStatus == "healthy"
    assert account.connectionHealth.connectionState == "connected"
    assert account.connectionHealth.syncStatus == "fresh"
    assert account.openPositionCount == 4
    assert account.openOrderCount == 2
    assert account.configurationVersion == 5
    assert account.activePortfolioName == "core-balanced"
    assert account.allocationSummary is not None


def test_broker_account_operations_detail_disables_unsupported_v1_actions() -> None:
    detail = _service().get_account("acct-paper")

    assert detail.account.accountId == "acct-paper"
    assert detail.configuration is not None
    assert detail.accountType == "paper"
    assert detail.alerts == []
    assert detail.syncRuns == []
    assert detail.recentActivity == []
    assert detail.capabilities.canReadBalances is True
    assert detail.capabilities.canReconnect is False
    assert detail.capabilities.canPauseSync is False
    assert detail.capabilities.canRefresh is False
    assert detail.capabilities.canAcknowledgeAlerts is False


def test_broker_account_operations_unknown_account_returns_404() -> None:
    service = _service(account=_trade_account())

    with pytest.raises(BrokerAccountOperationsError) as exc_info:
        service.get_account("missing")

    assert exc_info.value.status_code == 404


def test_broker_account_operations_unsupported_action_returns_501_after_account_lookup() -> None:
    service = _service()

    with pytest.raises(BrokerAccountOperationsError) as exc_info:
        service.refresh_account("acct-paper")

    assert exc_info.value.status_code == 501
    assert "not implemented" in exc_info.value.detail
