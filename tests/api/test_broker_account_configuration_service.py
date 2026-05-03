from __future__ import annotations

from datetime import datetime, timezone

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountConfiguration,
    BrokerTradingPolicy,
    BrokerTradingPolicyUpdateRequest,
)
from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountSummary,
    TradeCapabilityFlags,
    TradeDataFreshness,
)

from api.service.broker_account_configuration_service import BrokerAccountConfigurationService
from core.trade_desk_repository import TradeAccountRecord


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
            supportsOptions=False,
            readOnly=False,
        ),
        cash=100_000,
        buyingPower=100_000,
        positionCount=12,
        freshness=TradeDataFreshness(
            balancesState="fresh",
            positionsState="fresh",
            ordersState="fresh",
            balancesAsOf=_now(),
            positionsAsOf=_now(),
            ordersAsOf=_now(),
        ),
    )


class FakeConfigurationRepository:
    def __init__(self) -> None:
        self.saved_trading_policy: dict[str, object] | None = None

    def get_configuration(self, account_id: str) -> BrokerAccountConfiguration:
        return BrokerAccountConfiguration(
            accountId=account_id,
            configurationVersion=2,
            requestedPolicy=BrokerTradingPolicy(),
            effectivePolicy=BrokerTradingPolicy(),
        )

    def save_trading_policy(
        self,
        *,
        account_id: str,
        expected_configuration_version: int | None,
        requested_policy: BrokerTradingPolicy,
        effective_policy: BrokerTradingPolicy,
        warnings: list[str],
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountConfiguration:
        self.saved_trading_policy = {
            "expected_configuration_version": expected_configuration_version,
            "requested_policy": requested_policy,
            "effective_policy": effective_policy,
            "warnings": warnings,
            "actor": actor,
            "request_id": request_id,
            "granted_roles": granted_roles,
        }
        return BrokerAccountConfiguration(
            accountId=account_id,
            configurationVersion=3,
            requestedPolicy=requested_policy,
            effectivePolicy=effective_policy,
            warnings=warnings,
            updatedBy=actor,
        )

    def list_audit(self, account_id: str, *, limit: int = 25):
        return []


class FakeTradeRepository:
    def __init__(self, account: TradeAccountSummary) -> None:
        self.account = account

    def get_account_record(self, account_id: str):
        if account_id != self.account.accountId:
            return None
        return TradeAccountRecord(
            account=self.account,
            detail=TradeAccountDetail(account=self.account),
            providerAccountKey=None,
        )


class FakePortfolioRepository:
    def get_active_assignment(self, account_id: str):
        return None


def test_save_trading_policy_downgrades_effective_policy_and_warns_for_current_book() -> None:
    configuration_repo = FakeConfigurationRepository()
    service = BrokerAccountConfigurationService(
        configuration_repo,
        FakeTradeRepository(_account()),
        FakePortfolioRepository(),
    )

    response = service.save_trading_policy(
        "acct-paper",
        BrokerTradingPolicyUpdateRequest(
            expectedConfigurationVersion=2,
            requestedPolicy=BrokerTradingPolicy(
                maxOpenPositions=10,
                maxSinglePositionExposure={"mode": "pct_of_allocatable_capital", "value": 5.0},
                allowedSides=["long", "short"],
                allowedAssetClasses=["equity", "option"],
                requireOrderConfirmation=True,
            ),
        ),
        actor="desk@example.com",
        request_id="request-1",
        granted_roles=["AssetAllocation.AccountPolicy.Write"],
    )

    assert response.configurationVersion == 3
    assert response.requestedPolicy.allowedAssetClasses == ["equity", "option"]
    assert response.effectivePolicy.allowedAssetClasses == ["equity"]
    assert response.effectivePolicy.allowedSides == ["long", "short"]
    assert response.effectivePolicy.requireOrderConfirmation is True
    assert response.capabilities.canWriteTradingPolicy is True
    assert response.warnings == [
        "Current open positions exceed the tighter max-open-positions policy. "
        "New opening orders will remain blocked until the account is back within policy."
    ]
    assert configuration_repo.saved_trading_policy is not None
    assert configuration_repo.saved_trading_policy["expected_configuration_version"] == 2
