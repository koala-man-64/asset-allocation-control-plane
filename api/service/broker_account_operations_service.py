from __future__ import annotations

from datetime import datetime

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountActionResponse,
    BrokerAccountConfiguration,
    BrokerAccountDetail,
    BrokerAccountListResponse,
    BrokerAccountSummary,
    BrokerCapabilityFlags,
    BrokerConnectionHealth,
)
from asset_allocation_contracts.trade_desk import TradeAccountSummary, TradeDataFreshness

from api.service.broker_account_configuration_service import (
    BrokerAccountConfigurationError,
    BrokerAccountConfigurationService,
)
from api.service.broker_account_status_refresh_service import BrokerAccountStatusRefreshService
from api.service.settings import TradeDeskSettings
from core.trade_desk_repository import TradeDeskRepository, TradeAccountRecord, utc_now


class BrokerAccountOperationsError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class BrokerAccountOperationsService:
    def __init__(
        self,
        trade_repo: TradeDeskRepository,
        settings: TradeDeskSettings,
        configuration_service: BrokerAccountConfigurationService,
        refresh_service: BrokerAccountStatusRefreshService | None = None,
    ) -> None:
        self._trade_repo = trade_repo
        self._settings = settings
        self._configuration_service = configuration_service
        self._refresh_service = refresh_service

    def list_accounts(self) -> BrokerAccountListResponse:
        response = self._trade_repo.list_accounts()
        allowlist = self._account_allowlist()
        accounts = [account for account in response.accounts if not allowlist or account.accountId in allowlist]
        return BrokerAccountListResponse(
            accounts=[
                self._summary_from_trade_account(account, configuration=self._configuration_or_none(account.accountId))
                for account in accounts
            ],
            generatedAt=response.generatedAt or utc_now(),
        )

    def get_account(self, account_id: str) -> BrokerAccountDetail:
        record = self._account_record(account_id)
        configuration = self._configuration_or_none(account_id)
        summary = self._summary_from_trade_account(record.account, configuration=configuration)
        capabilities = self._detail_capabilities(record.account, configuration=configuration)
        trading_blocked_reason = self._trading_blocked_reason(record.account)
        return BrokerAccountDetail(
            account=summary,
            capabilities=capabilities,
            accountType="paper" if record.account.environment == "paper" else "other",
            tradingBlocked=record.account.readiness == "blocked" or record.account.killSwitchActive,
            tradingBlockedReason=trading_blocked_reason,
            alerts=[],
            syncRuns=[],
            recentActivity=[],
            configuration=configuration,
        )

    def reconnect_account(self, account_id: str) -> BrokerAccountActionResponse:
        record = self._account_record(account_id)
        return self._require_refresh_service().action_response(
            account_id=record.account.accountId,
            action="reconnect",
            trigger="reconnect",
        )

    def set_sync_paused(self, account_id: str, *, paused: bool) -> None:
        self._account_record(account_id)
        self._unsupported("pause sync" if paused else "resume sync")

    def refresh_account(self, account_id: str) -> BrokerAccountActionResponse:
        record = self._account_record(account_id)
        return self._require_refresh_service().action_response(
            account_id=record.account.accountId,
            action="refresh",
            trigger="manual",
        )

    def acknowledge_alert(self, account_id: str, alert_id: str) -> None:
        self._account_record(account_id)
        normalized_alert_id = str(alert_id or "").strip()
        if not normalized_alert_id:
            raise BrokerAccountOperationsError(400, "Alert id is required.")
        self._unsupported("alert acknowledgement")

    def _account_record(self, account_id: str) -> TradeAccountRecord:
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id:
            raise BrokerAccountOperationsError(404, "Trade account '' not found.")
        record = self._trade_repo.get_account_record(normalized_account_id)
        if record is None:
            raise BrokerAccountOperationsError(404, f"Trade account '{normalized_account_id}' not found.")
        allowlist = self._account_allowlist()
        if allowlist and normalized_account_id not in allowlist:
            raise BrokerAccountOperationsError(
                403,
                f"Trade account '{normalized_account_id}' is not allowlisted for account operations access.",
            )
        return record

    def _configuration_or_none(self, account_id: str) -> BrokerAccountConfiguration | None:
        try:
            return self._configuration_service.get_configuration(account_id)
        except BrokerAccountConfigurationError as exc:
            if exc.status_code == 404:
                return None
            raise BrokerAccountOperationsError(exc.status_code, exc.detail) from exc
        except ValueError as exc:
            raise BrokerAccountOperationsError(400, str(exc)) from exc

    def _account_allowlist(self) -> set[str]:
        return {account_id.strip() for account_id in self._settings.account_allowlist if account_id.strip()}

    def _summary_from_trade_account(
        self,
        account: TradeAccountSummary,
        *,
        configuration: BrokerAccountConfiguration | None,
    ) -> BrokerAccountSummary:
        health = self._connection_health(account)
        allocation = configuration.allocation if configuration and configuration.allocation.items else None
        return BrokerAccountSummary(
            accountId=account.accountId,
            broker=account.provider,
            name=account.name,
            accountNumberMasked=account.accountNumberMasked,
            baseCurrency=account.baseCurrency,
            overallStatus=health.overallStatus,
            tradeReadiness=account.readiness,
            tradeReadinessReason=account.readinessReason,
            highestAlertSeverity=self._highest_alert_severity(account),
            connectionHealth=health,
            equity=account.equity,
            cash=account.cash,
            buyingPower=account.buyingPower,
            openPositionCount=account.positionCount,
            openOrderCount=account.openOrderCount,
            lastSyncedAt=account.lastSyncedAt,
            snapshotAsOf=account.snapshotAsOf,
            activePortfolioName=configuration.allocation.portfolioName if configuration else None,
            strategyLabel=None,
            configurationVersion=configuration.configurationVersion if configuration else None,
            allocationSummary=allocation,
            alertCount=account.unresolvedAlertCount,
        )

    def _connection_health(self, account: TradeAccountSummary) -> BrokerConnectionHealth:
        sync_status = self._sync_status(account.freshness)
        overall_status = self._overall_status(account, sync_status=sync_status)
        latest_sync = self._latest_timestamp(
            account.lastSyncedAt,
            account.snapshotAsOf,
            account.freshness.balancesAsOf,
            account.freshness.positionsAsOf,
            account.freshness.ordersAsOf,
        )
        can_read_account = account.capabilities.canReadAccount
        connection_state = self._connection_state(can_read_account=can_read_account, sync_status=sync_status)
        reconnect_required = self._requires_reauth(account)
        return BrokerConnectionHealth(
            overallStatus=overall_status,
            authStatus="authenticated" if can_read_account else "reauth_required" if reconnect_required else "not_connected",
            connectionState="reconnect_required" if reconnect_required else connection_state,
            syncStatus=sync_status,
            lastCheckedAt=latest_sync or utc_now(),
            lastSuccessfulSyncAt=latest_sync if can_read_account and sync_status in {"fresh", "stale"} else None,
            lastFailedSyncAt=latest_sync if not can_read_account else None,
            authExpiresAt=None,
            staleReason=account.freshness.staleReason if sync_status in {"stale", "never_synced"} else None,
            failureMessage=self._failure_message(account),
            syncPaused=False,
        )

    @staticmethod
    def _sync_status(freshness: TradeDataFreshness) -> str:
        states = [freshness.balancesState, freshness.positionsState, freshness.ordersState]
        if all(state == "fresh" for state in states):
            return "fresh"
        if any(state == "stale" for state in states):
            return "stale"
        if all(state == "unknown" for state in states):
            return "never_synced"
        return "stale"

    @staticmethod
    def _connection_state(*, can_read_account: bool, sync_status: str) -> str:
        if not can_read_account:
            return "disconnected"
        if sync_status in {"stale", "never_synced", "failed"}:
            return "degraded"
        return "connected"

    @staticmethod
    def _overall_status(account: TradeAccountSummary, *, sync_status: str) -> str:
        if account.readiness == "blocked" or account.killSwitchActive or not account.capabilities.canReadAccount:
            return "critical"
        if account.readiness == "review" or sync_status in {"stale", "never_synced", "failed"}:
            return "warning"
        if account.unresolvedAlertCount > 0:
            return "warning"
        return "healthy"

    @staticmethod
    def _highest_alert_severity(account: TradeAccountSummary) -> str | None:
        if account.unresolvedAlertCount <= 0:
            return None
        if account.readiness == "blocked" or account.killSwitchActive:
            return "critical"
        return "warning"

    @staticmethod
    def _failure_message(account: TradeAccountSummary) -> str | None:
        if not account.capabilities.canReadAccount:
            return account.capabilities.unsupportedReason or account.readinessReason or "Broker account is not readable."
        if account.readiness == "blocked":
            return account.readinessReason or "Account is blocked from trading."
        if account.killSwitchActive:
            return "Account kill switch is active."
        return None

    @staticmethod
    def _trading_blocked_reason(account: TradeAccountSummary) -> str | None:
        if account.killSwitchActive:
            return "Account kill switch is active."
        if account.readiness == "blocked":
            return account.readinessReason or "Account is blocked from trading."
        return None

    @staticmethod
    def _latest_timestamp(*values: datetime | None) -> datetime | None:
        populated = [value for value in values if value is not None]
        return max(populated) if populated else None

    @staticmethod
    def _detail_capabilities(
        account: TradeAccountSummary,
        *,
        configuration: BrokerAccountConfiguration | None,
    ) -> BrokerCapabilityFlags:
        base = configuration.capabilities if configuration else BrokerCapabilityFlags()
        if configuration is None:
            can_write = not account.capabilities.readOnly
            base = BrokerCapabilityFlags(
                canReadBalances=account.capabilities.canReadAccount,
                canReadPositions=account.capabilities.canReadPositions,
                canReadOrders=account.capabilities.canReadOrders,
                canTrade=account.capabilities.canSubmitPaper
                or account.capabilities.canSubmitSandbox
                or account.capabilities.canSubmitLive,
                canReadTradingPolicy=account.capabilities.canReadAccount,
                canWriteTradingPolicy=can_write,
                canReadAllocation=account.capabilities.canReadAccount,
                canWriteAllocation=can_write,
                canReleaseTradeConfirmation=can_write,
                readOnlyReason=account.capabilities.unsupportedReason if account.capabilities.readOnly else None,
            )
        return base.model_copy(
            update={
                "canReconnect": True,
                "canPauseSync": False,
                "canRefresh": True,
                "canAcknowledgeAlerts": False,
            }
        )

    def _require_refresh_service(self) -> BrokerAccountStatusRefreshService:
        if self._refresh_service is None:
            raise BrokerAccountOperationsError(
                503,
                "Broker account status refresh service is not initialized.",
            )
        return self._refresh_service

    @staticmethod
    def _requires_reauth(account: TradeAccountSummary) -> bool:
        if account.capabilities.canReadAccount:
            return False
        text = " ".join(
            value
            for value in (
                account.capabilities.unsupportedReason,
                account.readinessReason,
                account.freshness.staleReason,
            )
            if value
        ).lower()
        return any(marker in text for marker in ("auth", "connect", "credential", "oauth", "reauth", "session", "token"))

    @staticmethod
    def _unsupported(action: str) -> None:
        raise BrokerAccountOperationsError(
            501,
            f"Broker account {action} is not implemented in Account Operations v1.",
        )
