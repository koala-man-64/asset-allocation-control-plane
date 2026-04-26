from __future__ import annotations

import re

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountAllocationUpdateRequest,
    BrokerAccountConfiguration,
    BrokerCapabilityFlags,
    BrokerStrategyAllocationItem,
    BrokerStrategyAllocationSummary,
    BrokerTradingPolicy,
    BrokerTradingPolicyUpdateRequest,
)
from asset_allocation_contracts.portfolio import (
    PortfolioAssignmentRequest,
    PortfolioSleeveAllocation,
    PortfolioUpsertRequest,
)

from core.broker_account_configuration_repository import BrokerAccountConfigurationRepository
from core.portfolio_repository import PortfolioRepository
from core.trade_desk_repository import TradeDeskRepository, utc_now


class BrokerAccountConfigurationError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class BrokerAccountConfigurationService:
    def __init__(
        self,
        configuration_repo: BrokerAccountConfigurationRepository,
        trade_repo: TradeDeskRepository,
        portfolio_repo: PortfolioRepository,
    ) -> None:
        self._configuration_repo = configuration_repo
        self._trade_repo = trade_repo
        self._portfolio_repo = portfolio_repo

    def get_configuration(self, account_id: str) -> BrokerAccountConfiguration:
        account_record = self._account_record(account_id)
        persisted = self._configuration_repo.get_configuration(account_id)
        allocation, allocation_warnings = self._load_allocation_summary(account_id)
        warnings = list(persisted.warnings if persisted else [])
        for warning in allocation_warnings:
            if warning not in warnings:
                warnings.append(warning)
        audit = self._configuration_repo.list_audit(account_id, limit=25)
        configuration = persisted or BrokerAccountConfiguration(
            accountId=account_id,
            configurationVersion=1,
            requestedPolicy=BrokerTradingPolicy(),
            effectivePolicy=self._effective_policy(BrokerTradingPolicy(), account_record.account.capabilities),
        )
        return configuration.model_copy(
            update={
                "accountId": account_id,
                "accountName": account_record.account.name,
                "baseCurrency": account_record.account.baseCurrency,
                "capabilities": self._map_capabilities(account_record.account.capabilities),
                "allocation": allocation if allocation.items else configuration.allocation,
                "warnings": warnings,
                "audit": audit,
            }
        )

    def save_trading_policy(
        self,
        account_id: str,
        payload: BrokerTradingPolicyUpdateRequest,
        *,
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountConfiguration:
        account_record = self._account_record(account_id)
        effective_policy = self._effective_policy(payload.requestedPolicy, account_record.account.capabilities)
        warnings = self._policy_warnings(payload.requestedPolicy, account_record.account)
        persisted = self._configuration_repo.save_trading_policy(
            account_id=account_id,
            expected_configuration_version=payload.expectedConfigurationVersion,
            requested_policy=payload.requestedPolicy,
            effective_policy=effective_policy,
            warnings=warnings,
            actor=actor,
            request_id=request_id,
            granted_roles=granted_roles,
        )
        allocation, allocation_warnings = self._load_allocation_summary(account_id)
        merged_warnings = [*warnings]
        for warning in allocation_warnings:
            if warning not in merged_warnings:
                merged_warnings.append(warning)
        return persisted.model_copy(
            update={
                "accountName": account_record.account.name,
                "baseCurrency": account_record.account.baseCurrency,
                "capabilities": self._map_capabilities(account_record.account.capabilities),
                "allocation": allocation if allocation.items else persisted.allocation,
                "warnings": merged_warnings,
                "audit": self._configuration_repo.list_audit(account_id, limit=25),
            }
        )

    def save_allocation(
        self,
        account_id: str,
        payload: BrokerAccountAllocationUpdateRequest,
        *,
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountConfiguration:
        account_record = self._account_record(account_id)
        account_detail = self._portfolio_repo.get_account_detail(account_id)
        if account_detail is None:
            raise BrokerAccountConfigurationError(
                404,
                f"Portfolio account '{account_id}' was not found for broker allocation updates.",
            )

        active_assignment = self._portfolio_repo.get_active_assignment(account_id)
        if active_assignment is None:
            raise BrokerAccountConfigurationError(
                409,
                f"Account '{account_id}' does not have an active portfolio assignment.",
            )
        active_revision = self._portfolio_repo.get_portfolio_revision(
            active_assignment.portfolioName,
            version=active_assignment.portfolioVersion,
        )
        if active_revision is None:
            raise BrokerAccountConfigurationError(
                404,
                f"Active portfolio revision '{active_assignment.portfolioName}:{active_assignment.portfolioVersion}' was not found.",
            )

        shared_count = self._portfolio_repo.count_active_assignments_for_portfolio(
            active_assignment.portfolioName,
            active_assignment.portfolioVersion,
            exclude_account_id=account_id,
        )
        shared_active_portfolio = shared_count > 0
        target_portfolio_name = (
            self._clone_portfolio_name(active_assignment.portfolioName, account_id)
            if shared_active_portfolio
            else active_assignment.portfolioName
        )
        saved_portfolio = self._portfolio_repo.save_portfolio(
            payload=PortfolioUpsertRequest(
                name=target_portfolio_name,
                description=active_revision.description,
                benchmarkSymbol=active_revision.benchmarkSymbol,
                allocationMode=payload.allocationMode,
                allocatableCapital=payload.allocatableCapital,
                allocations=self._to_portfolio_allocations(payload),
                notes=payload.notes or active_revision.notes,
            ),
            created_by=actor,
        )
        saved_version = (
            saved_portfolio.portfolio.activeVersion
            or saved_portfolio.portfolio.latestVersion
            or active_assignment.portfolioVersion
        )
        effective_from = payload.effectiveFrom or utc_now().date()
        assigned = self._portfolio_repo.assign_portfolio(
            account_id,
            PortfolioAssignmentRequest(
                accountVersion=account_detail.account.activeRevision or account_detail.account.latestRevision or 1,
                portfolioName=saved_portfolio.portfolio.name,
                portfolioVersion=saved_version,
                effectiveFrom=effective_from,
                notes=payload.notes,
            ),
        )
        saved_revision = self._portfolio_repo.get_portfolio_revision(
            saved_portfolio.portfolio.name,
            version=saved_version,
        )
        if saved_revision is None:
            raise BrokerAccountConfigurationError(
                500,
                f"Saved portfolio revision '{saved_portfolio.portfolio.name}:{saved_version}' could not be reloaded.",
            )
        allocation = self._build_allocation_summary(
            assignment=assigned,
            revision=saved_revision,
            shared_active_portfolio=shared_active_portfolio,
        )
        persisted = self._configuration_repo.save_allocation_summary(
            account_id=account_id,
            expected_configuration_version=payload.expectedConfigurationVersion,
            allocation=allocation,
            actor=actor,
            request_id=request_id,
            granted_roles=granted_roles,
        )
        warnings = list(persisted.warnings)
        if shared_active_portfolio:
            warnings.append("The active portfolio was shared and has been cloned for this account.")
        return persisted.model_copy(
            update={
                "accountName": account_record.account.name,
                "baseCurrency": account_record.account.baseCurrency,
                "capabilities": self._map_capabilities(account_record.account.capabilities),
                "allocation": allocation,
                "warnings": warnings,
                "audit": self._configuration_repo.list_audit(account_id, limit=25),
            }
        )

    def _account_record(self, account_id: str):
        record = self._trade_repo.get_account_record(account_id)
        if record is None:
            raise BrokerAccountConfigurationError(404, f"Trade account '{account_id}' not found.")
        return record

    @staticmethod
    def _map_capabilities(trade_capabilities) -> BrokerCapabilityFlags:
        can_write = not trade_capabilities.readOnly
        return BrokerCapabilityFlags(
            canReadBalances=trade_capabilities.canReadAccount,
            canReadPositions=trade_capabilities.canReadPositions,
            canReadOrders=trade_capabilities.canReadOrders,
            canTrade=trade_capabilities.canSubmitPaper
            or trade_capabilities.canSubmitSandbox
            or trade_capabilities.canSubmitLive,
            canReconnect=can_write,
            canPauseSync=can_write,
            canRefresh=trade_capabilities.canReadAccount,
            canAcknowledgeAlerts=can_write,
            canReadTradingPolicy=trade_capabilities.canReadAccount,
            canWriteTradingPolicy=can_write,
            canReadAllocation=trade_capabilities.canReadAccount,
            canWriteAllocation=can_write,
            canReleaseTradeConfirmation=can_write,
            readOnlyReason=trade_capabilities.unsupportedReason if trade_capabilities.readOnly else None,
        )

    def _load_allocation_summary(self, account_id: str) -> tuple[BrokerStrategyAllocationSummary, list[str]]:
        assignment = self._portfolio_repo.get_active_assignment(account_id)
        if assignment is None:
            return BrokerStrategyAllocationSummary(), []
        revision = self._portfolio_repo.get_portfolio_revision(
            assignment.portfolioName,
            version=assignment.portfolioVersion,
        )
        if revision is None:
            return BrokerStrategyAllocationSummary(), []
        shared_count = self._portfolio_repo.count_active_assignments_for_portfolio(
            assignment.portfolioName,
            assignment.portfolioVersion,
            exclude_account_id=account_id,
        )
        warnings: list[str] = []
        if shared_count > 0:
            warnings.append("Allocation edits will clone the shared active portfolio.")
        return self._build_allocation_summary(
            assignment=assignment,
            revision=revision,
            shared_active_portfolio=shared_count > 0,
        ), warnings

    @staticmethod
    def _effective_policy(requested: BrokerTradingPolicy, trade_capabilities) -> BrokerTradingPolicy:
        allowed_asset_classes: list[str] = []
        if "equity" in requested.allowedAssetClasses and (
            trade_capabilities.supportsEquities or trade_capabilities.supportsEtfs
        ):
            allowed_asset_classes.append("equity")
        if "option" in requested.allowedAssetClasses and getattr(trade_capabilities, "supportsOptions", False):
            allowed_asset_classes.append("option")
        if not allowed_asset_classes:
            allowed_asset_classes = ["equity"] if trade_capabilities.supportsEquities else []

        allowed_sides = [side for side in requested.allowedSides if side in {"long", "short"}]
        return BrokerTradingPolicy(
            maxOpenPositions=requested.maxOpenPositions,
            maxSinglePositionExposure=requested.maxSinglePositionExposure,
            allowedSides=allowed_sides or ["long"],
            allowedAssetClasses=allowed_asset_classes or ["equity"],
            requireOrderConfirmation=requested.requireOrderConfirmation,
        )

    @staticmethod
    def _policy_warnings(requested: BrokerTradingPolicy, trade_account) -> list[str]:
        warnings: list[str] = []
        if requested.maxOpenPositions is not None and trade_account.positionCount > requested.maxOpenPositions:
            warnings.append(
                "Current open positions exceed the tighter max-open-positions policy. "
                "New opening orders will remain blocked until the account is back within policy."
            )
        return warnings

    @staticmethod
    def _to_portfolio_allocations(
        payload: BrokerAccountAllocationUpdateRequest,
    ) -> list[PortfolioSleeveAllocation]:
        allocations: list[PortfolioSleeveAllocation] = []
        for item in payload.items:
            allocations.append(
                PortfolioSleeveAllocation(
                    sleeveId=item.sleeveId,
                    sleeveName=item.sleeveName,
                    strategy={
                        "strategyName": item.strategy.strategyName,
                        "strategyVersion": item.strategy.strategyVersion,
                    },
                    allocationMode=payload.allocationMode,
                    targetWeight=None
                    if payload.allocationMode == "notional_base_ccy"
                    else float(item.targetWeightPct or 0.0) / 100.0,
                    targetNotionalBaseCcy=item.targetNotionalBaseCcy,
                    derivedWeight=None
                    if payload.allocatableCapital in {None, 0}
                    or item.targetNotionalBaseCcy is None
                    else float(item.targetNotionalBaseCcy) / float(payload.allocatableCapital),
                    enabled=item.enabled,
                    notes=item.notes,
                )
            )
        return allocations

    @staticmethod
    def _build_allocation_summary(
        *,
        assignment,
        revision,
        shared_active_portfolio: bool,
    ) -> BrokerStrategyAllocationSummary:
        items: list[BrokerStrategyAllocationItem] = []
        for allocation in revision.allocations:
            if revision.allocationMode == "percent":
                target_weight_pct = round(float(allocation.targetWeight or 0.0) * 100.0, 2)
                derived_weight_pct = target_weight_pct
                target_notional = None
            else:
                target_weight_pct = None
                target_notional = allocation.targetNotionalBaseCcy
                derived_weight_pct = (
                    round(float(allocation.derivedWeight or 0.0) * 100.0, 2)
                    if allocation.derivedWeight is not None
                    else (
                        round(float(allocation.targetNotionalBaseCcy or 0.0) / float(revision.allocatableCapital) * 100.0, 2)
                        if revision.allocatableCapital
                        else None
                    )
                )
            items.append(
                BrokerStrategyAllocationItem(
                    sleeveId=allocation.sleeveId,
                    sleeveName=allocation.sleeveName,
                    strategy={
                        "strategyName": allocation.strategy.strategyName,
                        "strategyVersion": allocation.strategy.strategyVersion,
                    },
                    allocationMode=revision.allocationMode,
                    targetWeightPct=target_weight_pct,
                    targetNotionalBaseCcy=target_notional,
                    derivedWeightPct=derived_weight_pct,
                    enabled=allocation.enabled,
                    notes=allocation.notes,
                )
            )
        if revision.allocationMode == "percent":
            allocated_percent = round(sum(float(item.targetWeightPct or 0.0) for item in items if item.enabled), 2)
            remaining_percent = round(max(0.0, 100.0 - allocated_percent), 2)
            allocated_notional = None
            remaining_notional = None
        else:
            allocated_notional = round(
                sum(float(item.targetNotionalBaseCcy or 0.0) for item in items if item.enabled),
                2,
            )
            allocated_percent = (
                round(allocated_notional / float(revision.allocatableCapital) * 100.0, 2)
                if revision.allocatableCapital
                else None
            )
            remaining_notional = (
                round(max(0.0, float(revision.allocatableCapital) - allocated_notional), 2)
                if revision.allocatableCapital is not None
                else None
            )
            remaining_percent = (
                round(max(0.0, 100.0 - float(allocated_percent or 0.0)), 2)
                if allocated_percent is not None
                else None
            )
        return BrokerStrategyAllocationSummary(
            portfolioName=assignment.portfolioName,
            portfolioVersion=assignment.portfolioVersion,
            allocationMode=revision.allocationMode,
            allocatableCapital=revision.allocatableCapital,
            allocatedPercent=allocated_percent,
            allocatedNotionalBaseCcy=allocated_notional,
            remainingPercent=remaining_percent,
            remainingNotionalBaseCcy=remaining_notional,
            sharedActivePortfolio=shared_active_portfolio,
            effectiveFrom=assignment.effectiveFrom,
            items=items,
        )

    @staticmethod
    def _clone_portfolio_name(base_name: str, account_id: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", account_id.strip().lower()).strip("-") or "account"
        return f"{base_name}-{slug}"
