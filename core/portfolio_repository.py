from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from asset_allocation_contracts.portfolio import (
    PortfolioAccount,
    PortfolioAccountDetailResponse,
    PortfolioAccountListResponse,
    PortfolioAccountRevision,
    PortfolioAccountUpsertRequest,
    PortfolioAlert,
    PortfolioAlertListResponse,
    PortfolioAssignment,
    PortfolioAssignmentRequest,
    PortfolioDefinition,
    PortfolioDefinitionDetailResponse,
    PortfolioHistoryPoint,
    PortfolioHistoryResponse,
    PortfolioLedgerEvent,
    PortfolioLedgerEventPayload,
    PortfolioListResponse,
    PortfolioPosition,
    PortfolioPositionListResponse,
    PortfolioRevision,
    PortfolioSnapshot,
    PortfolioUpsertRequest,
    RebalanceProposal,
    RebalanceTradeProposal,
)
from asset_allocation_runtime_common.foundation.postgres import connect
from core.strategy_repository import StrategyRepository

logger = logging.getLogger(__name__)

_ACCOUNT_COLUMNS = [
    "accountId",
    "name",
    "description",
    "status",
    "mode",
    "accountingDepth",
    "cadenceMode",
    "baseCurrency",
    "benchmarkSymbol",
    "inceptionDate",
    "mandate",
    "latestRevision",
    "activeRevision",
    "activePortfolioName",
    "activePortfolioVersion",
    "createdAt",
    "updatedAt",
    "lastMaterializedAt",
    "openAlertCount",
]
_ACCOUNT_SELECT_SQL = """
    SELECT
        account_id,
        name,
        description,
        status,
        mode,
        accounting_depth,
        cadence_mode,
        base_currency,
        benchmark_symbol,
        inception_date,
        mandate,
        latest_revision,
        active_revision,
        active_portfolio_name,
        active_portfolio_version,
        created_at,
        updated_at,
        last_materialized_at,
        open_alert_count
"""
_ACCOUNT_REVISION_COLUMNS = [
    "accountId",
    "version",
    "name",
    "description",
    "mandate",
    "status",
    "mode",
    "accountingDepth",
    "cadenceMode",
    "baseCurrency",
    "benchmarkSymbol",
    "inceptionDate",
    "notes",
    "createdAt",
    "createdBy",
]
_PORTFOLIO_COLUMNS = [
    "name",
    "description",
    "benchmarkSymbol",
    "status",
    "latestVersion",
    "activeVersion",
    "createdAt",
    "updatedAt",
]
_PORTFOLIO_REVISION_COLUMNS = [
    "portfolioName",
    "version",
    "description",
    "benchmarkSymbol",
    "allocationMode",
    "allocatableCapital",
    "allocations",
    "notes",
    "publishedAt",
    "createdAt",
    "createdBy",
]
_ASSIGNMENT_COLUMNS = [
    "assignmentId",
    "accountId",
    "accountVersion",
    "portfolioName",
    "portfolioVersion",
    "effectiveFrom",
    "effectiveTo",
    "status",
    "notes",
    "createdAt",
]
_LEDGER_EVENT_COLUMNS = [
    "eventId",
    "accountId",
    "effectiveAt",
    "eventType",
    "currency",
    "cashAmount",
    "symbol",
    "quantity",
    "price",
    "commission",
    "slippageCost",
    "description",
]
_SNAPSHOT_COLUMNS = [
    "accountId",
    "accountName",
    "asOf",
    "nav",
    "cash",
    "grossExposure",
    "netExposure",
    "sinceInceptionPnl",
    "sinceInceptionReturn",
    "currentDrawdown",
    "maxDrawdown",
    "openAlertCount",
    "activeAssignment",
    "freshness",
    "slices",
]
_HISTORY_COLUMNS = [
    "asOf",
    "nav",
    "cash",
    "grossExposure",
    "netExposure",
    "periodPnl",
    "periodReturn",
    "cumulativePnl",
    "cumulativeReturn",
    "drawdown",
    "turnover",
    "costDragBps",
]
_POSITION_COLUMNS = [
    "asOf",
    "symbol",
    "quantity",
    "marketValue",
    "weight",
    "averageCost",
    "lastPrice",
    "unrealizedPnl",
    "realizedPnl",
    "contributors",
]
_ALERT_COLUMNS = [
    "alertId",
    "accountId",
    "severity",
    "status",
    "code",
    "title",
    "description",
    "detectedAt",
    "acknowledgedAt",
    "acknowledgedBy",
    "resolvedAt",
    "asOf",
]
_PROPOSAL_COLUMNS = [
    "proposalId",
    "accountId",
    "asOf",
    "portfolioName",
    "portfolioVersion",
    "blocked",
    "warnings",
    "blockedReasons",
    "estimatedCashImpact",
    "estimatedTurnover",
    "trades",
]
_MATERIALIZATION_CLAIM_TTL = timedelta(minutes=30)
_ALLOCATION_MODE_NOTIONAL = "notional_base_ccy"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def _parse_json(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default
    return default


def _normalize_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    if not text:
        return None
    return date.fromisoformat(text[:10])


def _row_to_model_payload(columns: list[str], row: tuple[Any, ...] | list[Any]) -> dict[str, Any]:
    return dict(zip(columns, row))


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.md5(_json_dumps(payload).encode("utf-8")).hexdigest()


def _slugify(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return normalized or uuid.uuid4().hex[:10]


def _synthetic_symbol(strategy_name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(strategy_name or "").upper()).strip("_")
    return (cleaned or "SLEEVE")[:32]


def _derive_allocation_weights(
    allocations: Any,
    *,
    allocation_mode: str,
    allocatable_capital: Any,
) -> Any:
    if allocation_mode != _ALLOCATION_MODE_NOTIONAL:
        return allocations

    if allocatable_capital is None or float(allocatable_capital) <= 0:
        raise ValueError("Notional portfolio allocations require positive allocatableCapital.")
    if not isinstance(allocations, list):
        return allocations

    capital = float(allocatable_capital)
    derived_allocations: list[Any] = []
    for allocation in allocations:
        if not isinstance(allocation, dict):
            derived_allocations.append(allocation)
            continue
        updated = dict(allocation)
        updated["allocationMode"] = allocation_mode
        target_notional = allocation.get("targetNotionalBaseCcy")
        if target_notional is None:
            derived_allocations.append(updated)
            continue
        updated["derivedWeight"] = float(target_notional) / capital
        derived_allocations.append(updated)
    return derived_allocations


def _portfolio_allocation_payloads(portfolio: Any) -> list[dict[str, Any]]:
    allocations = [allocation.model_dump(mode="json") for allocation in portfolio.allocations]
    return _derive_allocation_weights(
        allocations,
        allocation_mode=str(portfolio.allocationMode),
        allocatable_capital=portfolio.allocatableCapital,
    )


def _target_value_for_allocation(allocation: Any, *, allocation_mode: str, nav: float) -> float:
    if allocation_mode == _ALLOCATION_MODE_NOTIONAL:
        if allocation.targetNotionalBaseCcy is None:
            raise ValueError("Notional portfolio allocations require targetNotionalBaseCcy.")
        return float(allocation.targetNotionalBaseCcy)
    if allocation.targetWeight is None:
        raise ValueError("Percent portfolio allocations require targetWeight.")
    return nav * float(allocation.targetWeight)


class PortfolioRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. PortfolioRepository will not function.")

    def _require_dsn(self) -> str:
        if not self.dsn:
            raise ValueError("Database connection not configured")
        return self.dsn

    def _generate_account_id(self, name: str) -> str:
        base = f"acct-{_slugify(name)}"
        candidate = base
        suffix = 2
        while self.get_account(candidate) is not None:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    def _enqueue_materialization(
        self,
        account_id: str,
        *,
        reason: str,
        dependency_state: dict[str, Any] | None = None,
    ) -> None:
        dsn = self._require_dsn()
        state = dependency_state or self._build_materialization_dependency_state(account_id)
        fingerprint = _stable_hash(state)
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.portfolio_materialization_state (
                        account_id,
                        dependency_fingerprint,
                        dependency_state,
                        status,
                        last_error,
                        updated_at
                    )
                    VALUES (%s, %s, %s, 'dirty', %s, NOW())
                    ON CONFLICT (account_id)
                    DO UPDATE SET
                        dependency_fingerprint = EXCLUDED.dependency_fingerprint,
                        dependency_state = EXCLUDED.dependency_state,
                        status = CASE
                            WHEN core.portfolio_materialization_state.status = 'claimed'
                                 AND core.portfolio_materialization_state.claim_expires_at > NOW()
                            THEN 'claimed'
                            ELSE 'dirty'
                        END,
                        last_error = CASE
                            WHEN core.portfolio_materialization_state.status = 'claimed'
                                 AND core.portfolio_materialization_state.claim_expires_at > NOW()
                            THEN core.portfolio_materialization_state.last_error
                            ELSE EXCLUDED.last_error
                        END,
                        updated_at = NOW()
                    """,
                    (
                        account_id,
                        fingerprint,
                        _json_dumps(state),
                        f"dirty:{reason}"[:4000],
                    ),
                )

    def _build_materialization_dependency_state(self, account_id: str) -> dict[str, Any]:
        account = self.get_account(account_id)
        if account is None:
            raise LookupError(f"Portfolio account '{account_id}' not found.")
        assignment = self.get_active_assignment(account_id)
        latest_event = self.get_latest_ledger_event(account_id)
        return {
            "account": {
                "accountId": account.accountId,
                "activeRevision": account.activeRevision,
                "updatedAt": account.updatedAt.isoformat() if account.updatedAt else None,
            },
            "assignment": assignment.model_dump(mode="json") if assignment else None,
            "latestLedgerEvent": latest_event.model_dump(mode="json") if latest_event else None,
        }

    def list_accounts(self) -> PortfolioAccountListResponse:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_ACCOUNT_SELECT_SQL}
                    FROM core.portfolio_accounts
                    ORDER BY name
                    """
                )
                accounts = [PortfolioAccount.model_validate(_row_to_model_payload(_ACCOUNT_COLUMNS, row)) for row in cur.fetchall()]
        return PortfolioAccountListResponse(accounts=accounts)

    def get_account(self, account_id: str) -> PortfolioAccount | None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_ACCOUNT_SELECT_SQL}
                    FROM core.portfolio_accounts
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return PortfolioAccount.model_validate(_row_to_model_payload(_ACCOUNT_COLUMNS, row))

    def get_account_revision(self, account_id: str, *, version: int | None = None) -> PortfolioAccountRevision | None:
        dsn = self._require_dsn()
        sql = """
            SELECT
                account_id,
                version,
                name,
                description,
                mandate,
                status,
                mode,
                accounting_depth,
                cadence_mode,
                base_currency,
                benchmark_symbol,
                inception_date,
                notes,
                created_at,
                created_by
            FROM core.portfolio_account_revisions
            WHERE account_id = %s
        """
        params: list[Any] = [account_id]
        if version is None:
            sql += " ORDER BY version DESC LIMIT 1"
        else:
            sql += " AND version = %s"
            params.append(int(version))
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        if not row:
            return None
        return PortfolioAccountRevision.model_validate(_row_to_model_payload(_ACCOUNT_REVISION_COLUMNS, row))

    def get_account_detail(self, account_id: str) -> PortfolioAccountDetailResponse | None:
        account = self.get_account(account_id)
        if account is None:
            return None
        revision = self.get_account_revision(account_id, version=account.activeRevision)
        assignment = self.get_active_assignment(account_id)
        events = self.list_ledger_events(account_id, limit=25, offset=0)
        return PortfolioAccountDetailResponse(
            account=account,
            revision=revision,
            activeAssignment=assignment,
            recentLedgerEvents=events,
        )

    def save_account(
        self,
        *,
        account_id: str | None,
        payload: PortfolioAccountUpsertRequest,
        created_by: str | None = None,
    ) -> PortfolioAccountDetailResponse:
        dsn = self._require_dsn()
        resolved_account_id = account_id or self._generate_account_id(payload.name)
        existing = self.get_account(resolved_account_id)
        next_version = int(existing.latestRevision or 0) + 1 if existing else 1
        status = "active"
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.portfolio_accounts (
                        account_id,
                        name,
                        description,
                        status,
                        mode,
                        accounting_depth,
                        cadence_mode,
                        base_currency,
                        benchmark_symbol,
                        inception_date,
                        mandate,
                        latest_revision,
                        active_revision,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, 'internal_model_managed', 'position_level', 'strategy_native', %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (account_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        base_currency = EXCLUDED.base_currency,
                        benchmark_symbol = EXCLUDED.benchmark_symbol,
                        inception_date = EXCLUDED.inception_date,
                        mandate = EXCLUDED.mandate,
                        latest_revision = EXCLUDED.latest_revision,
                        active_revision = EXCLUDED.active_revision,
                        updated_at = NOW()
                    """,
                    (
                        resolved_account_id,
                        payload.name,
                        payload.description,
                        status,
                        payload.baseCurrency,
                        payload.benchmarkSymbol,
                        payload.inceptionDate,
                        payload.mandate,
                        next_version,
                        next_version,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO core.portfolio_account_revisions (
                        account_id,
                        version,
                        name,
                        description,
                        mandate,
                        status,
                        mode,
                        accounting_depth,
                        cadence_mode,
                        base_currency,
                        benchmark_symbol,
                        inception_date,
                        notes,
                        created_at,
                        created_by
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'internal_model_managed', 'position_level', 'strategy_native', %s, %s, %s, %s, NOW(), %s)
                    """,
                    (
                        resolved_account_id,
                        next_version,
                        payload.name,
                        payload.description,
                        payload.mandate,
                        status,
                        payload.baseCurrency,
                        payload.benchmarkSymbol,
                        payload.inceptionDate,
                        payload.notes,
                        _normalize_text(created_by),
                    ),
                )
                if payload.openingCash and existing is None:
                    cur.execute(
                        """
                        INSERT INTO core.portfolio_ledger_events (
                            event_id,
                            account_id,
                            effective_at,
                            event_type,
                            currency,
                            cash_amount,
                            commission,
                            slippage_cost,
                            description,
                            created_at
                        )
                        VALUES (%s, %s, %s, 'opening_balance', %s, %s, 0, 0, %s, NOW())
                        """,
                        (
                            uuid.uuid4().hex,
                            resolved_account_id,
                            datetime.combine(payload.inceptionDate, datetime.min.time(), tzinfo=timezone.utc),
                            payload.baseCurrency,
                            float(payload.openingCash),
                            "Opening balance",
                        ),
                    )
        self._enqueue_materialization(resolved_account_id, reason="account-upsert")
        detail = self.get_account_detail(resolved_account_id)
        if detail is None:
            raise LookupError(f"Portfolio account '{resolved_account_id}' not found after save.")
        return detail

    def list_portfolios(self) -> PortfolioListResponse:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        name,
                        description,
                        benchmark_symbol,
                        status,
                        latest_version,
                        active_version,
                        created_at,
                        updated_at
                    FROM core.portfolio_definitions
                    ORDER BY name
                    """
                )
                portfolios = [
                    PortfolioDefinition.model_validate(_row_to_model_payload(_PORTFOLIO_COLUMNS, row))
                    for row in cur.fetchall()
                ]
        return PortfolioListResponse(portfolios=portfolios)

    def get_portfolio(self, name: str) -> PortfolioDefinition | None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        name,
                        description,
                        benchmark_symbol,
                        status,
                        latest_version,
                        active_version,
                        created_at,
                        updated_at
                    FROM core.portfolio_definitions
                    WHERE name = %s
                    """,
                    (name,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return PortfolioDefinition.model_validate(_row_to_model_payload(_PORTFOLIO_COLUMNS, row))

    def get_portfolio_revision(self, name: str, *, version: int | None = None) -> PortfolioRevision | None:
        dsn = self._require_dsn()
        sql = """
            SELECT
                portfolio_name,
                version,
                description,
                benchmark_symbol,
                allocation_mode,
                allocatable_capital,
                allocations_json,
                notes,
                published_at,
                created_at,
                created_by
            FROM core.portfolio_revisions
            WHERE portfolio_name = %s
        """
        params: list[Any] = [name]
        if version is None:
            sql += " ORDER BY version DESC LIMIT 1"
        else:
            sql += " AND version = %s"
            params.append(int(version))
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        if not row:
            return None
        payload = _row_to_model_payload(_PORTFOLIO_REVISION_COLUMNS, row)
        payload["allocations"] = _parse_json(payload.get("allocations"), [])
        payload["allocations"] = _derive_allocation_weights(
            payload["allocations"],
            allocation_mode=str(payload.get("allocationMode") or "percent"),
            allocatable_capital=payload.get("allocatableCapital"),
        )
        return PortfolioRevision.model_validate(payload)

    def list_portfolio_revisions(self, name: str) -> list[PortfolioRevision]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        portfolio_name,
                        version,
                        description,
                        benchmark_symbol,
                        allocation_mode,
                        allocatable_capital,
                        allocations_json,
                        notes,
                        published_at,
                        created_at,
                        created_by
                    FROM core.portfolio_revisions
                    WHERE portfolio_name = %s
                    ORDER BY version DESC
                    """,
                    (name,),
                )
                rows = cur.fetchall()
        revisions: list[PortfolioRevision] = []
        for row in rows:
            payload = _row_to_model_payload(_PORTFOLIO_REVISION_COLUMNS, row)
            payload["allocations"] = _parse_json(payload.get("allocations"), [])
            payload["allocations"] = _derive_allocation_weights(
                payload["allocations"],
                allocation_mode=str(payload.get("allocationMode") or "percent"),
                allocatable_capital=payload.get("allocatableCapital"),
            )
            revisions.append(PortfolioRevision.model_validate(payload))
        return revisions

    def get_portfolio_detail(self, name: str) -> PortfolioDefinitionDetailResponse | None:
        portfolio = self.get_portfolio(name)
        if portfolio is None:
            return None
        active_revision = self.get_portfolio_revision(name, version=portfolio.activeVersion)
        revisions = self.list_portfolio_revisions(name)
        return PortfolioDefinitionDetailResponse(
            portfolio=portfolio,
            activeRevision=active_revision,
            revisions=revisions,
        )

    def save_portfolio(
        self,
        *,
        payload: PortfolioUpsertRequest,
        created_by: str | None = None,
    ) -> PortfolioDefinitionDetailResponse:
        dsn = self._require_dsn()
        existing = self.get_portfolio(payload.name)
        next_version = int(existing.latestVersion or 0) + 1 if existing else 1
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.portfolio_definitions (
                        name,
                        description,
                        benchmark_symbol,
                        status,
                        latest_version,
                        active_version,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, 'active', %s, %s, NOW(), NOW())
                    ON CONFLICT (name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        benchmark_symbol = EXCLUDED.benchmark_symbol,
                        status = EXCLUDED.status,
                        latest_version = EXCLUDED.latest_version,
                        active_version = EXCLUDED.active_version,
                        updated_at = NOW()
                    """,
                    (
                        payload.name,
                        payload.description,
                        payload.benchmarkSymbol,
                        next_version,
                        next_version,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO core.portfolio_revisions (
                        portfolio_name,
                        version,
                        description,
                        benchmark_symbol,
                        allocation_mode,
                        allocatable_capital,
                        allocations_json,
                        notes,
                        published_at,
                        created_at,
                        created_by
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
                    """,
                    (
                        payload.name,
                        next_version,
                        payload.description,
                        payload.benchmarkSymbol,
                        payload.allocationMode,
                        payload.allocatableCapital,
                        _json_dumps(_portfolio_allocation_payloads(payload)),
                        payload.notes,
                        _normalize_text(created_by),
                    ),
                )
                cur.execute(
                    """
                    SELECT DISTINCT account_id
                    FROM core.portfolio_assignments
                    WHERE portfolio_name = %s
                      AND status IN ('scheduled', 'active')
                    """,
                    (payload.name,),
                )
                affected_account_ids = [str(row[0]) for row in cur.fetchall()]
        for account_id in affected_account_ids:
            self._enqueue_materialization(account_id, reason="portfolio-upsert")
        detail = self.get_portfolio_detail(payload.name)
        if detail is None:
            raise LookupError(f"Portfolio '{payload.name}' not found after save.")
        return detail

    def list_ledger_events(self, account_id: str, *, limit: int, offset: int) -> list[PortfolioLedgerEvent]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        event_id,
                        account_id,
                        effective_at,
                        event_type,
                        currency,
                        cash_amount,
                        symbol,
                        quantity,
                        price,
                        commission,
                        slippage_cost,
                        description
                    FROM core.portfolio_ledger_events
                    WHERE account_id = %s
                    ORDER BY effective_at DESC, created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (account_id, max(1, int(limit)), max(0, int(offset))),
                )
                rows = cur.fetchall()
        return [PortfolioLedgerEvent.model_validate(_row_to_model_payload(_LEDGER_EVENT_COLUMNS, row)) for row in rows]

    def get_latest_ledger_event(self, account_id: str) -> PortfolioLedgerEvent | None:
        events = self.list_ledger_events(account_id, limit=1, offset=0)
        return events[0] if events else None

    def add_ledger_event(self, account_id: str, payload: PortfolioLedgerEventPayload) -> PortfolioLedgerEvent:
        dsn = self._require_dsn()
        if self.get_account(account_id) is None:
            raise LookupError(f"Portfolio account '{account_id}' not found.")
        event_id = uuid.uuid4().hex
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.portfolio_ledger_events (
                        event_id,
                        account_id,
                        effective_at,
                        event_type,
                        currency,
                        cash_amount,
                        symbol,
                        quantity,
                        price,
                        commission,
                        slippage_cost,
                        description,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        event_id,
                        account_id,
                        payload.effectiveAt,
                        payload.eventType,
                        payload.currency,
                        payload.cashAmount,
                        payload.symbol,
                        payload.quantity,
                        payload.price,
                        payload.commission,
                        payload.slippageCost,
                        payload.description,
                    ),
                )
        self._enqueue_materialization(account_id, reason=f"ledger:{payload.eventType}")
        return PortfolioLedgerEvent(eventId=event_id, accountId=account_id, **payload.model_dump(mode="python"))

    def get_active_assignment(self, account_id: str, *, as_of: date | None = None) -> PortfolioAssignment | None:
        dsn = self._require_dsn()
        resolved_as_of = as_of or _utc_now().date()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        assignment_id,
                        account_id,
                        account_version,
                        portfolio_name,
                        portfolio_version,
                        effective_from,
                        effective_to,
                        status,
                        notes,
                        created_at
                    FROM core.portfolio_assignments
                    WHERE account_id = %s
                      AND effective_from <= %s
                      AND (effective_to IS NULL OR effective_to >= %s)
                    ORDER BY effective_from DESC, created_at DESC
                    LIMIT 1
                    """,
                    (account_id, resolved_as_of, resolved_as_of),
                )
                row = cur.fetchone()
        if not row:
            return None
        return PortfolioAssignment.model_validate(_row_to_model_payload(_ASSIGNMENT_COLUMNS, row))

    def assign_portfolio(self, account_id: str, payload: PortfolioAssignmentRequest) -> PortfolioAssignment:
        dsn = self._require_dsn()
        account = self.get_account(account_id)
        if account is None:
            raise LookupError(f"Portfolio account '{account_id}' not found.")
        revision = self.get_account_revision(account_id, version=payload.accountVersion)
        if revision is None:
            raise LookupError(f"Portfolio account revision '{account_id}:{payload.accountVersion}' not found.")
        portfolio_revision = self.get_portfolio_revision(payload.portfolioName, version=payload.portfolioVersion)
        if portfolio_revision is None:
            raise LookupError(f"Portfolio revision '{payload.portfolioName}:{payload.portfolioVersion}' not found.")

        assignment_id = uuid.uuid4().hex
        status = "active" if payload.effectiveFrom <= _utc_now().date() else "scheduled"
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.portfolio_assignments
                    SET
                        status = 'ended',
                        effective_to = LEAST(COALESCE(effective_to, %s), %s)
                    WHERE account_id = %s
                      AND status IN ('scheduled', 'active')
                      AND effective_from <= %s
                    """,
                    (
                        payload.effectiveFrom - timedelta(days=1),
                        payload.effectiveFrom - timedelta(days=1),
                        account_id,
                        payload.effectiveFrom,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO core.portfolio_assignments (
                        assignment_id,
                        account_id,
                        account_version,
                        portfolio_name,
                        portfolio_version,
                        effective_from,
                        effective_to,
                        status,
                        notes,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s, NOW())
                    """,
                    (
                        assignment_id,
                        account_id,
                        payload.accountVersion,
                        payload.portfolioName,
                        payload.portfolioVersion,
                        payload.effectiveFrom,
                        status,
                        payload.notes,
                    ),
                )
                cur.execute(
                    """
                    UPDATE core.portfolio_accounts
                    SET
                        active_portfolio_name = %s,
                        active_portfolio_version = %s,
                        updated_at = NOW()
                    WHERE account_id = %s
                    """,
                    (payload.portfolioName, payload.portfolioVersion, account_id),
                )
        self._enqueue_materialization(account_id, reason="assignment")
        assignment = self.get_active_assignment(account_id, as_of=payload.effectiveFrom)
        if assignment is None:
            raise LookupError(f"Portfolio assignment for account '{account_id}' not found after save.")
        return assignment

    def get_snapshot(self, account_id: str) -> PortfolioSnapshot | None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        account_id,
                        account_name,
                        as_of,
                        nav,
                        cash,
                        gross_exposure,
                        net_exposure,
                        since_inception_pnl,
                        since_inception_return,
                        current_drawdown,
                        max_drawdown,
                        open_alert_count,
                        active_assignment_json,
                        freshness_json,
                        slices_json
                    FROM core.portfolio_snapshots
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        payload = _row_to_model_payload(_SNAPSHOT_COLUMNS, row)
        payload["activeAssignment"] = _parse_json(payload.get("activeAssignment"), None)
        payload["freshness"] = _parse_json(payload.get("freshness"), [])
        payload["slices"] = _parse_json(payload.get("slices"), [])
        return PortfolioSnapshot.model_validate(payload)

    def list_history(self, account_id: str, *, limit: int) -> PortfolioHistoryResponse:
        dsn = self._require_dsn()
        resolved_limit = max(1, int(limit))
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM core.portfolio_history WHERE account_id = %s", (account_id,))
                total = int((cur.fetchone() or (0,))[0] or 0)
                cur.execute(
                    """
                    SELECT
                        as_of,
                        nav,
                        cash,
                        gross_exposure,
                        net_exposure,
                        period_pnl,
                        period_return,
                        cumulative_pnl,
                        cumulative_return,
                        drawdown,
                        turnover,
                        cost_drag_bps
                    FROM core.portfolio_history
                    WHERE account_id = %s
                    ORDER BY as_of DESC
                    LIMIT %s
                    """,
                    (account_id, resolved_limit),
                )
                rows = list(reversed(cur.fetchall()))
        points = [PortfolioHistoryPoint.model_validate(_row_to_model_payload(_HISTORY_COLUMNS, row)) for row in rows]
        return PortfolioHistoryResponse(points=points, totalPoints=total, truncated=total > resolved_limit)

    def list_positions(
        self,
        account_id: str,
        *,
        as_of: date | None = None,
        limit: int,
        offset: int,
    ) -> PortfolioPositionListResponse:
        dsn = self._require_dsn()
        resolved_limit = max(1, int(limit))
        resolved_offset = max(0, int(offset))
        resolved_as_of = as_of
        if resolved_as_of is None:
            snapshot = self.get_snapshot(account_id)
            resolved_as_of = snapshot.asOf if snapshot else None
        if resolved_as_of is None:
            return PortfolioPositionListResponse(positions=[], total=0, limit=resolved_limit, offset=resolved_offset)
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM core.portfolio_positions WHERE account_id = %s AND as_of = %s",
                    (account_id, resolved_as_of),
                )
                total = int((cur.fetchone() or (0,))[0] or 0)
                cur.execute(
                    """
                    SELECT
                        as_of,
                        symbol,
                        quantity,
                        market_value,
                        weight,
                        average_cost,
                        last_price,
                        unrealized_pnl,
                        realized_pnl,
                        contributors_json
                    FROM core.portfolio_positions
                    WHERE account_id = %s
                      AND as_of = %s
                    ORDER BY market_value DESC, symbol ASC
                    LIMIT %s OFFSET %s
                    """,
                    (account_id, resolved_as_of, resolved_limit, resolved_offset),
                )
                rows = cur.fetchall()
        positions: list[PortfolioPosition] = []
        for row in rows:
            payload = _row_to_model_payload(_POSITION_COLUMNS, row)
            payload["contributors"] = _parse_json(payload.get("contributors"), [])
            positions.append(PortfolioPosition.model_validate(payload))
        return PortfolioPositionListResponse(
            positions=positions,
            total=total,
            limit=resolved_limit,
            offset=resolved_offset,
        )

    def list_alerts(self, account_id: str, *, include_resolved: bool = True) -> PortfolioAlertListResponse:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                predicate = "" if include_resolved else "AND status <> 'resolved'"
                cur.execute(
                    f"""
                    SELECT
                        alert_id,
                        account_id,
                        severity,
                        status,
                        code,
                        title,
                        description,
                        detected_at,
                        acknowledged_at,
                        acknowledged_by,
                        resolved_at,
                        as_of
                    FROM core.portfolio_alerts
                    WHERE account_id = %s
                    {predicate}
                    ORDER BY detected_at DESC, alert_id DESC
                    """,
                    (account_id,),
                )
                rows = cur.fetchall()
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM core.portfolio_alerts
                    WHERE account_id = %s
                      AND status = 'open'
                    """,
                    (account_id,),
                )
                open_count = int((cur.fetchone() or (0,))[0] or 0)
        alerts = [PortfolioAlert.model_validate(_row_to_model_payload(_ALERT_COLUMNS, row)) for row in rows]
        return PortfolioAlertListResponse(alerts=alerts, total=len(alerts), openCount=open_count)

    def create_rebalance_preview(
        self,
        account_id: str,
        *,
        as_of: date,
        notes: str = "",
    ) -> RebalanceProposal:
        account = self.get_account(account_id)
        if account is None:
            raise LookupError(f"Portfolio account '{account_id}' not found.")
        assignment = self.get_active_assignment(account_id, as_of=as_of)
        warnings: list[str] = []
        blocked_reasons: list[str] = []
        trades: list[RebalanceTradeProposal] = []
        estimated_cash_impact = 0.0
        estimated_turnover = 0.0
        portfolio_name = assignment.portfolioName if assignment else account.activePortfolioName or ""
        portfolio_version = assignment.portfolioVersion if assignment else int(account.activePortfolioVersion or 0)
        if assignment is None:
            blocked_reasons.append("No active portfolio assignment exists for the requested date.")
        portfolio_revision = (
            self.get_portfolio_revision(portfolio_name, version=portfolio_version) if portfolio_name and portfolio_version else None
        )
        if portfolio_revision is None:
            blocked_reasons.append("No pinned portfolio revision is available for rebalance preview.")
        snapshot = self.get_snapshot(account_id)
        nav = float(snapshot.nav) if snapshot else 0.0
        if nav <= 0:
            ledger_cash = self._sum_ledger_cash(account_id)
            nav = ledger_cash
        if nav <= 0:
            blocked_reasons.append("The account has no positive NAV to rebalance.")
        actual_by_sleeve: dict[str, float] = {}
        if snapshot:
            for slice_payload in snapshot.slices:
                actual_by_sleeve[slice_payload.sleeveId] = float(slice_payload.marketValue)
        else:
            warnings.append("No materialized snapshot was available; preview uses cash-only synthetic sleeve targets.")

        if portfolio_revision is not None and nav > 0:
            if not actual_by_sleeve:
                warnings.append("Preview is sleeve-level and synthetic because look-through positions are not materialized yet.")
            for allocation in portfolio_revision.allocations:
                current_value = actual_by_sleeve.get(allocation.sleeveId, 0.0)
                target_value = _target_value_for_allocation(
                    allocation,
                    allocation_mode=str(portfolio_revision.allocationMode),
                    nav=nav,
                )
                delta = target_value - current_value
                if abs(delta) < max(nav * 0.0025, 1.0):
                    continue
                estimated_price = 100.0
                quantity = round(abs(delta) / estimated_price, 6)
                trade = RebalanceTradeProposal(
                    sleeveId=allocation.sleeveId,
                    symbol=_synthetic_symbol(allocation.strategy.strategyName),
                    side="buy" if delta > 0 else "sell",
                    quantity=max(quantity, 0.000001),
                    estimatedPrice=estimated_price,
                    estimatedNotional=abs(delta),
                    estimatedCommission=0.0,
                    estimatedSlippageCost=0.0,
                )
                trades.append(trade)
                signed_cash = -abs(delta) if delta > 0 else abs(delta)
                estimated_cash_impact += signed_cash
                estimated_turnover += abs(delta)

        blocked = len(blocked_reasons) > 0
        estimated_turnover = (estimated_turnover / nav) if nav > 0 else 0.0
        proposal = RebalanceProposal(
            proposalId=uuid.uuid4().hex,
            accountId=account_id,
            asOf=as_of,
            portfolioName=portfolio_name or "unassigned",
            portfolioVersion=max(portfolio_version, 1),
            blocked=blocked,
            warnings=warnings,
            blockedReasons=blocked_reasons,
            estimatedCashImpact=estimated_cash_impact,
            estimatedTurnover=estimated_turnover,
            trades=trades,
        )
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.portfolio_rebalance_proposals (
                        proposal_id,
                        account_id,
                        as_of,
                        portfolio_name,
                        portfolio_version,
                        blocked,
                        warnings_json,
                        blocked_reasons_json,
                        estimated_cash_impact,
                        estimated_turnover,
                        trades_json,
                        notes,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        proposal.proposalId,
                        account_id,
                        as_of,
                        proposal.portfolioName,
                        proposal.portfolioVersion,
                        proposal.blocked,
                        _json_dumps(proposal.warnings),
                        _json_dumps(proposal.blockedReasons),
                        proposal.estimatedCashImpact,
                        proposal.estimatedTurnover,
                        _json_dumps([trade.model_dump(mode="json") for trade in proposal.trades]),
                        notes,
                    ),
                )
        return proposal

    def get_rebalance_proposal(self, proposal_id: str) -> RebalanceProposal | None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        proposal_id,
                        account_id,
                        as_of,
                        portfolio_name,
                        portfolio_version,
                        blocked,
                        warnings_json,
                        blocked_reasons_json,
                        estimated_cash_impact,
                        estimated_turnover,
                        trades_json
                    FROM core.portfolio_rebalance_proposals
                    WHERE proposal_id = %s
                    """,
                    (proposal_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        payload = _row_to_model_payload(_PROPOSAL_COLUMNS, row)
        payload["warnings"] = _parse_json(payload.get("warnings"), [])
        payload["blockedReasons"] = _parse_json(payload.get("blockedReasons"), [])
        payload["trades"] = _parse_json(payload.get("trades"), [])
        return RebalanceProposal.model_validate(payload)

    def apply_rebalance(self, account_id: str, *, proposal_id: str, executed_at: datetime, notes: str = "") -> dict[str, Any]:
        proposal = self.get_rebalance_proposal(proposal_id)
        if proposal is None or proposal.accountId != account_id:
            raise LookupError(f"Rebalance proposal '{proposal_id}' not found.")
        if proposal.blocked:
            raise ValueError("Blocked rebalance proposals cannot be applied.")
        account = self.get_account(account_id)
        if account is None:
            raise LookupError(f"Portfolio account '{account_id}' not found.")
        dsn = self._require_dsn()
        event_ids: list[str] = []
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                for trade in proposal.trades:
                    event_id = uuid.uuid4().hex
                    event_ids.append(event_id)
                    notional = float(trade.estimatedNotional)
                    commission = float(trade.estimatedCommission)
                    slippage = float(trade.estimatedSlippageCost)
                    cash_amount = -(notional + commission + slippage) if trade.side == "buy" else (notional - commission - slippage)
                    cur.execute(
                        """
                        INSERT INTO core.portfolio_ledger_events (
                            event_id,
                            account_id,
                            effective_at,
                            event_type,
                            currency,
                            cash_amount,
                            symbol,
                            quantity,
                            price,
                            commission,
                            slippage_cost,
                            description,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            event_id,
                            account_id,
                            executed_at,
                            "rebalance_buy" if trade.side == "buy" else "rebalance_sell",
                            account.baseCurrency,
                            cash_amount,
                            trade.symbol,
                            trade.quantity,
                            trade.estimatedPrice,
                            commission,
                            slippage,
                            f"Applied rebalance proposal {proposal_id}. {notes}".strip(),
                        ),
                    )
                cur.execute(
                    """
                    UPDATE core.portfolio_rebalance_proposals
                    SET applied_at = NOW()
                    WHERE proposal_id = %s
                    """,
                    (proposal_id,),
                )
        self._enqueue_materialization(account_id, reason="rebalance-apply")
        return {"status": "ok", "proposalId": proposal_id, "ledgerEventCount": len(event_ids)}

    def _sum_ledger_cash(self, account_id: str) -> float:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(SUM(cash_amount), 0) FROM core.portfolio_ledger_events WHERE account_id = %s",
                    (account_id,),
                )
                row = cur.fetchone()
        return float((row or (0.0,))[0] or 0.0)

    def claim_next_materialization(self, *, execution_name: str | None = None) -> dict[str, Any] | None:
        dsn = self._require_dsn()
        claim_token = uuid.uuid4().hex
        claimed_at = _utc_now()
        claim_expires_at = claimed_at + _MATERIALIZATION_CLAIM_TTL
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT account_id, dependency_fingerprint, dependency_state
                    FROM core.portfolio_materialization_state
                    WHERE status IN ('dirty', 'failed')
                       OR (status = 'claimed' AND (claim_expires_at IS NULL OR claim_expires_at <= NOW()))
                    ORDER BY updated_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row:
                    return None
                account_id = str(row[0])
                dependency_fingerprint = _normalize_text(row[1])
                dependency_state = _parse_json(row[2], {})
                cur.execute(
                    """
                    UPDATE core.portfolio_materialization_state
                    SET
                        status = 'claimed',
                        claim_token = %s,
                        claimed_by = %s,
                        claimed_at = %s,
                        claim_expires_at = %s,
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE account_id = %s
                    """,
                    (
                        claim_token,
                        _normalize_text(execution_name),
                        claimed_at,
                        claim_expires_at,
                        account_id,
                    ),
                )
        return {
            "accountId": account_id,
            "claimToken": claim_token,
            "dependencyFingerprint": dependency_fingerprint,
            "dependencyState": dependency_state,
        }

    def get_materialization_state(self, account_id: str) -> dict[str, Any] | None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        account_id,
                        dependency_fingerprint,
                        dependency_state,
                        status,
                        claim_token,
                        claimed_by,
                        claimed_at,
                        claim_expires_at,
                        last_materialized_at,
                        last_snapshot_as_of,
                        last_error,
                        updated_at
                    FROM core.portfolio_materialization_state
                    WHERE account_id = %s
                    """,
                    (account_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "accountId": row[0],
            "dependencyFingerprint": row[1],
            "dependencyState": _parse_json(row[2], {}),
            "status": row[3],
            "claimToken": row[4],
            "claimedBy": row[5],
            "claimedAt": row[6],
            "claimExpiresAt": row[7],
            "lastMaterializedAt": row[8],
            "lastSnapshotAsOf": row[9],
            "lastError": row[10],
            "updatedAt": row[11],
        }

    def start_materialization(self, account_id: str, *, claim_token: str, execution_name: str | None = None) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.portfolio_materialization_state
                    SET
                        status = 'claimed',
                        claimed_by = COALESCE(%s, claimed_by),
                        claimed_at = NOW(),
                        claim_expires_at = NOW() + INTERVAL '30 minutes',
                        updated_at = NOW()
                    WHERE account_id = %s
                      AND claim_token = %s
                    RETURNING account_id
                    """,
                    (_normalize_text(execution_name), account_id, claim_token),
                )
                if not cur.fetchone():
                    raise LookupError(f"Portfolio materialization claim not found for account '{account_id}'.")

    def heartbeat_materialization(self, account_id: str, *, claim_token: str) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.portfolio_materialization_state
                    SET
                        claimed_at = NOW(),
                        claim_expires_at = NOW() + INTERVAL '30 minutes',
                        updated_at = NOW()
                    WHERE account_id = %s
                      AND claim_token = %s
                      AND status = 'claimed'
                    RETURNING account_id
                    """,
                    (account_id, claim_token),
                )
                if not cur.fetchone():
                    raise LookupError(f"Portfolio materialization claim not found for account '{account_id}'.")

    def complete_materialization(
        self,
        account_id: str,
        *,
        claim_token: str,
        dependency_fingerprint: str | None,
        dependency_state: dict[str, Any] | None,
        snapshot: PortfolioSnapshot,
        history: list[PortfolioHistoryPoint],
        positions: list[PortfolioPosition],
        alerts: list[PortfolioAlert],
    ) -> dict[str, Any]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT dependency_fingerprint
                    FROM core.portfolio_materialization_state
                    WHERE account_id = %s
                      AND claim_token = %s
                    FOR UPDATE
                    """,
                    (account_id, claim_token),
                )
                row = cur.fetchone()
                if not row:
                    raise LookupError(f"Portfolio materialization claim not found for account '{account_id}'.")
                current_fingerprint = _normalize_text(row[0])
                cur.execute(
                    """
                    INSERT INTO core.portfolio_snapshots (
                        account_id,
                        account_name,
                        as_of,
                        nav,
                        cash,
                        gross_exposure,
                        net_exposure,
                        since_inception_pnl,
                        since_inception_return,
                        current_drawdown,
                        max_drawdown,
                        open_alert_count,
                        active_assignment_json,
                        freshness_json,
                        slices_json,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (account_id)
                    DO UPDATE SET
                        account_name = EXCLUDED.account_name,
                        as_of = EXCLUDED.as_of,
                        nav = EXCLUDED.nav,
                        cash = EXCLUDED.cash,
                        gross_exposure = EXCLUDED.gross_exposure,
                        net_exposure = EXCLUDED.net_exposure,
                        since_inception_pnl = EXCLUDED.since_inception_pnl,
                        since_inception_return = EXCLUDED.since_inception_return,
                        current_drawdown = EXCLUDED.current_drawdown,
                        max_drawdown = EXCLUDED.max_drawdown,
                        open_alert_count = EXCLUDED.open_alert_count,
                        active_assignment_json = EXCLUDED.active_assignment_json,
                        freshness_json = EXCLUDED.freshness_json,
                        slices_json = EXCLUDED.slices_json,
                        updated_at = NOW()
                    """,
                    (
                        account_id,
                        snapshot.accountName,
                        snapshot.asOf,
                        snapshot.nav,
                        snapshot.cash,
                        snapshot.grossExposure,
                        snapshot.netExposure,
                        snapshot.sinceInceptionPnl,
                        snapshot.sinceInceptionReturn,
                        snapshot.currentDrawdown,
                        snapshot.maxDrawdown,
                        snapshot.openAlertCount,
                        _json_dumps(snapshot.activeAssignment.model_dump(mode="json")) if snapshot.activeAssignment else None,
                        _json_dumps([item.model_dump(mode="json") for item in snapshot.freshness]),
                        _json_dumps([item.model_dump(mode="json") for item in snapshot.slices]),
                    ),
                )
                cur.execute("DELETE FROM core.portfolio_history WHERE account_id = %s", (account_id,))
                for point in history:
                    cur.execute(
                        """
                        INSERT INTO core.portfolio_history (
                            account_id,
                            as_of,
                            nav,
                            cash,
                            gross_exposure,
                            net_exposure,
                            period_pnl,
                            period_return,
                            cumulative_pnl,
                            cumulative_return,
                            drawdown,
                            turnover,
                            cost_drag_bps
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            account_id,
                            point.asOf,
                            point.nav,
                            point.cash,
                            point.grossExposure,
                            point.netExposure,
                            point.periodPnl,
                            point.periodReturn,
                            point.cumulativePnl,
                            point.cumulativeReturn,
                            point.drawdown,
                            point.turnover,
                            point.costDragBps,
                        ),
                    )
                cur.execute("DELETE FROM core.portfolio_positions WHERE account_id = %s", (account_id,))
                for position in positions:
                    cur.execute(
                        """
                        INSERT INTO core.portfolio_positions (
                            account_id,
                            as_of,
                            symbol,
                            quantity,
                            market_value,
                            weight,
                            average_cost,
                            last_price,
                            unrealized_pnl,
                            realized_pnl,
                            contributors_json
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            account_id,
                            position.asOf,
                            position.symbol,
                            position.quantity,
                            position.marketValue,
                            position.weight,
                            position.averageCost,
                            position.lastPrice,
                            position.unrealizedPnl,
                            position.realizedPnl,
                            _json_dumps([item.model_dump(mode="json") for item in position.contributors]),
                        ),
                    )
                cur.execute("DELETE FROM core.portfolio_alerts WHERE account_id = %s", (account_id,))
                for alert in alerts:
                    cur.execute(
                        """
                        INSERT INTO core.portfolio_alerts (
                            alert_id,
                            account_id,
                            severity,
                            status,
                            code,
                            title,
                            description,
                            detected_at,
                            acknowledged_at,
                            acknowledged_by,
                            resolved_at,
                            as_of
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            alert.alertId,
                            account_id,
                            alert.severity,
                            alert.status,
                            alert.code,
                            alert.title,
                            alert.description,
                            alert.detectedAt,
                            alert.acknowledgedAt,
                            alert.acknowledgedBy,
                            alert.resolvedAt,
                            alert.asOf,
                        ),
                    )
                cur.execute(
                    """
                    UPDATE core.portfolio_accounts
                    SET
                        last_materialized_at = NOW(),
                        open_alert_count = %s,
                        updated_at = NOW()
                    WHERE account_id = %s
                    """,
                    (
                        sum(1 for alert in alerts if alert.status == "open"),
                        account_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE core.portfolio_materialization_state
                    SET
                        status = CASE
                            WHEN dependency_fingerprint = %s THEN 'idle'
                            ELSE 'dirty'
                        END,
                        claim_token = NULL,
                        claimed_by = NULL,
                        claimed_at = NULL,
                        claim_expires_at = NULL,
                        last_materialized_at = NOW(),
                        last_snapshot_as_of = %s,
                        dependency_state = COALESCE(%s::jsonb, dependency_state),
                        last_error = NULL,
                        updated_at = NOW()
                    WHERE account_id = %s
                    """,
                    (
                        dependency_fingerprint,
                        snapshot.asOf,
                        _json_dumps(dependency_state) if dependency_state is not None else None,
                        account_id,
                    ),
                )
        return {
            "status": "ok",
            "accountId": account_id,
            "currentDependencyFingerprint": current_fingerprint,
        }

    def fail_materialization(self, account_id: str, *, claim_token: str, error: str) -> dict[str, Any]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.portfolio_materialization_state
                    SET
                        status = 'failed',
                        claim_token = NULL,
                        claimed_by = NULL,
                        claimed_at = NULL,
                        claim_expires_at = NULL,
                        last_error = %s,
                        updated_at = NOW()
                    WHERE account_id = %s
                      AND claim_token = %s
                    RETURNING account_id
                    """,
                    (str(error)[:4000], account_id, claim_token),
                )
                if not cur.fetchone():
                    raise LookupError(f"Portfolio materialization claim not found for account '{account_id}'.")
        return {"status": "ok", "accountId": account_id}

    def get_materialization_bundle(self, account_id: str, *, claim_token: str | None = None) -> dict[str, Any]:
        account_detail = self.get_account_detail(account_id)
        if account_detail is None:
            raise LookupError(f"Portfolio account '{account_id}' not found.")
        portfolio = None
        portfolio_revision = None
        if account_detail.activeAssignment is not None:
            portfolio = self.get_portfolio(account_detail.activeAssignment.portfolioName)
            portfolio_revision = self.get_portfolio_revision(
                account_detail.activeAssignment.portfolioName,
                version=account_detail.activeAssignment.portfolioVersion,
            )
        state = self.get_materialization_state(account_id) or {}
        if claim_token is not None:
            current_claim = str(state.get("claimToken") or "").strip()
            if current_claim and current_claim != claim_token:
                raise LookupError(f"Portfolio materialization claim not found for account '{account_id}'.")
        snapshot = self.get_snapshot(account_id)
        freshness = snapshot.freshness if snapshot else []
        strategy_dependencies: list[dict[str, Any]] = []
        if portfolio_revision is not None:
            strategy_repo = StrategyRepository(self._require_dsn())
            for allocation in portfolio_revision.allocations:
                dependency = strategy_repo.get_strategy_revision(
                    allocation.strategy.strategyName,
                    version=allocation.strategy.strategyVersion,
                )
                if dependency:
                    strategy_dependencies.append(dependency)
        latest_event = self.get_latest_ledger_event(account_id)
        as_of = snapshot.asOf if snapshot else (_normalize_date(latest_event.effectiveAt) if latest_event else None)
        return {
            "account": account_detail.account.model_dump(mode="json"),
            "accountRevision": account_detail.revision.model_dump(mode="json") if account_detail.revision else None,
            "activeAssignment": account_detail.activeAssignment.model_dump(mode="json") if account_detail.activeAssignment else None,
            "portfolio": portfolio.model_dump(mode="json") if portfolio else None,
            "portfolioRevision": portfolio_revision.model_dump(mode="json") if portfolio_revision else None,
            "ledgerEvents": [event.model_dump(mode="json") for event in self.list_ledger_events(account_id, limit=2000, offset=0)],
            "alerts": [alert.model_dump(mode="json") for alert in self.list_alerts(account_id).alerts],
            "freshness": [item.model_dump(mode="json") for item in freshness],
            "dependencyFingerprint": state.get("dependencyFingerprint"),
            "dependencyState": state.get("dependencyState") or {},
            "asOf": as_of.isoformat() if isinstance(as_of, date) else None,
            "claimToken": claim_token,
            "strategyDependencies": strategy_dependencies,
        }

    def rebuild_materializations(self, *, account_id: str | None = None) -> dict[str, Any]:
        self._require_dsn()
        if account_id:
            self._enqueue_materialization(account_id, reason="rebuild")
            return {"status": "ok", "accountIds": [account_id], "count": 1}
        account_ids = [account.accountId for account in self.list_accounts().accounts]
        for current_account_id in account_ids:
            self._enqueue_materialization(current_account_id, reason="rebuild-all")
        return {"status": "ok", "accountIds": account_ids, "count": len(account_ids)}

    def list_materialization_state(self) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        account_id,
                        dependency_fingerprint,
                        dependency_state,
                        status,
                        claim_token,
                        claimed_by,
                        claimed_at,
                        claim_expires_at,
                        last_materialized_at,
                        last_snapshot_as_of,
                        last_error,
                        updated_at
                    FROM core.portfolio_materialization_state
                    ORDER BY updated_at DESC, account_id ASC
                    """
                )
                rows = cur.fetchall()
        state_rows: list[dict[str, Any]] = []
        for row in rows:
            state_rows.append(
                {
                    "accountId": row[0],
                    "dependencyFingerprint": row[1],
                    "dependencyState": _parse_json(row[2], {}),
                    "status": row[3],
                    "claimToken": row[4],
                    "claimedBy": row[5],
                    "claimedAt": row[6],
                    "claimExpiresAt": row[7],
                    "lastMaterializedAt": row[8],
                    "lastSnapshotAsOf": row[9],
                    "lastError": row[10],
                    "updatedAt": row[11],
                }
            )
        return state_rows
