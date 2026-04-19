from __future__ import annotations

import os
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional

from psycopg.rows import dict_row

from asset_allocation_contracts.government_signals import (
    CongressTradeEvent,
    CongressTradeEventListResponse,
    GovernmentContractEvent,
    GovernmentContractEventListResponse,
    GovernmentSignalAlert,
    GovernmentSignalAlertListResponse,
    GovernmentSignalIssuerSummaryResponse,
    GovernmentSignalMappingOverrideRequest,
    GovernmentSignalMappingOverrideResponse,
    GovernmentSignalMappingReviewItem,
    GovernmentSignalMappingReviewResponse,
    GovernmentSignalPortfolioExposureRequest,
    GovernmentSignalPortfolioExposureResponse,
    GovernmentSignalPortfolioIssuerExposure,
    IssuerGovernmentSignalDaily,
)
from asset_allocation_runtime_common.foundation.postgres import connect

_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,15}$")
_MAPPING_STATUS_BY_ACTION = {
    "map": "mapped",
    "ignore": "ignored",
    "defer": "pending_review",
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    if symbol is None:
        return None
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return None
    if not _SYMBOL_RE.fullmatch(normalized):
        raise ValueError(f"Invalid symbol {symbol!r}.")
    return normalized


def _iso_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    text = str(value).strip()
    return text or None


class GovernmentSignalsRepository:
    def __init__(self, dsn: Optional[str] = None) -> None:
        self.dsn = (dsn or os.environ.get("POSTGRES_DSN") or "").strip()

    def _require_dsn(self) -> str:
        if not self.dsn:
            raise ValueError("Postgres is required for government signals features.")
        return self.dsn

    def _fetch_rows(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        with connect(self._require_dsn()) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    def _fetch_value(self, sql: str, params: Iterable[Any] = ()) -> Any:
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                row = cur.fetchone()
        return row[0] if row else None

    @staticmethod
    def _where_clause(filters: list[str]) -> str:
        if not filters:
            return ""
        return " WHERE " + " AND ".join(filters)

    def list_congress_events(
        self,
        *,
        symbol: Optional[str] = None,
        member_id: Optional[str] = None,
        chamber: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> CongressTradeEventListResponse:
        filters: list[str] = []
        params: list[Any] = []

        normalized_symbol = _normalize_symbol(symbol)
        if normalized_symbol:
            filters.append("issuer_ticker = %s")
            params.append(normalized_symbol)
        if member_id:
            filters.append("member_id = %s")
            params.append(str(member_id).strip())
        if chamber:
            filters.append("chamber = %s")
            params.append(str(chamber).strip().lower())
        if from_date:
            filters.append("traded_at::date >= %s")
            params.append(from_date)
        if to_date:
            filters.append("traded_at::date <= %s")
            params.append(to_date)

        where_sql = self._where_clause(filters)
        total = int(
            self._fetch_value(
                f"SELECT COUNT(*) FROM gold.government_signal_congress_events{where_sql}",
                params,
            )
            or 0
        )
        rows = self._fetch_rows(
            f"""
            SELECT
                event_id,
                source_name,
                source_event_key,
                member_id,
                member_name,
                chamber,
                party,
                state,
                district,
                committee_names,
                traded_at,
                filed_at,
                notified_at,
                relationship_type,
                transaction_type,
                filing_status,
                amendment_flag,
                late_filing_days,
                asset_name,
                asset_description,
                asset_type,
                issuer_name,
                issuer_ticker,
                amount_lower_usd,
                amount_upper_usd,
                amount_bucket_label,
                comments,
                excess_return,
                confidence,
                mapping_status,
                created_at,
                updated_at
            FROM gold.government_signal_congress_events
            {where_sql}
            ORDER BY traded_at DESC, event_id DESC
            LIMIT %s
            OFFSET %s
            """,
            [*params, int(limit), int(offset)],
        )
        events = [CongressTradeEvent.model_validate(row) for row in rows]
        return CongressTradeEventListResponse(events=events, total=total, limit=int(limit), offset=int(offset))

    def list_contract_events(
        self,
        *,
        symbol: Optional[str] = None,
        awarding_agency: Optional[str] = None,
        event_type: Optional[str] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> GovernmentContractEventListResponse:
        filters: list[str] = []
        params: list[Any] = []

        normalized_symbol = _normalize_symbol(symbol)
        if normalized_symbol:
            filters.append("recipient_ticker = %s")
            params.append(normalized_symbol)
        if awarding_agency:
            filters.append("awarding_agency = %s")
            params.append(str(awarding_agency).strip())
        if event_type:
            filters.append("event_type = %s")
            params.append(str(event_type).strip().lower())
        if from_date:
            filters.append("event_at::date >= %s")
            params.append(from_date)
        if to_date:
            filters.append("event_at::date <= %s")
            params.append(to_date)

        where_sql = self._where_clause(filters)
        total = int(
            self._fetch_value(
                f"SELECT COUNT(*) FROM gold.government_signal_contract_events{where_sql}",
                params,
            )
            or 0
        )
        rows = self._fetch_rows(
            f"""
            SELECT
                event_id,
                source_name,
                source_event_key,
                event_type,
                event_at,
                recipient_name,
                recipient_ticker,
                awarding_agency,
                funding_agency,
                award_id,
                parent_award_id,
                opportunity_id,
                solicitation_id,
                title,
                description,
                award_amount_usd,
                obligation_delta_usd,
                outlay_delta_usd,
                cumulative_obligation_usd,
                modification_number,
                option_exercise_flag,
                termination_flag,
                cancellation_flag,
                protest_flag,
                naics_code,
                psc_code,
                competition_type,
                set_aside_type,
                contract_vehicle,
                place_of_performance_country,
                place_of_performance_state,
                confidence,
                mapping_status,
                created_at,
                updated_at
            FROM gold.government_signal_contract_events
            {where_sql}
            ORDER BY event_at DESC, event_id DESC
            LIMIT %s
            OFFSET %s
            """,
            [*params, int(limit), int(offset)],
        )
        events = [GovernmentContractEvent.model_validate(row) for row in rows]
        return GovernmentContractEventListResponse(events=events, total=total, limit=int(limit), offset=int(offset))

    def list_alerts(
        self,
        *,
        symbol: Optional[str] = None,
        severity: Optional[str] = None,
        as_of_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> GovernmentSignalAlertListResponse:
        filters: list[str] = []
        params: list[Any] = []

        normalized_symbol = _normalize_symbol(symbol)
        if normalized_symbol:
            filters.append("symbol = %s")
            params.append(normalized_symbol)
        if severity:
            filters.append("severity = %s")
            params.append(str(severity).strip().lower())
        if as_of_date:
            filters.append("as_of_date = %s")
            params.append(as_of_date)

        where_sql = self._where_clause(filters)
        total = int(
            self._fetch_value(
                f"SELECT COUNT(*) FROM gold.government_signal_alerts{where_sql}",
                params,
            )
            or 0
        )
        rows = self._fetch_rows(
            f"""
            SELECT
                alert_id,
                symbol,
                as_of_date,
                alert_type,
                severity,
                title,
                summary,
                congress_signal_score,
                contract_signal_score,
                composite_signal_score,
                source_event_ids,
                created_at
            FROM gold.government_signal_alerts
            {where_sql}
            ORDER BY as_of_date DESC, created_at DESC NULLS LAST, alert_id DESC
            LIMIT %s
            OFFSET %s
            """,
            [*params, int(limit), int(offset)],
        )
        alerts = [GovernmentSignalAlert.model_validate(row) for row in rows]
        return GovernmentSignalAlertListResponse(alerts=alerts, total=total, limit=int(limit), offset=int(offset))

    def list_mapping_review(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> GovernmentSignalMappingReviewResponse:
        filters: list[str] = []
        params: list[Any] = []
        if status:
            filters.append("status = %s")
            params.append(str(status).strip().lower())
        where_sql = self._where_clause(filters)
        total = int(
            self._fetch_value(
                f"SELECT COUNT(*) FROM core.government_signal_entity_map{where_sql}",
                params,
            )
            or 0
        )
        rows = self._fetch_rows(
            f"""
            SELECT
                mapping_id,
                source_name,
                entity_type,
                raw_key,
                raw_name,
                proposed_symbol,
                confidence,
                status,
                reason,
                updated_at
            FROM core.government_signal_entity_map
            {where_sql}
            ORDER BY
                CASE status
                    WHEN 'pending_review' THEN 0
                    WHEN 'mapped' THEN 1
                    WHEN 'ignored' THEN 2
                    ELSE 3
                END,
                updated_at DESC,
                mapping_id DESC
            LIMIT %s
            OFFSET %s
            """,
            [*params, int(limit), int(offset)],
        )
        items = [GovernmentSignalMappingReviewItem.model_validate(row) for row in rows]
        return GovernmentSignalMappingReviewResponse(items=items, total=total, limit=int(limit), offset=int(offset))

    def apply_mapping_override(
        self,
        *,
        mapping_id: str,
        request: GovernmentSignalMappingOverrideRequest,
        actor: Optional[str] = None,
    ) -> GovernmentSignalMappingOverrideResponse:
        normalized_mapping_id = str(mapping_id or "").strip()
        if not normalized_mapping_id:
            raise ValueError("mapping_id is required.")

        status = _MAPPING_STATUS_BY_ACTION[request.action]
        symbol = _normalize_symbol(request.symbol)
        reason = (request.reason or "").strip() or None
        override_id = f"gov-map-{uuid.uuid4().hex}"

        with connect(self._require_dsn()) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT proposed_symbol
                    FROM core.government_signal_entity_map
                    WHERE mapping_id = %s
                    """,
                    (normalized_mapping_id,),
                )
                existing = cur.fetchone()
                if not existing:
                    raise KeyError(f"mapping_id={normalized_mapping_id!r} not found.")

                persisted_symbol = symbol
                if request.action == "defer":
                    persisted_symbol = existing.get("proposed_symbol")

                cur.execute(
                    """
                    INSERT INTO core.government_signal_mapping_overrides (
                        override_id,
                        mapping_id,
                        action,
                        symbol,
                        reason,
                        created_by,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        override_id,
                        normalized_mapping_id,
                        request.action,
                        persisted_symbol,
                        reason,
                        actor,
                    ),
                )
                cur.execute(
                    """
                    UPDATE core.government_signal_entity_map
                    SET proposed_symbol = %s,
                        status = %s,
                        reason = %s,
                        updated_at = NOW()
                    WHERE mapping_id = %s
                    RETURNING mapping_id, status, proposed_symbol AS symbol, updated_at
                    """,
                    (
                        persisted_symbol if request.action == "map" else None if request.action == "ignore" else persisted_symbol,
                        status,
                        reason,
                        normalized_mapping_id,
                    ),
                )
                row = cur.fetchone()

        if not row:
            raise KeyError(f"mapping_id={normalized_mapping_id!r} not found.")
        return GovernmentSignalMappingOverrideResponse.model_validate(dict(row))

    def _resolve_as_of_date(self, *, symbol: Optional[str] = None, requested: Optional[date] = None) -> date:
        if requested is not None:
            return requested
        if symbol:
            resolved = self._fetch_value(
                "SELECT MAX(as_of_date) FROM gold.government_signal_issuer_daily WHERE symbol = %s",
                (_normalize_symbol(symbol),),
            )
        else:
            resolved = self._fetch_value("SELECT MAX(as_of_date) FROM gold.government_signal_issuer_daily")
        if resolved is None:
            raise ValueError("No government signal issuer_daily rows are available.")
        if isinstance(resolved, datetime):
            return resolved.date()
        if isinstance(resolved, date):
            return resolved
        return date.fromisoformat(str(resolved))

    def get_issuer_summary(
        self,
        *,
        symbol: str,
        as_of_date: Optional[date] = None,
        recent_limit: int = 20,
    ) -> GovernmentSignalIssuerSummaryResponse:
        normalized_symbol = _normalize_symbol(symbol)
        resolved_date = self._resolve_as_of_date(symbol=normalized_symbol, requested=as_of_date)
        daily_rows = self._fetch_rows(
            """
            SELECT *
            FROM gold.government_signal_issuer_daily
            WHERE symbol = %s
              AND as_of_date = %s
            """,
            (normalized_symbol, resolved_date),
        )
        if not daily_rows:
            raise KeyError(f"symbol={normalized_symbol!r} has no issuer daily row for {resolved_date.isoformat()}.")

        issuer_daily = IssuerGovernmentSignalDaily.model_validate(daily_rows[0])
        congress = self.list_congress_events(
            symbol=normalized_symbol,
            to_date=resolved_date,
            limit=min(int(recent_limit), 100),
            offset=0,
        ).events
        contracts = self.list_contract_events(
            symbol=normalized_symbol,
            to_date=resolved_date,
            limit=min(int(recent_limit), 100),
            offset=0,
        ).events
        alerts = self.list_alerts(
            symbol=normalized_symbol,
            as_of_date=resolved_date,
            limit=min(int(recent_limit), 100),
            offset=0,
        ).alerts
        return GovernmentSignalIssuerSummaryResponse(
            symbol=normalized_symbol,
            issuer_name=issuer_daily.issuer_name,
            as_of_date=resolved_date,
            issuer_daily=issuer_daily,
            recent_congress_trades=congress,
            recent_contract_events=contracts,
            active_alerts=alerts,
        )

    def build_portfolio_exposure(
        self,
        request: GovernmentSignalPortfolioExposureRequest,
    ) -> GovernmentSignalPortfolioExposureResponse:
        resolved_date = self._resolve_as_of_date(requested=request.as_of_date)
        holdings = request.holdings
        symbols = [_normalize_symbol(holding.symbol) for holding in holdings]
        rows = self._fetch_rows(
            """
            SELECT *
            FROM gold.government_signal_issuer_daily
            WHERE as_of_date = %s
              AND symbol = ANY(%s)
            """,
            (resolved_date, symbols),
        )
        by_symbol = {
            str(row.get("symbol") or "").strip().upper(): IssuerGovernmentSignalDaily.model_validate(row)
            for row in rows
        }
        alert_rows = self._fetch_rows(
            """
            SELECT
                alert_id,
                symbol,
                as_of_date,
                alert_type,
                severity,
                title,
                summary,
                congress_signal_score,
                contract_signal_score,
                composite_signal_score,
                source_event_ids,
                created_at
            FROM gold.government_signal_alerts
            WHERE as_of_date = %s
              AND symbol = ANY(%s)
            ORDER BY created_at DESC NULLS LAST, alert_id DESC
            """,
            (resolved_date, symbols),
        )
        alerts_by_symbol: dict[str, list[GovernmentSignalAlert]] = {}
        for row in alert_rows:
            key = str(row.get("symbol") or "").strip().upper()
            alerts_by_symbol.setdefault(key, []).append(GovernmentSignalAlert.model_validate(row))

        exposures: list[GovernmentSignalPortfolioIssuerExposure] = []
        unmatched_symbols: list[str] = []
        matched = 0
        total_market_value = 0.0
        total_portfolio_weight = 0.0
        any_market_value = False
        any_weight = False

        for holding in holdings:
            normalized_symbol = _normalize_symbol(holding.symbol)
            if holding.market_value is not None:
                total_market_value += float(holding.market_value)
                any_market_value = True
            if holding.portfolio_weight is not None:
                total_portfolio_weight += float(holding.portfolio_weight)
                any_weight = True

            issuer_daily = by_symbol.get(normalized_symbol)
            symbol_alerts = alerts_by_symbol.get(normalized_symbol, [])
            is_matched = issuer_daily is not None
            if is_matched:
                matched += 1
            else:
                unmatched_symbols.append(normalized_symbol)
            exposures.append(
                GovernmentSignalPortfolioIssuerExposure(
                    symbol=normalized_symbol,
                    issuer_name=issuer_daily.issuer_name if issuer_daily else None,
                    matched=is_matched,
                    market_value=holding.market_value,
                    portfolio_weight=holding.portfolio_weight,
                    issuer_daily=issuer_daily,
                    alerts=symbol_alerts,
                )
            )

        return GovernmentSignalPortfolioExposureResponse(
            as_of_date=resolved_date,
            holdings_analyzed=len(holdings),
            matched_holdings=matched,
            unmatched_symbols=unmatched_symbols,
            total_market_value=total_market_value if any_market_value else None,
            total_portfolio_weight=total_portfolio_weight if any_weight else None,
            exposures=exposures,
        )
