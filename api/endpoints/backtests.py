from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from collections.abc import Callable
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request
from api.service.backtest_contracts_compat import (
    BacktestAttributionExposureResponse,
    BacktestDataProvenance,
    BacktestExecutionAssumptions,
    BacktestLookupRequest,
    BacktestLookupResponse,
    BacktestReplayTimelineResponse,
    BacktestResultLinks,
    BacktestRunComparisonRequest,
    BacktestRunComparisonResponse,
    BacktestRunDetailResponse,
    BacktestRunResponse,
    BacktestRunRequest,
    BacktestSummary,
    BacktestStreamEvent,
    BacktestValidationReport,
    ClosedPositionListResponse,
    RunListResponse,
    RunPinsResponse,
    RunRecordResponse,
    RunStatusResponse,
    StrategyReferenceInput,
    TradeRole,
)
from pydantic import BaseModel, ConfigDict, Field
from psycopg import Error as PsycopgError
from starlette.responses import StreamingResponse

from api.service.dependencies import get_auth_manager, get_settings, validate_auth
from core.backtest_job_control import resolve_backtest_job_name, trigger_backtest_job
from core.backtest_request_resolution import ResolvedBacktestRequest, resolve_backtest_request
from core.backtest_repository import BacktestRepository

logger = logging.getLogger(__name__)

router = APIRouter()


class SubmitBacktestRequest(BaseModel):
    strategyName: str = Field(..., min_length=1, max_length=128)
    strategyVersion: int | None = Field(default=None, ge=1)
    startTs: datetime
    endTs: datetime
    barSize: str = Field(..., min_length=1, max_length=32)
    runName: str | None = Field(default=None, max_length=255)


class BacktestResponseMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")

    results_schema_version: int = Field(..., ge=1)
    bar_size: str | None = None
    periods_per_year: int | None = Field(default=None, ge=1)
    strategy_scope: Literal["long_only"] = "long_only"


class SummaryResponse(BacktestSummary):
    metadata: BacktestResponseMetadata | None = None


class TimeseriesPointResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str
    portfolio_value: float
    drawdown: float
    daily_return: float | None = None
    period_return: float | None = None
    cumulative_return: float | None = None
    cash: float | None = None
    gross_exposure: float | None = None
    net_exposure: float | None = None
    turnover: float | None = None
    commission: float | None = None
    slippage_cost: float | None = None
    trade_count: int | None = None


class TimeseriesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metadata: BacktestResponseMetadata | None = None
    points: list[TimeseriesPointResponse]
    total_points: int
    truncated: bool


class RollingMetricPointResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str
    window_days: int
    window_periods: int | None = None
    rolling_return: float | None = None
    rolling_volatility: float | None = None
    rolling_sharpe: float | None = None
    rolling_max_drawdown: float | None = None
    turnover_sum: float | None = None
    commission_sum: float | None = None
    slippage_cost_sum: float | None = None
    n_trades_sum: float | None = None
    gross_exposure_avg: float | None = None
    net_exposure_avg: float | None = None


class RollingMetricsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    metadata: BacktestResponseMetadata | None = None
    points: list[RollingMetricPointResponse]
    total_points: int
    truncated: bool


class TradeResponse(BaseModel):
    execution_date: str
    symbol: str
    quantity: float
    price: float
    notional: float
    commission: float
    slippage_cost: float
    cash_after: float
    position_id: str | None = None
    trade_role: TradeRole | None = None


class TradeListResponse(BaseModel):
    trades: list[TradeResponse]
    total: int
    limit: int
    offset: int


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for backtest features.")
    return dsn


def _periods_per_year_for_bar_size(bar_size: str | None) -> int | None:
    raw = str(bar_size or "").strip().lower()
    if not raw:
        return None
    if raw in {"1d", "d", "daily"}:
        return 252
    if raw in {"1wk", "1w", "weekly"}:
        return 52
    if raw in {"1mo", "mo", "monthly"}:
        return 12

    match = re.fullmatch(r"(?:(\d+))?(m|h)", raw)
    if not match:
        return None

    count = int(match.group(1) or "1")
    unit = match.group(2)
    minutes = count if unit == "m" else count * 60
    periods = round(252 * 390 / max(1, minutes))
    return max(1, int(periods))


def _backtest_metadata(run: dict[str, Any]) -> BacktestResponseMetadata:
    bar_size = str(run.get("bar_size") or "").strip() or None
    return BacktestResponseMetadata.model_validate(
        {
            "results_schema_version": int(run.get("results_schema_version") or 1),
            "bar_size": bar_size,
            "periods_per_year": _periods_per_year_for_bar_size(bar_size),
            "strategy_scope": "long_only",
        }
    )


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _model_payload(model_type: type[BaseModel], value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        return model_type.model_validate(value).model_dump(mode="json", exclude_none=True)
    except Exception:
        return None


def _execution_assumptions_payload(run: dict[str, Any]) -> dict[str, Any] | None:
    config = run.get("config") if isinstance(run.get("config"), dict) else {}
    effective_config = run.get("effective_config") if isinstance(run.get("effective_config"), dict) else {}
    execution = effective_config.get("execution") if isinstance(effective_config.get("execution"), dict) else {}
    raw_assumptions = config.get("assumptions") or execution.get("assumptions")
    return _model_payload(BacktestExecutionAssumptions, raw_assumptions)


def _run_request_payload(run: dict[str, Any]) -> dict[str, Any] | None:
    raw_config = run.get("config") if isinstance(run.get("config"), dict) else {}
    if not raw_config:
        return None

    payload = {
        "strategyRef": raw_config.get("strategyRef"),
        "strategyConfig": raw_config.get("strategyConfig"),
        "startTs": raw_config.get("startTs") or run.get("start_ts"),
        "endTs": raw_config.get("endTs") or run.get("end_ts"),
        "barSize": raw_config.get("barSize") or run.get("bar_size"),
        "runName": raw_config.get("runName") or run.get("run_name"),
        "assumptions": raw_config.get("assumptions") or _execution_assumptions_payload(run),
    }
    return _model_payload(BacktestRunRequest, {key: value for key, value in payload.items() if value is not None})


def _data_provenance_payload(run: dict[str, Any]) -> dict[str, Any]:
    effective_config = run.get("effective_config") if isinstance(run.get("effective_config"), dict) else {}
    raw = {}
    for key in ("dataProvenance", "provenance", "data"):
        value = effective_config.get(key)
        if isinstance(value, dict):
            raw = value
            break

    quality = str(raw.get("quality") or "").strip().lower()
    if quality not in {"complete", "partial", "missing", "contradictory"}:
        quality = "partial" if raw else "missing"

    warnings = list(raw.get("warnings") or []) if isinstance(raw.get("warnings"), list) else []
    if not raw:
        warnings.append("Data provenance is not yet embedded in the run effective config.")

    payload = {
        "quality": quality,
        "dataSnapshotId": raw.get("dataSnapshotId") or raw.get("snapshotId"),
        "vendor": raw.get("vendor"),
        "source": raw.get("source") or "postgres_backtest_results",
        "loadId": raw.get("loadId"),
        "schemaVersion": raw.get("schemaVersion") or str(run.get("results_schema_version") or ""),
        "adjustmentPolicy": raw.get("adjustmentPolicy"),
        "symbolMapVersion": raw.get("symbolMapVersion"),
        "corporateActionState": raw.get("corporateActionState"),
        "coveragePct": _safe_float(raw.get("coveragePct")),
        "nullCount": _safe_int(raw.get("nullCount")),
        "gapCount": _safe_int(raw.get("gapCount")),
        "staleCount": _safe_int(raw.get("staleCount")),
        "quarantined": bool(raw.get("quarantined") or False),
        "warnings": warnings,
    }
    return BacktestDataProvenance.model_validate(payload).model_dump(mode="json")


def _validation_check(
    *,
    code: str,
    label: str,
    verdict: Literal["pass", "warn", "block"],
    severity: Literal["info", "warning", "critical"] = "info",
    message: str = "",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "label": label,
        "verdict": verdict,
        "severity": severity,
        "message": message,
        "evidence": evidence or {},
    }


def _validation_report_payload(
    *,
    resolved_request: ResolvedBacktestRequest | None = None,
    duplicate_run: dict[str, Any] | None = None,
    inflight_run: dict[str, Any] | None = None,
    blocked_reason: str | None = None,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    blocked_reasons: list[str] = []

    if blocked_reason:
        blocked_reasons.append(blocked_reason)
        checks.append(
            _validation_check(
                code="request_resolution",
                label="Request resolution",
                verdict="block",
                severity="critical",
                message=blocked_reason,
            )
        )
    elif resolved_request is not None:
        checks.append(
            _validation_check(
                code="request_resolution",
                label="Request resolution",
                verdict="pass",
                message="Strategy, pins, calendar window, and request fingerprint resolved.",
                evidence={
                    "configFingerprint": resolved_request.config_fingerprint,
                    "requestFingerprint": resolved_request.request_fingerprint,
                    "barsResolved": len(resolved_request.schedule),
                    "strategyName": resolved_request.definition.strategy_name,
                    "strategyVersion": resolved_request.definition.strategy_version,
                },
            )
        )
        checks.append(
            _validation_check(
                code="execution_window",
                label="Execution window",
                verdict="pass",
                message="Window is valid for the requested bar size.",
                evidence={
                    "startTs": resolved_request.start_ts.isoformat(),
                    "endTs": resolved_request.end_ts.isoformat(),
                    "barSize": resolved_request.bar_size,
                },
            )
        )
        assumptions = resolved_request.request_payload.get("assumptions") or {}
        checks.append(
            _validation_check(
                code="execution_assumptions",
                label="Execution assumptions",
                verdict="pass",
                message="Execution assumptions are included in the request fingerprint.",
                evidence={"assumptions": assumptions},
            )
        )

    if duplicate_run:
        warnings.append("A completed run already exists for this exact request fingerprint.")
        checks.append(
            _validation_check(
                code="duplicate_completed_run",
                label="Duplicate completed run",
                verdict="warn",
                severity="warning",
                message="Review or compare the existing completed run before launching new work.",
                evidence={"run": _run_status_payload(duplicate_run)},
            )
        )
    else:
        checks.append(
            _validation_check(
                code="duplicate_completed_run",
                label="Duplicate completed run",
                verdict="pass",
                message="No completed run found for this exact request fingerprint.",
            )
        )

    if inflight_run:
        warnings.append("A matching run is already queued or running.")
        checks.append(
            _validation_check(
                code="inflight_run",
                label="Inflight run",
                verdict="warn",
                severity="warning",
                message="Reuse the inflight run instead of dispatching duplicate work.",
                evidence={"run": _run_status_payload(inflight_run)},
            )
        )
    else:
        checks.append(
            _validation_check(
                code="inflight_run",
                label="Inflight run",
                verdict="pass",
                message="No queued or running run found for this exact request fingerprint.",
            )
        )

    verdict: Literal["pass", "warn", "block"] = "pass"
    if blocked_reasons:
        verdict = "block"
    elif warnings:
        verdict = "warn"

    return BacktestValidationReport.model_validate(
        {
            "verdict": verdict,
            "checks": checks,
            "blockedReasons": blocked_reasons,
            "warnings": warnings,
            "duplicateRun": _run_status_payload(duplicate_run) if duplicate_run else None,
            "reusedInflightRun": _run_status_payload(inflight_run) if inflight_run else None,
            "generatedAt": _now_utc(),
        }
    ).model_dump(mode="json")


def _run_review_validation_payload(run: dict[str, Any]) -> dict[str, Any]:
    status = str(run.get("status") or "unknown")
    blocked_reasons: list[str] = []
    warnings: list[str] = []
    status_verdict: Literal["pass", "warn", "block"] = "pass"
    status_severity: Literal["info", "warning", "critical"] = "info"
    if status == "failed":
        status_verdict = "block"
        status_severity = "critical"
        blocked_reasons.append(str(run.get("error") or "Backtest run failed."))
    elif status != "completed":
        status_verdict = "warn"
        status_severity = "warning"
        warnings.append("Run is not complete; published result evidence may be absent.")

    publication_verdict: Literal["pass", "warn", "block"] = "pass"
    publication_severity: Literal["info", "warning", "critical"] = "info"
    if not run.get("results_ready_at"):
        publication_verdict = "warn" if status != "failed" else "block"
        publication_severity = "warning" if status != "failed" else "critical"
        message = "Result publication timestamp is absent."
        if status == "failed":
            blocked_reasons.append(message)
        else:
            warnings.append(message)
    else:
        message = "Results are published."

    verdict: Literal["pass", "warn", "block"] = "pass"
    if blocked_reasons:
        verdict = "block"
    elif warnings:
        verdict = "warn"

    return BacktestValidationReport.model_validate(
        {
            "verdict": verdict,
            "checks": [
                _validation_check(
                    code="run_status",
                    label="Run status",
                    verdict=status_verdict,
                    severity=status_severity,
                    message=f"Run status is {status}.",
                    evidence={"runId": run.get("run_id"), "status": status},
                ),
                _validation_check(
                    code="result_publication",
                    label="Result publication",
                    verdict=publication_verdict,
                    severity=publication_severity,
                    message=message,
                    evidence={"resultsReadyAt": run.get("results_ready_at")},
                ),
            ],
            "blockedReasons": blocked_reasons,
            "warnings": warnings,
            "generatedAt": _now_utc(),
        }
    ).model_dump(mode="json")


def _attach_metadata(payload: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    payload["metadata"] = _backtest_metadata(run).model_dump(mode="json")
    return payload


def _run_record_payload(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run.get("run_id"),
        "status": run.get("status"),
        "submitted_at": run.get("submitted_at"),
        "started_at": run.get("started_at"),
        "completed_at": run.get("completed_at"),
        "run_name": run.get("run_name"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "error": run.get("error"),
        "strategy_name": run.get("strategy_name"),
        "strategy_version": run.get("strategy_version"),
        "bar_size": run.get("bar_size"),
        "execution_name": run.get("execution_name"),
    }


def _run_status_payload(run: dict[str, Any]) -> dict[str, Any]:
    pins_payload: dict[str, Any] | None = None
    effective_config = run.get("effective_config")
    if isinstance(effective_config, dict):
        raw_pins = effective_config.get("pins")
        if isinstance(raw_pins, dict):
            pins_payload = RunPinsResponse.model_validate(raw_pins).model_dump(mode="json")

    return {
        **_run_record_payload(run),
        "results_ready_at": run.get("results_ready_at"),
        "results_schema_version": run.get("results_schema_version"),
        "pins": pins_payload,
    }


def _result_links_payload(run_id: str) -> dict[str, Any]:
    base_path = f"/api/backtests/{run_id}"
    return BacktestResultLinks.model_validate(
        {
            "summaryUrl": f"{base_path}/summary",
            "metricsTimeseriesUrl": f"{base_path}/metrics/timeseries",
            "metricsRollingUrl": f"{base_path}/metrics/rolling",
            "tradesUrl": f"{base_path}/trades",
            "closedPositionsUrl": f"{base_path}/positions/closed",
        }
    ).model_dump(mode="json")


def _run_detail_payload(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run["run_id"])
    warnings: list[str] = []
    provenance = _data_provenance_payload(run)
    if provenance.get("quality") == "missing":
        warnings.append("Data provenance is not yet published in the run metadata.")
    if run.get("status") == "failed" and run.get("error"):
        warnings.append(str(run["error"]))

    return BacktestRunDetailResponse.model_validate(
        {
            "run": _run_status_payload(run),
            "request": _run_request_payload(run),
            "effectiveConfig": run.get("effective_config") if isinstance(run.get("effective_config"), dict) else {},
            "configHash": run.get("config_fingerprint") or run.get("canonical_fingerprint"),
            "requestHash": run.get("request_fingerprint"),
            "owner": run.get("submitted_by"),
            "assumptions": _execution_assumptions_payload(run),
            "validation": _run_review_validation_payload(run),
            "provenance": provenance,
            "links": _result_links_payload(run_id),
            "warnings": warnings,
        }
    ).model_dump(mode="json")


def _trade_replay_event_payload(run_id: str, trade: dict[str, Any], sequence: int) -> dict[str, Any]:
    quantity = _safe_float(trade.get("quantity"))
    price = _safe_float(trade.get("price"))
    symbol = str(trade.get("symbol") or "").strip() or None
    role = str(trade.get("trade_role") or "").strip().lower()
    side = "Buy" if quantity is not None and quantity >= 0 else "Sell"
    quantity_text = abs(quantity) if quantity is not None else "unknown quantity"
    price_text = f" @ {price:g}" if price is not None else ""
    event_type: Literal[
        "signal",
        "order_decision",
        "fill_assumption",
        "position_update",
        "risk_limit",
        "exit",
        "corporate_action",
        "data_event",
        "cash",
    ] = "exit" if role == "exit" else "fill_assumption"
    transaction_cost = sum(
        value for value in (_safe_float(trade.get("commission")), _safe_float(trade.get("slippage_cost"))) if value is not None
    )

    return {
        "eventId": f"{run_id}:{sequence}",
        "sequence": sequence,
        "timestamp": trade.get("execution_date") or _now_utc(),
        "eventType": event_type,
        "symbol": symbol,
        "ruleId": trade.get("exit_rule_id"),
        "source": "simulated",
        "summary": f"{side} {quantity_text} {symbol or 'symbol'}{price_text}",
        "beforeCash": None,
        "afterCash": _safe_float(trade.get("cash_after")),
        "beforeGrossExposure": None,
        "afterGrossExposure": None,
        "beforeNetExposure": None,
        "afterNetExposure": None,
        "beforePositions": [],
        "afterPositions": [],
        "transactionCost": transaction_cost,
        "benchmarkPrice": None,
        "evidence": {
            "derivedFrom": "core.backtest_trades",
            "positionId": trade.get("position_id"),
            "tradeRole": trade.get("trade_role"),
            "notional": _safe_float(trade.get("notional")),
            "commission": _safe_float(trade.get("commission")),
            "slippageCost": _safe_float(trade.get("slippage_cost")),
        },
        "warnings": [],
    }


def _gross_to_net_bridge_payload(summary: dict[str, Any]) -> dict[str, Any]:
    initial_cash = _safe_float(summary.get("initial_cash"))

    def drag(cost: Any) -> float | None:
        cost_value = _safe_float(cost)
        if cost_value is None or not initial_cash:
            return None
        return -abs(cost_value) / initial_cash

    return {
        "grossReturn": _safe_float(summary.get("gross_total_return")),
        "commissionDrag": drag(summary.get("total_commission")),
        "slippageDrag": drag(summary.get("total_slippage_cost")),
        "spreadDrag": None,
        "marketImpactDrag": None,
        "borrowFinancingDrag": None,
        "netReturn": _safe_float(summary.get("total_return")),
        "costDragBps": _safe_float(summary.get("cost_drag_bps")),
    }


def _attribution_payload(
    *,
    run_id: str,
    summary: dict[str, Any],
    closed_positions: list[dict[str, Any]],
) -> dict[str, Any]:
    warnings = [
        "Attribution is derived from published summary and closed-position ledgers; canonical decomposition remains backend-owned."
    ]
    gross_return = _safe_float(summary.get("gross_total_return"))
    net_return = _safe_float(summary.get("total_return"))
    total_cost = _safe_float(summary.get("total_transaction_cost"))
    slices: list[dict[str, Any]] = []
    if gross_return is not None or net_return is not None or total_cost is not None:
        slices.append(
            {
                "kind": "implementation",
                "name": "Implementation cost",
                "contributionReturn": (net_return - gross_return) if net_return is not None and gross_return is not None else None,
                "contributionPnl": -abs(total_cost) if total_cost is not None else None,
                "exposureAvg": None,
                "tradeCount": _safe_int(summary.get("trades")),
                "notes": ["Commission and slippage are included where published."],
            }
        )

    ordered_positions = sorted(
        closed_positions,
        key=lambda item: abs(_safe_float(item.get("realized_pnl")) or 0.0),
        reverse=True,
    )
    concentration = [
        {
            "kind": "outlier",
            "name": str(position.get("symbol") or position.get("position_id") or "position"),
            "contributionReturn": _safe_float(position.get("realized_return")),
            "contributionPnl": _safe_float(position.get("realized_pnl")),
            "exposureAvg": None,
            "tradeCount": None,
            "notes": [f"Position {position.get('position_id')}"] if position.get("position_id") else [],
        }
        for position in ordered_positions[:10]
    ]

    return BacktestAttributionExposureResponse.model_validate(
        {
            "runId": run_id,
            "asOf": _now_utc(),
            "grossToNet": _gross_to_net_bridge_payload(summary),
            "slices": slices,
            "concentration": concentration,
            "grossExposureAvg": _safe_float(summary.get("avg_gross_exposure")),
            "netExposureAvg": _safe_float(summary.get("avg_net_exposure")),
            "turnover": None,
            "warnings": warnings,
        }
    ).model_dump(mode="json")


def _comparison_signature(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "startDate": run.get("start_date"),
        "endDate": run.get("end_date"),
        "barSize": run.get("bar_size"),
        "resultsSchemaVersion": run.get("results_schema_version"),
        "assumptions": _execution_assumptions_payload(run) or {},
    }


def _comparison_metrics_payload(
    *,
    requested_keys: list[str],
    run_ids: list[str],
    summaries: dict[str, dict[str, Any]],
    alignment: Literal["aligned", "caveated", "blocked"],
) -> list[dict[str, Any]]:
    catalog: dict[str, tuple[str, str, Literal["higher", "lower"]]] = {
        "total_return": ("Net return", "ratio", "higher"),
        "gross_total_return": ("Gross return", "ratio", "higher"),
        "sharpe_ratio": ("Sharpe", "ratio", "higher"),
        "max_drawdown": ("Max drawdown", "ratio", "higher"),
        "cost_drag_bps": ("Cost drag", "bps", "lower"),
    }
    metric_keys = requested_keys or list(catalog.keys())
    metrics: list[dict[str, Any]] = []
    for key in metric_keys:
        label, unit, direction = catalog.get(key, (key, "value", "higher"))
        values = {run_id: _safe_float(summaries.get(run_id, {}).get(key)) for run_id in run_ids}
        winner_run_id: str | None = None
        numeric_values = {run_id: value for run_id, value in values.items() if value is not None}
        if alignment == "aligned" and numeric_values:
            winner_run_id = (
                min(numeric_values, key=numeric_values.get)
                if direction == "lower"
                else max(numeric_values, key=numeric_values.get)
            )
        metrics.append(
            {
                "metric": key,
                "label": label,
                "unit": unit,
                "values": values,
                "winnerRunId": winner_run_id,
                "notes": "" if winner_run_id else "No winner selected because runs are not fully aligned.",
            }
        )
    return metrics


def _stream_url(run_id: str) -> str:
    return f"/api/backtests/{run_id}/events"


def _create_run_from_resolved_request(
    repo: BacktestRepository,
    *,
    resolved_request: ResolvedBacktestRequest,
    run_name: str | None,
    submitted_by: str | None,
) -> dict[str, Any]:
    return repo.create_run(
        config=resolved_request.request_payload,
        effective_config=resolved_request.effective_config,
        run_name=run_name,
        start_ts=resolved_request.start_ts,
        end_ts=resolved_request.end_ts,
        bar_size=resolved_request.bar_size,
        strategy_name=resolved_request.definition.strategy_name,
        strategy_version=resolved_request.definition.strategy_version,
        ranking_schema_name=resolved_request.definition.ranking_schema_name,
        ranking_schema_version=resolved_request.definition.ranking_schema_version,
        universe_name=resolved_request.definition.ranking_universe_name,
        universe_version=resolved_request.definition.ranking_universe_version,
        regime_model_name=resolved_request.definition.regime_model_name,
        regime_model_version=resolved_request.definition.regime_model_version,
        config_fingerprint=resolved_request.config_fingerprint,
        request_fingerprint=resolved_request.request_fingerprint,
        submitted_by=submitted_by,
    )


def _dispatch_backtest_run(
    repo: BacktestRepository,
    *,
    run: dict[str, Any],
) -> dict[str, Any]:
    try:
        job_name = resolve_backtest_job_name()
    except ValueError:
        raise HTTPException(status_code=500, detail="BACKTEST_ACA_JOB_NAME is invalid.")

    try:
        job_response = _trigger_backtest_job(job_name)
    except ValueError as exc:
        logger.warning(
            "backtest_run_event outcome=dispatch_failed run_id=%s request_fingerprint=%s error=%s",
            run.get("run_id"),
            run.get("request_fingerprint"),
            exc,
        )
        raise HTTPException(status_code=502, detail=f"Failed to trigger backtest job: {exc}") from exc

    execution_name = str(job_response.get("executionName") or "").strip() or None
    if execution_name:
        _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: repo.set_execution_name(str(run["run_id"]), execution_name),
        )
        run = _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: repo.get_run(str(run["run_id"])),
        ) or run
    return run


def _terminal_stream_payload(
    repo: BacktestRepository,
    *,
    event: Literal["completed", "failed"],
    run: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event": event,
        "run": _run_status_payload(run),
    }
    if event == "completed":
        summary = _postgres_or_503(
            "Postgres is unavailable for backtest features.",
            lambda: repo.get_summary(str(run["run_id"])),
        )
        if summary is not None:
            payload["summary"] = summary
        payload["metadata"] = _backtest_metadata(run).model_dump(mode="json")
        payload["links"] = _result_links_payload(str(run["run_id"]))
    return BacktestStreamEvent.model_validate(payload).model_dump(mode="json")


def _encode_sse_event(event: str, payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, separators=(",", ":"), default=str)
    return f"event: {event}\ndata: {body}\n\n".encode("utf-8")


def _actor_from_request(request: Request) -> str | None:
    settings = get_settings(request)
    if settings.anonymous_local_auth_enabled:
        return None
    try:
        ctx = get_auth_manager(request).authenticate_headers(dict(request.headers))
    except Exception:
        return None
    if ctx.subject:
        return ctx.subject
    for key in ("preferred_username", "email", "upn"):
        value = ctx.claims.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _trigger_backtest_job(job_name: str) -> dict[str, Any]:
    return trigger_backtest_job(job_name)


def _postgres_or_503[T](detail: str, action: Callable[[], T]) -> T:
    try:
        return action()
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail=detail) from exc


def _require_run(repo: BacktestRepository, run_id: str) -> dict[str, Any]:
    run = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.get_run(run_id),
    )
    if not run:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")
    return run


def _require_published_run(repo: BacktestRepository, run_id: str) -> dict[str, Any]:
    run = _require_run(repo, run_id)
    if run.get("status") != "completed" or not run.get("results_ready_at"):
        raise HTTPException(
            status_code=409,
            detail=f"Run '{run_id}' exists but Postgres results are not fully published yet.",
        )
    return run


@router.get("", response_model=RunListResponse)
@router.get("/", response_model=RunListResponse)
async def list_backtests(
    request: Request,
    status: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> RunListResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    runs = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.list_runs(status=status, query=q, limit=limit, offset=offset),
    )
    return RunListResponse.model_validate(
        {"runs": [_run_record_payload(run) for run in runs], "limit": limit, "offset": offset}
    )


@router.post("", response_model=RunRecordResponse)
@router.post("/", response_model=RunRecordResponse)
async def submit_backtest(payload: SubmitBacktestRequest, request: Request) -> RunRecordResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = BacktestRepository(dsn)
    started_at = time.perf_counter()
    try:
        resolved_request = _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: resolve_backtest_request(
                dsn,
                strategy_ref=StrategyReferenceInput(
                    strategyName=payload.strategyName,
                    strategyVersion=payload.strategyVersion,
                ),
                strategy_config=None,
                start_ts=payload.startTs,
                end_ts=payload.endTs,
                bar_size=payload.barSize,
                assumptions=getattr(payload, "assumptions", None),
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    run = _postgres_or_503(
        "Postgres is unavailable for backtest submission.",
        lambda: _create_run_from_resolved_request(
            repo,
            resolved_request=resolved_request,
            run_name=payload.runName,
            submitted_by=_actor_from_request(request),
        ),
    )
    run = _dispatch_backtest_run(repo, run=run)
    logger.info(
        "backtest_run_event outcome=legacy_submit_created run_id=%s request_fingerprint=%s execution_name=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
        run.get("run_id"),
        run.get("request_fingerprint"),
        run.get("execution_name"),
        run.get("strategy_name"),
        run.get("strategy_version"),
        _actor_from_request(request),
        round((time.perf_counter() - started_at) * 1000, 2),
    )
    return RunRecordResponse.model_validate(_run_record_payload(run))


@router.post("/results/lookup", response_model=BacktestLookupResponse)
async def lookup_backtest_results(
    payload: BacktestLookupRequest,
    request: Request,
) -> BacktestLookupResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = BacktestRepository(dsn)
    actor = _actor_from_request(request)
    started_at = time.perf_counter()

    logger.info("backtest_lookup_event outcome=request_received actor=%s", actor)
    try:
        resolved_request = _postgres_or_503(
            "Postgres is unavailable for backtest lookup.",
            lambda: resolve_backtest_request(
                dsn,
                strategy_ref=payload.strategyRef,
                strategy_config=payload.strategyConfig,
                start_ts=payload.startTs,
                end_ts=payload.endTs,
                bar_size=payload.barSize,
                assumptions=getattr(payload, "assumptions", None),
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    completed_run = _postgres_or_503(
        "Postgres is unavailable for backtest lookup.",
        lambda: repo.find_latest_completed_request_run(request_fingerprint=resolved_request.request_fingerprint),
    )
    latency_ms = round((time.perf_counter() - started_at) * 1000, 2)
    log_context = (
        resolved_request.config_fingerprint,
        resolved_request.request_fingerprint,
        resolved_request.definition.strategy_name,
        resolved_request.definition.strategy_version,
        actor,
        latency_ms,
    )
    if completed_run:
        summary = _postgres_or_503(
            "Postgres is unavailable for backtest lookup.",
            lambda: repo.get_summary(str(completed_run["run_id"])),
        )
        logger.info(
            "backtest_lookup_event outcome=completed_hit run_id=%s config_fingerprint=%s request_fingerprint=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
            completed_run.get("run_id"),
            *log_context,
        )
        return BacktestLookupResponse.model_validate(
            {
                "found": True,
                "state": "completed",
                "run": _run_status_payload(completed_run),
                "result": summary,
                "links": _result_links_payload(str(completed_run["run_id"])),
            }
        )

    inflight_run = _postgres_or_503(
        "Postgres is unavailable for backtest lookup.",
        lambda: repo.find_latest_inflight_request_run(request_fingerprint=resolved_request.request_fingerprint),
    )
    if inflight_run:
        logger.info(
            "backtest_lookup_event outcome=inflight_hit run_id=%s config_fingerprint=%s request_fingerprint=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
            inflight_run.get("run_id"),
            *log_context,
        )
        return BacktestLookupResponse.model_validate(
            {
                "found": False,
                "state": inflight_run.get("status"),
                "run": _run_status_payload(inflight_run),
                "result": None,
                "links": None,
            }
        )

    failed_run = _postgres_or_503(
        "Postgres is unavailable for backtest lookup.",
        lambda: repo.find_latest_failed_request_run(request_fingerprint=resolved_request.request_fingerprint),
    )
    if failed_run:
        logger.info(
            "backtest_lookup_event outcome=failed_hit run_id=%s config_fingerprint=%s request_fingerprint=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
            failed_run.get("run_id"),
            *log_context,
        )
        return BacktestLookupResponse.model_validate(
            {
                "found": False,
                "state": "failed",
                "run": _run_status_payload(failed_run),
                "result": None,
                "links": None,
            }
        )

    logger.info(
        "backtest_lookup_event outcome=miss config_fingerprint=%s request_fingerprint=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
        *log_context,
    )
    return BacktestLookupResponse.model_validate(
        {
            "found": False,
            "state": "not_run",
            "run": None,
            "result": None,
            "links": None,
        }
    )


@router.post("/runs", response_model=BacktestRunResponse)
async def run_backtest(
    payload: BacktestRunRequest,
    request: Request,
) -> BacktestRunResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = BacktestRepository(dsn)
    actor = _actor_from_request(request)
    started_at = time.perf_counter()

    logger.info("backtest_run_event outcome=request_received actor=%s", actor)
    try:
        resolved_request = _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: resolve_backtest_request(
                dsn,
                strategy_ref=payload.strategyRef,
                strategy_config=payload.strategyConfig,
                start_ts=payload.startTs,
                end_ts=payload.endTs,
                bar_size=payload.barSize,
                assumptions=getattr(payload, "assumptions", None),
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    inflight_run = _postgres_or_503(
        "Postgres is unavailable for backtest submission.",
        lambda: repo.find_latest_inflight_request_run(request_fingerprint=resolved_request.request_fingerprint),
    )
    latency_ms = round((time.perf_counter() - started_at) * 1000, 2)
    if inflight_run:
        logger.info(
            "backtest_run_event outcome=reused_inflight run_id=%s config_fingerprint=%s request_fingerprint=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
            inflight_run.get("run_id"),
            resolved_request.config_fingerprint,
            resolved_request.request_fingerprint,
            resolved_request.definition.strategy_name,
            resolved_request.definition.strategy_version,
            actor,
            latency_ms,
        )
        return BacktestRunResponse.model_validate(
            {
                "run": _run_status_payload(inflight_run),
                "created": False,
                "reusedInflight": True,
                "streamUrl": _stream_url(str(inflight_run["run_id"])),
            }
        )

    run = _postgres_or_503(
        "Postgres is unavailable for backtest submission.",
        lambda: _create_run_from_resolved_request(
            repo,
            resolved_request=resolved_request,
            run_name=getattr(payload, "runName", None),
            submitted_by=actor,
        ),
    )
    run = _dispatch_backtest_run(repo, run=run)
    logger.info(
        "backtest_run_event outcome=created run_id=%s config_fingerprint=%s request_fingerprint=%s strategy_name=%s strategy_version=%s actor=%s latency_ms=%s",
        run.get("run_id"),
        resolved_request.config_fingerprint,
        resolved_request.request_fingerprint,
        resolved_request.definition.strategy_name,
        resolved_request.definition.strategy_version,
        actor,
        round((time.perf_counter() - started_at) * 1000, 2),
    )
    return BacktestRunResponse.model_validate(
        {
            "run": _run_status_payload(run),
            "created": True,
            "reusedInflight": False,
            "streamUrl": _stream_url(str(run["run_id"])),
        }
    )


@router.post("/validation", response_model=BacktestValidationReport)
async def validate_backtest_request(
    payload: BacktestRunRequest,
    request: Request,
) -> BacktestValidationReport:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = BacktestRepository(dsn)

    try:
        resolved_request = _postgres_or_503(
            "Postgres is unavailable for backtest validation.",
            lambda: resolve_backtest_request(
                dsn,
                strategy_ref=payload.strategyRef,
                strategy_config=payload.strategyConfig,
                start_ts=payload.startTs,
                end_ts=payload.endTs,
                bar_size=payload.barSize,
                assumptions=getattr(payload, "assumptions", None),
            ),
        )
    except ValueError as exc:
        return BacktestValidationReport.model_validate(_validation_report_payload(blocked_reason=str(exc)))

    duplicate_run = _postgres_or_503(
        "Postgres is unavailable for backtest validation.",
        lambda: repo.find_latest_completed_request_run(request_fingerprint=resolved_request.request_fingerprint),
    )
    inflight_run = _postgres_or_503(
        "Postgres is unavailable for backtest validation.",
        lambda: repo.find_latest_inflight_request_run(request_fingerprint=resolved_request.request_fingerprint),
    )
    return BacktestValidationReport.model_validate(
        _validation_report_payload(
            resolved_request=resolved_request,
            duplicate_run=duplicate_run,
            inflight_run=inflight_run,
        )
    )


@router.post("/compare", response_model=BacktestRunComparisonResponse)
async def compare_backtest_runs(
    payload: BacktestRunComparisonRequest,
    request: Request,
) -> BacktestRunComparisonResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run_ids = [payload.baselineRunId, *payload.challengerRunIds]
    runs = [_require_run(repo, run_id) for run_id in run_ids]

    blocked_reasons: list[str] = []
    summaries: dict[str, dict[str, Any]] = {}
    for run in runs:
        run_id = str(run["run_id"])
        if run.get("status") != "completed" or not run.get("results_ready_at"):
            blocked_reasons.append(f"Run '{run_id}' is not completed with published results.")
            continue
        summary = _postgres_or_503(
            "Postgres is unavailable for backtest comparison.",
            lambda run_id=run_id: repo.get_summary(run_id),
        )
        if summary is None:
            blocked_reasons.append(f"Run '{run_id}' has no published summary.")
        else:
            summaries[run_id] = summary

    alignment_warnings: list[str] = []
    if not blocked_reasons:
        baseline_signature = _comparison_signature(runs[0])
        for run in runs[1:]:
            challenger_signature = _comparison_signature(run)
            for key, baseline_value in baseline_signature.items():
                challenger_value = challenger_signature.get(key)
                if challenger_value != baseline_value:
                    alignment_warnings.append(
                        f"Run '{run['run_id']}' differs from baseline on {key}; comparison winner is suppressed."
                    )

    alignment: Literal["aligned", "caveated", "blocked"] = "aligned"
    if blocked_reasons:
        alignment = "blocked"
    elif alignment_warnings:
        alignment = "caveated"

    return BacktestRunComparisonResponse.model_validate(
        {
            "asOf": _now_utc(),
            "alignment": alignment,
            "baselineRunId": payload.baselineRunId,
            "runs": [_run_status_payload(run) for run in runs],
            "metrics": _comparison_metrics_payload(
                requested_keys=payload.metricKeys,
                run_ids=run_ids,
                summaries=summaries,
                alignment=alignment,
            ),
            "alignmentWarnings": alignment_warnings,
            "blockedReasons": blocked_reasons,
        }
    )


@router.get("/{run_id}/detail", response_model=BacktestRunDetailResponse)
async def get_run_detail(run_id: str, request: Request) -> BacktestRunDetailResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = _require_run(repo, run_id)
    return BacktestRunDetailResponse.model_validate(_run_detail_payload(run))


@router.get("/{run_id}/replay", response_model=BacktestReplayTimelineResponse)
async def get_replay_timeline(
    run_id: str,
    request: Request,
    limit: int = Query(default=500, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    symbol: str | None = Query(default=None, min_length=1, max_length=32),
) -> BacktestReplayTimelineResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    _require_published_run(repo, run_id)

    warnings = ["Replay events are simulated unless a specific event source says broker_fill."]
    normalized_symbol = str(symbol or "").strip().upper()
    if normalized_symbol:
        all_trades = _postgres_or_503(
            "Postgres is unavailable for backtest replay.",
            lambda: repo.list_trades(run_id, limit=10000, offset=0),
        )
        filtered_trades = [
            trade for trade in all_trades if str(trade.get("symbol") or "").strip().upper() == normalized_symbol
        ]
        total = len(filtered_trades)
        trades = filtered_trades[offset : offset + limit]
        warnings.append("Symbol filter was applied to trade-ledger replay events.")
        if len(all_trades) >= 10000:
            warnings.append("Replay symbol filtering is capped at the first 10000 trade rows.")
    else:
        total = _postgres_or_503(
            "Postgres is unavailable for backtest replay.",
            lambda: repo.count_trades(run_id),
        )
        trades = _postgres_or_503(
            "Postgres is unavailable for backtest replay.",
            lambda: repo.list_trades(run_id, limit=limit, offset=offset),
        )

    if total == 0:
        warnings.append("No trade-ledger replay events are available for this run.")

    next_offset = offset + len(trades) if offset + len(trades) < total else None
    return BacktestReplayTimelineResponse.model_validate(
        {
            "runId": run_id,
            "events": [
                _trade_replay_event_payload(run_id, trade, offset + index)
                for index, trade in enumerate(trades)
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
            "nextOffset": next_offset,
            "warnings": warnings,
        }
    )


@router.get("/{run_id}/attribution-exposure", response_model=BacktestAttributionExposureResponse)
async def get_attribution_exposure(
    run_id: str,
    request: Request,
) -> BacktestAttributionExposureResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    _require_published_run(repo, run_id)
    summary = _postgres_or_503(
        "Postgres is unavailable for backtest attribution.",
        lambda: repo.get_summary(run_id),
    )
    if summary is None:
        raise HTTPException(status_code=404, detail=f"Summary for run '{run_id}' not found.")
    closed_positions = _postgres_or_503(
        "Postgres is unavailable for backtest attribution.",
        lambda: repo.list_closed_positions(run_id, limit=250, offset=0),
    )
    return BacktestAttributionExposureResponse.model_validate(
        _attribution_payload(run_id=run_id, summary=summary, closed_positions=closed_positions)
    )


@router.get(
    "/{run_id}/events",
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {
                        "type": "string",
                    }
                }
            },
            "description": "Server-sent backtest run events.",
        }
    },
)
async def stream_backtest_events(run_id: str, request: Request) -> StreamingResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    initial_run = _require_run(repo, run_id)
    actor = _actor_from_request(request)

    async def event_generator() -> Any:
        close_reason = "stream_complete"
        logger.info(
            "backtest_stream_event outcome=opened run_id=%s request_fingerprint=%s actor=%s",
            run_id,
            initial_run.get("request_fingerprint"),
            actor,
        )
        last_status_payload = _run_status_payload(initial_run)
        accepted_payload = BacktestStreamEvent.model_validate(
            {"event": "accepted", "run": last_status_payload}
        ).model_dump(mode="json")
        yield _encode_sse_event("accepted", accepted_payload)

        current_run = initial_run
        if current_run.get("status") == "failed":
            payload = _terminal_stream_payload(repo, event="failed", run=current_run)
            yield _encode_sse_event("failed", payload)
            close_reason = "terminal_failed"
        elif current_run.get("status") == "completed" and current_run.get("results_ready_at"):
            payload = _terminal_stream_payload(repo, event="completed", run=current_run)
            yield _encode_sse_event("completed", payload)
            close_reason = "terminal_completed"

        if close_reason != "stream_complete":
            logger.info(
                "backtest_stream_event outcome=closed run_id=%s request_fingerprint=%s actor=%s reason=%s",
                run_id,
                current_run.get("request_fingerprint"),
                actor,
                close_reason,
            )
            return

        while True:
            if await request.is_disconnected():
                close_reason = "client_disconnect"
                break
            await asyncio.sleep(1.0)
            current_run = _postgres_or_503(
                "Postgres is unavailable for backtest features.",
                lambda: repo.get_run(run_id),
            ) or current_run
            current_status_payload = _run_status_payload(current_run)
            if current_status_payload != last_status_payload:
                event_name = "status"
                if current_run.get("status") == "failed":
                    event_name = "failed"
                elif current_run.get("status") == "completed" and current_run.get("results_ready_at"):
                    event_name = "completed"

                if event_name in {"completed", "failed"}:
                    payload = _terminal_stream_payload(repo, event=event_name, run=current_run)
                    logger.info(
                        "backtest_stream_event outcome=terminal event=%s run_id=%s request_fingerprint=%s actor=%s",
                        event_name,
                        run_id,
                        current_run.get("request_fingerprint"),
                        actor,
                    )
                    yield _encode_sse_event(event_name, payload)
                    close_reason = f"terminal_{event_name}"
                    break

                payload = BacktestStreamEvent.model_validate(
                    {"event": "status", "run": current_status_payload}
                ).model_dump(mode="json")
                yield _encode_sse_event("status", payload)
                last_status_payload = current_status_payload
                continue

            heartbeat_payload = BacktestStreamEvent.model_validate(
                {"event": "heartbeat", "run": current_status_payload}
            ).model_dump(mode="json")
            yield _encode_sse_event("heartbeat", heartbeat_payload)

        logger.info(
            "backtest_stream_event outcome=closed run_id=%s request_fingerprint=%s actor=%s reason=%s",
            run_id,
            current_run.get("request_fingerprint"),
            actor,
            close_reason,
        )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/{run_id}/status", response_model=RunStatusResponse)
async def get_status(run_id: str, request: Request) -> RunStatusResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = _require_run(repo, run_id)
    return RunStatusResponse.model_validate(_run_status_payload(run))


@router.get("/{run_id}/summary", response_model=SummaryResponse)
async def get_summary(run_id: str, request: Request) -> SummaryResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = _require_published_run(repo, run_id)
    summary = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.get_summary(run_id),
    )
    if summary is None:
        raise HTTPException(status_code=404, detail=f"Summary for run '{run_id}' not found.")
    return SummaryResponse.model_validate(_attach_metadata(summary, run))


@router.get("/{run_id}/metrics/timeseries", response_model=TimeseriesResponse)
async def get_timeseries(
    run_id: str,
    request: Request,
    max_points: int = Query(default=5000, ge=1, le=25000),
) -> TimeseriesResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = _require_published_run(repo, run_id)
    total = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.count_timeseries(run_id),
    )
    truncated = total > max_points
    offset = max(0, total - max_points) if truncated else 0
    points = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.list_timeseries(run_id, limit=max_points if truncated else None, offset=offset),
    )
    for point in points:
        period_return = point.get("period_return")
        if period_return is None:
            period_return = point.get("daily_return")
        point["period_return"] = period_return
        if point.get("daily_return") is None:
            point["daily_return"] = period_return
    return TimeseriesResponse.model_validate(
        {
            "metadata": _backtest_metadata(run).model_dump(mode="json"),
            "points": points,
            "total_points": total,
            "truncated": truncated,
        }
    )


@router.get("/{run_id}/metrics/rolling", response_model=RollingMetricsResponse)
async def get_rolling_metrics(
    run_id: str,
    request: Request,
    window_days: int = Query(default=63, ge=2, le=504),
    max_points: int = Query(default=5000, ge=1, le=25000),
) -> RollingMetricsResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = _require_published_run(repo, run_id)
    total = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.count_rolling_metrics(run_id, window_days=window_days),
    )
    truncated = total > max_points
    offset = max(0, total - max_points) if truncated else 0
    points = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.list_rolling_metrics(
            run_id,
            window_days=window_days,
            limit=max_points if truncated else None,
            offset=offset,
        ),
    )
    for point in points:
        window_periods = point.get("window_periods")
        if window_periods is None:
            window_periods = point.get("window_days")
        point["window_periods"] = window_periods
        if point.get("window_days") is None:
            point["window_days"] = window_periods
    return RollingMetricsResponse.model_validate(
        {
            "metadata": _backtest_metadata(run).model_dump(mode="json"),
            "points": points,
            "total_points": total,
            "truncated": truncated,
        }
    )


@router.get("/{run_id}/trades", response_model=TradeListResponse)
async def get_trades(
    run_id: str,
    request: Request,
    limit: int = Query(default=2000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
) -> TradeListResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    _require_published_run(repo, run_id)
    total = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.count_trades(run_id),
    )
    trades = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.list_trades(run_id, limit=limit, offset=offset),
    )
    return TradeListResponse.model_validate(
        {
            "trades": trades,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )


@router.get("/{run_id}/positions/closed", response_model=ClosedPositionListResponse)
async def get_closed_positions(
    run_id: str,
    request: Request,
    limit: int = Query(default=2000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
) -> ClosedPositionListResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    _require_published_run(repo, run_id)
    total = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.count_closed_positions(run_id),
    )
    positions = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.list_closed_positions(run_id, limit=limit, offset=offset),
    )
    return ClosedPositionListResponse.model_validate(
        {
            "positions": positions,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )
