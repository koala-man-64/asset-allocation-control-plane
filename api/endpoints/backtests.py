from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from collections.abc import Callable
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request
from asset_allocation_contracts.backtest import BacktestSummary
from pydantic import BaseModel, ConfigDict, Field
from psycopg import Error as PsycopgError

from api.service.dependencies import get_auth_manager, get_settings, validate_auth
from core.backtest_job_control import resolve_backtest_job_name, trigger_backtest_job
from core.backtest_repository import BacktestRepository
from core.backtest_runtime import (
    resolve_backtest_definition,
    validate_backtest_submission,
)

logger = logging.getLogger(__name__)

router = APIRouter()


class SubmitBacktestRequest(BaseModel):
    strategyName: str = Field(..., min_length=1, max_length=128)
    strategyVersion: int | None = Field(default=None, ge=1)
    startTs: datetime
    endTs: datetime
    barSize: str = Field(..., min_length=1, max_length=32)
    runName: str | None = Field(default=None, max_length=255)


class RunRecordResponse(BaseModel):
    run_id: str
    status: str
    submitted_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    run_name: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    error: str | None = None


class RunListResponse(BaseModel):
    runs: list[RunRecordResponse]
    limit: int
    offset: int


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def _attach_metadata(payload: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    payload["metadata"] = _backtest_metadata(run).model_dump(mode="json")
    return payload


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
    return RunListResponse.model_validate({"runs": runs, "limit": limit, "offset": offset})


@router.post("", response_model=RunRecordResponse)
@router.post("/", response_model=RunRecordResponse)
async def submit_backtest(payload: SubmitBacktestRequest, request: Request) -> RunRecordResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    repo = BacktestRepository(dsn)
    start_ts = _ensure_utc(payload.startTs)
    end_ts = _ensure_utc(payload.endTs)
    if end_ts <= start_ts:
        raise HTTPException(status_code=400, detail="endTs must be after startTs.")

    try:
        definition = _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: resolve_backtest_definition(
                dsn,
                strategy_name=payload.strategyName,
                strategy_version=payload.strategyVersion,
            ),
        )
        schedule = _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: validate_backtest_submission(
                dsn,
                definition=definition,
                start_ts=start_ts,
                end_ts=end_ts,
                bar_size=payload.barSize,
            ),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    effective_config = {
        "strategy": definition.strategy_config_raw,
        "pins": {
            "strategyName": definition.strategy_name,
            "strategyVersion": definition.strategy_version,
            "rankingSchemaName": definition.ranking_schema_name,
            "rankingSchemaVersion": definition.ranking_schema_version,
            "universeName": definition.ranking_universe_name,
            "universeVersion": definition.ranking_universe_version,
            "regimeModelName": definition.regime_model_name,
            "regimeModelVersion": definition.regime_model_version,
        },
        "execution": {
            "startTs": start_ts.isoformat(),
            "endTs": end_ts.isoformat(),
            "barSize": payload.barSize,
            "barsResolved": len(schedule),
        },
    }
    run = _postgres_or_503(
        "Postgres is unavailable for backtest submission.",
        lambda: repo.create_run(
            config=payload.model_dump(mode="json"),
            effective_config=effective_config,
            run_name=payload.runName,
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=payload.barSize,
            strategy_name=definition.strategy_name,
            strategy_version=definition.strategy_version,
            ranking_schema_name=definition.ranking_schema_name,
            ranking_schema_version=definition.ranking_schema_version,
            universe_name=definition.ranking_universe_name,
            universe_version=definition.ranking_universe_version,
            regime_model_name=definition.regime_model_name,
            regime_model_version=definition.regime_model_version,
            submitted_by=_actor_from_request(request),
        ),
    )

    try:
        job_name = resolve_backtest_job_name()
    except ValueError:
        raise HTTPException(status_code=500, detail="BACKTEST_ACA_JOB_NAME is invalid.")
    try:
        job_response = _trigger_backtest_job(job_name)
    except ValueError as exc:
        logger.warning("backtest_lifecycle_event submit_dispatch_failed run_id=%s error=%s", run.get("run_id"), exc)
        raise HTTPException(status_code=502, detail=f"Failed to trigger backtest job: {exc}") from exc

    if job_response.get("executionName"):
        _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: repo.set_execution_name(str(run["run_id"]), str(job_response["executionName"])),
        )
        run = _postgres_or_503(
            "Postgres is unavailable for backtest submission.",
            lambda: repo.get_run(str(run["run_id"])),
        ) or run
    logger.info(
        "backtest_lifecycle_event submit_dispatched run_id=%s execution_name=%s strategy_name=%s",
        run.get("run_id"),
        job_response.get("executionName"),
        run.get("strategy_name"),
    )
    return RunRecordResponse.model_validate(run)


@router.get("/{run_id}/status", response_model=RunRecordResponse)
async def get_status(run_id: str, request: Request) -> RunRecordResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = _require_run(repo, run_id)
    return RunRecordResponse.model_validate(run)


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
