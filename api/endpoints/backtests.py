from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from api.service.dependencies import get_auth_manager, get_settings, validate_auth
from core.backtest_artifacts import list_artifacts, read_artifact_bytes
from core.backtest_repository import BacktestRepository
from core.backtest_runtime import (
    load_rolling_metrics,
    load_summary,
    load_timeseries,
    load_trades,
    resolve_backtest_definition,
    validate_backtest_submission,
)
from monitoring.arm_client import ArmConfig, AzureArmClient

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
    output_dir: str | None = None
    adls_container: str | None = None
    adls_prefix: str | None = None
    error: str | None = None


class RunListResponse(BaseModel):
    runs: list[RunRecordResponse]
    limit: int
    offset: int


class SummaryResponse(BaseModel):
    run_id: str | None = None
    run_name: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    total_return: float | None = None
    annualized_return: float | None = None
    annualized_volatility: float | None = None
    sharpe_ratio: float | None = None
    max_drawdown: float | None = None
    trades: int | None = None
    initial_cash: float | None = None
    final_equity: float | None = None


class TimeseriesPointResponse(BaseModel):
    date: str
    portfolio_value: float
    drawdown: float
    daily_return: float | None = None
    cumulative_return: float | None = None
    cash: float | None = None
    gross_exposure: float | None = None
    net_exposure: float | None = None
    turnover: float | None = None
    commission: float | None = None
    slippage_cost: float | None = None


class TimeseriesResponse(BaseModel):
    points: list[TimeseriesPointResponse]
    total_points: int
    truncated: bool


class RollingMetricPointResponse(BaseModel):
    date: str
    window_days: int
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


class ArtifactEntryResponse(BaseModel):
    name: str
    path: str
    size: int | None = None
    updatedAt: str | None = None
    contentType: str | None = None


class ArtifactListResponse(BaseModel):
    artifacts: list[ArtifactEntryResponse]


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for backtest features.")
    return dsn


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _extract_arm_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        return (response.text or "").strip()
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            detail = error.get("message") or error.get("detail")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()
        if isinstance(error, str) and error.strip():
            return error.strip()
        detail = payload.get("message")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
    if isinstance(payload, str):
        return payload.strip()
    return (response.text or "").strip()


def _trigger_backtest_job(job_name: str) -> dict[str, Any]:
    subscription_id = str(os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID") or "").strip()
    resource_group = str(os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP") or "").strip()
    if not subscription_id or not resource_group:
        if _is_truthy(os.environ.get("TEST_MODE")):
            return {"status": "queued", "executionName": None}
        raise ValueError("Azure job triggering is not configured.")
    api_version = str(os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION") or "").strip() or ArmConfig.api_version
    timeout_raw = str(os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS") or "").strip()
    try:
        timeout_seconds = float(timeout_raw) if timeout_raw else 5.0
    except ValueError:
        timeout_seconds = 5.0
    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )
    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=job_name)
            payload = arm.post_json(f"{job_url}/start")
    except httpx.HTTPStatusError as exc:
        message = _extract_arm_error_message(exc.response)
        raise ValueError(message or str(exc)) from exc
    execution_name = None
    if isinstance(payload, dict):
        execution_name = str(payload.get("name") or "").strip() or None
    return {"status": "queued", "executionName": execution_name}


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
    runs = repo.list_runs(status=status, query=q, limit=limit, offset=offset)
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
        definition = resolve_backtest_definition(
            dsn,
            strategy_name=payload.strategyName,
            strategy_version=payload.strategyVersion,
        )
        schedule = validate_backtest_submission(
            dsn,
            definition=definition,
            start_ts=start_ts,
            end_ts=end_ts,
            bar_size=payload.barSize,
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
    run = repo.create_run(
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
        output_dir=str(os.environ.get("BACKTEST_OUTPUT_DIR") or "").strip() or None,
        adls_container=str(os.environ.get("AZURE_CONTAINER_COMMON") or "").strip() or None,
        adls_prefix=f"backtests/{definition.strategy_name}",
    )

    job_name = str(os.environ.get("BACKTEST_ACA_JOB_NAME") or "backtests-job").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", job_name):
        raise HTTPException(status_code=500, detail="BACKTEST_ACA_JOB_NAME is invalid.")
    try:
        job_response = _trigger_backtest_job(job_name)
    except ValueError as exc:
        logger.warning("Backtest job trigger failed for run_id=%s: %s", run.get("run_id"), exc)
        raise HTTPException(status_code=502, detail=f"Failed to trigger backtest job: {exc}") from exc

    if job_response.get("executionName"):
        repo.set_execution_name(str(run["run_id"]), str(job_response["executionName"]))
        run = repo.get_run(str(run["run_id"])) or run
    return RunRecordResponse.model_validate(run)


@router.get("/{run_id}/status", response_model=RunRecordResponse)
async def get_status(run_id: str, request: Request) -> RunRecordResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    run = repo.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found.")
    return RunRecordResponse.model_validate(run)


@router.get("/{run_id}/summary", response_model=SummaryResponse)
async def get_summary(run_id: str, request: Request) -> SummaryResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    try:
        summary = load_summary(run_id, repo=repo)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return SummaryResponse.model_validate(summary)


@router.get("/{run_id}/metrics/timeseries", response_model=TimeseriesResponse)
async def get_timeseries(
    run_id: str,
    request: Request,
    max_points: int = Query(default=5000, ge=1, le=25000),
) -> TimeseriesResponse:
    validate_auth(request)
    _require_postgres_dsn(request)
    frame = load_timeseries(run_id)
    if frame.empty:
        raise HTTPException(status_code=404, detail=f"Timeseries artifact missing for run '{run_id}'.")
    total = len(frame)
    truncated = total > max_points
    if truncated:
        frame = frame.tail(max_points).reset_index(drop=True)
    return TimeseriesResponse.model_validate(
        {
            "points": frame.to_dict("records"),
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
    _require_postgres_dsn(request)
    frame = load_rolling_metrics(run_id, window_days=window_days)
    if frame.empty:
        raise HTTPException(status_code=404, detail=f"Rolling metrics artifact missing for run '{run_id}'.")
    total = len(frame)
    truncated = total > max_points
    if truncated:
        frame = frame.tail(max_points).reset_index(drop=True)
    return RollingMetricsResponse.model_validate(
        {
            "points": frame.to_dict("records"),
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
    _require_postgres_dsn(request)
    frame = load_trades(run_id)
    if frame.empty:
        raise HTTPException(status_code=404, detail=f"Trades artifact missing for run '{run_id}'.")
    total = len(frame)
    paged = frame.iloc[offset : offset + limit].reset_index(drop=True)
    return TradeListResponse.model_validate(
        {
            "trades": paged.to_dict("records"),
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )


@router.get("/{run_id}/artifacts", response_model=ArtifactListResponse)
async def list_run_artifacts(run_id: str, request: Request) -> ArtifactListResponse:
    validate_auth(request)
    _require_postgres_dsn(request)
    return ArtifactListResponse.model_validate({"artifacts": list_artifacts(run_id)})


@router.get("/{run_id}/artifacts/{name:path}")
async def get_artifact_content(run_id: str, name: str, request: Request) -> Response:
    validate_auth(request)
    _require_postgres_dsn(request)
    payload = read_artifact_bytes(run_id, name)
    if payload is None:
        raise HTTPException(status_code=404, detail=f"Artifact '{name}' not found for run '{run_id}'.")
    content_type = next(
        (
            artifact.get("contentType")
            for artifact in list_artifacts(run_id)
            if str(artifact.get("name") or "") == name
        ),
        None,
    ) or "application/octet-stream"
    return Response(content=payload, media_type=str(content_type))
