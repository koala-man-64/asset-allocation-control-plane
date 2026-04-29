from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime
from collections.abc import Callable
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request
from api.service.backtest_contracts_compat import (
    BacktestLookupRequest,
    BacktestLookupResponse,
    BacktestPolicyEventListResponse,
    BacktestResultLinks,
    BacktestRunResponse,
    BacktestRunRequest,
    BacktestSummary,
    BacktestStreamEvent,
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


@router.get("/{run_id}/policy-events", response_model=BacktestPolicyEventListResponse)
async def get_policy_events(
    run_id: str,
    request: Request,
    limit: int = Query(default=2000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
) -> BacktestPolicyEventListResponse:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    _require_published_run(repo, run_id)
    total = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.count_policy_events(run_id),
    )
    events = _postgres_or_503(
        "Postgres is unavailable for backtest features.",
        lambda: repo.list_policy_events(run_id, limit=limit, offset=offset),
    )
    return BacktestPolicyEventListResponse.model_validate(
        {
            "events": events,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    )
