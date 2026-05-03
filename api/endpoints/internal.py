from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from anyio import from_thread
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from psycopg import Error as PsycopgError
from asset_allocation_contracts.results import ResultsReconcileRequest, ResultsReconcileResponse
from asset_allocation_contracts.strategy_publication import (
    StrategyPublicationReconcileSignalRequest,
    StrategyPublicationReconcileSignalResponse,
)
from asset_allocation_contracts.symbol_enrichment import (
    SymbolCleanupRunSummary,
    SymbolCleanupWorkItem,
    SymbolEnrichmentResolveRequest,
    SymbolEnrichmentResolveResponse,
)
from asset_allocation_contracts.backtest import (
    BacktestClaimRequest,
    BacktestCompleteRequest,
    BacktestFailRequest,
    BacktestReconcileResponse,
    BacktestStartRequest,
)
from api.service.intraday_contracts_compat import (
    IntradayMonitorClaimRequest,
    IntradayMonitorClaimResponse,
    IntradayMonitorCompleteRequest,
    IntradayMonitorFailRequest,
    IntradayMonitorRunSummary,
    IntradayRefreshBatchSummary,
    IntradayRefreshClaimRequest,
    IntradayRefreshClaimResponse,
    IntradayRefreshCompleteRequest,
    IntradayRefreshFailRequest,
)

from api.service.dependencies import (
    get_ai_relay_gateway,
    get_settings,
    require_intraday_monitor_job_access,
    require_results_reconcile_job_access,
    require_strategy_publication_signal_access,
    require_symbol_enrichment_job_access,
    validate_auth,
)
from api.service.realtime import manager as realtime_manager
from api.service.symbol_enrichment_service import resolve_symbol_profile as resolve_symbol_profile_via_ai
from core.backtest_reconcile import reconcile_backtest_runs
from core.backtest_repository import BacktestRepository, BacktestResultsNotReadyError
from core.symbol_enrichment_repository import (
    claim_next_symbol_cleanup_work,
    complete_symbol_cleanup_work,
    fail_symbol_cleanup_work,
    get_symbol_cleanup_run,
)
from asset_allocation_runtime_common.foundation.postgres import connect
from core.ranking_repository import RankingRepository
from core.regime_repository import RegimeRepository
from core.results_freshness import (
    claim_next_ranking_refresh,
    complete_ranking_refresh,
    fail_ranking_refresh,
    record_strategy_publication_reconcile_signal,
    reconcile_results_freshness,
)
from core.intraday_monitor_repository import (
    claim_next_intraday_monitor_run,
    claim_next_intraday_refresh_batch,
    complete_intraday_monitor_run,
    complete_intraday_refresh_batch,
    fail_intraday_monitor_run,
    fail_intraday_refresh_batch,
    list_intraday_symbol_status,
)
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository
from .intraday import REALTIME_TOPIC_INTRADAY_MONITOR, REALTIME_TOPIC_INTRADAY_REFRESH

logger = logging.getLogger(__name__)

router = APIRouter()


class RankingRefreshClaimRequest(BaseModel):
    executionName: str | None = None


class RankingRefreshCompleteRequest(BaseModel):
    claimToken: str
    runId: str | None = None
    dependencyFingerprint: str | None = None
    dependencyState: dict[str, Any] | None = None


class RankingRefreshFailRequest(BaseModel):
    claimToken: str
    error: str


class SymbolCleanupClaimRequest(BaseModel):
    executionName: str | None = None


class SymbolCleanupCompleteRequest(BaseModel):
    result: SymbolEnrichmentResolveResponse | None = None


class SymbolCleanupFailRequest(BaseModel):
    error: str


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for internal control-plane endpoints.")
    return dsn


def _not_found(kind: str, name: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"{kind} '{name}' not found.")


def _emit_intraday_realtime(topic: str, event_type: str, payload: dict[str, Any] | None = None) -> None:
    message = {
        "type": event_type,
        "payload": payload or {},
        "emittedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        from_thread.run(realtime_manager.broadcast, topic, message)
    except RuntimeError:
        return
    except Exception:
        logger.debug("Failed to broadcast intraday realtime event.", exc_info=True)


def _probe_postgres(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


@router.get("/strategies")
async def list_strategies(request: Request) -> list[dict[str, Any]]:
    validate_auth(request)
    return StrategyRepository(_require_postgres_dsn(request)).list_strategies()


@router.get("/strategies/{name}")
async def get_strategy(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    strategy = StrategyRepository(_require_postgres_dsn(request)).get_strategy(name)
    if not strategy:
        raise _not_found("Strategy", name)
    return strategy


@router.get("/strategies/{name}/revision")
async def get_strategy_revision(name: str, request: Request, version: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
    validate_auth(request)
    revision = StrategyRepository(_require_postgres_dsn(request)).get_strategy_revision(name, version=version)
    if not revision:
        raise _not_found("Strategy revision", name)
    return revision


@router.get("/rankings")
async def list_ranking_schemas(request: Request) -> list[dict[str, Any]]:
    validate_auth(request)
    return RankingRepository(_require_postgres_dsn(request)).list_ranking_schemas()


@router.get("/rankings/{name}")
async def get_ranking_schema(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    schema = RankingRepository(_require_postgres_dsn(request)).get_ranking_schema(name)
    if not schema:
        raise _not_found("Ranking schema", name)
    return schema


@router.get("/rankings/{name}/revision")
async def get_ranking_schema_revision(
    name: str,
    request: Request,
    version: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    validate_auth(request)
    revision = RankingRepository(_require_postgres_dsn(request)).get_ranking_schema_revision(name, version=version)
    if not revision:
        raise _not_found("Ranking schema revision", name)
    return revision


@router.post("/results/reconcile", response_model=ResultsReconcileResponse)
async def reconcile_results(request: Request, payload: ResultsReconcileRequest) -> ResultsReconcileResponse:
    require_results_reconcile_job_access(request)
    return ResultsReconcileResponse.model_validate(
        reconcile_results_freshness(
            _require_postgres_dsn(request),
            dry_run=payload.dryRun,
            execution_name=str(request.headers.get("X-Caller-Execution") or "").strip() or None,
        )
    )


@router.post(
    "/strategy-publications/reconcile-signal",
    response_model=StrategyPublicationReconcileSignalResponse,
)
async def record_strategy_publication_reconcile_signal_route(
    payload: StrategyPublicationReconcileSignalRequest,
    request: Request,
) -> StrategyPublicationReconcileSignalResponse:
    require_strategy_publication_signal_access(
        request,
        producer_job_name=payload.metadata.producerJobName,
    )
    return record_strategy_publication_reconcile_signal(_require_postgres_dsn(request), payload)


@router.post("/rankings/refresh/claim")
async def claim_ranking_refresh(payload: RankingRefreshClaimRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    work = claim_next_ranking_refresh(_require_postgres_dsn(request), execution_name=payload.executionName)
    return {"work": work}


@router.post("/rankings/refresh/{strategy_name}/complete")
async def complete_ranking_refresh_work(
    strategy_name: str,
    payload: RankingRefreshCompleteRequest,
    request: Request,
) -> dict[str, Any]:
    validate_auth(request)
    try:
        return complete_ranking_refresh(
            _require_postgres_dsn(request),
            strategy_name=strategy_name,
            claim_token=payload.claimToken,
            run_id=payload.runId,
            dependency_fingerprint=payload.dependencyFingerprint,
            dependency_state=payload.dependencyState,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/rankings/refresh/{strategy_name}/fail")
async def fail_ranking_refresh_work(
    strategy_name: str,
    payload: RankingRefreshFailRequest,
    request: Request,
) -> dict[str, Any]:
    validate_auth(request)
    try:
        return fail_ranking_refresh(
            _require_postgres_dsn(request),
            strategy_name=strategy_name,
            claim_token=payload.claimToken,
            error=payload.error,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/symbol-cleanup/claim")
async def claim_symbol_cleanup_work_route(
    payload: SymbolCleanupClaimRequest,
    request: Request,
) -> dict[str, SymbolCleanupWorkItem | None]:
    require_symbol_enrichment_job_access(request)
    work = claim_next_symbol_cleanup_work(
        _require_postgres_dsn(request),
        execution_name=payload.executionName,
    )
    return {"work": work}


@router.post("/symbol-cleanup/{work_id}/complete", response_model=SymbolCleanupRunSummary)
async def complete_symbol_cleanup_work_route(
    work_id: str,
    payload: SymbolCleanupCompleteRequest,
    request: Request,
) -> SymbolCleanupRunSummary:
    require_symbol_enrichment_job_access(request, require_enabled=False)
    try:
        return complete_symbol_cleanup_work(
            _require_postgres_dsn(request),
            work_id=work_id,
            result=payload.result.model_dump(mode="json") if payload.result is not None else None,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/symbol-cleanup/{work_id}/fail", response_model=SymbolCleanupRunSummary)
async def fail_symbol_cleanup_work_route(
    work_id: str,
    payload: SymbolCleanupFailRequest,
    request: Request,
) -> SymbolCleanupRunSummary:
    require_symbol_enrichment_job_access(request, require_enabled=False)
    try:
        return fail_symbol_cleanup_work(
            _require_postgres_dsn(request),
            work_id=work_id,
            error=payload.error,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/symbol-cleanup/runs/{run_id}", response_model=SymbolCleanupRunSummary)
async def get_symbol_cleanup_run_route(run_id: str, request: Request) -> SymbolCleanupRunSummary:
    require_symbol_enrichment_job_access(request, require_enabled=False)
    run = get_symbol_cleanup_run(_require_postgres_dsn(request), run_id)
    if run is None:
        raise _not_found("Symbol cleanup run", run_id)
    return run


@router.post("/intraday-monitor/claim", response_model=IntradayMonitorClaimResponse)
async def claim_intraday_monitor_run_route(
    payload: IntradayMonitorClaimRequest,
    request: Request,
) -> IntradayMonitorClaimResponse:
    require_intraday_monitor_job_access(request)
    dsn = _require_postgres_dsn(request)
    claimed = claim_next_intraday_monitor_run(
        dsn,
        execution_name=payload.executionName,
    )
    if claimed is None:
        return IntradayMonitorClaimResponse(run=None, watchlist=None, currentSymbolStatuses=[], claimToken=None)
    run, watchlist, claim_token = claimed
    _, current_statuses = list_intraday_symbol_status(
        dsn,
        watchlist_id=run.watchlistId,
        limit=max(len(watchlist.symbols), 1),
        offset=0,
    )
    response = IntradayMonitorClaimResponse(
        run=run,
        watchlist=watchlist,
        currentSymbolStatuses=current_statuses,
        claimToken=claim_token,
    )
    _emit_intraday_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "run.claimed",
        {"run": run.model_dump(mode="json")},
    )
    return response


@router.post("/intraday-monitor/runs/{run_id}/complete", response_model=IntradayMonitorRunSummary)
async def complete_intraday_monitor_run_route(
    run_id: str,
    payload: IntradayMonitorCompleteRequest,
    request: Request,
) -> IntradayMonitorRunSummary:
    require_intraday_monitor_job_access(request, require_enabled=False)
    try:
        run = complete_intraday_monitor_run(
            _require_postgres_dsn(request),
            run_id=run_id,
            claim_token=payload.claimToken,
            symbol_statuses=payload.symbolStatuses,
            events=payload.events,
            refresh_symbols=payload.refreshSymbols,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    _emit_intraday_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "run.completed",
        {"run": run.model_dump(mode="json")},
    )
    return run


@router.post("/intraday-monitor/runs/{run_id}/fail", response_model=IntradayMonitorRunSummary)
async def fail_intraday_monitor_run_route(
    run_id: str,
    payload: IntradayMonitorFailRequest,
    request: Request,
) -> IntradayMonitorRunSummary:
    require_intraday_monitor_job_access(request, require_enabled=False)
    try:
        run = fail_intraday_monitor_run(
            _require_postgres_dsn(request),
            run_id=run_id,
            claim_token=payload.claimToken,
            error=payload.error,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _emit_intraday_realtime(
        REALTIME_TOPIC_INTRADAY_MONITOR,
        "run.failed",
        {"run": run.model_dump(mode="json")},
    )
    return run


@router.post("/intraday-refresh/claim", response_model=IntradayRefreshClaimResponse)
async def claim_intraday_refresh_batch_route(
    payload: IntradayRefreshClaimRequest,
    request: Request,
) -> IntradayRefreshClaimResponse:
    require_intraday_monitor_job_access(request)
    claimed = claim_next_intraday_refresh_batch(
        _require_postgres_dsn(request),
        execution_name=payload.executionName,
    )
    if claimed is None:
        return IntradayRefreshClaimResponse(batch=None, claimToken=None)
    batch, claim_token = claimed
    response = IntradayRefreshClaimResponse(batch=batch, claimToken=claim_token)
    _emit_intraday_realtime(
        REALTIME_TOPIC_INTRADAY_REFRESH,
        "refresh.claimed",
        {"batch": batch.model_dump(mode="json")},
    )
    return response


@router.post("/intraday-refresh/batches/{batch_id}/complete", response_model=IntradayRefreshBatchSummary)
async def complete_intraday_refresh_batch_route(
    batch_id: str,
    payload: IntradayRefreshCompleteRequest,
    request: Request,
) -> IntradayRefreshBatchSummary:
    require_intraday_monitor_job_access(request, require_enabled=False)
    try:
        batch = complete_intraday_refresh_batch(
            _require_postgres_dsn(request),
            batch_id=batch_id,
            claim_token=payload.claimToken,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _emit_intraday_realtime(
        REALTIME_TOPIC_INTRADAY_REFRESH,
        "refresh.completed",
        {"batch": batch.model_dump(mode="json")},
    )
    return batch


@router.post("/intraday-refresh/batches/{batch_id}/fail", response_model=IntradayRefreshBatchSummary)
async def fail_intraday_refresh_batch_route(
    batch_id: str,
    payload: IntradayRefreshFailRequest,
    request: Request,
) -> IntradayRefreshBatchSummary:
    require_intraday_monitor_job_access(request, require_enabled=False)
    try:
        batch = fail_intraday_refresh_batch(
            _require_postgres_dsn(request),
            batch_id=batch_id,
            claim_token=payload.claimToken,
            error=payload.error,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _emit_intraday_realtime(
        REALTIME_TOPIC_INTRADAY_REFRESH,
        "refresh.failed",
        {"batch": batch.model_dump(mode="json")},
    )
    return batch


@router.post("/symbol-enrichment/resolve", response_model=SymbolEnrichmentResolveResponse)
async def resolve_symbol_enrichment_route(
    payload: SymbolEnrichmentResolveRequest,
    request: Request,
) -> SymbolEnrichmentResolveResponse:
    auth_context = require_symbol_enrichment_job_access(request)
    settings = get_settings(request).symbol_enrichment
    try:
        resolved = await resolve_symbol_profile_via_ai(
            gateway=get_ai_relay_gateway(request),
            auth_context=auth_context,
            request_payload=payload,
            model_name=settings.model,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if resolved.confidence is None or resolved.confidence < settings.confidence_min:
        raise HTTPException(
            status_code=422,
            detail=(
                "Symbol enrichment confidence below threshold: "
                f"{resolved.confidence!r} < {settings.confidence_min}."
            ),
        )
    return resolved


@router.get("/universes/{name}")
async def get_universe_config(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    universe = UniverseRepository(_require_postgres_dsn(request)).get_universe_config(name)
    if not universe:
        raise _not_found("Universe config", name)
    return universe


@router.get("/universes/{name}/revision")
async def get_universe_config_revision(
    name: str,
    request: Request,
    version: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    validate_auth(request)
    revision = UniverseRepository(_require_postgres_dsn(request)).get_universe_config_revision(name, version=version)
    if not revision:
        raise _not_found("Universe config revision", name)
    return revision


@router.get("/regimes/current")
async def get_regime_current(
    request: Request,
    modelName: str = Query(default="default-regime", min_length=1),
    modelVersion: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    validate_auth(request)
    payload = RegimeRepository(_require_postgres_dsn(request)).get_regime_latest(
        model_name=modelName,
        model_version=modelVersion,
    )
    if not payload:
        raise _not_found("Regime current snapshot", modelName)
    return payload


@router.get("/regimes/models/active")
async def list_active_regime_models(request: Request) -> list[dict[str, Any]]:
    validate_auth(request)
    return RegimeRepository(_require_postgres_dsn(request)).list_active_regime_model_revisions()


@router.get("/regimes/models/{name}/active")
async def get_active_regime_model(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    payload = RegimeRepository(_require_postgres_dsn(request)).get_active_regime_model_revision(name)
    if not payload:
        raise _not_found("Active regime model", name)
    return payload


@router.get("/regimes/models/{name}/revision")
async def get_regime_model_revision(
    name: str,
    request: Request,
    version: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    validate_auth(request)
    payload = RegimeRepository(_require_postgres_dsn(request)).get_regime_model_revision(name, version=version)
    if not payload:
        raise _not_found("Regime model revision", name)
    return payload


@router.get("/backtests/runs/{run_id}")
async def get_backtest_run(run_id: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    run = BacktestRepository(_require_postgres_dsn(request)).get_run(run_id)
    if not run:
        raise _not_found("Backtest run", run_id)
    return run


@router.get("/backtests/ready")
async def ready_backtests(request: Request) -> dict[str, str]:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    try:
        _probe_postgres(dsn)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Postgres is unavailable for backtest readiness.") from exc
    return {"status": "ready"}


@router.get("/intraday/ready")
async def ready_intraday(request: Request) -> dict[str, str]:
    require_intraday_monitor_job_access(request)
    dsn = _require_postgres_dsn(request)
    try:
        _probe_postgres(dsn)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Postgres is unavailable for intraday readiness.") from exc
    return {"status": "ready"}


@router.post("/backtests/runs/claim")
async def claim_backtest_run(payload: BacktestClaimRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    run = BacktestRepository(_require_postgres_dsn(request)).claim_next_run(execution_name=payload.executionName)
    return {"run": run}


@router.post("/backtests/runs/reconcile", response_model=BacktestReconcileResponse)
async def reconcile_backtest_run_queue(request: Request) -> BacktestReconcileResponse:
    validate_auth(request)
    return reconcile_backtest_runs(_require_postgres_dsn(request))


@router.post("/backtests/runs/{run_id}/start")
async def start_backtest_run(run_id: str, payload: BacktestStartRequest, request: Request) -> dict[str, str]:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    if not repo.get_run(run_id):
        raise _not_found("Backtest run", run_id)
    repo.start_run(run_id, execution_name=payload.executionName)
    return {"status": "ok"}


@router.post("/backtests/runs/{run_id}/heartbeat")
async def backtest_heartbeat(run_id: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    if not repo.get_run(run_id):
        raise _not_found("Backtest run", run_id)
    repo.update_heartbeat(run_id)
    return {"status": "ok"}


@router.post("/backtests/runs/{run_id}/complete")
async def complete_backtest_run(run_id: str, payload: BacktestCompleteRequest, request: Request) -> dict[str, str]:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    if not repo.get_run(run_id):
        raise _not_found("Backtest run", run_id)
    try:
        repo.complete_run(run_id, summary=payload.summary)
    except BacktestResultsNotReadyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "ok"}


@router.post("/backtests/runs/{run_id}/fail")
async def fail_backtest_run(run_id: str, payload: BacktestFailRequest, request: Request) -> dict[str, str]:
    validate_auth(request)
    repo = BacktestRepository(_require_postgres_dsn(request))
    if not repo.get_run(run_id):
        raise _not_found("Backtest run", run_id)
    repo.fail_run(run_id, error=payload.error)
    return {"status": "ok"}
