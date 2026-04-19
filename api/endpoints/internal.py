from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from psycopg import Error as PsycopgError
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

from api.service.dependencies import (
    get_ai_relay_gateway,
    get_settings,
    require_symbol_enrichment_job_access,
    validate_auth,
)
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
    reconcile_results_freshness,
)
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

router = APIRouter()


class ResultsReconcileRequest(BaseModel):
    dryRun: bool = False


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


@router.post("/results/reconcile")
async def reconcile_results(request: Request, payload: ResultsReconcileRequest | None = None) -> dict[str, Any]:
    validate_auth(request)
    body = payload or ResultsReconcileRequest()
    return reconcile_results_freshness(_require_postgres_dsn(request), dry_run=body.dryRun)


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
