from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from psycopg import Error as PsycopgError
from asset_allocation_contracts.backtest import (
    BacktestClaimRequest,
    BacktestCompleteRequest,
    BacktestFailRequest,
    BacktestReconcileResponse,
    BacktestStartRequest,
)

from api.service.dependencies import validate_auth
from core.backtest_reconcile import reconcile_backtest_runs
from core.backtest_repository import BacktestRepository, BacktestResultsNotReadyError
from core.postgres import connect
from core.ranking_repository import RankingRepository
from core.regime_repository import RegimeRepository
from core.strategy_repository import StrategyRepository
from core.universe_repository import UniverseRepository

logger = logging.getLogger(__name__)

router = APIRouter()


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
