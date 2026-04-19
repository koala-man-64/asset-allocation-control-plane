from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from psycopg import Error as PsycopgError
from asset_allocation_contracts.portfolio import PortfolioAlert, PortfolioHistoryPoint, PortfolioPosition, PortfolioSnapshot

from api.service.dependencies import validate_auth
from asset_allocation_runtime_common.foundation.postgres import connect
from core.portfolio_repository import PortfolioRepository

logger = logging.getLogger(__name__)

router = APIRouter()
compat_router = APIRouter()


class PortfolioMaterializationClaimRequest(BaseModel):
    executionName: str | None = None


class PortfolioMaterializationHeartbeatRequest(BaseModel):
    claimToken: str = Field(..., min_length=1)


class PortfolioMaterializationCompleteRequest(BaseModel):
    claimToken: str = Field(..., min_length=1)
    dependencyFingerprint: str | None = None
    dependencyState: dict[str, Any] | None = None
    snapshot: PortfolioSnapshot
    history: list[PortfolioHistoryPoint] = Field(default_factory=list)
    positions: list[PortfolioPosition] = Field(default_factory=list)
    alerts: list[PortfolioAlert] = Field(default_factory=list)


class PortfolioMaterializationFailRequest(BaseModel):
    claimToken: str = Field(..., min_length=1)
    error: str = Field(..., min_length=1)


class PortfolioMaterializationRebuildRequest(BaseModel):
    accountId: str | None = None


class PortfolioMaterializationStartRequest(BaseModel):
    claimToken: str = Field(..., min_length=1)
    executionName: str | None = None


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for internal portfolio endpoints.")
    return dsn


def _repo(request: Request) -> PortfolioRepository:
    return PortfolioRepository(_require_postgres_dsn(request))


def _probe_postgres(dsn: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


@router.get("/ready")
@compat_router.get("/portfolio-materializations/ready")
async def ready_portfolios(request: Request) -> dict[str, str]:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    try:
        _probe_postgres(dsn)
    except PsycopgError as exc:
        raise HTTPException(status_code=503, detail="Postgres is unavailable for portfolio readiness.") from exc
    return {"status": "ready"}


@router.post("/materializations/claim")
@compat_router.post("/portfolio-materializations/claim")
async def claim_portfolio_materialization(
    payload: PortfolioMaterializationClaimRequest,
    request: Request,
) -> dict[str, Any]:
    validate_auth(request)
    return {"work": _repo(request).claim_next_materialization(execution_name=payload.executionName)}


@router.get("/accounts/{account_id}/bundle")
@compat_router.get("/portfolio-materializations/accounts/{account_id}/bundle")
async def get_portfolio_materialization_bundle(
    account_id: str,
    request: Request,
    claimToken: str | None = None,
) -> dict[str, Any]:
    validate_auth(request)
    try:
        return _repo(request).get_materialization_bundle(account_id, claim_token=claimToken)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/accounts/{account_id}/start")
@compat_router.post("/portfolio-materializations/accounts/{account_id}/start")
async def start_portfolio_materialization(
    account_id: str,
    payload: PortfolioMaterializationStartRequest,
    request: Request,
) -> dict[str, str]:
    validate_auth(request)
    try:
        _repo(request).start_materialization(
            account_id,
            claim_token=payload.claimToken,
            execution_name=payload.executionName,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "ok"}


@router.post("/materializations/{account_id}/heartbeat")
@compat_router.post("/portfolio-materializations/accounts/{account_id}/heartbeat")
async def heartbeat_portfolio_materialization(
    account_id: str,
    payload: PortfolioMaterializationHeartbeatRequest,
    request: Request,
) -> dict[str, str]:
    validate_auth(request)
    try:
        _repo(request).heartbeat_materialization(account_id, claim_token=payload.claimToken)
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "ok"}


@router.post("/materializations/{account_id}/complete")
@compat_router.post("/portfolio-materializations/accounts/{account_id}/complete")
async def complete_portfolio_materialization(
    account_id: str,
    payload: PortfolioMaterializationCompleteRequest,
    request: Request,
) -> dict[str, Any]:
    validate_auth(request)
    try:
        return _repo(request).complete_materialization(
            account_id,
            claim_token=payload.claimToken,
            dependency_fingerprint=payload.dependencyFingerprint,
            dependency_state=payload.dependencyState,
            snapshot=payload.snapshot,
            history=payload.history,
            positions=payload.positions,
            alerts=payload.alerts,
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/materializations/{account_id}/fail")
@compat_router.post("/portfolio-materializations/accounts/{account_id}/fail")
async def fail_portfolio_materialization(
    account_id: str,
    payload: PortfolioMaterializationFailRequest,
    request: Request,
) -> dict[str, Any]:
    validate_auth(request)
    try:
        return _repo(request).fail_materialization(account_id, claim_token=payload.claimToken, error=payload.error)
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/materializations/rebuild")
@compat_router.post("/portfolio-materializations/rebuild")
async def rebuild_portfolio_materializations(
    request: Request,
    payload: PortfolioMaterializationRebuildRequest | None = None,
) -> dict[str, Any]:
    validate_auth(request)
    body = payload or PortfolioMaterializationRebuildRequest()
    return _repo(request).rebuild_materializations(account_id=body.accountId)


@router.get("/materializations/stale")
@compat_router.get("/portfolio-materializations/stale")
async def list_portfolio_materializations(request: Request) -> dict[str, Any]:
    validate_auth(request)
    rows = _repo(request).list_materialization_state()
    return {"rows": rows, "count": len(rows)}


@compat_router.get("/portfolio-accounts/{account_id}")
async def get_portfolio_account_detail_compat(account_id: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    detail = _repo(request).get_account_detail(account_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Portfolio account '{account_id}' not found.")
    return detail.model_dump(mode="json")
