from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, HTTPException, Query, Request
from asset_allocation_contracts.portfolio import (
    PortfolioAccountDetailResponse,
    PortfolioAccountListResponse,
    PortfolioAccountUpsertRequest,
    PortfolioAlertListResponse,
    PortfolioAssignment,
    PortfolioAssignmentRequest,
    PortfolioDefinitionDetailResponse,
    PortfolioForecastAssumption,
    PortfolioForecastHorizon,
    PortfolioForecastResponse,
    PortfolioHistoryResponse,
    PortfolioLedgerEvent,
    PortfolioLedgerEventPayload,
    PortfolioListResponse,
    PortfolioNextRebalanceResponse,
    PortfolioPositionListResponse,
    PortfolioRevision,
    PortfolioSnapshot,
    PortfolioUpsertRequest,
    RebalanceProposal,
    PortfolioRebalanceApplyRequest,
    PortfolioRebalancePreviewRequest,
)

from api.service.dependencies import validate_auth
from core.portfolio_repository import PortfolioRepository

logger = logging.getLogger(__name__)

router = APIRouter()


def _repo(request: Request) -> PortfolioRepository:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for portfolio workspace endpoints.")
    return PortfolioRepository(dsn)


@router.get("/portfolio-accounts", response_model=PortfolioAccountListResponse)
async def list_portfolio_accounts(request: Request) -> PortfolioAccountListResponse:
    validate_auth(request)
    return _repo(request).list_accounts()


@router.post("/portfolio-accounts", response_model=PortfolioAccountDetailResponse)
async def create_portfolio_account(
    payload: PortfolioAccountUpsertRequest,
    request: Request,
) -> PortfolioAccountDetailResponse:
    validate_auth(request)
    try:
        return _repo(request).save_account(account_id=None, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio-accounts/{account_id}", response_model=PortfolioAccountDetailResponse)
async def get_portfolio_account(account_id: str, request: Request) -> PortfolioAccountDetailResponse:
    validate_auth(request)
    detail = _repo(request).get_account_detail(account_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Portfolio account '{account_id}' not found.")
    return detail


@router.put("/portfolio-accounts/{account_id}", response_model=PortfolioAccountDetailResponse)
async def update_portfolio_account(
    account_id: str,
    payload: PortfolioAccountUpsertRequest,
    request: Request,
) -> PortfolioAccountDetailResponse:
    validate_auth(request)
    try:
        return _repo(request).save_account(account_id=account_id, payload=payload)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/portfolio-accounts/{account_id}/assignments", response_model=PortfolioAssignment)
async def assign_portfolio(
    account_id: str,
    payload: PortfolioAssignmentRequest,
    request: Request,
) -> PortfolioAssignment:
    validate_auth(request)
    try:
        return _repo(request).assign_portfolio(account_id, payload)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/portfolio-accounts/{account_id}/ledger-events", response_model=PortfolioLedgerEvent)
async def post_portfolio_ledger_event(
    account_id: str,
    payload: PortfolioLedgerEventPayload,
    request: Request,
) -> PortfolioLedgerEvent:
    validate_auth(request)
    try:
        return _repo(request).add_ledger_event(account_id, payload)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/portfolio-accounts/{account_id}/rebalances/preview", response_model=RebalanceProposal)
async def preview_portfolio_rebalance(
    account_id: str,
    payload: PortfolioRebalancePreviewRequest,
    request: Request,
) -> RebalanceProposal:
    validate_auth(request)
    try:
        return _repo(request).create_rebalance_preview(account_id, as_of=payload.asOf, notes=payload.notes)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/portfolio-accounts/{account_id}/rebalances/apply")
async def apply_portfolio_rebalance(
    account_id: str,
    payload: PortfolioRebalanceApplyRequest,
    request: Request,
) -> dict[str, object]:
    validate_auth(request)
    try:
        return _repo(request).apply_rebalance(
            account_id,
            proposal_id=payload.proposalId,
            executed_at=payload.executedAt,
            notes=payload.notes,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolio-accounts/{account_id}/snapshot", response_model=PortfolioSnapshot)
async def get_portfolio_snapshot(account_id: str, request: Request) -> PortfolioSnapshot:
    validate_auth(request)
    snapshot = _repo(request).get_snapshot(account_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"Portfolio snapshot for account '{account_id}' not found.")
    return snapshot


@router.get("/portfolio-accounts/{account_id}/history", response_model=PortfolioHistoryResponse)
async def get_portfolio_history(
    account_id: str,
    request: Request,
    limit: int = Query(default=252, ge=1, le=5000),
) -> PortfolioHistoryResponse:
    validate_auth(request)
    return _repo(request).list_history(account_id, limit=limit)


@router.get("/portfolio-accounts/{account_id}/forecast", response_model=PortfolioForecastResponse)
async def get_portfolio_forecast(
    account_id: str,
    request: Request,
    modelName: str = Query(default="default-regime", min_length=1),
    modelVersion: int | None = Query(default=None, ge=1),
    horizon: PortfolioForecastHorizon = Query(default="3M"),
    assumption: PortfolioForecastAssumption = Query(default="current"),
    costDragOverrideBps: float = Query(default=0.0),
) -> PortfolioForecastResponse:
    validate_auth(request)
    try:
        return _repo(request).get_forecast(
            account_id,
            model_name=modelName,
            model_version=modelVersion,
            horizon=horizon,
            assumption=assumption,
            cost_drag_override_bps=costDragOverrideBps,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/portfolio-accounts/{account_id}/next-rebalance", response_model=PortfolioNextRebalanceResponse)
async def get_portfolio_next_rebalance(
    account_id: str,
    request: Request,
    asOf: date | None = Query(default=None),
) -> PortfolioNextRebalanceResponse:
    validate_auth(request)
    try:
        return _repo(request).get_next_rebalance(account_id, as_of=asOf)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/portfolio-accounts/{account_id}/positions", response_model=PortfolioPositionListResponse)
async def get_portfolio_positions(
    account_id: str,
    request: Request,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    asOf: str | None = Query(default=None),
) -> PortfolioPositionListResponse:
    validate_auth(request)
    return _repo(request).list_positions(
        account_id,
        as_of=None if asOf is None else date.fromisoformat(asOf),
        limit=limit,
        offset=offset,
    )


@router.get("/portfolio-accounts/{account_id}/alerts", response_model=PortfolioAlertListResponse)
async def get_portfolio_alerts(
    account_id: str,
    request: Request,
    includeResolved: bool = Query(default=True),
) -> PortfolioAlertListResponse:
    validate_auth(request)
    return _repo(request).list_alerts(account_id, include_resolved=includeResolved)


@router.get("/portfolios", response_model=PortfolioListResponse)
async def list_portfolios(request: Request) -> PortfolioListResponse:
    validate_auth(request)
    return _repo(request).list_portfolios()


@router.post("/portfolios", response_model=PortfolioDefinitionDetailResponse)
async def save_portfolio_definition(
    payload: PortfolioUpsertRequest,
    request: Request,
) -> PortfolioDefinitionDetailResponse:
    validate_auth(request)
    try:
        return _repo(request).save_portfolio(payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/portfolios/{name}/revisions", response_model=list[PortfolioRevision])
async def list_portfolio_revisions(name: str, request: Request) -> list[PortfolioRevision]:
    validate_auth(request)
    return _repo(request).list_portfolio_revisions(name)


@router.get("/portfolios/{name}", response_model=PortfolioDefinitionDetailResponse)
async def get_portfolio_definition(name: str, request: Request) -> PortfolioDefinitionDetailResponse:
    validate_auth(request)
    detail = _repo(request).get_portfolio_detail(name)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"Portfolio '{name}' not found.")
    return detail
