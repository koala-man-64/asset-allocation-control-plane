from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountListResponse,
    TradeOrderCancelRequest,
    TradeOrderCancelResponse,
    TradeOrderHistoryResponse,
    TradeOrderPlaceRequest,
    TradeOrderPlaceResponse,
    TradeOrderPreviewRequest,
    TradeOrderPreviewResponse,
    TradePositionListResponse,
)

from api.service.auth import AuthContext
from api.service.dependencies import (
    require_trade_desk_cancel_access,
    require_trade_desk_live_access,
    require_trade_desk_place_access,
    require_trade_desk_preview_access,
    require_trade_desk_read_access,
)
from api.service.trade_desk_service import TradeDeskError, TradeDeskService
from core.trade_desk_repository import TradeDeskRepository

router = APIRouter()


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _service(request: Request) -> TradeDeskService:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for trade desk endpoints.")
    return TradeDeskService(TradeDeskRepository(dsn), request.app.state.settings.trade_desk)


def _actor(auth_context: AuthContext) -> str | None:
    return str(auth_context.subject or "").strip() or None


def _handle_trade_desk_error(exc: TradeDeskError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.get("/trade-accounts", response_model=TradeAccountListResponse)
async def list_trade_accounts(
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> TradeAccountListResponse:
    _set_no_store(response)
    try:
        return _service(request).list_accounts()
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.get("/trade-accounts/{account_id}", response_model=TradeAccountDetail)
async def get_trade_account(
    account_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> TradeAccountDetail:
    _set_no_store(response)
    try:
        return _service(request).get_account(account_id)
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.get("/trade-accounts/{account_id}/positions", response_model=TradePositionListResponse)
async def get_trade_positions(
    account_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> TradePositionListResponse:
    _set_no_store(response)
    try:
        return _service(request).list_positions(account_id)
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.get("/trade-accounts/{account_id}/orders", response_model=TradeOrderHistoryResponse)
async def get_trade_orders(
    account_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> TradeOrderHistoryResponse:
    _set_no_store(response)
    try:
        return _service(request).list_orders(account_id)
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.get("/trade-accounts/{account_id}/history", response_model=TradeOrderHistoryResponse)
async def get_trade_history(
    account_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> TradeOrderHistoryResponse:
    _set_no_store(response)
    try:
        return _service(request).list_history(account_id)
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.post("/trade-accounts/{account_id}/orders/preview", response_model=TradeOrderPreviewResponse)
async def preview_trade_order(
    account_id: str,
    payload: TradeOrderPreviewRequest,
    request: Request,
    response: Response,
    auth_context: AuthContext = Depends(require_trade_desk_preview_access),
) -> TradeOrderPreviewResponse:
    _set_no_store(response)
    try:
        return _service(request).preview_order(account_id, payload, actor=_actor(auth_context))
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.post("/trade-accounts/{account_id}/orders", response_model=TradeOrderPlaceResponse)
async def place_trade_order(
    account_id: str,
    payload: TradeOrderPlaceRequest,
    request: Request,
    response: Response,
    auth_context: AuthContext = Depends(require_trade_desk_place_access),
) -> TradeOrderPlaceResponse:
    _set_no_store(response)
    if payload.environment == "live":
        auth_context = require_trade_desk_live_access(request)
    try:
        return _service(request).place_order(account_id, payload, actor=_actor(auth_context))
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise


@router.post("/trade-accounts/{account_id}/orders/{order_id}/cancel", response_model=TradeOrderCancelResponse)
async def cancel_trade_order(
    account_id: str,
    order_id: str,
    payload: TradeOrderCancelRequest,
    request: Request,
    response: Response,
    auth_context: AuthContext = Depends(require_trade_desk_cancel_access),
) -> TradeOrderCancelResponse:
    _set_no_store(response)
    try:
        return _service(request).cancel_order(account_id, order_id, payload, actor=_actor(auth_context))
    except TradeDeskError as exc:
        _handle_trade_desk_error(exc)
        raise
