from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from asset_allocation_contracts.notifications import (
    CreateNotificationRequest,
    NotificationActionDetailResponse,
    NotificationDecisionRequest,
    NotificationStatusResponse,
)

from api.service.auth import AuthContext
from api.service.dependencies import require_notification_read_access, require_notification_write_access
from api.service.notification_service import NotificationError, NotificationService
from api.service.trade_desk_service import TradeDeskService
from core.notification_repository import NotificationRepository
from core.trade_desk_repository import TradeDeskRepository

router = APIRouter()


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _actor(auth_context: AuthContext) -> str | None:
    return str(auth_context.subject or "").strip() or None


def _service(request: Request) -> NotificationService:
    settings = request.app.state.settings
    dsn = str(settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for notification endpoints.")

    def _trade_desk_service() -> TradeDeskService:
        return TradeDeskService(TradeDeskRepository(dsn), settings.trade_desk)

    return NotificationService(
        NotificationRepository(dsn),
        settings.notifications,
        request.app.state.notification_delivery_client,
        _trade_desk_service,
    )


def _handle_notification_error(exc: NotificationError) -> None:
    raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc


@router.post("/notifications", response_model=NotificationStatusResponse)
async def create_notification(
    payload: CreateNotificationRequest,
    request: Request,
    response: Response,
    auth_context: AuthContext = Depends(require_notification_write_access),
) -> NotificationStatusResponse:
    _set_no_store(response)
    try:
        return _service(request).create_notification(payload, actor=_actor(auth_context))
    except NotificationError as exc:
        _handle_notification_error(exc)
        raise


@router.get("/notifications/{request_id}", response_model=NotificationStatusResponse)
async def get_notification_status(
    request_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_notification_read_access),
) -> NotificationStatusResponse:
    _set_no_store(response)
    try:
        return _service(request).get_status(request_id)
    except NotificationError as exc:
        _handle_notification_error(exc)
        raise


@router.get("/notifications/actions/{token}", response_model=NotificationActionDetailResponse)
async def get_notification_action(
    token: str,
    request: Request,
    response: Response,
) -> NotificationActionDetailResponse:
    _set_no_store(response)
    try:
        return _service(request).get_action_detail(token)
    except NotificationError as exc:
        _handle_notification_error(exc)
        raise


@router.post("/notifications/actions/{token}/approve", response_model=NotificationStatusResponse)
async def approve_notification_action(
    token: str,
    payload: NotificationDecisionRequest,
    request: Request,
    response: Response,
) -> NotificationStatusResponse:
    _set_no_store(response)
    if payload.decision != "approve":
        raise HTTPException(status_code=400, detail="Decision payload must be 'approve'.")
    try:
        return _service(request).decide(token, payload)
    except NotificationError as exc:
        _handle_notification_error(exc)
        raise


@router.post("/notifications/actions/{token}/deny", response_model=NotificationStatusResponse)
async def deny_notification_action(
    token: str,
    payload: NotificationDecisionRequest,
    request: Request,
    response: Response,
) -> NotificationStatusResponse:
    _set_no_store(response)
    if payload.decision != "deny":
        raise HTTPException(status_code=400, detail="Decision payload must be 'deny'.")
    try:
        return _service(request).decide(token, payload)
    except NotificationError as exc:
        _handle_notification_error(exc)
        raise
