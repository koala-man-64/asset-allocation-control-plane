from __future__ import annotations

import logging
from typing import Any, Mapping, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from api.openapi_models import ProviderCallbackUrlResponse
from api.service.auth import AuthContext
from api.service.dependencies import get_settings, require_schwab_access, require_schwab_trade_access, validate_auth
from api.service.schwab_gateway import (
    SchwabGateway,
    SchwabGatewayAmbiguousWriteError,
    SchwabGatewaySessionExpiredError,
    SchwabGatewayValidationError,
)
from schwab.errors import (
    SchwabAuthError,
    SchwabError,
    SchwabNotConfiguredError,
    SchwabNotFoundError,
    SchwabRateLimitError,
    SchwabServerError,
)


logger = logging.getLogger("asset-allocation.api.schwab")
router = APIRouter()


class SchwabConnectCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1, max_length=4096)
    state: str = Field(min_length=1, max_length=256)


class SchwabOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order: dict[str, Any] = Field(min_length=1)


def _get_gateway(request: Request) -> SchwabGateway:
    gateway = getattr(request.app.state, "schwab_gateway", None)
    if isinstance(gateway, SchwabGateway):
        return gateway
    raise HTTPException(status_code=500, detail="Schwab gateway is not initialized.")


def _handle_schwab_error(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, SchwabGatewayValidationError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, SchwabGatewaySessionExpiredError):
        if exc.payload:
            raise HTTPException(status_code=409, detail={"message": str(exc), **exc.payload}) from exc
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, SchwabGatewayAmbiguousWriteError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, SchwabNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, SchwabNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, SchwabRateLimitError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(exc, SchwabAuthError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, SchwabServerError):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if isinstance(exc, SchwabError):
        status_code = exc.status_code if exc.status_code in {400, 401, 403, 404, 409, 429, 503} else 502
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    if isinstance(exc, ValueError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


def _require_provider_callback_url(request: Request) -> str:
    callback_url = get_settings(request).get_provider_callback_url("schwab")
    if callback_url:
        return callback_url
    raise HTTPException(
        status_code=503,
        detail="Schwab callback URL is not configured. Set SCHWAB_APP_CALLBACK_URL or API_PUBLIC_BASE_URL.",
    )


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _encode(payload: Any) -> Any:
    return jsonable_encoder(payload)


def _order_payload(payload: SchwabOrderRequest) -> Mapping[str, Any]:
    if not payload.order:
        raise HTTPException(status_code=400, detail="order is required.")
    return payload.order


@router.post("/connect/start")
def schwab_connect_start(
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.start_connect(subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.post("/connect/complete")
def schwab_connect_complete(
    payload: SchwabConnectCompleteRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.complete_connect(code=payload.code, state=payload.state, subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/connect/callback")
def schwab_connect_callback(
    response: Response,
    code: str = Query(..., min_length=1),
    state: str = Query(..., min_length=1),
    session: str | None = Query(default=None),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    del session
    _set_no_store(response)
    try:
        return _encode(gateway.complete_connect_from_callback(code=code, state=state))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/connect/callback-url", response_model=ProviderCallbackUrlResponse)
def schwab_connect_callback_url(
    request: Request,
    _auth_context: AuthContext = Depends(validate_auth),
) -> JSONResponse:
    payload = ProviderCallbackUrlResponse(callback_url=_require_provider_callback_url(request))
    return JSONResponse(payload.model_dump(mode="json"), headers={"Cache-Control": "no-store"})


@router.get("/session")
def schwab_session(
    response: Response,
    _auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    return _encode(gateway.get_session_state())


@router.post("/disconnect")
def schwab_disconnect(
    response: Response,
    _auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    return _encode(gateway.disconnect())


@router.get("/account-numbers")
def schwab_account_numbers(
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_account_numbers(subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts")
def schwab_accounts(
    response: Response,
    fields: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_accounts(subject=auth_context.subject, fields=fields))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}")
def schwab_account(
    account_number: str,
    response: Response,
    fields: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_account(account_number=account_number, subject=auth_context.subject, fields=fields))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}/balance")
def schwab_balance(
    account_number: str,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_balance(account_number=account_number, subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}/positions")
def schwab_positions(
    account_number: str,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_positions(account_number=account_number, subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/orders")
def schwab_all_orders(
    response: Response,
    max_results: Optional[int] = Query(default=None, ge=1, le=3000, alias="maxResults"),
    from_entered_time: Optional[str] = Query(default=None, alias="fromEnteredTime"),
    to_entered_time: Optional[str] = Query(default=None, alias="toEnteredTime"),
    status: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_orders(
                subject=auth_context.subject,
                max_results=max_results,
                from_entered_time=from_entered_time,
                to_entered_time=to_entered_time,
                status=status,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}/orders")
def schwab_account_orders(
    account_number: str,
    response: Response,
    max_results: Optional[int] = Query(default=None, ge=1, le=3000, alias="maxResults"),
    from_entered_time: Optional[str] = Query(default=None, alias="fromEnteredTime"),
    to_entered_time: Optional[str] = Query(default=None, alias="toEnteredTime"),
    status: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_orders(
                subject=auth_context.subject,
                account_number=account_number,
                max_results=max_results,
                from_entered_time=from_entered_time,
                to_entered_time=to_entered_time,
                status=status,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.post("/accounts/{account_number}/orders/preview")
def schwab_preview_order(
    account_number: str,
    payload: SchwabOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_trade_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.preview_order(
                account_number=account_number,
                order=_order_payload(payload),
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.post("/accounts/{account_number}/orders")
def schwab_place_order(
    account_number: str,
    payload: SchwabOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_trade_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.place_order(
                account_number=account_number,
                order=_order_payload(payload),
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}/orders/{order_id}")
def schwab_order(
    account_number: str,
    order_id: str,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_order(account_number=account_number, order_id=order_id, subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.put("/accounts/{account_number}/orders/{order_id}")
def schwab_replace_order(
    account_number: str,
    order_id: str,
    payload: SchwabOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_trade_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.replace_order(
                account_number=account_number,
                order_id=order_id,
                order=_order_payload(payload),
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.delete("/accounts/{account_number}/orders/{order_id}")
def schwab_cancel_order(
    account_number: str,
    order_id: str,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_trade_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.cancel_order(account_number=account_number, order_id=order_id, subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}/transactions")
def schwab_transactions(
    account_number: str,
    response: Response,
    start_date: Optional[str] = Query(default=None, alias="startDate"),
    end_date: Optional[str] = Query(default=None, alias="endDate"),
    types: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_transactions(
                account_number=account_number,
                subject=auth_context.subject,
                start_date=start_date,
                end_date=end_date,
                types=types,
                symbol=symbol,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/accounts/{account_number}/transactions/{transaction_id}")
def schwab_transaction(
    account_number: str,
    transaction_id: str,
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.get_transaction(
                account_number=account_number,
                transaction_id=transaction_id,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_schwab_error(exc)
        raise


@router.get("/user-preference")
def schwab_user_preference(
    response: Response,
    auth_context: AuthContext = Depends(require_schwab_access),
    gateway: SchwabGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_user_preference(subject=auth_context.subject))
    except Exception as exc:
        _handle_schwab_error(exc)
        raise
