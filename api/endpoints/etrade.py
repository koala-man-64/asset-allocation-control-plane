from __future__ import annotations

import logging
from decimal import Decimal
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from api.openapi_models import ProviderCallbackUrlResponse
from api.service.auth import AuthContext
from api.service.dependencies import get_settings, require_etrade_access, require_etrade_trade_access, validate_auth
from api.service.etrade_gateway import ETradeGateway
from etrade_provider import (
    ETradeApiError,
    ETradeBrokerAuthError,
    ETradeError,
    ETradeInactiveSessionError,
    ETradeNotConfiguredError,
    ETradeRateLimitError,
    ETradeSessionExpiredError,
    ETradeValidationError,
)

logger = logging.getLogger("asset-allocation.api.etrade")
router = APIRouter()

ETradeEnvironment = Literal["sandbox", "live"]
ETradeAssetType = Literal["equity", "option"]
ETradeOptionType = Literal["CALL", "PUT"]


class ETradeConnectStartRequest(BaseModel):
    environment: ETradeEnvironment


class ETradeConnectCompleteRequest(BaseModel):
    environment: ETradeEnvironment
    verifier: str = Field(min_length=1, max_length=128)


class ETradeDisconnectRequest(BaseModel):
    environment: ETradeEnvironment


class ETradeOptionContractRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(min_length=1, max_length=32)
    call_put: ETradeOptionType
    expiry_year: int = Field(ge=2000, le=2100)
    expiry_month: int = Field(ge=1, le=12)
    expiry_day: int = Field(ge=1, le=31)
    strike_price: Decimal = Field(gt=0)

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return str(value).strip().upper()


class ETradeOrderPreviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: ETradeEnvironment
    account_key: str = Field(min_length=1, max_length=128)
    asset_type: ETradeAssetType
    symbol: Optional[str] = Field(default=None, min_length=1, max_length=32)
    side: str = Field(min_length=1, max_length=32)
    quantity: Decimal = Field(gt=0)
    price_type: str = Field(min_length=1, max_length=32)
    limit_price: Optional[Decimal] = Field(default=None, gt=0)
    stop_price: Optional[Decimal] = Field(default=None, gt=0)
    term: str = Field(min_length=1, max_length=32)
    session: str = Field(min_length=1, max_length=32)
    all_or_none: bool = False
    option: Optional[ETradeOptionContractRequest] = None

    @field_validator("account_key")
    @classmethod
    def _normalize_account_key(cls, value: str) -> str:
        return str(value).strip()

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    @field_validator("side", "price_type", "term", "session")
    @classmethod
    def _normalize_upper(cls, value: str) -> str:
        return str(value).strip().upper()

    @model_validator(mode="after")
    def _validate_shape(self) -> "ETradeOrderPreviewRequest":
        if self.asset_type == "equity":
            if not self.symbol:
                raise ValueError("symbol is required for equity orders.")
            if self.option is not None:
                raise ValueError("option details are not allowed for equity orders.")
        else:
            if self.option is None:
                raise ValueError("option details are required for option orders.")
            if self.symbol is not None:
                raise ValueError("symbol must not be set for option orders; use option.symbol.")
        return self


class ETradePlaceOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: ETradeEnvironment
    preview_id: str = Field(min_length=1, max_length=64)


class ETradeCancelOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: ETradeEnvironment
    account_key: str = Field(min_length=1, max_length=128)
    order_id: int = Field(gt=0)


def _get_gateway(request: Request) -> ETradeGateway:
    gateway = getattr(request.app.state, "etrade_gateway", None)
    if isinstance(gateway, ETradeGateway):
        return gateway
    raise HTTPException(status_code=500, detail="E*TRADE gateway is not initialized.")


def _handle_etrade_error(exc: Exception) -> None:
    if isinstance(exc, ETradeNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, ETradeRateLimitError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(exc, ETradeValidationError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, ETradeInactiveSessionError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, ETradeSessionExpiredError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, ETradeBrokerAuthError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, ETradeApiError):
        status_code = exc.status_code if exc.status_code in {400, 401, 403, 409, 429, 503} else 502
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    if isinstance(exc, ETradeError):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


def _split_symbols(value: str) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").split(","):
        symbol = str(raw).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def _require_provider_callback_url(request: Request) -> str:
    callback_url = get_settings(request).get_provider_callback_url("etrade")
    if callback_url:
        return callback_url
    raise HTTPException(
        status_code=503,
        detail="E*TRADE callback URL is not configured. Set ETRADE_CALLBACK_URL or API_PUBLIC_BASE_URL.",
    )


@router.post("/connect/start")
def etrade_connect_start(
    payload: ETradeConnectStartRequest,
    request: Request,
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    del request, auth_context
    try:
        response = gateway.start_connect(environment=payload.environment)
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.post("/connect/complete")
def etrade_connect_complete(
    payload: ETradeConnectCompleteRequest,
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    del auth_context
    try:
        response = gateway.complete_connect(environment=payload.environment, verifier=payload.verifier)
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/connect/callback")
def etrade_connect_callback(
    oauth_token: str = Query(..., min_length=1),
    oauth_verifier: str = Query(..., min_length=1),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    try:
        response = gateway.complete_connect_from_callback(request_token=oauth_token, verifier=oauth_verifier)
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/connect/callback-url", response_model=ProviderCallbackUrlResponse)
def etrade_connect_callback_url(
    request: Request,
    _auth_context: AuthContext = Depends(validate_auth),
) -> JSONResponse:
    payload = ProviderCallbackUrlResponse(callback_url=_require_provider_callback_url(request))
    return JSONResponse(payload.model_dump(mode="json"), headers={"Cache-Control": "no-store"})


@router.get("/session")
def etrade_session(
    environment: Optional[ETradeEnvironment] = Query(default=None),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    del auth_context
    try:
        response = gateway.get_session_state(environment=environment)
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.post("/disconnect")
def etrade_disconnect(
    payload: ETradeDisconnectRequest,
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    del auth_context
    try:
        response = gateway.disconnect(environment=payload.environment)
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/accounts", responses={204: {"description": "No Content"}})
def etrade_accounts(
    environment: ETradeEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> Response:
    try:
        response = gateway.list_accounts(environment=environment, subject=auth_context.subject)
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    if response is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/accounts/{account_key}/balance")
def etrade_balance(
    account_key: str,
    environment: ETradeEnvironment = Query(...),
    account_type: Optional[str] = Query(default=None),
    real_time_nav: bool = Query(default=False),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    try:
        response = gateway.get_balance(
            environment=environment,
            account_key=account_key,
            subject=auth_context.subject,
            account_type=account_type,
            real_time_nav=real_time_nav,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/accounts/{account_key}/portfolio", responses={204: {"description": "No Content"}})
def etrade_portfolio(
    account_key: str,
    environment: ETradeEnvironment = Query(...),
    count: Optional[int] = Query(default=None, ge=1, le=50),
    page_number: Optional[int] = Query(default=None, ge=1, alias="pageNumber"),
    sort_by: Optional[str] = Query(default=None, alias="sortBy"),
    sort_order: Optional[str] = Query(default=None, alias="sortOrder"),
    market_session: Optional[str] = Query(default=None, alias="marketSession"),
    totals_required: bool = Query(default=False, alias="totalsRequired"),
    lots_required: bool = Query(default=False, alias="lotsRequired"),
    view: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> Response:
    try:
        response = gateway.get_portfolio(
            environment=environment,
            account_key=account_key,
            subject=auth_context.subject,
            count=count,
            page_number=page_number,
            sort_by=sort_by,
            sort_order=sort_order,
            market_session=market_session,
            totals_required=totals_required,
            lots_required=lots_required,
            view=view,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    if response is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/accounts/{account_key}/transactions", responses={204: {"description": "No Content"}})
def etrade_transactions(
    account_key: str,
    environment: ETradeEnvironment = Query(...),
    start_date: Optional[str] = Query(default=None, alias="startDate"),
    end_date: Optional[str] = Query(default=None, alias="endDate"),
    sort_order: Optional[str] = Query(default=None, alias="sortOrder"),
    marker: Optional[str] = Query(default=None),
    count: Optional[int] = Query(default=None, ge=1, le=50),
    transaction_group: Optional[str] = Query(default=None, alias="transactionGroup"),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> Response:
    try:
        response = gateway.list_transactions(
            environment=environment,
            account_key=account_key,
            subject=auth_context.subject,
            start_date=start_date,
            end_date=end_date,
            sort_order=sort_order,
            marker=marker,
            count=count,
            transaction_group=transaction_group,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    if response is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/accounts/{account_key}/transactions/{transaction_id}", responses={204: {"description": "No Content"}})
def etrade_transaction_details(
    account_key: str,
    transaction_id: str,
    environment: ETradeEnvironment = Query(...),
    store_id: Optional[str] = Query(default=None, alias="storeId"),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> Response:
    try:
        response = gateway.get_transaction_details(
            environment=environment,
            account_key=account_key,
            transaction_id=transaction_id,
            subject=auth_context.subject,
            store_id=store_id,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    if response is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/quotes")
def etrade_quotes(
    environment: ETradeEnvironment = Query(...),
    symbols: str = Query(..., description="Comma-separated E*TRADE symbols or option symbols."),
    detail_flag: Optional[str] = Query(default=None, alias="detailFlag"),
    require_earnings_date: bool = Query(default=False, alias="requireEarningsDate"),
    override_symbol_count: bool = Query(default=False, alias="overrideSymbolCount"),
    skip_mini_options_check: bool = Query(default=False, alias="skipMiniOptionsCheck"),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    parsed_symbols = _split_symbols(symbols)
    if not parsed_symbols:
        raise HTTPException(status_code=400, detail="symbols is required.")
    try:
        response = gateway.get_quotes(
            environment=environment,
            symbols=parsed_symbols,
            subject=auth_context.subject,
            detail_flag=detail_flag,
            require_earnings_date=require_earnings_date,
            override_symbol_count=override_symbol_count,
            skip_mini_options_check=skip_mini_options_check,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.get("/orders")
def etrade_orders(
    environment: ETradeEnvironment = Query(...),
    account_key: str = Query(..., min_length=1, alias="account_key"),
    count: Optional[int] = Query(default=None, ge=1, le=100),
    marker: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    from_date: Optional[str] = Query(default=None, alias="fromDate"),
    to_date: Optional[str] = Query(default=None, alias="toDate"),
    symbol: Optional[str] = Query(default=None),
    security_type: Optional[str] = Query(default=None, alias="securityType"),
    transaction_type: Optional[str] = Query(default=None, alias="transactionType"),
    market_session: Optional[str] = Query(default=None, alias="marketSession"),
    auth_context: AuthContext = Depends(require_etrade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    try:
        response = gateway.list_orders(
            environment=environment,
            account_key=account_key,
            subject=auth_context.subject,
            count=count,
            marker=marker,
            status=status,
            from_date=from_date,
            to_date=to_date,
            symbol=symbol,
            security_type=security_type,
            transaction_type=transaction_type,
            market_session=market_session,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.post("/orders/preview")
def etrade_preview_order(
    payload: ETradeOrderPreviewRequest,
    auth_context: AuthContext = Depends(require_etrade_trade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    try:
        response = gateway.preview_order(
            environment=payload.environment,
            order=payload.model_dump(mode="python"),
            subject=auth_context.subject,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.post("/orders/place")
def etrade_place_order(
    payload: ETradePlaceOrderRequest,
    auth_context: AuthContext = Depends(require_etrade_trade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    try:
        response = gateway.place_order(
            environment=payload.environment,
            preview_id=payload.preview_id,
            subject=auth_context.subject,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})


@router.post("/orders/cancel")
def etrade_cancel_order(
    payload: ETradeCancelOrderRequest,
    auth_context: AuthContext = Depends(require_etrade_trade_access),
    gateway: ETradeGateway = Depends(_get_gateway),
) -> JSONResponse:
    try:
        response = gateway.cancel_order(
            environment=payload.environment,
            account_key=payload.account_key,
            order_id=payload.order_id,
            subject=auth_context.subject,
        )
    except Exception as exc:
        _handle_etrade_error(exc)
        raise
    return JSONResponse(response, headers={"Cache-Control": "no-store"})
