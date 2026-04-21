from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from alpaca.errors import (
    AlpacaAmbiguousWriteError,
    AlpacaAuthError,
    AlpacaConflictError,
    AlpacaError,
    AlpacaInvalidResponseError,
    AlpacaNetworkError,
    AlpacaNotConfiguredError,
    AlpacaNotFoundError,
    AlpacaPermissionError,
    AlpacaRateLimitError,
    AlpacaServerError,
    AlpacaTimeoutError,
    AlpacaValidationError,
)
from api.service.alpaca_gateway import AlpacaGateway
from api.service.auth import AuthContext
from api.service.dependencies import require_alpaca_access, require_alpaca_trade_access

logger = logging.getLogger("asset-allocation.api.alpaca")
router = APIRouter()

AlpacaEnvironment = Literal["paper", "live"]


class AlpacaAccountResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    account_number: str
    status: str
    currency: str
    cash: float
    equity: float
    buying_power: float
    daytrade_count: int
    created_at: datetime


class AlpacaPositionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    qty: float
    market_value: float
    avg_entry_price: float
    current_price: float
    change_today: float
    unrealized_pl: float
    side: str


class AlpacaOrderResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    client_order_id: str
    symbol: str
    created_at: datetime
    updated_at: datetime
    submitted_at: datetime
    filled_at: Optional[datetime] = None
    expired_at: Optional[datetime] = None
    canceled_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    asset_id: str
    asset_class: str
    qty: float
    filled_qty: float
    type: str
    side: str
    time_in_force: str
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    status: str


class AlpacaCancelOrderResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: AlpacaEnvironment
    order_id: str
    canceled: bool


class AlpacaSubmitOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: AlpacaEnvironment
    symbol: str = Field(min_length=1, max_length=32)
    qty: Decimal = Field(gt=0)
    side: str = Field(min_length=1, max_length=32)
    type: str = Field(default="market", min_length=1, max_length=32)
    time_in_force: str = Field(default="day", min_length=1, max_length=32)
    limit_price: Optional[Decimal] = Field(default=None, gt=0)
    stop_price: Optional[Decimal] = Field(default=None, gt=0)
    client_order_id: Optional[str] = Field(default=None, min_length=1, max_length=128)

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        return str(value).strip().upper()

    @field_validator("side", "type", "time_in_force")
    @classmethod
    def _normalize_lower(cls, value: str) -> str:
        return str(value).strip().lower()

    @field_validator("client_order_id")
    @classmethod
    def _normalize_client_order_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def _validate_price_fields(self) -> "AlpacaSubmitOrderRequest":
        if self.type in {"limit", "stop_limit"} and self.limit_price is None:
            raise ValueError("limit_price is required for limit and stop_limit orders.")
        if self.type in {"stop", "stop_limit"} and self.stop_price is None:
            raise ValueError("stop_price is required for stop and stop_limit orders.")
        return self


class AlpacaReplaceOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: AlpacaEnvironment
    qty: Optional[Decimal] = Field(default=None, gt=0)
    limit_price: Optional[Decimal] = Field(default=None, gt=0)
    stop_price: Optional[Decimal] = Field(default=None, gt=0)
    client_order_id: Optional[str] = Field(default=None, min_length=1, max_length=128)

    @field_validator("client_order_id")
    @classmethod
    def _normalize_client_order_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @model_validator(mode="after")
    def _validate_update_fields(self) -> "AlpacaReplaceOrderRequest":
        if (
            self.qty is None
            and self.limit_price is None
            and self.stop_price is None
            and self.client_order_id is None
        ):
            raise ValueError("At least one order field must be provided.")
        return self


def _get_gateway(request: Request) -> AlpacaGateway:
    gateway = getattr(request.app.state, "alpaca_gateway", None)
    if isinstance(gateway, AlpacaGateway):
        return gateway
    raise HTTPException(status_code=500, detail="Alpaca gateway is not initialized.")


def _handle_alpaca_error(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, AlpacaNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, AlpacaValidationError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, AlpacaNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, (AlpacaAmbiguousWriteError, AlpacaConflictError)):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, AlpacaRateLimitError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(
        exc,
        (
            AlpacaAuthError,
            AlpacaPermissionError,
            AlpacaTimeoutError,
            AlpacaNetworkError,
            AlpacaServerError,
            AlpacaInvalidResponseError,
            AlpacaError,
        ),
    ):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


def _split_symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    symbols: list[str] = []
    seen: set[str] = set()
    for raw in str(value).split(","):
        symbol = str(raw).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols or None


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _encode(payload: Any) -> Any:
    return jsonable_encoder(payload)


@router.get("/account", response_model=AlpacaAccountResponse)
def alpaca_account(
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_alpaca_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_account(environment=environment, subject=auth_context.subject))
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.get("/positions", response_model=list[AlpacaPositionResponse])
def alpaca_positions(
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_alpaca_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.list_positions(environment=environment, subject=auth_context.subject))
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.get("/orders/by-client-order-id", response_model=AlpacaOrderResponse)
def alpaca_order_by_client_order_id(
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    client_order_id: str = Query(..., min_length=1, max_length=128),
    auth_context: AuthContext = Depends(require_alpaca_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.get_order_by_client_order_id(
                environment=environment,
                client_order_id=client_order_id,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.get("/orders/{order_id}", response_model=AlpacaOrderResponse)
def alpaca_order(
    order_id: str,
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_alpaca_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_order(environment=environment, order_id=order_id, subject=auth_context.subject))
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.get("/orders", response_model=list[AlpacaOrderResponse])
def alpaca_orders(
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    status: str = Query(default="open", min_length=1, max_length=32),
    limit: int = Query(default=500, ge=1, le=500),
    after: Optional[datetime] = Query(default=None),
    until: Optional[datetime] = Query(default=None),
    nested: bool = Query(default=False),
    symbols: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_alpaca_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_orders(
                environment=environment,
                subject=auth_context.subject,
                status=str(status).strip().lower(),
                limit=limit,
                after=after,
                until=until,
                nested=nested,
                symbols=_split_symbols(symbols),
            )
        )
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.post("/orders", response_model=AlpacaOrderResponse)
def alpaca_submit_order(
    payload: AlpacaSubmitOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_alpaca_trade_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        order_payload = payload.model_dump(mode="python", exclude={"environment"}, exclude_none=True)
        order_payload["qty"] = float(order_payload["qty"])
        if "limit_price" in order_payload:
            order_payload["limit_price"] = float(order_payload["limit_price"])
        if "stop_price" in order_payload:
            order_payload["stop_price"] = float(order_payload["stop_price"])
        return _encode(
            gateway.submit_order(
                environment=payload.environment,
                order=order_payload,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.patch("/orders/{order_id}", response_model=AlpacaOrderResponse)
def alpaca_replace_order(
    order_id: str,
    payload: AlpacaReplaceOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_alpaca_trade_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        order_payload = payload.model_dump(mode="python", exclude={"environment"}, exclude_none=True)
        if "qty" in order_payload:
            order_payload["qty"] = float(order_payload["qty"])
        if "limit_price" in order_payload:
            order_payload["limit_price"] = float(order_payload["limit_price"])
        if "stop_price" in order_payload:
            order_payload["stop_price"] = float(order_payload["stop_price"])
        return _encode(
            gateway.replace_order(
                environment=payload.environment,
                order_id=order_id,
                order=order_payload,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.delete("/orders/{order_id}", response_model=AlpacaCancelOrderResponse)
def alpaca_cancel_order(
    order_id: str,
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_alpaca_trade_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.cancel_order(
                environment=environment,
                order_id=order_id,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise


@router.delete("/orders")
def alpaca_cancel_all_orders(
    response: Response,
    environment: AlpacaEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_alpaca_trade_access),
    gateway: AlpacaGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.cancel_all_orders(environment=environment, subject=auth_context.subject))
    except Exception as exc:
        _handle_alpaca_error(exc)
        raise
