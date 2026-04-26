from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from api.service.auth import AuthContext
from api.service.dependencies import require_kalshi_access, require_kalshi_trade_access
from api.service.kalshi_gateway import KalshiGateway
from kalshi import (
    KalshiAmbiguousWriteError,
    KalshiAuthError,
    KalshiConflictError,
    KalshiError,
    KalshiInvalidResponseError,
    KalshiNetworkError,
    KalshiNotConfiguredError,
    KalshiNotFoundError,
    KalshiPermissionError,
    KalshiRateLimitError,
    KalshiServerError,
    KalshiTimeoutError,
    KalshiValidationError,
    serialize_payload,
)

logger = logging.getLogger("asset-allocation.api.kalshi")
router = APIRouter()

KalshiEnvironment = Literal["demo", "live"]

_COUNT_QUANTUM = Decimal("0.01")
_PRICE_QUANTUM = Decimal("0.0001")


def _decimal_places(value: Decimal) -> int:
    exponent = value.as_tuple().exponent
    return 0 if exponent >= 0 else -exponent


def _format_decimal(value: Decimal, quantum: Decimal, places: int) -> str:
    return format(value.quantize(quantum), f".{places}f")


def _normalize_optional_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_ticker(value: str) -> str:
    return str(value).strip().upper()


class _KalshiOrderCommon(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: KalshiEnvironment
    ticker: str = Field(min_length=1, max_length=128)
    side: Literal["yes", "no"]
    action: Literal["buy", "sell"]
    client_order_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    count: Optional[int] = Field(default=None, ge=1)
    count_fp: Optional[Decimal] = Field(default=None, gt=0)
    yes_price: Optional[int] = Field(default=None, ge=1, le=99)
    no_price: Optional[int] = Field(default=None, ge=1, le=99)
    yes_price_dollars: Optional[Decimal] = Field(default=None, gt=0, lt=1)
    no_price_dollars: Optional[Decimal] = Field(default=None, gt=0, lt=1)
    time_in_force: Optional[Literal["fill_or_kill", "good_till_canceled", "immediate_or_cancel"]] = None
    subaccount: int = Field(default=0, ge=0)

    @field_validator("ticker")
    @classmethod
    def _normalize_ticker_value(cls, value: str) -> str:
        return _normalize_ticker(value)

    @field_validator("client_order_id")
    @classmethod
    def _normalize_client_order_id(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_optional_str(value)

    @field_validator("count_fp")
    @classmethod
    def _validate_count_fp_precision(cls, value: Optional[Decimal]) -> Optional[Decimal]:
        if value is not None and _decimal_places(value) > 2:
            raise ValueError("count_fp must have at most 2 decimal places.")
        return value

    @field_validator("yes_price_dollars", "no_price_dollars")
    @classmethod
    def _validate_price_precision(cls, value: Optional[Decimal]) -> Optional[Decimal]:
        if value is not None and _decimal_places(value) > 4:
            raise ValueError("price_dollars fields must have at most 4 decimal places.")
        return value

    def _validate_count_pair(self) -> None:
        if self.count is None and self.count_fp is None:
            raise ValueError("count or count_fp is required.")
        if self.count is not None and self.count_fp is not None and self.count_fp != Decimal(self.count):
            raise ValueError("count and count_fp must match when both are provided.")

    def _validate_price_fields(self) -> None:
        price_fields = [
            self.yes_price,
            self.no_price,
            self.yes_price_dollars,
            self.no_price_dollars,
        ]
        populated_count = sum(1 for item in price_fields if item is not None)
        if populated_count != 1:
            raise ValueError(
                "Exactly one of yes_price, no_price, yes_price_dollars, and no_price_dollars must be provided."
            )

    def _apply_common_order_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.client_order_id is not None:
            payload["client_order_id"] = self.client_order_id
        if self.count is not None:
            payload["count"] = self.count
        if self.count_fp is not None:
            payload["count_fp"] = _format_decimal(self.count_fp, _COUNT_QUANTUM, 2)
        if self.yes_price is not None:
            payload["yes_price"] = self.yes_price
        if self.no_price is not None:
            payload["no_price"] = self.no_price
        if self.yes_price_dollars is not None:
            payload["yes_price_dollars"] = _format_decimal(self.yes_price_dollars, _PRICE_QUANTUM, 4)
        if self.no_price_dollars is not None:
            payload["no_price_dollars"] = _format_decimal(self.no_price_dollars, _PRICE_QUANTUM, 4)
        if self.time_in_force is not None:
            payload["time_in_force"] = self.time_in_force
        payload["subaccount"] = self.subaccount
        return payload


class KalshiCreateOrderRequest(_KalshiOrderCommon):
    type: Literal["limit"] = "limit"
    expiration_ts: Optional[int] = None
    buy_max_cost: Optional[int] = Field(default=None, ge=1)
    post_only: Optional[bool] = None
    reduce_only: Optional[bool] = None
    sell_position_floor: Optional[int] = Field(default=None, ge=0)
    self_trade_prevention_type: Optional[Literal["taker_at_cross", "maker"]] = None
    order_group_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    cancel_order_on_pause: Optional[bool] = None

    @field_validator("order_group_id")
    @classmethod
    def _normalize_order_group_id(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_optional_str(value)

    @model_validator(mode="after")
    def _validate_order(self) -> "KalshiCreateOrderRequest":
        self._validate_count_pair()
        self._validate_price_fields()
        if self.sell_position_floor not in {None, 0}:
            raise ValueError("sell_position_floor is deprecated and only accepts 0.")
        return self

    def to_order_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ticker": self.ticker,
            "side": self.side,
            "action": self.action,
            "type": self.type,
        }
        self._apply_common_order_fields(payload)
        if self.expiration_ts is not None:
            payload["expiration_ts"] = self.expiration_ts
        if self.buy_max_cost is not None:
            payload["buy_max_cost"] = self.buy_max_cost
        if self.post_only is not None:
            payload["post_only"] = self.post_only
        if self.reduce_only is not None:
            payload["reduce_only"] = self.reduce_only
        if self.sell_position_floor is not None:
            payload["sell_position_floor"] = self.sell_position_floor
        if self.self_trade_prevention_type is not None:
            payload["self_trade_prevention_type"] = self.self_trade_prevention_type
        if self.order_group_id is not None:
            payload["order_group_id"] = self.order_group_id
        if self.cancel_order_on_pause is not None:
            payload["cancel_order_on_pause"] = self.cancel_order_on_pause
        return payload


class KalshiAmendOrderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: KalshiEnvironment
    ticker: str = Field(min_length=1, max_length=128)
    side: Literal["yes", "no"]
    action: Literal["buy", "sell"]
    subaccount: int = Field(default=0, ge=0)
    client_order_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    updated_client_order_id: Optional[str] = Field(default=None, min_length=1, max_length=128)
    count: Optional[int] = Field(default=None, ge=1)
    count_fp: Optional[Decimal] = Field(default=None, gt=0)
    yes_price: Optional[int] = Field(default=None, ge=1, le=99)
    no_price: Optional[int] = Field(default=None, ge=1, le=99)
    yes_price_dollars: Optional[Decimal] = Field(default=None, gt=0, lt=1)
    no_price_dollars: Optional[Decimal] = Field(default=None, gt=0, lt=1)

    @field_validator("ticker")
    @classmethod
    def _normalize_ticker_value(cls, value: str) -> str:
        return _normalize_ticker(value)

    @field_validator("client_order_id", "updated_client_order_id")
    @classmethod
    def _normalize_optional_ids(cls, value: Optional[str]) -> Optional[str]:
        return _normalize_optional_str(value)

    @field_validator("count_fp")
    @classmethod
    def _validate_count_fp_precision(cls, value: Optional[Decimal]) -> Optional[Decimal]:
        if value is not None and _decimal_places(value) > 2:
            raise ValueError("count_fp must have at most 2 decimal places.")
        return value

    @field_validator("yes_price_dollars", "no_price_dollars")
    @classmethod
    def _validate_price_precision(cls, value: Optional[Decimal]) -> Optional[Decimal]:
        if value is not None and _decimal_places(value) > 4:
            raise ValueError("price_dollars fields must have at most 4 decimal places.")
        return value

    @model_validator(mode="after")
    def _validate_order(self) -> "KalshiAmendOrderRequest":
        if self.count is not None and self.count_fp is not None and self.count_fp != Decimal(self.count):
            raise ValueError("count and count_fp must match when both are provided.")

        price_fields = [
            self.yes_price,
            self.no_price,
            self.yes_price_dollars,
            self.no_price_dollars,
        ]
        populated_prices = sum(1 for item in price_fields if item is not None)
        if populated_prices > 1:
            raise ValueError(
                "At most one of yes_price, no_price, yes_price_dollars, and no_price_dollars may be provided."
            )

        if (
            self.client_order_id is None
            and self.updated_client_order_id is None
            and self.count is None
            and self.count_fp is None
            and populated_prices == 0
        ):
            raise ValueError("At least one amendable field must be provided.")
        return self

    def to_order_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ticker": self.ticker,
            "side": self.side,
            "action": self.action,
            "subaccount": self.subaccount,
        }
        if self.client_order_id is not None:
            payload["client_order_id"] = self.client_order_id
        if self.updated_client_order_id is not None:
            payload["updated_client_order_id"] = self.updated_client_order_id
        if self.count is not None:
            payload["count"] = self.count
        if self.count_fp is not None:
            payload["count_fp"] = _format_decimal(self.count_fp, _COUNT_QUANTUM, 2)
        if self.yes_price is not None:
            payload["yes_price"] = self.yes_price
        if self.no_price is not None:
            payload["no_price"] = self.no_price
        if self.yes_price_dollars is not None:
            payload["yes_price_dollars"] = _format_decimal(self.yes_price_dollars, _PRICE_QUANTUM, 4)
        if self.no_price_dollars is not None:
            payload["no_price_dollars"] = _format_decimal(self.no_price_dollars, _PRICE_QUANTUM, 4)
        return payload


def _get_gateway(request: Request) -> KalshiGateway:
    gateway = getattr(request.app.state, "kalshi_gateway", None)
    if isinstance(gateway, KalshiGateway):
        return gateway
    raise HTTPException(status_code=500, detail="Kalshi gateway is not initialized.")


def _handle_kalshi_error(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, KalshiNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, KalshiValidationError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, KalshiNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, (KalshiAmbiguousWriteError, KalshiConflictError)):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if isinstance(exc, KalshiRateLimitError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(
        exc,
        (
            KalshiAuthError,
            KalshiPermissionError,
            KalshiTimeoutError,
            KalshiNetworkError,
            KalshiServerError,
            KalshiInvalidResponseError,
            KalshiError,
        ),
    ):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _encode(payload: Any) -> Any:
    return serialize_payload(payload)


@router.get("/markets")
def kalshi_markets(
    response: Response,
    environment: KalshiEnvironment = Query(...),
    limit: int = Query(default=100, ge=1, le=1000),
    cursor: Optional[str] = Query(default=None),
    event_ticker: Optional[str] = Query(default=None),
    series_ticker: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    tickers: Optional[str] = Query(default=None),
    min_close_ts: Optional[int] = Query(default=None),
    max_close_ts: Optional[int] = Query(default=None),
    min_updated_ts: Optional[int] = Query(default=None),
    mve_filter: Optional[str] = Query(default=None),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_markets(
                environment=environment,
                limit=limit,
                cursor=cursor,
                event_ticker=event_ticker,
                series_ticker=series_ticker,
                status=status,
                tickers=tickers,
                min_close_ts=min_close_ts,
                max_close_ts=max_close_ts,
                min_updated_ts=min_updated_ts,
                mve_filter=mve_filter,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/markets/{ticker}")
def kalshi_market(
    ticker: str,
    response: Response,
    environment: KalshiEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_market(environment=environment, ticker=ticker, subject=auth_context.subject))
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/markets/{ticker}/orderbook")
def kalshi_orderbook(
    ticker: str,
    response: Response,
    environment: KalshiEnvironment = Query(...),
    depth: int = Query(default=0, ge=0, le=100),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.get_orderbook(
                environment=environment,
                ticker=ticker,
                depth=depth,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/balance")
def kalshi_balance(
    response: Response,
    environment: KalshiEnvironment = Query(...),
    subaccount: int = Query(default=0, ge=0),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.get_balance(environment=environment, subaccount=subaccount, subject=auth_context.subject)
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/positions")
def kalshi_positions(
    response: Response,
    environment: KalshiEnvironment = Query(...),
    cursor: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    count_filter: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    event_ticker: Optional[str] = Query(default=None),
    subaccount: int = Query(default=0, ge=0),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_positions(
                environment=environment,
                cursor=cursor,
                limit=limit,
                count_filter=count_filter,
                ticker=ticker,
                event_ticker=event_ticker,
                subaccount=subaccount,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/orders/queue-positions")
def kalshi_queue_positions(
    response: Response,
    environment: KalshiEnvironment = Query(...),
    market_tickers: Optional[str] = Query(default=None),
    event_ticker: Optional[str] = Query(default=None),
    subaccount: int = Query(default=0, ge=0),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.get_queue_positions(
                environment=environment,
                market_tickers=market_tickers,
                event_ticker=event_ticker,
                subaccount=subaccount,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/orders/{order_id}/queue-position")
def kalshi_order_queue_position(
    order_id: str,
    response: Response,
    environment: KalshiEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.get_order_queue_position(
                environment=environment,
                order_id=order_id,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/orders/{order_id}")
def kalshi_order(
    order_id: str,
    response: Response,
    environment: KalshiEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_order(environment=environment, order_id=order_id, subject=auth_context.subject))
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/orders")
def kalshi_orders(
    response: Response,
    environment: KalshiEnvironment = Query(...),
    ticker: Optional[str] = Query(default=None),
    event_ticker: Optional[str] = Query(default=None),
    min_ts: Optional[int] = Query(default=None),
    max_ts: Optional[int] = Query(default=None),
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    cursor: Optional[str] = Query(default=None),
    subaccount: Optional[int] = Query(default=None, ge=0),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.list_orders(
                environment=environment,
                ticker=ticker,
                event_ticker=event_ticker,
                min_ts=min_ts,
                max_ts=max_ts,
                status=status,
                limit=limit,
                cursor=cursor,
                subaccount=subaccount,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.get("/account/limits")
def kalshi_account_limits(
    response: Response,
    environment: KalshiEnvironment = Query(...),
    auth_context: AuthContext = Depends(require_kalshi_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(gateway.get_account_limits(environment=environment, subject=auth_context.subject))
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.post("/orders")
def kalshi_create_order(
    payload: KalshiCreateOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_kalshi_trade_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.create_order(
                environment=payload.environment,
                order=payload.to_order_payload(),
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.post("/orders/{order_id}/amend")
def kalshi_amend_order(
    order_id: str,
    payload: KalshiAmendOrderRequest,
    response: Response,
    auth_context: AuthContext = Depends(require_kalshi_trade_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.amend_order(
                environment=payload.environment,
                order_id=order_id,
                order=payload.to_order_payload(),
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise


@router.delete("/orders/{order_id}")
def kalshi_cancel_order(
    order_id: str,
    response: Response,
    environment: KalshiEnvironment = Query(...),
    subaccount: Optional[int] = Query(default=None, ge=0),
    auth_context: AuthContext = Depends(require_kalshi_trade_access),
    gateway: KalshiGateway = Depends(_get_gateway),
) -> Any:
    _set_no_store(response)
    try:
        return _encode(
            gateway.cancel_order(
                environment=environment,
                order_id=order_id,
                subaccount=subaccount,
                subject=auth_context.subject,
            )
        )
    except Exception as exc:
        _handle_kalshi_error(exc)
        raise
