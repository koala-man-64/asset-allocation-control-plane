from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from alpha_vantage import AlphaVantageError, AlphaVantageInvalidSymbolError, AlphaVantageThrottleError
from api.service.alpha_vantage_gateway import (
    AlphaVantageGateway,
    AlphaVantageNotConfiguredError,
    normalize_earnings_calendar_horizon,
    alpha_vantage_caller_context,
)
from api.service.dependencies import validate_auth

logger = logging.getLogger("asset-allocation.api.alpha_vantage")

router = APIRouter()


def _parse_iso_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date={value!r} (expected YYYY-MM-DD).") from exc
    return parsed.isoformat()


def _get_gateway(request: Request) -> AlphaVantageGateway:
    gateway = getattr(request.app.state, "alpha_vantage_gateway", None)
    if isinstance(gateway, AlphaVantageGateway):
        return gateway
    raise HTTPException(status_code=500, detail="Alpha Vantage gateway is not initialized.")


def _handle_alpha_vantage_error(exc: Exception) -> None:
    if isinstance(exc, AlphaVantageNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, AlphaVantageThrottleError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(exc, AlphaVantageInvalidSymbolError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, AlphaVantageError):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


def _caller_context(request: Request):
    return alpha_vantage_caller_context(
        caller_job=request.headers.get("X-Caller-Job"),
        caller_execution=request.headers.get("X-Caller-Execution"),
    )


@router.get("/listing-status")
def get_listing_status(
    request: Request,
    state: str = Query(default="active", description="active|delisted"),
    date: Optional[str] = Query(default=None, description="Optional snapshot date (YYYY-MM-DD)."),
    gateway: AlphaVantageGateway = Depends(_get_gateway),
) -> Response:
    validate_auth(request)
    parsed_date = _parse_iso_date(date)
    normalized_state = (state or "").strip().lower() or "active"
    if normalized_state not in {"active", "delisted"}:
        raise HTTPException(status_code=400, detail="state must be 'active' or 'delisted'.")
    try:
        with _caller_context(request):
            csv_text = gateway.get_listing_status_csv(state=normalized_state, date=parsed_date)
    except Exception as exc:
        _handle_alpha_vantage_error(exc)
        raise
    return Response(content=csv_text, media_type="text/csv", headers={"Cache-Control": "no-store"})


@router.get("/time-series/daily")
def get_daily_time_series(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    outputsize: str = Query(default="compact", description="compact|full"),
    adjusted: bool = Query(default=False, description="When true, uses TIME_SERIES_DAILY_ADJUSTED."),
    gateway: AlphaVantageGateway = Depends(_get_gateway),
) -> Response:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")
    out = (outputsize or "").strip().lower() or "compact"
    if out not in {"compact", "full"}:
        raise HTTPException(status_code=400, detail="outputsize must be 'compact' or 'full'.")
    try:
        with _caller_context(request):
            csv_text = gateway.get_daily_time_series_csv(symbol=sym, outputsize=out, adjusted=bool(adjusted))
    except Exception as exc:
        _handle_alpha_vantage_error(exc)
        raise
    return Response(content=csv_text, media_type="text/csv", headers={"Cache-Control": "no-store"})


@router.get("/earnings")
def get_earnings(
    request: Request,
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)."),
    gateway: AlphaVantageGateway = Depends(_get_gateway),
) -> JSONResponse:
    validate_auth(request)
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=400, detail="symbol is required.")
    try:
        with _caller_context(request):
            payload = gateway.get_earnings(symbol=sym)
    except Exception as exc:
        _handle_alpha_vantage_error(exc)
        raise
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/earnings-calendar")
def get_earnings_calendar(
    request: Request,
    symbol: Optional[str] = Query(default=None, description="Optional ticker symbol filter (e.g. AAPL)."),
    horizon: str = Query(default="12month", description="3month|6month|12month"),
    gateway: AlphaVantageGateway = Depends(_get_gateway),
) -> Response:
    validate_auth(request)
    sym = str(symbol or "").strip().upper() or None
    try:
        normalized_horizon = normalize_earnings_calendar_horizon(horizon)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        with _caller_context(request):
            csv_text = gateway.get_earnings_calendar_csv(symbol=sym, horizon=normalized_horizon)
    except Exception as exc:
        _handle_alpha_vantage_error(exc)
        raise
    return Response(content=csv_text, media_type="text/csv", headers={"Cache-Control": "no-store"})

