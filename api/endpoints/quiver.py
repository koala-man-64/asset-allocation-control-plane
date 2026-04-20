from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from api.service.dependencies import require_quiver_access
from api.service.quiver_gateway import QuiverGateway, quiver_caller_context
from quiver_provider.errors import (
    QuiverAuthError,
    QuiverEntitlementError,
    QuiverInvalidRequestError,
    QuiverNotConfiguredError,
    QuiverNotFoundError,
    QuiverProtocolError,
    QuiverRateLimitError,
    QuiverTimeoutError,
    QuiverUnavailableError,
)

logger = logging.getLogger("asset-allocation.api.quiver")

router = APIRouter()
_MAX_PAGE = 10_000
_MAX_PAGE_SIZE = 500


def _parse_iso_date(value: Optional[str], *, field_name: str) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}={value!r} (expected YYYY-MM-DD).") from exc
    return parsed.isoformat()


def _parse_ticker(value: Optional[str], *, field_name: str = "ticker") -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if not text:
        raise HTTPException(status_code=400, detail=f"{field_name} is required.")
    return text


def _parse_strict_bool(value: Optional[str], *, field_name: str) -> Optional[bool]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise HTTPException(status_code=400, detail=f"{field_name} must be 'true' or 'false'.")


def _bounded_page(value: Optional[int], *, field_name: str) -> Optional[int]:
    if value is None:
        return None
    if value < 1 or value > _MAX_PAGE:
        raise HTTPException(status_code=400, detail=f"{field_name} must be between 1 and {_MAX_PAGE}.")
    return value


def _bounded_page_size(value: Optional[int], *, field_name: str = "page_size") -> Optional[int]:
    if value is None:
        return None
    if value < 1 or value > _MAX_PAGE_SIZE:
        raise HTTPException(status_code=400, detail=f"{field_name} must be between 1 and {_MAX_PAGE_SIZE}.")
    return value


def _json_response(payload) -> JSONResponse:
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


def _get_gateway(request: Request) -> QuiverGateway:
    gateway = getattr(request.app.state, "quiver_gateway", None)
    if isinstance(gateway, QuiverGateway):
        return gateway
    raise HTTPException(status_code=500, detail="Quiver gateway is not initialized.")


def _handle_quiver_error(exc: Exception) -> None:
    if isinstance(exc, HTTPException):
        raise exc
    if isinstance(exc, QuiverNotConfiguredError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if isinstance(exc, QuiverInvalidRequestError):
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if isinstance(exc, QuiverRateLimitError):
        raise HTTPException(status_code=429, detail=str(exc)) from exc
    if isinstance(exc, QuiverNotFoundError):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, QuiverTimeoutError):
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    if isinstance(exc, (QuiverAuthError, QuiverEntitlementError, QuiverProtocolError)):
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if isinstance(exc, QuiverUnavailableError):
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=f"Unexpected error: {type(exc).__name__}: {exc}") from exc


@router.get("/live/congress-trading")
def get_live_congress_trading(
    request: Request,
    normalized: Optional[str] = Query(default=None),
    representative: Optional[str] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_congress_trading(
                normalized=_parse_strict_bool(normalized, field_name="normalized"),
                representative=str(representative or "").strip() or None,
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/historical/congress-trading/{ticker}")
def get_historical_congress_trading(
    request: Request,
    ticker: str,
    analyst: Optional[str] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_historical_congress_trading(
                ticker=_parse_ticker(ticker),
                analyst=str(analyst or "").strip() or None,
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/senate-trading")
def get_live_senate_trading(
    request: Request,
    name: Optional[str] = Query(default=None),
    options: Optional[str] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_senate_trading(
                name=str(name or "").strip() or None,
                options=_parse_strict_bool(options, field_name="options"),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/historical/senate-trading/{ticker}")
def get_historical_senate_trading(
    request: Request,
    ticker: str,
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_historical_senate_trading(ticker=_parse_ticker(ticker))
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/house-trading")
def get_live_house_trading(
    request: Request,
    name: Optional[str] = Query(default=None),
    options: Optional[str] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_house_trading(
                name=str(name or "").strip() or None,
                options=_parse_strict_bool(options, field_name="options"),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/historical/house-trading/{ticker}")
def get_historical_house_trading(
    request: Request,
    ticker: str,
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_historical_house_trading(ticker=_parse_ticker(ticker))
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/gov-contracts")
def get_live_gov_contracts(
    request: Request,
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_gov_contracts()
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/historical/gov-contracts/{ticker}")
def get_historical_gov_contracts(
    request: Request,
    ticker: str,
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_historical_gov_contracts(ticker=_parse_ticker(ticker))
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/gov-contracts-all")
def get_live_gov_contracts_all(
    request: Request,
    date_value: Optional[str] = Query(default=None, alias="date"),
    page: Optional[int] = Query(default=None),
    page_size: Optional[int] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_gov_contracts_all(
                date=_parse_iso_date(date_value, field_name="date"),
                page=_bounded_page(page, field_name="page"),
                page_size=_bounded_page_size(page_size),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/historical/gov-contracts-all/{ticker}")
def get_historical_gov_contracts_all(
    request: Request,
    ticker: str,
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_historical_gov_contracts_all(ticker=_parse_ticker(ticker))
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/insiders")
def get_live_insiders(
    request: Request,
    ticker: Optional[str] = Query(default=None),
    date_value: Optional[str] = Query(default=None, alias="date"),
    uploaded: Optional[str] = Query(default=None),
    limit_codes: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=None),
    page_size: Optional[int] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_insiders(
                ticker=_parse_ticker(ticker) if ticker is not None else None,
                date=_parse_iso_date(date_value, field_name="date"),
                uploaded=_parse_iso_date(uploaded, field_name="uploaded"),
                limit_codes=_parse_strict_bool(limit_codes, field_name="limit_codes"),
                page=_bounded_page(page, field_name="page"),
                page_size=_bounded_page_size(page_size),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/sec13f")
def get_live_sec13f(
    request: Request,
    ticker: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    date_value: Optional[str] = Query(default=None, alias="date"),
    period: Optional[str] = Query(default=None),
    today: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=None),
    page_size: Optional[int] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_sec13f(
                ticker=_parse_ticker(ticker) if ticker is not None else None,
                owner=str(owner or "").strip() or None,
                date=_parse_iso_date(date_value, field_name="date"),
                period=_parse_iso_date(period, field_name="period"),
                today=_parse_strict_bool(today, field_name="today"),
                page=_bounded_page(page, field_name="page"),
                page_size=_bounded_page_size(page_size),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/sec13f-changes")
def get_live_sec13f_changes(
    request: Request,
    ticker: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    date_value: Optional[str] = Query(default=None, alias="date"),
    period: Optional[str] = Query(default=None),
    today: Optional[str] = Query(default=None),
    most_recent: Optional[str] = Query(default=None),
    show_new_funds: Optional[str] = Query(default=None),
    mobile: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=None),
    page_size: Optional[int] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_sec13f_changes(
                ticker=_parse_ticker(ticker) if ticker is not None else None,
                owner=str(owner or "").strip() or None,
                date=_parse_iso_date(date_value, field_name="date"),
                period=_parse_iso_date(period, field_name="period"),
                today=_parse_strict_bool(today, field_name="today"),
                most_recent=_parse_strict_bool(most_recent, field_name="most_recent"),
                show_new_funds=_parse_strict_bool(show_new_funds, field_name="show_new_funds"),
                mobile=_parse_strict_bool(mobile, field_name="mobile"),
                page=_bounded_page(page, field_name="page"),
                page_size=_bounded_page_size(page_size),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/lobbying")
def get_live_lobbying(
    request: Request,
    all_records: Optional[str] = Query(default=None, alias="all"),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    page: Optional[int] = Query(default=None),
    page_size: Optional[int] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    parsed_from = _parse_iso_date(date_from, field_name="date_from")
    parsed_to = _parse_iso_date(date_to, field_name="date_to")
    if parsed_from and parsed_to and parsed_from > parsed_to:
        raise HTTPException(status_code=400, detail="'date_from' must be <= 'date_to'.")
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_lobbying(
                all_records=_parse_strict_bool(all_records, field_name="all"),
                date_from=parsed_from,
                date_to=parsed_to,
                page=_bounded_page(page, field_name="page"),
                page_size=_bounded_page_size(page_size),
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/historical/lobbying/{ticker}")
def get_historical_lobbying(
    request: Request,
    ticker: str,
    page: Optional[int] = Query(default=None),
    page_size: Optional[int] = Query(default=None),
    query: Optional[str] = Query(default=None),
    query_ticker: Optional[str] = Query(default=None, alias="queryTicker"),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_historical_lobbying(
                ticker=_parse_ticker(ticker),
                page=_bounded_page(page, field_name="page"),
                page_size=_bounded_page_size(page_size),
                query=str(query or "").strip() or None,
                query_ticker=_parse_ticker(query_ticker, field_name="queryTicker") if query_ticker is not None else None,
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/etf-holdings")
def get_live_etf_holdings(
    request: Request,
    etf: Optional[str] = Query(default=None),
    ticker: Optional[str] = Query(default=None),
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_etf_holdings(
                etf=_parse_ticker(etf, field_name="etf") if etf is not None else None,
                ticker=_parse_ticker(ticker) if ticker is not None else None,
            )
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)


@router.get("/live/congress-holdings")
def get_live_congress_holdings(
    request: Request,
    gateway: QuiverGateway = Depends(_get_gateway),
) -> JSONResponse:
    require_quiver_access(request)
    try:
        with quiver_caller_context(
            caller_job=request.headers.get("X-Caller-Job"),
            caller_execution=request.headers.get("X-Caller-Execution"),
        ):
            payload = gateway.get_live_congress_holdings()
    except Exception as exc:
        _handle_quiver_error(exc)
        raise
    return _json_response(payload)
