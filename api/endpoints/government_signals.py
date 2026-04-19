from __future__ import annotations

import re
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Path, Query, Request

from asset_allocation_contracts.government_signals import (
    CongressTradeEventListResponse,
    GovernmentContractEventListResponse,
    GovernmentSignalAlertListResponse,
    GovernmentSignalIssuerSummaryResponse,
    GovernmentSignalMappingOverrideRequest,
    GovernmentSignalMappingOverrideResponse,
    GovernmentSignalMappingReviewResponse,
    GovernmentSignalPortfolioExposureRequest,
    GovernmentSignalPortfolioExposureResponse,
)
from api.service.dependencies import get_settings, validate_auth
from core.government_signals_repository import GovernmentSignalsRepository

router = APIRouter()
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9._-]{0,15}$")


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized or not _SYMBOL_RE.fullmatch(normalized):
        raise HTTPException(status_code=400, detail=f"Invalid symbol {symbol!r}.")
    return normalized


def _get_repo(request: Request) -> GovernmentSignalsRepository:
    dsn = str(get_settings(request).postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for government signals features.")
    return GovernmentSignalsRepository(dsn)


def _actor(request: Request) -> Optional[str]:
    auth = getattr(request.app.state, "auth", None)
    if auth is None:
        return None
    context = auth.authenticate_headers(dict(request.headers))
    if context.subject:
        return str(context.subject).strip() or None
    for key in ("preferred_username", "email", "upn"):
        value = context.claims.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


@router.get("/events/congress", response_model=CongressTradeEventListResponse)
def list_congress_events(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    member_id: Optional[str] = Query(default=None),
    chamber: Optional[str] = Query(default=None),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to_date: Optional[date] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> CongressTradeEventListResponse:
    validate_auth(request)
    repo = _get_repo(request)
    try:
        return repo.list_congress_events(
            symbol=symbol,
            member_id=member_id,
            chamber=chamber,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/events/contracts", response_model=GovernmentContractEventListResponse)
def list_contract_events(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    awarding_agency: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    from_date: Optional[date] = Query(default=None, alias="from"),
    to_date: Optional[date] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> GovernmentContractEventListResponse:
    validate_auth(request)
    repo = _get_repo(request)
    try:
        return repo.list_contract_events(
            symbol=symbol,
            awarding_agency=awarding_agency,
            event_type=event_type,
            from_date=from_date,
            to_date=to_date,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/issuers/{symbol}/summary", response_model=GovernmentSignalIssuerSummaryResponse)
def get_issuer_summary(
    request: Request,
    symbol: str = Path(..., min_length=1, max_length=32),
    as_of_date: Optional[date] = Query(default=None),
    recent_limit: int = Query(default=20, ge=1, le=100),
) -> GovernmentSignalIssuerSummaryResponse:
    validate_auth(request)
    repo = _get_repo(request)
    try:
        return repo.get_issuer_summary(
            symbol=_normalize_symbol(symbol),
            as_of_date=as_of_date,
            recent_limit=recent_limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/alerts", response_model=GovernmentSignalAlertListResponse)
def list_alerts(
    request: Request,
    symbol: Optional[str] = Query(default=None),
    severity: Optional[str] = Query(default=None),
    as_of_date: Optional[date] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> GovernmentSignalAlertListResponse:
    validate_auth(request)
    repo = _get_repo(request)
    try:
        return repo.list_alerts(
            symbol=symbol,
            severity=severity,
            as_of_date=as_of_date,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/entity-mappings/review", response_model=GovernmentSignalMappingReviewResponse)
def list_mapping_review(
    request: Request,
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> GovernmentSignalMappingReviewResponse:
    validate_auth(request)
    repo = _get_repo(request)
    return repo.list_mapping_review(status=status, limit=limit, offset=offset)


@router.post(
    "/entity-mappings/{mapping_id}/override",
    response_model=GovernmentSignalMappingOverrideResponse,
)
def apply_mapping_override(
    request: Request,
    payload: GovernmentSignalMappingOverrideRequest,
    mapping_id: str = Path(..., min_length=1, max_length=128),
) -> GovernmentSignalMappingOverrideResponse:
    validate_auth(request)
    repo = _get_repo(request)
    try:
        return repo.apply_mapping_override(
            mapping_id=mapping_id,
            request=payload,
            actor=_actor(request),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/portfolio/exposure", response_model=GovernmentSignalPortfolioExposureResponse)
def build_portfolio_exposure(
    request: Request,
    payload: GovernmentSignalPortfolioExposureRequest,
) -> GovernmentSignalPortfolioExposureResponse:
    validate_auth(request)
    repo = _get_repo(request)
    try:
        return repo.build_portfolio_exposure(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
