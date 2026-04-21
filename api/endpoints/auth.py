from __future__ import annotations

import logging
from typing import Any

from asset_allocation_contracts.ui_config import AuthSessionStatus
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.service.dependencies import get_settings, validate_auth


router = APIRouter()
logger = logging.getLogger("asset-allocation.api.auth.endpoint")


def _claim_text(claims: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = claims.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _claim_roles(claims: dict[str, Any]) -> list[str]:
    raw_roles = claims.get("roles") or []
    if not isinstance(raw_roles, list):
        return []
    return sorted({str(role).strip() for role in raw_roles if str(role).strip()})


@router.get("/session", response_model=AuthSessionStatus, summary="Get auth session")
async def get_auth_session(request: Request) -> JSONResponse:
    request_id = str(getattr(request.state, "request_id", "") or request.headers.get("x-request-id", "") or "-")
    logger.info(
        "Auth session route entered: request_id=%s path=%s method=%s host=%s",
        request_id,
        request.url.path,
        request.method,
        request.headers.get("host", ""),
    )
    auth_context = validate_auth(request)
    settings = get_settings(request)
    claims = auth_context.claims if isinstance(auth_context.claims, dict) else {}

    payload = AuthSessionStatus(
        authMode=auth_context.mode,
        subject=auth_context.subject or "anonymous",
        displayName=_claim_text(claims, "name"),
        username=_claim_text(claims, "preferred_username", "upn", "email"),
        requiredRoles=list(settings.oidc_required_roles),
        grantedRoles=_claim_roles(claims),
    )
    logger.info(
        "Auth session route completed: request_id=%s mode=%s subject=%s required_roles=%s granted_roles=%s",
        request_id,
        auth_context.mode,
        auth_context.subject or "anonymous",
        list(settings.oidc_required_roles),
        _claim_roles(claims),
    )

    return JSONResponse(
        payload.model_dump(mode="json"),
        headers={"Cache-Control": "no-store"},
    )
