from __future__ import annotations

from typing import Any

from asset_allocation_contracts.ui_config import AuthSessionStatus
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.service.dependencies import get_settings, validate_auth


router = APIRouter()


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

    return JSONResponse(
        payload.model_dump(mode="json"),
        headers={"Cache-Control": "no-store"},
    )
