from __future__ import annotations

import logging
from typing import Any

from asset_allocation_contracts.ui_config import AuthSessionStatus, PasswordAuthSessionRequest
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response

from api.service.auth import AuthContext, AuthError
from api.service.dependencies import get_auth_manager, get_settings, require_same_origin, validate_auth


router = APIRouter()
logger = logging.getLogger("asset-allocation.api.auth.endpoint")
BREAK_GLASS_REASON_HEADER = "x-break-glass-reason"


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


def _auth_session_payload(request: Request, auth_context: AuthContext) -> AuthSessionStatus:
    settings = get_settings(request)
    claims = auth_context.claims if isinstance(auth_context.claims, dict) else {}
    required_roles = list(settings.oidc_required_roles) if auth_context.mode == "oidc" else []
    return AuthSessionStatus(
        authMode=auth_context.mode,
        subject=auth_context.subject or "anonymous",
        displayName=_claim_text(claims, "name"),
        username=_claim_text(claims, "preferred_username", "upn", "email"),
        requiredRoles=required_roles,
        grantedRoles=_claim_roles(claims),
    )


def _json_session_response(payload: AuthSessionStatus) -> JSONResponse:
    return JSONResponse(
        payload.model_dump(mode="json"),
        headers={"Cache-Control": "no-store"},
    )


def _client_ip(request: Request) -> str:
    forwarded_for = str(request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    return str(getattr(getattr(request, "client", None), "host", "") or "unknown").strip() or "unknown"


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
    payload = _auth_session_payload(request, auth_context)
    logger.info(
        "Auth session route completed: request_id=%s mode=%s source=%s subject=%s required_roles=%s granted_roles=%s",
        request_id,
        auth_context.mode,
        auth_context.source,
        auth_context.subject or "anonymous",
        list(settings.oidc_required_roles),
        _claim_roles(claims),
    )

    return _json_session_response(payload)


@router.post("/session", response_model=AuthSessionStatus, summary="Create auth session")
async def create_auth_session(request: Request) -> JSONResponse:
    request_id = str(getattr(request.state, "request_id", "") or request.headers.get("x-request-id", "") or "-")
    auth_manager = get_auth_manager(request)
    settings = get_settings(request)
    body = None
    attempt_mode = "oidc"
    content_type = str(request.headers.get("content-type") or "").lower()
    if "application/json" in content_type:
        try:
            body = await request.json()
        except Exception:
            body = None
    try:
        request_context = {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "host": request.headers.get("host", ""),
            "origin": request.headers.get("origin", ""),
            "referer": request.headers.get("referer", ""),
            "user_agent": request.headers.get("user-agent", ""),
            "client_ip": _client_ip(request),
        }
        if isinstance(body, dict) and "password" in body:
            attempt_mode = "password"
            require_same_origin(request)
            payload = PasswordAuthSessionRequest.model_validate(body)
            auth_context = auth_manager.authenticate_password(
                payload.password,
                client_ip=request_context["client_ip"],
                break_glass_reason=request.headers.get(BREAK_GLASS_REASON_HEADER),
                request_context=request_context,
            )
        else:
            auth_context = auth_manager.authenticate_bearer_headers(
                dict(request.headers),
                request_context=request_context,
            )
    except AuthError as exc:
        if attempt_mode == "oidc":
            logger.warning(
                "oidc_bootstrap_fail: request_id=%s status=%s detail=%s path=%s host=%s",
                request_id,
                exc.status_code,
                exc.detail,
                request.url.path,
                request.headers.get("host", ""),
            )
        headers: dict[str, str] = {}
        if exc.www_authenticate:
            headers["WWW-Authenticate"] = exc.www_authenticate
        raise HTTPException(status_code=exc.status_code, detail=exc.detail, headers=headers) from exc
    payload = _auth_session_payload(request, auth_context)
    response = _json_session_response(payload)
    session_bundle = None
    if settings.cookie_auth_sessions_enabled:
        session_bundle = auth_manager.issue_session_cookie(auth_context)
        auth_manager.set_session_cookies(response, session_bundle)
    if auth_context.mode == "oidc":
        logger.info(
            "oidc_bootstrap_success: request_id=%s subject=%s session_id=%s oid=%s tid=%s granted_roles=%s",
            request_id,
            auth_context.subject or "-",
            session_bundle.session_id if session_bundle is not None else "-",
            _claim_text(auth_context.claims, "oid"),
            _claim_text(auth_context.claims, "tid"),
            _claim_roles(auth_context.claims),
        )
    else:
        logger.info(
            "Auth session route created session: request_id=%s mode=%s subject=%s session_id=%s cookie_mode=%s",
            request_id,
            auth_context.mode,
            auth_context.subject or "-",
            session_bundle.session_id if session_bundle is not None else "-",
            settings.cookie_auth_sessions_enabled,
        )
    return response


@router.delete("/session", status_code=status.HTTP_204_NO_CONTENT, summary="Clear auth session")
async def delete_auth_session(request: Request) -> Response:
    auth_context = validate_auth(request)
    response = Response(status_code=status.HTTP_204_NO_CONTENT, headers={"Cache-Control": "no-store"})
    get_auth_manager(request).clear_session_cookies(response)
    logger.info(
        "session_cookie_cleared: request_id=%s path=%s subject=%s mode=%s session_id=%s",
        str(getattr(request.state, "request_id", "") or request.headers.get("x-request-id", "") or "-"),
        request.url.path,
        auth_context.subject or "-",
        auth_context.mode,
        auth_context.session_id or "-",
    )
    return response
