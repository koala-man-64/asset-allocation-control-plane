import logging
from typing import Any, Dict
from fastapi import HTTPException, Request
from monitoring.ttl_cache import TtlCache

from api.service.auth import AuthContext, AuthManager
from api.service.openai_responses_gateway import OpenAIResponsesGateway
from api.service.realtime_tickets import WebSocketTicketStore
from api.service.settings import ServiceSettings

logger = logging.getLogger("asset-allocation.api.auth")

def get_settings(request: Request) -> ServiceSettings:
    return request.app.state.settings




def get_auth_manager(request: Request) -> AuthManager:
    return request.app.state.auth


def get_ai_relay_gateway(request: Request) -> OpenAIResponsesGateway:
    return request.app.state.ai_relay_gateway


def get_system_health_cache(request: Request) -> TtlCache[Dict[str, Any]]:
    return request.app.state.system_health_cache


def get_websocket_ticket_store(request: Request) -> WebSocketTicketStore:
    return request.app.state.websocket_ticket_store


from api.service.auth import AuthError


def validate_auth(request: Request) -> AuthContext:
    auth = get_auth_manager(request)

    try:
        ctx = auth.authenticate_headers(dict(request.headers))
        if ctx.mode == "anonymous":
            logger.info(
                "Auth bypassed for local/test runtime: path=%s host=%s",
                request.url.path,
                request.headers.get("host", ""),
            )
        logger.info(
            "Auth ok: mode=%s subject=%s path=%s",
            ctx.mode,
            ctx.subject or "-",
            request.url.path,
        )
        return ctx
    except AuthError as exc:
        headers: Dict[str, str] = {}
        if exc.www_authenticate:
            headers["WWW-Authenticate"] = exc.www_authenticate
        logger.warning(
            "Auth failed: status=%s detail=%s path=%s",
            exc.status_code,
            exc.detail,
            request.url.path,
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail, headers=headers) from exc


def _claim_roles(claims: dict[str, Any]) -> set[str]:
    raw_roles = claims.get("roles") or []
    if not isinstance(raw_roles, list):
        return set()
    return {str(role).strip() for role in raw_roles if str(role).strip()}


def require_ai_relay_access(request: Request) -> AuthContext:
    auth_context = validate_auth(request)
    if auth_context.mode == "anonymous":
        return auth_context

    settings = get_settings(request)
    required_roles = set(settings.ai_relay.required_roles)
    granted_roles = _claim_roles(auth_context.claims if isinstance(auth_context.claims, dict) else {})
    missing = sorted(role for role in required_roles if role not in granted_roles)
    if missing:
        logger.warning(
            "AI relay authz failed: subject=%s missing_roles=%s path=%s",
            auth_context.subject or "-",
            missing,
            request.url.path,
        )
        raise HTTPException(status_code=403, detail=f"Missing required roles: {', '.join(missing)}.")
    return auth_context

