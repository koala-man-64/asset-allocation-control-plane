import logging
from typing import Any, Dict
from fastapi import Request
from monitoring.ttl_cache import TtlCache

from api.service.auth import AuthContext, AuthManager
from api.service.realtime_tickets import WebSocketTicketStore
from api.service.settings import ServiceSettings

logger = logging.getLogger("asset-allocation.api.auth")

def get_settings(request: Request) -> ServiceSettings:
    return request.app.state.settings




def get_auth_manager(request: Request) -> AuthManager:
    return request.app.state.auth


def get_system_health_cache(request: Request) -> TtlCache[Dict[str, Any]]:
    return request.app.state.system_health_cache


def get_websocket_ticket_store(request: Request) -> WebSocketTicketStore:
    return request.app.state.websocket_ticket_store


from fastapi import HTTPException
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

