import logging
from typing import Any, Dict
from fastapi import HTTPException, Request
from monitoring.ttl_cache import TtlCache

from api.service.auth import AuthContext, AuthManager
from api.service.etrade_gateway import ETradeGateway
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


def get_etrade_gateway(request: Request) -> ETradeGateway:
    return request.app.state.etrade_gateway


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


def _require_configured_roles(
    *,
    request: Request,
    auth_context: AuthContext,
    required_roles: list[str],
    log_prefix: str,
) -> None:
    if auth_context.mode == "anonymous":
        return
    missing = sorted(
        role
        for role in {role.strip() for role in required_roles if role.strip()}
        if role not in _claim_roles(auth_context.claims if isinstance(auth_context.claims, dict) else {})
    )
    if missing:
        logger.warning(
            "%s authz failed: subject=%s missing_roles=%s path=%s",
            log_prefix,
            auth_context.subject or "-",
            missing,
            request.url.path,
        )
        raise HTTPException(status_code=403, detail=f"Missing required roles: {', '.join(missing)}.")


def require_ai_relay_access(request: Request) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request)
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.ai_relay.required_roles,
        log_prefix="AI relay",
    )
    return auth_context


def require_quiver_access(request: Request, *, require_enabled: bool = True) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).quiver
    if require_enabled and not settings.enabled:
        raise HTTPException(status_code=503, detail="Quiver integration is disabled.")

    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.required_roles,
        log_prefix="Quiver",
    )
    return auth_context


def require_etrade_access(request: Request, *, require_enabled: bool = True) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).etrade
    if require_enabled and not settings.enabled:
        raise HTTPException(status_code=503, detail="E*TRADE integration is disabled.")

    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.required_roles,
        log_prefix="E*TRADE",
    )
    return auth_context


def require_etrade_trade_access(request: Request) -> AuthContext:
    auth_context = require_etrade_access(request)
    settings = get_settings(request).etrade
    if not settings.trading_enabled:
        raise HTTPException(status_code=503, detail="E*TRADE trading is disabled.")

    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.trading_required_roles,
        log_prefix="E*TRADE trade",
    )
    return auth_context


def require_symbol_enrichment_job_access(request: Request, *, require_enabled: bool = True) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).symbol_enrichment
    if require_enabled and not settings.enabled:
        raise HTTPException(status_code=503, detail="Symbol enrichment is disabled.")

    caller_job = str(request.headers.get("X-Caller-Job") or "").strip()
    if not caller_job:
        raise HTTPException(status_code=400, detail="X-Caller-Job header is required.")

    allowed_jobs = {job.strip() for job in settings.allowed_jobs if job.strip()}
    if caller_job not in allowed_jobs:
        logger.warning(
            "Symbol enrichment authz failed: subject=%s caller_job=%s path=%s",
            auth_context.subject or "-",
            caller_job,
            request.url.path,
        )
        raise HTTPException(status_code=403, detail="Caller job is not allowed to use symbol enrichment.")
    return auth_context


def require_intraday_operator_access(request: Request, *, require_enabled: bool = True) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).intraday_monitor
    if require_enabled and not settings.enabled:
        raise HTTPException(status_code=503, detail="Intraday monitoring is disabled.")
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.operator_required_roles,
        log_prefix="Intraday operator",
    )
    return auth_context


def require_intraday_monitor_job_access(request: Request, *, require_enabled: bool = True) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).intraday_monitor
    if require_enabled and not settings.enabled:
        raise HTTPException(status_code=503, detail="Intraday monitoring is disabled.")

    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.jobs_required_roles,
        log_prefix="Intraday job",
    )

    caller_job = str(request.headers.get("X-Caller-Job") or "").strip()
    if not caller_job:
        raise HTTPException(status_code=400, detail="X-Caller-Job header is required.")

    allowed_jobs = {job.strip() for job in settings.allowed_jobs if job.strip()}
    if caller_job not in allowed_jobs:
        logger.warning(
            "Intraday job authz failed: subject=%s caller_job=%s path=%s",
            auth_context.subject or "-",
            caller_job,
            request.url.path,
        )
        raise HTTPException(status_code=403, detail="Caller job is not allowed to use intraday monitoring.")
    return auth_context

