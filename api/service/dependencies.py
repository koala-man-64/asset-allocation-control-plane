import logging
import hmac
from inspect import Parameter, signature
from typing import Any, Dict
from fastapi import HTTPException, Request
from monitoring.ttl_cache import TtlCache

from api.service.alpaca_gateway import AlpacaGateway
from api.service.auth import AuthContext, AuthManager, summarize_auth_claims_for_logs
from api.service.etrade_gateway import ETradeGateway
from api.service.openai_responses_gateway import OpenAIResponsesGateway
from api.service.realtime_tickets import WebSocketTicketStore
from api.service.schwab_gateway import SchwabGateway
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


def get_alpaca_gateway(request: Request) -> AlpacaGateway:
    return request.app.state.alpaca_gateway


def get_schwab_gateway(request: Request) -> SchwabGateway:
    return request.app.state.schwab_gateway


def get_system_health_cache(request: Request) -> TtlCache[Dict[str, Any]]:
    return request.app.state.system_health_cache


def get_websocket_ticket_store(request: Request) -> WebSocketTicketStore:
    return request.app.state.websocket_ticket_store


from api.service.auth import AuthError


def _request_id(request: Request) -> str:
    return str(
        getattr(getattr(request, "state", object()), "request_id", "")
        or request.headers.get("x-request-id", "")
        or "-"
    )


def _request_context(request: Request) -> Dict[str, str]:
    return {
        "request_id": _request_id(request),
        "method": request.method,
        "path": request.url.path,
        "host": request.headers.get("host", ""),
        "origin": request.headers.get("origin", ""),
        "referer": request.headers.get("referer", ""),
    }


def _authenticate_headers(
    auth: AuthManager,
    headers: Dict[str, str],
    *,
    request_context: Dict[str, str],
) -> AuthContext:
    try:
        parameters = signature(auth.authenticate_headers).parameters.values()
    except (TypeError, ValueError):
        parameters = ()

    supports_request_context = any(
        parameter.kind == Parameter.VAR_KEYWORD or parameter.name == "request_context"
        for parameter in parameters
    )

    if supports_request_context:
        return auth.authenticate_headers(headers, request_context=request_context)

    return auth.authenticate_headers(headers)


def _authenticate_request(
    auth: AuthManager,
    request: Request,
    *,
    request_context: Dict[str, str],
) -> AuthContext:
    authenticate_request = getattr(auth, "authenticate_request", None)
    if callable(authenticate_request):
        return authenticate_request(
            dict(request.headers),
            dict(request.cookies),
            request_context=request_context,
        )
    return _authenticate_headers(auth, dict(request.headers), request_context=request_context)


def _require_csrf_for_cookie_auth(request: Request, auth_context: AuthContext) -> None:
    if auth_context.source != "session-cookie":
        return
    if request.method.upper() in {"GET", "HEAD", "OPTIONS", "TRACE"}:
        return

    settings = get_settings(request)
    header_token = str(request.headers.get("x-csrf-token") or "").strip()
    cookie_token = str(request.cookies.get(settings.auth_session_csrf_cookie_name) or "").strip()
    expected_token = str(auth_context.csrf_token or "").strip()

    if (
        not header_token
        or not cookie_token
        or not expected_token
        or not hmac.compare_digest(header_token, expected_token)
        or not hmac.compare_digest(cookie_token, expected_token)
    ):
        logger.warning(
            "CSRF rejected for cookie-auth request: request_id=%s method=%s path=%s has_header=%s has_cookie=%s",
            _request_id(request),
            request.method,
            request.url.path,
            bool(header_token),
            bool(cookie_token),
        )
        raise HTTPException(status_code=403, detail="CSRF token is missing or invalid.")


def validate_auth(request: Request) -> AuthContext:
    auth = get_auth_manager(request)
    request_context = _request_context(request)

    try:
        ctx = _authenticate_request(auth, request, request_context=request_context)
        _require_csrf_for_cookie_auth(request, ctx)
        if ctx.session_renewal is not None:
            request.state.auth_session_renewal = ctx.session_renewal
        if ctx.mode == "anonymous":
            logger.info(
                "Auth bypassed for local/test runtime: request_id=%s path=%s host=%s",
                request_context["request_id"],
                request_context["path"],
                request_context["host"],
            )
        logger.info(
            "Auth ok: request_id=%s mode=%s subject=%s path=%s claims=%s",
            request_context["request_id"],
            ctx.mode,
            ctx.subject or "-",
            request_context["path"],
            summarize_auth_claims_for_logs(ctx.claims if isinstance(ctx.claims, dict) else {}),
        )
        return ctx
    except AuthError as exc:
        headers: Dict[str, str] = {}
        if exc.www_authenticate:
            headers["WWW-Authenticate"] = exc.www_authenticate
        logger.warning(
            "Auth failed: request_id=%s status=%s detail=%s path=%s",
            request_context["request_id"],
            exc.status_code,
            exc.detail,
            request_context["path"],
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
    granted_roles = sorted(_claim_roles(auth_context.claims if isinstance(auth_context.claims, dict) else {}))
    configured_roles = sorted({role.strip() for role in required_roles if role.strip()})
    logger.info(
        "%s authz check: request_id=%s subject=%s required_roles=%s granted_roles=%s path=%s",
        log_prefix,
        _request_id(request),
        auth_context.subject or "-",
        configured_roles,
        granted_roles,
        request.url.path,
    )
    missing = sorted(
        role
        for role in configured_roles
        if role not in granted_roles
    )
    if missing:
        logger.warning(
            "%s authz failed: request_id=%s subject=%s missing_roles=%s granted_roles=%s path=%s",
            log_prefix,
            _request_id(request),
            auth_context.subject or "-",
            missing,
            granted_roles,
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


def require_data_discovery_read_access(request: Request) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).data_discovery
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.required_roles,
        log_prefix="Data discovery read",
    )
    return auth_context


def require_data_discovery_write_access(request: Request) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).data_discovery
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.write_required_roles,
        log_prefix="Data discovery write",
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


def require_schwab_access(request: Request, *, require_enabled: bool = True) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).schwab
    if require_enabled and not settings.enabled:
        raise HTTPException(status_code=503, detail="Schwab integration is disabled.")

    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.required_roles,
        log_prefix="Schwab",
    )
    return auth_context


def require_schwab_trade_access(request: Request) -> AuthContext:
    auth_context = require_schwab_access(request)
    settings = get_settings(request).schwab
    if not settings.trading_enabled:
        raise HTTPException(status_code=503, detail="Schwab trading is disabled.")

    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.trading_required_roles,
        log_prefix="Schwab trade",
    )
    return auth_context


def require_alpaca_access(request: Request) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).alpaca
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.required_roles,
        log_prefix="Alpaca",
    )
    return auth_context


def require_alpaca_trade_access(request: Request) -> AuthContext:
    auth_context = require_alpaca_access(request)
    settings = get_settings(request).alpaca
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.trading_required_roles,
        log_prefix="Alpaca trade",
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
            "Symbol enrichment authz failed: request_id=%s subject=%s caller_job=%s path=%s",
            _request_id(request),
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
            "Intraday job authz failed: request_id=%s subject=%s caller_job=%s path=%s",
            _request_id(request),
            auth_context.subject or "-",
            caller_job,
            request.url.path,
        )
        raise HTTPException(status_code=403, detail="Caller job is not allowed to use intraday monitoring.")
    return auth_context


def require_trade_desk_read_access(request: Request) -> AuthContext:
    auth_context = validate_auth(request)
    settings = get_settings(request).trade_desk
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.read_required_roles,
        log_prefix="Trade desk read",
    )
    return auth_context


def require_trade_desk_preview_access(request: Request) -> AuthContext:
    auth_context = require_trade_desk_read_access(request)
    settings = get_settings(request).trade_desk
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.preview_required_roles,
        log_prefix="Trade desk preview",
    )
    return auth_context


def require_trade_desk_place_access(request: Request) -> AuthContext:
    auth_context = require_trade_desk_read_access(request)
    settings = get_settings(request).trade_desk
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.place_required_roles,
        log_prefix="Trade desk place",
    )
    return auth_context


def require_trade_desk_cancel_access(request: Request) -> AuthContext:
    auth_context = require_trade_desk_read_access(request)
    settings = get_settings(request).trade_desk
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.cancel_required_roles,
        log_prefix="Trade desk cancel",
    )
    return auth_context


def require_trade_desk_live_access(request: Request) -> AuthContext:
    auth_context = require_trade_desk_place_access(request)
    settings = get_settings(request).trade_desk
    _require_configured_roles(
        request=request,
        auth_context=auth_context,
        required_roles=settings.live_required_roles,
        log_prefix="Trade desk live",
    )
    return auth_context

