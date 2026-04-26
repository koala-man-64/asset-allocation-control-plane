import json
import logging
import os
import time
import asyncio
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from asset_allocation_contracts.ui_config import UiRuntimeConfig
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

from api.endpoints import (
    ai,
    alpaca,
    auth,
    alpha_vantage,
    backtests,
    broker_accounts,
    data,
    etrade,
    government_signals,
    intraday,
    internal,
    kalshi,
    massive,
    notifications,
    portfolio_internal,
    portfolios,
    postgres,
    quiver,
    rankings,
    realtime,
    regimes,
    schwab,
    strategies,
    system,
    trade_desk,
    universes,
)
from api.service.auth import AuthManager
from api.service.alpaca_gateway import AlpacaGateway
from api.service.alpha_vantage_gateway import AlphaVantageGateway
from api.service.dependencies import validate_auth
from api.service.etrade_gateway import ETradeGateway
from api.service.kalshi_gateway import KalshiGateway
from api.service.log_streaming import LogStreamManager
from api.service.openapi_schema import stabilize_openapi_schema
from api.service.massive_gateway import MassiveGateway
from api.service.notification_delivery import build_notification_delivery_client
from api.service.openai_responses_gateway import OpenAIResponsesGateway
from api.service.quiver_gateway import QuiverGateway
from api.service.realtime_tickets import WebSocketTicketStore
from api.service.schwab_gateway import SchwabGateway
from api.service.settings import ServiceSettings
from core.log_redaction import install_log_redaction, redact_text
from api.service.realtime import manager as realtime_manager
from monitoring.ttl_cache import TtlCache
from asset_allocation_runtime_common.market_data.delta_core import get_delta_storage_auth_diagnostics
from core.redaction import redact_sensitive_value, summarize_query_params

install_log_redaction()
logger = logging.getLogger("asset-allocation.api")


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))


def _background_workers_enabled() -> bool:
    """
    Background workers run outside tests and are skipped in test harnesses.
    """
    return not _is_test_environment()


async def _shutdown_background_task(
    task: asyncio.Task[None] | None,
    *,
    stop_event: asyncio.Event | None,
    task_name: str,
    graceful_timeout_seconds: float = 2.0,
) -> None:
    """
    Stop a background task without leaking CancelledError during app shutdown.
    """
    if task is None:
        return

    if stop_event is not None:
        stop_event.set()

    # Already complete: await once to surface non-cancellation errors in logs.
    if task.done():
        try:
            await task
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("Background task '%s' completed with error: %s", task_name, exc)
        return

    try:
        await asyncio.wait_for(task, timeout=graceful_timeout_seconds)
        logger.info("Background task '%s' stopped gracefully.", task_name)
        return
    except asyncio.TimeoutError:
        logger.warning(
            "Background task '%s' did not stop within %.2fs; cancelling.",
            task_name,
            graceful_timeout_seconds,
        )
    except asyncio.CancelledError:
        logger.info("Background task '%s' cancellation acknowledged during graceful stop.", task_name)
        return
    except Exception as exc:
        logger.warning("Background task '%s' exited with error during graceful stop: %s", task_name, exc)
        return

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Background task '%s' cancelled after timeout.", task_name)
        return
    except Exception as exc:
        logger.warning("Background task '%s' raised after cancellation: %s", task_name, exc)


def _normalize_root_prefix(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")


def _parse_env_list(value: str | None) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []

    # Accept either JSON array syntax or a comma-separated list.
    if raw.startswith("["):
        try:
            decoded = json.loads(raw)
        except Exception:
            decoded = None
        else:
            if isinstance(decoded, list):
                return [str(item).strip() for item in decoded if str(item).strip()]

    return [item.strip() for item in raw.split(",") if item.strip()]


def _get_cors_allow_origins() -> list[str]:
    configured = _parse_env_list(os.environ.get("API_CORS_ALLOW_ORIGINS"))
    if configured:
        origins = configured
    else:
        origins = [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:5174",
            "http://127.0.0.1:5174",
            "http://localhost:3000",
        ]

    # CORSMiddleware does not allow credentials with wildcard origins.
    if "*" in origins:
        logger.warning(
            "API_CORS_ALLOW_ORIGINS contains '*', which is incompatible with allow_credentials=true. "
            "Dropping '*' and keeping explicit origins only."
        )
        origins = [origin for origin in origins if origin != "*"]

    # De-dup while preserving order.
    return list(dict.fromkeys(origins))


def create_app() -> FastAPI:
    # ... (existing inner functions) ...
    log_stream_manager = LogStreamManager(realtime_manager)

    def _seed_runtime_state(app: FastAPI, settings: ServiceSettings) -> None:
        app.state.settings = settings
        if not hasattr(app.state, "auth"):
            app.state.auth = AuthManager(settings)
        if not hasattr(app.state, "alpha_vantage_gateway"):
            app.state.alpha_vantage_gateway = AlphaVantageGateway()
        if not hasattr(app.state, "massive_gateway"):
            app.state.massive_gateway = MassiveGateway()
        if not hasattr(app.state, "quiver_gateway"):
            app.state.quiver_gateway = QuiverGateway(settings.quiver)
        if not hasattr(app.state, "etrade_gateway"):
            app.state.etrade_gateway = ETradeGateway(settings.etrade)
        if not hasattr(app.state, "alpaca_gateway"):
            app.state.alpaca_gateway = AlpacaGateway(settings.alpaca)
        if not hasattr(app.state, "kalshi_gateway"):
            app.state.kalshi_gateway = KalshiGateway(settings.kalshi)
        if not hasattr(app.state, "schwab_gateway"):
            app.state.schwab_gateway = SchwabGateway(settings.schwab)
        if not hasattr(app.state, "ai_relay_gateway"):
            app.state.ai_relay_gateway = OpenAIResponsesGateway(settings.ai_relay)
        if not hasattr(app.state, "notification_delivery_client"):
            app.state.notification_delivery_client = build_notification_delivery_client(settings.notifications)
        if not hasattr(app.state, "log_stream_manager"):
            app.state.log_stream_manager = log_stream_manager
        if not hasattr(app.state, "websocket_ticket_store"):
            app.state.websocket_ticket_store = WebSocketTicketStore(ttl_seconds=60)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = getattr(app.state, "settings", ServiceSettings.from_env())
        _seed_runtime_state(app, settings)
        logger.info(
            "Resolved service capabilities: auth=%s auth_required=%s ui_auth_provider=%s browser_oidc=%s postgres=%s",
            settings.auth_summary,
            settings.auth_required,
            settings.ui_auth_provider,
            settings.browser_oidc_enabled,
            bool(settings.postgres_dsn),
        )

        workers_enabled = _background_workers_enabled()
        logger.info(
            "Background worker policy resolved: active=%s test_env=%s",
            workers_enabled,
            _is_test_environment(),
        )

        if settings.postgres_dsn:
            try:
                from asset_allocation_runtime_common.foundation.config import reload_settings
                from asset_allocation_runtime_common.foundation.debug_symbols import refresh_debug_symbols_from_db
                from asset_allocation_runtime_common.foundation.runtime_config import (
                    DEFAULT_ENV_OVERRIDE_KEYS,
                    apply_runtime_config_to_env,
                )

                if workers_enabled and not _is_test_environment():
                    baseline_env: dict[str, str | None] = {
                        key: os.environ.get(key) for key in sorted(DEFAULT_ENV_OVERRIDE_KEYS)
                    }
                    app.state.runtime_config_baseline = baseline_env

                    def _apply_and_reconcile() -> dict[str, str]:
                        applied = apply_runtime_config_to_env(
                            dsn=settings.postgres_dsn,
                            scopes_by_precedence=["global"],
                            raise_on_error=True,
                        )

                        # If a key is no longer overridden, revert to its baseline value.
                        for key in DEFAULT_ENV_OVERRIDE_KEYS:
                            if key in applied:
                                continue
                            baseline = baseline_env.get(key)
                            if baseline is None:
                                os.environ.pop(key, None)
                            else:
                                os.environ[key] = baseline

                        reload_settings()
                        debug_symbols = refresh_debug_symbols_from_db(dsn=settings.postgres_dsn)
                        app.state.runtime_config_applied = applied

                        try:
                            import hashlib

                            digest = hashlib.sha256(
                                json.dumps(applied, sort_keys=True, separators=(",", ":")).encode("utf-8")
                            ).hexdigest()
                            if getattr(app.state, "runtime_config_hash", None) != digest:
                                app.state.runtime_config_hash = digest
                                logger.info(
                                    "Runtime config refreshed: keys=%s hash=%s",
                                    sorted(applied.keys()),
                                    digest[:12],
                                )
                        except Exception:
                            pass

                        try:
                            import hashlib

                            digest = hashlib.sha256(
                                json.dumps(list(debug_symbols), separators=(",", ":")).encode("utf-8")
                            ).hexdigest()
                            if getattr(app.state, "debug_symbols_hash", None) != digest:
                                app.state.debug_symbols_hash = digest
                                logger.info(
                                    "Debug symbols refreshed: count=%s hash=%s",
                                    len(debug_symbols),
                                    digest[:12],
                                )
                        except Exception:
                            pass
                        return applied

                    _apply_and_reconcile()

            except Exception as exc:
                logger.warning("Runtime config overrides not applied: %s", exc)

        def _system_health_ttl_seconds() -> float:
            raw = os.environ.get("SYSTEM_HEALTH_TTL_SECONDS", "300")
            try:
                ttl = float(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid float for SYSTEM_HEALTH_TTL_SECONDS={raw!r}") from exc
            if ttl <= 0:
                raise ValueError("SYSTEM_HEALTH_TTL_SECONDS must be > 0.")
            return ttl

        app.state.system_health_cache = TtlCache(ttl_seconds=_system_health_ttl_seconds())

        try:
            storage_diag = get_delta_storage_auth_diagnostics(container=None)
            logger.info(
                "Delta storage auth resolved: mode=%s account=%s key_source=%s options=%s has_conn_str=%s has_account_key=%s has_access_key=%s has_sas=%s has_client_secret=%s has_identity_endpoint=%s",
                storage_diag.get("mode"),
                storage_diag.get("accountName"),
                storage_diag.get("accountKeySource"),
                ",".join(storage_diag.get("optionKeys", [])),
                storage_diag.get("hasConnectionString"),
                storage_diag.get("hasAccountKeyEnv"),
                storage_diag.get("hasAccessKeyEnv"),
                storage_diag.get("hasSasTokenEnv"),
                storage_diag.get("hasClientSecretEnv"),
                storage_diag.get("hasIdentityEndpoint"),
            )
        except Exception as exc:
            logger.warning("Failed to resolve Delta storage auth diagnostics: %s", exc)

        yield

        try:
            app.state.alpha_vantage_gateway.close()
        except Exception:
            pass

        try:
            app.state.massive_gateway.close()
        except Exception:
            pass

        try:
            app.state.quiver_gateway.close()
        except Exception:
            pass

        try:
            app.state.etrade_gateway.close()
        except Exception:
            pass

        try:
            app.state.alpaca_gateway.close()
        except Exception:
            pass

        try:
            app.state.kalshi_gateway.close()
        except Exception:
            pass

        try:
            app.state.schwab_gateway.close()
        except Exception:
            pass

        await log_stream_manager.shutdown()

    app = FastAPI(
        title="Asset Allocation API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    _seed_runtime_state(app, ServiceSettings.from_env())
    logger.info("Service application starting")

    content_security_policy = (os.environ.get("API_CSP") or "").strip()

    @app.exception_handler(HTTPException)
    async def _redacted_http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            {"detail": redact_sensitive_value(exc.detail)},
            status_code=exc.status_code,
            headers=exc.headers,
        )

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        try:
            start = time.monotonic()
            path = request.url.path or ""
            request_id = str(request.headers.get("x-request-id") or "").strip() or str(uuid.uuid4())
            request.state.request_id = request_id
            auth_header = str(request.headers.get("authorization") or "").strip()
            query_keys = sorted({str(key) for key in request.query_params.keys()})
            try:
                query_param_count = len(request.query_params.multi_items())
            except Exception:
                query_param_count = len(request.query_params)

            query_summary = summarize_query_params(request.url.query)
            logger.info(
                "HTTP request: request_id=%s method=%s path=%s query_param_count=%s query_keys=%s host=%s origin=%s referer=%s forwarded_for=%s auth_present=%s",
                request_id,
                request.method,
                path,
                query_param_count,
                query_keys,
                redact_text(request.headers.get("host", "")),
                redact_text(request.headers.get("origin", "")),
                redact_text(request.headers.get("referer", "")),
                redact_text(request.headers.get("x-forwarded-for", "")),
                auth_header.lower().startswith("bearer "),
            )

            response = await call_next(request)
            elapsed_ms = (time.monotonic() - start) * 1000.0
            auth_session_renewal = getattr(request.state, "auth_session_renewal", None)
            if auth_session_renewal is not None:
                request.app.state.auth.set_session_cookies(response, auth_session_renewal)

            # Safe logic for headers
            if path.startswith("/assets/") and response.status_code == 200:
                response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")

            response.headers.setdefault("X-Request-ID", request_id)
            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault("X-Frame-Options", "DENY")
            if content_security_policy:
                response.headers.setdefault("Content-Security-Policy", content_security_policy)

            logger.info(
                "HTTP response: request_id=%s method=%s path=%s status=%s duration_ms=%.2f",
                request_id,
                request.method,
                path,
                response.status_code,
                elapsed_ms,
            )

            return response

        except Exception:
            logger.exception(
                "HTTP middleware unhandled error: request_id=%s method=%s path=%s",
                getattr(request.state, "request_id", "-"),
                request.method,
                request.url.path,
            )
            raise

    # CORS Configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_allow_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    api_root_prefix = app.state.settings.api_root_prefix
    api_prefixes = ["/api"]
    if api_root_prefix:
        api_prefixes.append(f"{api_root_prefix}/api")

    def _get_openapi_schema() -> dict:
        if app.openapi_schema is None:
            app.openapi_schema = stabilize_openapi_schema(
                get_openapi(
                    title=app.title,
                    version=app.version,
                    routes=app.routes,
                )
            )
        return app.openapi_schema

    app.openapi = _get_openapi_schema  # type: ignore[method-assign]

    def _enforce_public_surface_auth(request: Request) -> None:
        settings: ServiceSettings = request.app.state.settings
        if settings.auth_required:
            validate_auth(request)

    def _register_docs_routes(api_prefix: str) -> None:
        docs_path = f"{api_prefix}/docs"
        openapi_path = f"{api_prefix}/openapi.json"
        oauth2_redirect_path = f"{docs_path}/oauth2-redirect"

        async def openapi_json(request: Request) -> JSONResponse:
            _enforce_public_surface_auth(request)
            return JSONResponse(_get_openapi_schema())

        async def swagger_ui(request: Request) -> Response:
            _enforce_public_surface_auth(request)
            return get_swagger_ui_html(
                openapi_url=openapi_path,
                title=f"{app.title} - Swagger UI",
                oauth2_redirect_url=oauth2_redirect_path,
            )

        async def swagger_ui_redirect() -> Response:
            return get_swagger_ui_oauth2_redirect_html()

        app.add_api_route(
            openapi_path,
            openapi_json,
            methods=["GET"],
            include_in_schema=False,
            name=f"openapi:{api_prefix}",
        )
        app.add_api_route(
            docs_path,
            swagger_ui,
            methods=["GET"],
            include_in_schema=False,
            name=f"swagger:{api_prefix}",
        )
        app.add_api_route(
            oauth2_redirect_path,
            swagger_ui_redirect,
            methods=["GET"],
            include_in_schema=False,
            name=f"swagger-oauth2:{api_prefix}",
        )

    for api_prefix in api_prefixes:
        _register_docs_routes(api_prefix)

    primary_api_prefix = f"{api_root_prefix}/api" if api_root_prefix else "/api"

    @app.get("/docs", include_in_schema=False)
    def docs_redirect(request: Request) -> RedirectResponse:
        _enforce_public_surface_auth(request)
        return RedirectResponse(
            url=f"{primary_api_prefix}/docs",
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    @app.get("/openapi.json", include_in_schema=False)
    def openapi_redirect(request: Request) -> RedirectResponse:
        _enforce_public_surface_auth(request)
        return RedirectResponse(
            url=f"{primary_api_prefix}/openapi.json",
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    for api_prefix in api_prefixes:
        app.include_router(ai.router, prefix=f"{api_prefix}/ai", tags=["AI"])
        app.include_router(data.router, prefix=f"{api_prefix}/data", tags=["Data"])
        app.include_router(auth.router, prefix=f"{api_prefix}/auth", tags=["Auth"])
        app.include_router(broker_accounts.router, prefix=api_prefix, tags=["Broker Accounts"])
        app.include_router(intraday.router, prefix=f"{api_prefix}/intraday", tags=["Intraday"])
        app.include_router(system.router, prefix=f"{api_prefix}/system", tags=["System"])
        app.include_router(postgres.router, prefix=f"{api_prefix}/system/postgres", tags=["Postgres"])
        app.include_router(universes.router, prefix=f"{api_prefix}/universes", tags=["Universes"])
        app.include_router(strategies.router, prefix=f"{api_prefix}/strategies", tags=["Strategies"])
        app.include_router(portfolios.router, prefix=api_prefix, tags=["Portfolios"])
        app.include_router(trade_desk.router, prefix=api_prefix, tags=["Trade Desk"])
        app.include_router(notifications.router, prefix=api_prefix, tags=["Notifications"])
        app.include_router(rankings.router, prefix=f"{api_prefix}/rankings", tags=["Rankings"])
        app.include_router(regimes.router, prefix=f"{api_prefix}/regimes", tags=["Regimes"])
        app.include_router(backtests.router, prefix=f"{api_prefix}/backtests", tags=["Backtests"])
        app.include_router(
            government_signals.router,
            prefix=f"{api_prefix}/government-signals",
            tags=["Government Signals"],
        )
        app.include_router(internal.router, prefix=f"{api_prefix}/internal", tags=["Internal"])
        app.include_router(portfolio_internal.router, prefix=f"{api_prefix}/internal/portfolios", tags=["Internal"])
        app.include_router(portfolio_internal.compat_router, prefix=f"{api_prefix}/internal", tags=["Internal"])
        app.include_router(realtime.router, prefix=api_prefix, tags=["Realtime"])
        app.include_router(
            alpha_vantage.router,
            prefix=f"{api_prefix}/providers/alpha-vantage",
            tags=["AlphaVantage"],
        )
        app.include_router(
            massive.router,
            prefix=f"{api_prefix}/providers/massive",
            tags=["Massive"],
        )
        app.include_router(
            quiver.router,
            prefix=f"{api_prefix}/providers/quiver",
            tags=["Quiver"],
        )
        app.include_router(
            alpaca.router,
            prefix=f"{api_prefix}/providers/alpaca",
            tags=["Alpaca"],
        )
        app.include_router(
            kalshi.router,
            prefix=f"{api_prefix}/providers/kalshi",
            tags=["Kalshi"],
        )
        app.include_router(
            etrade.router,
            prefix=f"{api_prefix}/providers/etrade",
            tags=["ETrade"],
        )
        app.include_router(
            schwab.router,
            prefix=f"{api_prefix}/providers/schwab",
            tags=["Schwab"],
        )

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ready"})

    @app.get("/config.js")
    async def get_ui_config(request: Request):
        settings: ServiceSettings = app.state.settings
        cfg = UiRuntimeConfig.model_validate(
            {
                "apiBaseUrl": settings.ui_oidc_config.get("apiBaseUrl") or "/api",
                "authSessionMode": settings.auth_session_mode,
                "authProvider": settings.ui_auth_provider,
                "oidcAuthority": settings.ui_oidc_config.get("authority") if settings.ui_auth_provider == "oidc" else None,
                "oidcClientId": settings.ui_oidc_config.get("clientId") if settings.ui_auth_provider == "oidc" else None,
                "oidcScopes": settings.ui_oidc_config.get("scope") if settings.ui_auth_provider == "oidc" else [],
                "oidcRedirectUri": settings.ui_oidc_config.get("redirectUri") if settings.ui_auth_provider == "oidc" else None,
                "oidcPostLogoutRedirectUri": settings.ui_oidc_config.get("postLogoutRedirectUri")
                if settings.ui_auth_provider == "oidc"
                else None,
                "oidcAudience": settings.oidc_audience if settings.ui_auth_provider == "oidc" else [],
                "oidcEnabled": settings.ui_auth_provider == "oidc" and settings.browser_oidc_enabled,
                "authRequired": settings.auth_required,
            }
        ).model_dump(mode="json")

        logger.info(
            "Serving /config.js: authProvider=%s oidcEnabled=%s authRequired=%s authSessionMode=%s apiBaseUrl=%s scopes=%s",
            cfg.get("authProvider"),
            cfg.get("oidcEnabled"),
            cfg.get("authRequired"),
            cfg.get("authSessionMode"),
            cfg.get("apiBaseUrl"),
            cfg.get("oidcScopes"),
        )
        content = "\n".join(
            [
                f"window.__API_UI_CONFIG__ = {json.dumps(cfg)};",
            ]
        )
        return Response(
            content=content,
            media_type="application/javascript",
            headers={"Cache-Control": "no-store"},
        )

    ui_dist_env = os.environ.get("UI_DIST_DIR")
    if ui_dist_env:
        dist_path = Path(ui_dist_env).resolve()
        if dist_path.exists() and dist_path.is_dir():
            logger.info("Serving UI from %s", dist_path)
            from fastapi.staticfiles import StaticFiles

            assets_path = dist_path / "assets"
            if assets_path.exists():
                app.mount("/assets", StaticFiles(directory=str(assets_path)), name="assets")

            @app.get("/{rest_of_path:path}")
            async def serve_index(rest_of_path: str):
                file_path = dist_path / rest_of_path
                if rest_of_path and file_path.exists() and file_path.is_file():
                    return FileResponse(file_path)
                return FileResponse(dist_path / "index.html", headers={"Cache-Control": "no-store"})
        else:
            logger.warning("UI_DIST_DIR set but invalid: %s", ui_dist_env)
    else:
        logger.info("UI_DIST_DIR not set. UI will not be served.")

    return app


app = create_app()
