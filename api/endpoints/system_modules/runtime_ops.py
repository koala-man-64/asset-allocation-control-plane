from types import ModuleType
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


class RuntimeConfigUpsertRequest(BaseModel):
    key: str = Field(..., description="Configuration key (env-var style).")
    scope: str = Field(default="global", description="Scope for this key (e.g., global or job:<name>).")
    value: str = Field(default="", description="Raw string value to apply (can be empty).")
    description: Optional[str] = Field(default=None, description="Optional human-readable description.")


class DebugSymbolsUpdateRequest(BaseModel):
    symbols: str = Field(
        ...,
        description="Comma-separated list or JSON array. Row presence means the allowlist is active.",
    )


def build_router(
    *,
    runtime: ModuleType,
    runtime_config_upsert_request_model: Any,
    debug_symbols_update_request_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/runtime-config/catalog")
    def get_runtime_config_catalog(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        default_env_override_keys = _runtime_attr(runtime, "DEFAULT_ENV_OVERRIDE_KEYS")
        runtime_config_catalog = _runtime_attr(runtime, "RUNTIME_CONFIG_CATALOG")

        validate_auth(request)
        items = []
        for key in sorted(default_env_override_keys):
            meta = runtime_config_catalog.get(key, {})
            items.append(
                {
                    "key": key,
                    "description": str(meta.get("description") or ""),
                    "example": str(meta.get("example") or ""),
                }
            )
        return JSONResponse({"items": items}, headers={"Cache-Control": "no-store"})

    @router.get("/runtime-config")
    def get_runtime_config(request: Request, scope: str = Query("global")) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        list_runtime_config = _runtime_attr(runtime, "list_runtime_config")
        default_env_override_keys = _runtime_attr(runtime, "DEFAULT_ENV_OVERRIDE_KEYS")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        iso = _runtime_attr(runtime, "_iso")
        os_module = _runtime_attr(runtime, "os")

        validate_auth(request)

        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        resolved_scope = str(scope or "").strip() or "global"
        try:
            rows = list_runtime_config(dsn, scopes=[resolved_scope], keys=sorted(default_env_override_keys))
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to load runtime config: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to load runtime config.")
            raise HTTPException(status_code=502, detail=f"Failed to load runtime config: {exc}") from exc

        return JSONResponse(
            {
                "scope": resolved_scope,
                "items": [
                    {
                        "scope": item.scope,
                        "key": item.key,
                        "value": item.value,
                        "description": item.description,
                        "updatedAt": iso(item.updated_at),
                        "updatedBy": item.updated_by,
                    }
                    for item in rows
                ],
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/runtime-config")
    def set_runtime_config(payload: runtime_config_upsert_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        default_env_override_keys = _runtime_attr(runtime, "DEFAULT_ENV_OVERRIDE_KEYS")
        normalize_env_override = _runtime_attr(runtime, "normalize_env_override")
        get_actor = _runtime_attr(runtime, "_get_actor")
        upsert_runtime_config = _runtime_attr(runtime, "upsert_runtime_config")
        runtime_config_catalog = _runtime_attr(runtime, "RUNTIME_CONFIG_CATALOG")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        iso = _runtime_attr(runtime, "_iso")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        runtime_topic = _runtime_attr(runtime, "REALTIME_TOPIC_RUNTIME_CONFIG")
        os_module = _runtime_attr(runtime, "os")

        validate_auth(request)

        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        key = str(payload.key or "").strip()
        if not key:
            raise HTTPException(status_code=400, detail="key is required.")
        if key not in default_env_override_keys:
            raise HTTPException(status_code=400, detail="Key is not allowed for DB override.")

        scope = str(payload.scope or "").strip() or "global"
        try:
            normalized_value = normalize_env_override(key, payload.value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        actor = get_actor(request)
        try:
            row = upsert_runtime_config(
                dsn=dsn,
                scope=scope,
                key=key,
                value=normalized_value,
                description=payload.description or runtime_config_catalog.get(key, {}).get("description"),
                actor=actor,
            )
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to update runtime config: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to update runtime config.")
            raise HTTPException(status_code=502, detail=f"Failed to update runtime config: {exc}") from exc

        response_payload = {
            "scope": row.scope,
            "key": row.key,
            "value": row.value,
            "description": row.description,
            "updatedAt": iso(row.updated_at),
            "updatedBy": row.updated_by,
        }
        emit_realtime(
            runtime_topic,
            "RUNTIME_CONFIG_CHANGED",
            {
                "scope": row.scope,
                "key": row.key,
                "present": True,
            },
        )
        return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})

    @router.delete("/runtime-config/{key}")
    def remove_runtime_config(key: str, request: Request, scope: str = Query("global")) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        default_env_override_keys = _runtime_attr(runtime, "DEFAULT_ENV_OVERRIDE_KEYS")
        delete_runtime_config = _runtime_attr(runtime, "delete_runtime_config")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        runtime_topic = _runtime_attr(runtime, "REALTIME_TOPIC_RUNTIME_CONFIG")
        os_module = _runtime_attr(runtime, "os")

        validate_auth(request)

        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        resolved = str(key or "").strip()
        if not resolved:
            raise HTTPException(status_code=400, detail="key is required.")
        if resolved not in default_env_override_keys:
            raise HTTPException(status_code=400, detail="Key is not allowed for DB override.")

        resolved_scope = str(scope or "").strip() or "global"
        try:
            deleted = delete_runtime_config(dsn=dsn, scope=resolved_scope, key=resolved)
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to delete runtime config: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to delete runtime config.")
            raise HTTPException(status_code=502, detail=f"Failed to delete runtime config: {exc}") from exc

        response_payload = {"scope": resolved_scope, "key": resolved, "deleted": bool(deleted)}
        emit_realtime(
            runtime_topic,
            "RUNTIME_CONFIG_CHANGED",
            {
                "scope": resolved_scope,
                "key": resolved,
                "deleted": bool(deleted),
            },
        )
        return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})

    @router.get("/debug-symbols")
    def get_debug_symbols(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        read_debug_symbols_state = _runtime_attr(runtime, "read_debug_symbols_state")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        iso = _runtime_attr(runtime, "_iso")
        os_module = _runtime_attr(runtime, "os")

        validate_auth(request)

        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        try:
            state = read_debug_symbols_state(dsn)
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to load debug symbols: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to load debug symbols.")
            raise HTTPException(status_code=502, detail=f"Failed to load debug symbols: {exc}") from exc

        if state is None:
            raise HTTPException(status_code=404, detail="Debug symbols are not configured.")

        return JSONResponse(
            {
                "symbols": state.symbols_raw,
                "updatedAt": iso(state.updated_at),
                "updatedBy": state.updated_by,
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.put("/debug-symbols")
    def set_debug_symbols(payload: debug_symbols_update_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        replace_debug_symbols_state = _runtime_attr(runtime, "replace_debug_symbols_state")
        get_actor = _runtime_attr(runtime, "_get_actor")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        iso = _runtime_attr(runtime, "_iso")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        debug_topic = _runtime_attr(runtime, "REALTIME_TOPIC_DEBUG_SYMBOLS")
        os_module = _runtime_attr(runtime, "os")

        validate_auth(request)

        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        raw_text = str(payload.symbols or "").strip()
        if not raw_text:
            raise HTTPException(status_code=400, detail="Debug symbols are required.")

        actor = get_actor(request)
        try:
            state = replace_debug_symbols_state(
                dsn=dsn,
                symbols=raw_text,
                actor=actor,
            )
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to update debug symbols: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to update debug symbols.")
            raise HTTPException(status_code=502, detail=f"Failed to update debug symbols: {exc}") from exc

        response_payload = {
            "symbols": state.symbols_raw,
            "updatedAt": iso(state.updated_at),
            "updatedBy": state.updated_by,
        }
        emit_realtime(
            debug_topic,
            "DEBUG_SYMBOLS_CHANGED",
            {
                "present": True,
                "symbolCount": len(state.symbols),
            },
        )
        return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})

    @router.delete("/debug-symbols")
    def remove_debug_symbols(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        delete_debug_symbols_state = _runtime_attr(runtime, "delete_debug_symbols_state")
        postgres_error = _runtime_attr(runtime, "PostgresError")
        logger = _runtime_attr(runtime, "logger")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        debug_topic = _runtime_attr(runtime, "REALTIME_TOPIC_DEBUG_SYMBOLS")
        os_module = _runtime_attr(runtime, "os")

        validate_auth(request)

        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os_module.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        try:
            deleted = delete_debug_symbols_state(dsn=dsn)
        except postgres_error as exc:
            raise HTTPException(status_code=503, detail=f"Failed to delete debug symbols: {exc}") from exc
        except Exception as exc:
            logger.exception("Failed to delete debug symbols.")
            raise HTTPException(status_code=502, detail=f"Failed to delete debug symbols: {exc}") from exc

        emit_realtime(
            debug_topic,
            "DEBUG_SYMBOLS_CHANGED",
            {
                "present": False,
                "deleted": bool(deleted),
            },
        )
        return JSONResponse({"deleted": bool(deleted)}, headers={"Cache-Control": "no-store"})

    return router, {
        "get_runtime_config_catalog": get_runtime_config_catalog,
        "get_runtime_config": get_runtime_config,
        "set_runtime_config": set_runtime_config,
        "remove_runtime_config": remove_runtime_config,
        "get_debug_symbols": get_debug_symbols,
        "set_debug_symbols": set_debug_symbols,
        "remove_debug_symbols": remove_debug_symbols,
    }
