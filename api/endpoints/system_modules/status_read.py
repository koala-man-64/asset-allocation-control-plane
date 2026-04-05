from __future__ import annotations

import json
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, List, Literal, Optional, Sequence

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from api.service.dependencies import get_settings, get_system_health_cache
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.control_plane import collect_jobs_and_executions
from monitoring.system_health import collect_system_health_snapshot
from monitoring.ttl_cache import TtlCache

from .domain_metadata import DomainMetadataSnapshotResponse, _build_domain_metadata_snapshot_payload

logger = logging.getLogger("asset-allocation.api.system.status_read")


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def _system_attr(name: str, default: Any) -> Any:
    system_module = sys.modules.get("api.endpoints.system")
    if system_module is None:
        return default
    return getattr(system_module, name, default)


def _compat_export(name: str, target: Any) -> Any:
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        resolved = _system_attr(name, target)
        if resolved is _wrapper:
            resolved = target
        return resolved(*args, **kwargs)

    _wrapper.__name__ = getattr(target, "__name__", name)
    _wrapper.__doc__ = getattr(target, "__doc__", None)
    _wrapper.__module__ = __name__
    return _wrapper


def _sanitize_system_health_json_value(value: Any) -> tuple[Any, int]:
    if value is None or isinstance(value, (str, bool)):
        return value, 0

    if isinstance(value, int):
        return value, 0

    if isinstance(value, float):
        return (value, 0) if math.isfinite(value) else (None, 1)

    if isinstance(value, dict):
        sanitized: Dict[Any, Any] = {}
        replacements = 0
        for key, item in value.items():
            sanitized_item, item_replacements = _sanitize_system_health_json_value(item)
            sanitized[key] = sanitized_item
            replacements += item_replacements
        return sanitized, replacements

    if isinstance(value, (list, tuple)):
        sanitized_items: List[Any] = []
        replacements = 0
        for item in value:
            sanitized_item, item_replacements = _sanitize_system_health_json_value(item)
            sanitized_items.append(sanitized_item)
            replacements += item_replacements
        return sanitized_items, replacements

    try:
        if hasattr(value, "isoformat") and callable(value.isoformat):
            return value.isoformat(), 0
    except Exception:
        pass

    try:
        coerced = value.item() if hasattr(value, "item") and callable(value.item) else value
    except Exception:
        coerced = value

    if coerced is not value:
        return _sanitize_system_health_json_value(coerced)

    return value, 0


def _resolve_system_health_payload(
    request: Request,
    *,
    refresh: bool,
) -> tuple[Dict[str, Any], bool, bool]:
    settings_fn = _system_attr("get_settings", get_settings)
    system_health_cache_fn = _system_attr("get_system_health_cache", get_system_health_cache)
    collect_system_health_snapshot_fn = _system_attr(
        "collect_system_health_snapshot",
        collect_system_health_snapshot,
    )
    module_logger = _system_attr("logger", logger)

    settings = settings_fn(request)

    include_ids = False
    if settings.auth_required:
        raw_env = os.environ.get("SYSTEM_HEALTH_VERBOSE_IDS")
        raw = raw_env.strip().lower() if raw_env else ""
        include_ids = raw in {"1", "true", "t", "yes", "y", "on"}

    cache: TtlCache[Dict[str, Any]] = system_health_cache_fn(request)

    def _refresh() -> Dict[str, Any]:
        return collect_system_health_snapshot_fn(include_resource_ids=include_ids)

    try:
        result = cache.get(_refresh, force_refresh=bool(refresh))
    except Exception as exc:
        module_logger.exception("System health cache refresh failed.")
        raise HTTPException(status_code=503, detail=f"System health unavailable: {exc}") from exc

    payload: Dict[str, Any] = dict(result.value or {})
    request_id = request.headers.get("x-request-id", "")
    module_logger.info(
        "System health payload ready: cache_hit=%s refresh_error=%s layers=%s alerts=%s resources=%s recent_jobs=%s",
        result.cache_hit,
        bool(result.refresh_error),
        len(payload.get("dataLayers") or []),
        len(payload.get("alerts") or []),
        len(payload.get("resources") or []),
        len(payload.get("recentJobs") or []),
    )
    recent_runs_preview: list[str] = []
    for run in (payload.get("recentJobs") or [])[:10]:
        if not isinstance(run, dict):
            continue
        job_name = str(run.get("jobName") or "").strip() or "?"
        status = str(run.get("status") or "").strip() or "unknown"
        start_time = str(run.get("startTime") or "").strip() or "n/a"
        recent_runs_preview.append(f"{job_name}:{status}@{start_time}")
    if recent_runs_preview:
        module_logger.info("System health recentJobs preview: %s", " | ".join(recent_runs_preview))
    elif payload.get("recentJobs") == []:
        module_logger.warning("System health recentJobs is empty.")

    payload, sanitization_replacements = _sanitize_system_health_json_value(payload)
    if sanitization_replacements:
        module_logger.warning(
            "System health payload sanitized before JSON response: replacements=%s request_id=%s",
            sanitization_replacements,
            request_id,
        )

    return payload, bool(result.cache_hit), bool(result.refresh_error)


def _extract_arm_error_message(response: httpx.Response) -> str:
    """
    Best-effort extraction of a human-friendly error message from ARM responses.

    Some ARM endpoints return a JSON string like:
      "Reason: Bad Request. Body: {\"error\":\"...\",\"success\":false}"
    """

    def _from_mapping(payload: Dict[str, Any]) -> str:
        err = payload.get("error")
        if isinstance(err, dict):
            message = err.get("message") or err.get("Message") or err.get("detail") or err.get("details")
            if isinstance(message, str) and message.strip():
                return message.strip()
            code = err.get("code") or err.get("Code")
            if isinstance(code, str) and code.strip():
                return code.strip()
            return json.dumps(err, ensure_ascii=False)
        if isinstance(err, str) and err.strip():
            return err.strip()
        message = payload.get("message") or payload.get("Message")
        if isinstance(message, str) and message.strip():
            return message.strip()
        return json.dumps(payload, ensure_ascii=False)

    def _from_text(text: str) -> str:
        cleaned = (text or "").strip()
        if not cleaned:
            return ""
        match = re.search(r"Body:\s*(\{.*\})\s*$", cleaned)
        if match:
            fragment = match.group(1)
            try:
                nested = json.loads(fragment)
            except json.JSONDecodeError:
                return cleaned
            if isinstance(nested, dict):
                return _from_mapping(nested)
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
            return fragment
        return cleaned

    try:
        payload = response.json()
    except Exception:
        return _from_text(response.text)

    if isinstance(payload, dict):
        return _from_mapping(payload)
    if isinstance(payload, str):
        return _from_text(payload)
    return _from_text(response.text)


def _normalize_job_name_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _status_view_domain_job_names(system_health_payload: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    seen: set[str] = set()

    for layer in system_health_payload.get("dataLayers") or []:
        if not isinstance(layer, dict):
            continue
        for domain in layer.get("domains") or []:
            if not isinstance(domain, dict):
                continue
            job_name = str(domain.get("jobName") or "").strip()
            if not job_name:
                continue
            key = _normalize_job_name_key(job_name)
            if not key or key in seen:
                continue
            seen.add(key)
            names.append(job_name)

    return names


def _merge_live_job_resources(
    existing_resources: Sequence[Dict[str, Any]],
    live_resources: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    target_keys = {
        _normalize_job_name_key(resource.get("name"))
        for resource in live_resources
        if isinstance(resource, dict)
        and str(resource.get("resourceType") or "").strip() == "Microsoft.App/jobs"
        and _normalize_job_name_key(resource.get("name"))
    }

    merged: List[Dict[str, Any]] = []
    for resource in existing_resources:
        if not isinstance(resource, dict):
            continue
        resource_type = str(resource.get("resourceType") or "").strip()
        resource_key = _normalize_job_name_key(resource.get("name"))
        if resource_type == "Microsoft.App/jobs" and resource_key in target_keys:
            continue
        merged.append(dict(resource))

    merged.extend(dict(resource) for resource in live_resources if isinstance(resource, dict))
    return merged


def _same_job_run(left: Dict[str, Any], right: Dict[str, Any]) -> bool:
    job_names_match = _normalize_job_name_key(left.get("jobName")) == _normalize_job_name_key(
        right.get("jobName")
    )

    left_execution_name = str(left.get("executionName") or "").strip()
    right_execution_name = str(right.get("executionName") or "").strip()
    left_start_time = str(left.get("startTime") or "").strip()
    right_start_time = str(right.get("startTime") or "").strip()

    execution_names_match = bool(
        left_execution_name and right_execution_name and left_execution_name == right_execution_name
    )
    start_times_match = bool(left_start_time and right_start_time and left_start_time == right_start_time)

    return job_names_match and (
        execution_names_match or (not (left_execution_name and right_execution_name) and start_times_match)
    )


def _merge_live_job_runs(
    existing_runs: Sequence[Dict[str, Any]],
    live_runs: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = [dict(run) for run in existing_runs if isinstance(run, dict)]

    for live_run in live_runs:
        if not isinstance(live_run, dict):
            continue
        if not _normalize_job_name_key(live_run.get("jobName")):
            continue

        filtered = [run for run in merged if not _same_job_run(run, live_run)]
        filtered.insert(0, dict(live_run))
        merged = filtered

    return merged


def _overlay_live_domain_job_runtime(system_health_payload: Dict[str, Any]) -> Dict[str, Any]:
    split_csv = _system_attr("_split_csv", lambda raw: [item.strip() for item in (raw or "").split(",") if item.strip()])
    utc_timestamp = _system_attr("_utc_timestamp", lambda: datetime.now(timezone.utc).isoformat())
    arm_config_cls = _system_attr("ArmConfig", ArmConfig)
    azure_arm_client = _system_attr("AzureArmClient", AzureArmClient)
    collect_jobs_and_executions_fn = _system_attr("collect_jobs_and_executions", collect_jobs_and_executions)
    module_logger = _system_attr("logger", logger)

    job_names = _status_view_domain_job_names(system_health_payload)
    if not job_names:
        return system_health_payload

    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""
    allowlist = split_csv(os.environ.get("SYSTEM_HEALTH_ARM_JOBS"))

    if not (subscription_id and resource_group and allowlist):
        return system_health_payload

    allowlist_index = {_normalize_job_name_key(name): name for name in allowlist}
    requested_job_names: List[str] = []
    seen: set[str] = set()
    for job_name in job_names:
        key = _normalize_job_name_key(job_name)
        resolved = allowlist_index.get(key)
        if not resolved or key in seen:
            continue
        seen.add(key)
        requested_job_names.append(resolved)

    if not requested_job_names:
        return system_health_payload

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = arm_config_cls.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = arm_config_cls(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    checked_at = utc_timestamp()
    try:
        with azure_arm_client(cfg) as arm:
            live_resources_raw, live_runs = collect_jobs_and_executions_fn(
                arm,
                job_names=requested_job_names,
                last_checked_iso=checked_at,
                include_ids=False,
                max_executions_per_job=1,
                resource_health_enabled=False,
            )
    except Exception as exc:
        module_logger.warning(
            "Status-view live domain job runtime overlay failed for jobs=%s error=%s",
            requested_job_names,
            exc,
            exc_info=True,
        )
        return system_health_payload

    live_resources = [resource.to_dict(include_ids=False) for resource in live_resources_raw]

    payload = dict(system_health_payload)
    payload["resources"] = _merge_live_job_resources(payload.get("resources") or [], live_resources)
    payload["recentJobs"] = _merge_live_job_runs(payload.get("recentJobs") or [], live_runs)
    return payload


def build_system_status_view(request: Request, refresh: bool = False) -> Dict[str, Any]:
    build_domain_metadata_snapshot_payload_fn = _system_attr(
        "_build_domain_metadata_snapshot_payload",
        _build_domain_metadata_snapshot_payload,
    )
    utc_timestamp = _system_attr("_utc_timestamp", lambda: datetime.now(timezone.utc).isoformat())

    system_health_payload, system_health_cache_hit, _refresh_error = _resolve_system_health_payload(
        request,
        refresh=bool(refresh),
    )
    system_health_payload = _overlay_live_domain_job_runtime(system_health_payload)
    metadata_snapshot_payload = build_domain_metadata_snapshot_payload_fn(refresh=bool(refresh))
    return {
        "version": 1,
        "generatedAt": utc_timestamp(),
        "systemHealth": system_health_payload,
        "metadataSnapshot": metadata_snapshot_payload,
        "sources": {
            "systemHealth": "cache" if system_health_cache_hit else "live-refresh",
            "metadataSnapshot": "persisted-snapshot",
        },
    }


class SymbolSyncStateResponse(BaseModel):
    id: int
    last_refreshed_at: Optional[str] = None
    last_refreshed_sources: Optional[Dict[str, Any]] = None
    last_refresh_error: Optional[str] = None


class SystemStatusViewSources(BaseModel):
    systemHealth: Literal["cache", "live-refresh"]
    metadataSnapshot: Literal["persisted-snapshot"] = "persisted-snapshot"


class SystemStatusViewResponse(BaseModel):
    version: int = 1
    generatedAt: str
    systemHealth: Dict[str, Any] = Field(default_factory=dict)
    metadataSnapshot: DomainMetadataSnapshotResponse = Field(default_factory=DomainMetadataSnapshotResponse)
    sources: SystemStatusViewSources


_sanitize_system_health_json_value_impl = _sanitize_system_health_json_value
_sanitize_system_health_json_value = _compat_export(
    "_sanitize_system_health_json_value",
    _sanitize_system_health_json_value_impl,
)
_resolve_system_health_payload_impl = _resolve_system_health_payload
_resolve_system_health_payload = _compat_export(
    "_resolve_system_health_payload",
    _resolve_system_health_payload_impl,
)
_extract_arm_error_message_impl = _extract_arm_error_message
_extract_arm_error_message = _compat_export("_extract_arm_error_message", _extract_arm_error_message_impl)
_normalize_job_name_key_impl = _normalize_job_name_key
_normalize_job_name_key = _compat_export("_normalize_job_name_key", _normalize_job_name_key_impl)
_status_view_domain_job_names_impl = _status_view_domain_job_names
_status_view_domain_job_names = _compat_export(
    "_status_view_domain_job_names",
    _status_view_domain_job_names_impl,
)
_merge_live_job_resources_impl = _merge_live_job_resources
_merge_live_job_resources = _compat_export("_merge_live_job_resources", _merge_live_job_resources_impl)
_same_job_run_impl = _same_job_run
_same_job_run = _compat_export("_same_job_run", _same_job_run_impl)
_merge_live_job_runs_impl = _merge_live_job_runs
_merge_live_job_runs = _compat_export("_merge_live_job_runs", _merge_live_job_runs_impl)
_overlay_live_domain_job_runtime_impl = _overlay_live_domain_job_runtime
_overlay_live_domain_job_runtime = _compat_export(
    "_overlay_live_domain_job_runtime",
    _overlay_live_domain_job_runtime_impl,
)
build_system_status_view_impl = build_system_status_view
build_system_status_view = _compat_export("build_system_status_view", build_system_status_view_impl)


def build_router(
    *,
    runtime: ModuleType,
    symbol_sync_state_response_model: Any,
    system_status_view_response_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/health")
    def system_health(request: Request, refresh: bool = Query(False)) -> JSONResponse:
        route_logger = _runtime_attr(runtime, "logger")
        validate_auth = _runtime_attr(runtime, "validate_auth")
        resolve_system_health_payload = _runtime_attr(runtime, "_resolve_system_health_payload")

        request_id = request.headers.get("x-request-id", "")
        route_logger.info(
            "System health request: refresh=%s path=%s host=%s fwd=%s request_id=%s",
            refresh,
            request.url.path,
            request.headers.get("host", ""),
            request.headers.get("x-forwarded-for", ""),
            request_id,
        )
        validate_auth(request)
        payload, cache_hit, refresh_error = resolve_system_health_payload(request, refresh=bool(refresh))

        headers: dict[str, str] = {
            "Cache-Control": "no-store",
            "X-System-Health-Cache": "hit" if cache_hit else "miss",
        }
        if refresh_error:
            headers["X-System-Health-Cache-Degraded"] = "1"
        return JSONResponse(payload, headers=headers)

    @router.get("/symbol-sync-state", response_model=symbol_sync_state_response_model)
    def get_symbol_sync_state_endpoint(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        get_symbol_sync_state = _runtime_attr(runtime, "get_symbol_sync_state")
        iso = _runtime_attr(runtime, "_iso")
        route_logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        try:
            state = get_symbol_sync_state(dsn)
        except Exception as exc:
            route_logger.exception("Failed to load symbol sync state.")
            raise HTTPException(status_code=500, detail=f"Failed to load symbol sync state: {exc}") from exc

        if not state:
            return JSONResponse(
                {
                    "id": 1,
                    "last_refreshed_at": None,
                    "last_refreshed_sources": None,
                    "last_refresh_error": None,
                },
                headers={"Cache-Control": "no-store"},
            )

        return JSONResponse(
            {
                "id": state["id"],
                "last_refreshed_at": iso(state["last_refreshed_at"]),
                "last_refreshed_sources": state["last_refreshed_sources"],
                "last_refresh_error": state["last_refresh_error"],
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.get("/status-view", response_model=system_status_view_response_model)
    def system_status_view(request: Request, refresh: bool = Query(False)) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        build_system_status_view = _runtime_attr(runtime, "build_system_status_view")

        validate_auth(request)
        payload = build_system_status_view(request, refresh=bool(refresh))
        return JSONResponse(
            payload,
            headers={
                "Cache-Control": "no-store",
                "X-System-Health-Cache": "hit"
                if payload.get("sources", {}).get("systemHealth") == "cache"
                else "miss",
                "X-Domain-Metadata-Source": "persisted-snapshot",
            },
        )

    @router.get("/lineage")
    def system_lineage(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_lineage_snapshot = _runtime_attr(runtime, "get_lineage_snapshot")
        route_logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        payload = get_lineage_snapshot()
        route_logger.info(
            "System lineage generated: layers=%s strategies=%s domains=%s",
            len(payload.get("layers") or []),
            len(payload.get("strategies") or []),
            len((payload.get("impactsByDomain") or {}).keys()),
        )
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})

    return router, {
        "system_health": system_health,
        "get_symbol_sync_state_endpoint": get_symbol_sync_state_endpoint,
        "system_status_view": system_status_view,
        "system_lineage": system_lineage,
    }
