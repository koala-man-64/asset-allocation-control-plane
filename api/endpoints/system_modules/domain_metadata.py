import hashlib
import json
import logging
import os
import sys
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import domain_metadata_snapshots
from asset_allocation_runtime_common.market_data.domain_metadata_snapshots import build_snapshot_miss_payload as build_snapshot_miss_payload_from_snapshots
from monitoring.domain_metadata import collect_domain_metadata

logger = logging.getLogger("asset-allocation.api.system.domain_metadata")

_ALLOWED_LAYERS = {"bronze", "silver", "gold", "platinum"}
_ALLOWED_DOMAINS = {"market", "finance", "earnings", "price-target", "platinum"}


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


def _fallback_normalize_layer(value: str) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _ALLOWED_LAYERS else None


def _fallback_normalize_domain(value: str) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _ALLOWED_DOMAINS else None


class DomainDateRange(BaseModel):
    min: Optional[str] = None
    max: Optional[str] = None
    column: Optional[str] = None
    source: Optional[Literal["partition", "stats", "artifact"]] = None


class DomainMetadataResponse(BaseModel):
    layer: str
    domain: str
    container: str
    type: Literal["blob", "delta"]
    computedAt: str
    folderLastModified: Optional[str] = None
    cachedAt: Optional[str] = None
    cacheSource: Optional[Literal["snapshot", "live-refresh"]] = None
    symbolCount: Optional[int] = None
    columns: List[str] = Field(default_factory=list)
    columnCount: Optional[int] = None
    financeSubfolderSymbolCounts: Optional[Dict[str, int]] = None
    dateRange: Optional[DomainDateRange] = None
    totalRows: Optional[int] = None
    fileCount: Optional[int] = None
    totalBytes: Optional[int] = None
    deltaVersion: Optional[int] = None
    tablePath: Optional[str] = None
    prefix: Optional[str] = None
    blacklistedSymbolCount: Optional[int] = None
    metadataPath: Optional[str] = None
    metadataSource: Optional[Literal["artifact", "scan"]] = None
    warnings: List[str] = Field(default_factory=list)


class DomainMetadataSnapshotResponse(BaseModel):
    version: int = 1
    updatedAt: Optional[str] = None
    entries: Dict[str, DomainMetadataResponse] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)


_DOMAIN_METADATA_CACHE_FILE_DEFAULT = "metadata/domain-metadata.json"
_DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS = 30.0
_DOMAIN_METADATA_DOCUMENT_CACHE_LOCK = threading.Lock()
_DOMAIN_METADATA_DOCUMENT_CACHE: Optional[Dict[str, Any]] = None
_DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = 0.0
_DOMAIN_METADATA_UI_CACHE_FILE_DEFAULT = "metadata/ui-cache/domain-metadata-snapshot.json"


def _normalize_domain_metadata_targets(targets: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)

    normalized: List[Dict[str, str]] = []
    seen: set[str] = set()
    for target in targets:
        layer = normalize_layer(str(target.get("layer") or ""))
        domain = normalize_domain(str(target.get("domain") or ""))
        if not layer or not domain:
            continue
        key = f"{layer}/{domain}"
        if key in seen:
            continue
        seen.add(key)
        normalized.append({"layer": layer, "domain": domain})
    return normalized


def _extract_domain_metadata_targets_from_entries(entries: Dict[str, Any]) -> List[Dict[str, str]]:
    targets: List[Dict[str, str]] = []
    for key, value in (entries or {}).items():
        if isinstance(value, dict):
            layer = value.get("layer")
            domain = value.get("domain")
        else:
            layer = None
            domain = None
        if layer and domain:
            targets.append({"layer": layer, "domain": domain})
            continue
        if isinstance(key, str) and "/" in key:
            layer_key, domain_key = key.split("/", 1)
            targets.append({"layer": layer_key, "domain": domain_key})
    return _normalize_domain_metadata_targets(targets)


def _emit_domain_metadata_snapshot_changed(
    reason: Literal["refresh", "ui-cache-write", "purge"],
    targets: Sequence[Dict[str, Any]],
) -> None:
    emit_realtime = _system_attr("_emit_realtime", lambda *args, **kwargs: None)
    system_health_topic = _system_attr("REALTIME_TOPIC_SYSTEM_HEALTH", "system-health")
    utc_timestamp = _system_attr("_utc_timestamp", lambda: datetime.now(timezone.utc).isoformat())

    emit_realtime(
        system_health_topic,
        "DOMAIN_METADATA_SNAPSHOT_CHANGED",
        {
            "reason": reason,
            "targets": _normalize_domain_metadata_targets(targets),
            "updatedAt": utc_timestamp(),
        },
    )


def _domain_metadata_cache_path() -> str:
    configured = (os.environ.get("DOMAIN_METADATA_CACHE_PATH") or "").strip()
    return configured or _DOMAIN_METADATA_CACHE_FILE_DEFAULT


def _domain_metadata_ui_cache_path() -> str:
    configured = (os.environ.get("DOMAIN_METADATA_UI_CACHE_PATH") or "").strip()
    return configured or _DOMAIN_METADATA_UI_CACHE_FILE_DEFAULT


def _domain_metadata_snapshot_cache_ttl_seconds() -> float:
    raw = (os.environ.get("DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS") or "").strip()
    if not raw:
        return _DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS
    try:
        ttl = float(raw)
    except ValueError:
        logger.warning(
            "Invalid DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS=%r. Using default=%s.",
            raw,
            _DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS,
        )
        return _DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS
    if ttl < 0:
        return 0.0
    return ttl


def _cache_domain_metadata_document(payload: Dict[str, Any]) -> None:
    ttl = _domain_metadata_snapshot_cache_ttl_seconds()
    with _DOMAIN_METADATA_DOCUMENT_CACHE_LOCK:
        global _DOMAIN_METADATA_DOCUMENT_CACHE
        global _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT
        if ttl <= 0:
            _DOMAIN_METADATA_DOCUMENT_CACHE = None
            _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = 0.0
            return
        _DOMAIN_METADATA_DOCUMENT_CACHE = deepcopy(payload)
        _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = time.monotonic() + ttl


def _invalidate_domain_metadata_document_cache() -> None:
    with _DOMAIN_METADATA_DOCUMENT_CACHE_LOCK:
        global _DOMAIN_METADATA_DOCUMENT_CACHE
        global _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT
        _DOMAIN_METADATA_DOCUMENT_CACHE = None
        _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = 0.0


def _domain_metadata_cache_key(layer: str, domain: str) -> str:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    normalized_layer = normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = normalize_domain(domain) or str(domain or "").strip().lower()
    return f"{normalized_layer}/{normalized_domain}"


def _default_domain_metadata_document() -> Dict[str, Any]:
    return {"version": 1, "updatedAt": None, "entries": {}}


def _load_domain_metadata_document(force_refresh: bool = False) -> Dict[str, Any]:
    if not force_refresh:
        now = time.monotonic()
        with _DOMAIN_METADATA_DOCUMENT_CACHE_LOCK:
            cached = _DOMAIN_METADATA_DOCUMENT_CACHE
            expires_at = _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT
            if isinstance(cached, dict) and now < expires_at:
                return deepcopy(cached)

    path = _domain_metadata_cache_path()
    payload = mdc.get_common_json_content(path)
    if not isinstance(payload, dict):
        payload = _default_domain_metadata_document()

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    _cache_domain_metadata_document(payload)
    return payload


def _read_cached_domain_metadata_snapshot(
    layer: str,
    domain: str,
    *,
    force_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)

    key = _domain_metadata_cache_key(layer, domain)
    payload = _load_domain_metadata_document(force_refresh=force_refresh)
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return None

    raw_entry = entries.get(key)
    if not isinstance(raw_entry, dict):
        return None

    raw_metadata = raw_entry.get("metadata")
    metadata = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
    if not metadata:
        return None

    normalized_layer = normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = normalize_domain(domain) or str(domain or "").strip().lower()
    metadata["layer"] = normalized_layer
    metadata["domain"] = normalized_domain

    cached_at = raw_entry.get("cachedAt")
    if not isinstance(cached_at, str):
        cached_at = ""
    if cached_at:
        metadata["cachedAt"] = cached_at
    metadata["cacheSource"] = "snapshot"
    return metadata


def _write_cached_domain_metadata_snapshot(layer: str, domain: str, metadata: Dict[str, Any]) -> str:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    utc_timestamp = _system_attr("_utc_timestamp", lambda: datetime.now(timezone.utc).isoformat())

    if not isinstance(metadata, dict):
        raise ValueError("metadata payload must be a JSON object.")

    normalized_layer = normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = normalize_domain(domain) or str(domain or "").strip().lower()
    key = _domain_metadata_cache_key(normalized_layer, normalized_domain)

    payload = _load_domain_metadata_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        payload["entries"] = entries

    now = utc_timestamp()
    metadata_payload = dict(metadata)
    metadata_payload["layer"] = normalized_layer
    metadata_payload["domain"] = normalized_domain
    metadata_payload["cachedAt"] = now
    metadata_payload["cacheSource"] = "snapshot"

    previous_entry = entries.get(key)
    history: List[Dict[str, Any]] = []
    if isinstance(previous_entry, dict):
        previous_history = previous_entry.get("history")
        if isinstance(previous_history, list):
            for item in previous_history[-199:]:
                if isinstance(item, dict):
                    history.append(dict(item))

    history.append(
        {
            "timestamp": now,
            "symbolCount": metadata_payload.get("symbolCount"),
            "columnCount": metadata_payload.get("columnCount"),
            "fileCount": metadata_payload.get("fileCount"),
            "totalRows": metadata_payload.get("totalRows"),
            "totalBytes": metadata_payload.get("totalBytes"),
            "deltaVersion": metadata_payload.get("deltaVersion"),
        }
    )

    entries[key] = {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "cachedAt": now,
        "metadata": metadata_payload,
        "history": history[-200:],
    }
    payload["version"] = 1
    payload["updatedAt"] = now
    mdc.save_common_json_content(payload, _domain_metadata_cache_path())
    _cache_domain_metadata_document(payload)
    return now


def _refresh_domain_metadata_snapshot(layer: str, domain: str) -> Dict[str, Any]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    collect_domain_metadata_fn = _system_attr("collect_domain_metadata", collect_domain_metadata)
    module_logger = _system_attr("logger", logger)

    normalized_layer = normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = normalize_domain(domain) or str(domain or "").strip().lower()

    try:
        metadata = collect_domain_metadata_fn(
            layer=normalized_layer,
            domain=normalized_domain,
            force_refresh=True,
        )
    except HTTPException:
        raise
    except Exception as exc:
        module_logger.exception(
            "Domain metadata live refresh failed: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(
            status_code=503,
            detail=(
                "Failed to refresh domain metadata live for "
                f"{normalized_layer}/{normalized_domain}: {exc}"
            ),
        ) from exc

    try:
        persisted = domain_metadata_snapshots.write_domain_metadata_snapshot_documents(
            layer=normalized_layer,
            domain=normalized_domain,
            metadata=metadata,
            snapshot_path=_domain_metadata_cache_path(),
            ui_snapshot_path=_domain_metadata_ui_cache_path(),
        )
    except Exception as exc:
        module_logger.exception(
            "Domain metadata snapshot persist failed after live refresh: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(
            status_code=503,
            detail=(
                "Failed to persist refreshed domain metadata for "
                f"{normalized_layer}/{normalized_domain}: {exc}"
            ),
        ) from exc

    _invalidate_domain_metadata_document_cache()
    _emit_domain_metadata_snapshot_changed(
        "refresh",
        [{"layer": normalized_layer, "domain": normalized_domain}],
    )
    response_payload = dict(persisted)
    response_payload["cacheSource"] = "live-refresh"
    return response_payload


def _extract_cached_domain_metadata_snapshots(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}

    extracted: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_entry in entries.items():
        if not isinstance(raw_entry, dict):
            continue

        raw_metadata = raw_entry.get("metadata")
        metadata = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
        if not metadata:
            continue

        layer = normalize_layer(str(metadata.get("layer") or raw_entry.get("layer") or ""))
        domain = normalize_domain(str(metadata.get("domain") or raw_entry.get("domain") or ""))
        if not layer or not domain:
            if isinstance(raw_key, str) and "/" in raw_key:
                prefix, suffix = raw_key.split("/", 1)
                layer = layer or normalize_layer(prefix)
                domain = domain or normalize_domain(suffix)
        if not layer or not domain:
            continue

        key = _domain_metadata_cache_key(layer, domain)
        metadata["layer"] = layer
        metadata["domain"] = domain

        cached_at = raw_entry.get("cachedAt")
        if not isinstance(cached_at, str):
            cached_at = ""
        if cached_at:
            metadata["cachedAt"] = cached_at
        metadata["cacheSource"] = "snapshot"
        extracted[key] = metadata

    return extracted


def _parse_domain_metadata_filter(
    raw: Optional[str],
    *,
    param_name: str,
    normalizer: Callable[[str], Optional[str]],
    allowed_values: Optional[set[str]] = None,
) -> Optional[set[str]]:
    split_csv = _system_attr("_split_csv", lambda value: [item.strip() for item in (value or "").split(",") if item.strip()])

    text = (raw or "").strip()
    if not text:
        return None
    items = split_csv(text)
    if not items:
        return set()
    normalized: set[str] = set()
    for item in items:
        value = normalizer(item)
        if not value:
            raise HTTPException(
                status_code=400,
                detail=f"{param_name} contains unsupported value: {item!r}.",
            )
        if allowed_values is not None and value not in allowed_values:
            raise HTTPException(
                status_code=400,
                detail=f"{param_name} contains unsupported value: {item!r}.",
            )
        normalized.add(value)
    return normalized


def _build_domain_metadata_snapshot_payload(
    *,
    layers: Optional[str] = None,
    domains: Optional[str] = None,
    refresh: bool = False,
) -> Dict[str, Any]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    module_logger = _system_attr("logger", logger)

    layer_filter = _parse_domain_metadata_filter(
        layers,
        param_name="layers",
        normalizer=lambda value: normalize_layer(value),
        allowed_values=set(_ALLOWED_LAYERS),
    )
    domain_filter = _parse_domain_metadata_filter(
        domains,
        param_name="domains",
        normalizer=lambda value: normalize_domain(value),
    )

    try:
        snapshot_doc = _load_domain_metadata_document(force_refresh=bool(refresh))
    except Exception as exc:
        module_logger.warning("Domain metadata snapshot load failed: %s", exc)
        snapshot_doc = _default_domain_metadata_document()
        _invalidate_domain_metadata_document_cache()

    all_entries = _extract_cached_domain_metadata_snapshots(snapshot_doc)
    filtered_entries: Dict[str, Dict[str, Any]] = {}
    warnings: List[str] = []

    for key, metadata in all_entries.items():
        layer = normalize_layer(str(metadata.get("layer") or ""))
        domain = normalize_domain(str(metadata.get("domain") or ""))
        if not layer or not domain:
            continue
        if layer_filter is not None and layer not in layer_filter:
            continue
        if domain_filter is not None and domain not in domain_filter:
            continue
        filtered_entries[key] = metadata

    return {
        "version": int(snapshot_doc.get("version") or 1),
        "updatedAt": snapshot_doc.get("updatedAt"),
        "entries": filtered_entries,
        "warnings": warnings,
    }


_normalize_domain_metadata_targets_impl = _normalize_domain_metadata_targets
_normalize_domain_metadata_targets = _compat_export(
    "_normalize_domain_metadata_targets",
    _normalize_domain_metadata_targets_impl,
)
_extract_domain_metadata_targets_from_entries_impl = _extract_domain_metadata_targets_from_entries
_extract_domain_metadata_targets_from_entries = _compat_export(
    "_extract_domain_metadata_targets_from_entries",
    _extract_domain_metadata_targets_from_entries_impl,
)
_emit_domain_metadata_snapshot_changed_impl = _emit_domain_metadata_snapshot_changed
_emit_domain_metadata_snapshot_changed = _compat_export(
    "_emit_domain_metadata_snapshot_changed",
    _emit_domain_metadata_snapshot_changed_impl,
)
_domain_metadata_cache_path_impl = _domain_metadata_cache_path
_domain_metadata_cache_path = _compat_export("_domain_metadata_cache_path", _domain_metadata_cache_path_impl)
_domain_metadata_ui_cache_path_impl = _domain_metadata_ui_cache_path
_domain_metadata_ui_cache_path = _compat_export(
    "_domain_metadata_ui_cache_path",
    _domain_metadata_ui_cache_path_impl,
)
_domain_metadata_snapshot_cache_ttl_seconds_impl = _domain_metadata_snapshot_cache_ttl_seconds
_domain_metadata_snapshot_cache_ttl_seconds = _compat_export(
    "_domain_metadata_snapshot_cache_ttl_seconds",
    _domain_metadata_snapshot_cache_ttl_seconds_impl,
)
_cache_domain_metadata_document_impl = _cache_domain_metadata_document
_cache_domain_metadata_document = _compat_export(
    "_cache_domain_metadata_document",
    _cache_domain_metadata_document_impl,
)
_invalidate_domain_metadata_document_cache_impl = _invalidate_domain_metadata_document_cache
_invalidate_domain_metadata_document_cache = _compat_export(
    "_invalidate_domain_metadata_document_cache",
    _invalidate_domain_metadata_document_cache_impl,
)
_domain_metadata_cache_key_impl = _domain_metadata_cache_key
_domain_metadata_cache_key = _compat_export("_domain_metadata_cache_key", _domain_metadata_cache_key_impl)
_default_domain_metadata_document_impl = _default_domain_metadata_document
_default_domain_metadata_document = _compat_export(
    "_default_domain_metadata_document",
    _default_domain_metadata_document_impl,
)
_load_domain_metadata_document_impl = _load_domain_metadata_document
_load_domain_metadata_document = _compat_export(
    "_load_domain_metadata_document",
    _load_domain_metadata_document_impl,
)
_read_cached_domain_metadata_snapshot_impl = _read_cached_domain_metadata_snapshot
_read_cached_domain_metadata_snapshot = _compat_export(
    "_read_cached_domain_metadata_snapshot",
    _read_cached_domain_metadata_snapshot_impl,
)
_write_cached_domain_metadata_snapshot_impl = _write_cached_domain_metadata_snapshot
_write_cached_domain_metadata_snapshot = _compat_export(
    "_write_cached_domain_metadata_snapshot",
    _write_cached_domain_metadata_snapshot_impl,
)
_refresh_domain_metadata_snapshot_impl = _refresh_domain_metadata_snapshot
_refresh_domain_metadata_snapshot = _compat_export(
    "_refresh_domain_metadata_snapshot",
    _refresh_domain_metadata_snapshot_impl,
)
_extract_cached_domain_metadata_snapshots_impl = _extract_cached_domain_metadata_snapshots
_extract_cached_domain_metadata_snapshots = _compat_export(
    "_extract_cached_domain_metadata_snapshots",
    _extract_cached_domain_metadata_snapshots_impl,
)
_parse_domain_metadata_filter_impl = _parse_domain_metadata_filter
_parse_domain_metadata_filter = _compat_export("_parse_domain_metadata_filter", _parse_domain_metadata_filter_impl)
_build_domain_metadata_snapshot_payload_impl = _build_domain_metadata_snapshot_payload
_build_domain_metadata_snapshot_payload = _compat_export(
    "_build_domain_metadata_snapshot_payload",
    _build_domain_metadata_snapshot_payload_impl,
)


def build_router(
    *,
    runtime: ModuleType,
    domain_metadata_response_model: Any,
    domain_metadata_snapshot_response_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/domain-metadata", response_model=domain_metadata_response_model)
    def domain_metadata(
        request: Request,
        layer: str = Query(..., description="Medallion layer key (bronze|silver|gold|platinum)"),
        domain: str = Query(..., description="Domain key (market|finance|earnings|price-target|platinum)"),
        refresh: bool = Query(
            default=False,
            description="When true, collect live metadata, persist refreshed snapshot documents, and return the refreshed payload.",
        ),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        reject_removed_query_params = _runtime_attr(runtime, "_reject_removed_query_params")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        refresh_domain_metadata_snapshot = _runtime_attr(runtime, "_refresh_domain_metadata_snapshot")
        read_cached_domain_metadata_snapshot = _runtime_attr(runtime, "_read_cached_domain_metadata_snapshot")
        build_snapshot_miss_payload = getattr(
            runtime,
            "build_snapshot_miss_payload",
            build_snapshot_miss_payload_from_snapshots,
        )
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        reject_removed_query_params(request, "cacheOnly")
        normalized_layer = normalize_layer(layer)
        normalized_domain = normalize_domain(domain)
        if not normalized_layer:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not normalized_domain:
            raise HTTPException(status_code=400, detail="domain is required.")

        if refresh:
            payload = refresh_domain_metadata_snapshot(normalized_layer, normalized_domain)
            headers: Dict[str, str] = {
                "Cache-Control": "no-store",
                "X-Domain-Metadata-Source": "live-refresh",
            }
            cached_at = payload.get("cachedAt")
            if isinstance(cached_at, str) and cached_at.strip():
                headers["X-Domain-Metadata-Cached-At"] = cached_at
            return JSONResponse(payload, headers=headers)

        try:
            payload = read_cached_domain_metadata_snapshot(
                normalized_layer,
                normalized_domain,
                force_refresh=False,
            )
        except Exception as exc:
            logger.warning(
                "Domain metadata snapshot read failed. layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            payload = None

        if payload is None:
            placeholder_payload = build_snapshot_miss_payload(
                layer=normalized_layer,
                domain=normalized_domain,
            )
            return JSONResponse(
                placeholder_payload,
                headers={
                    "Cache-Control": "no-store",
                    "X-Domain-Metadata-Source": "snapshot-miss",
                    "X-Domain-Metadata-Cache-Miss": "1",
                },
            )

        headers: Dict[str, str] = {
            "Cache-Control": "no-store",
            "X-Domain-Metadata-Source": "snapshot",
        }
        cached_at = payload.get("cachedAt")
        if isinstance(cached_at, str) and cached_at.strip():
            headers["X-Domain-Metadata-Cached-At"] = cached_at
        return JSONResponse(payload, headers=headers)

    @router.get("/domain-metadata/snapshot", response_model=domain_metadata_snapshot_response_model)
    def domain_metadata_snapshot(
        request: Request,
        layers: Optional[str] = Query(
            default=None,
            description="Optional comma-separated layer filter (e.g. bronze,silver,gold).",
        ),
        domains: Optional[str] = Query(
            default=None,
            description="Optional comma-separated domain filter (e.g. market,finance,earnings,price-target).",
        ),
        refresh: bool = Query(
            default=False,
            description="When true, bypass the in-process snapshot document cache before reading persisted metadata.",
        ),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        reject_removed_query_params = _runtime_attr(runtime, "_reject_removed_query_params")
        build_domain_metadata_snapshot_payload = _runtime_attr(runtime, "_build_domain_metadata_snapshot_payload")

        validate_auth(request)
        reject_removed_query_params(request, "cacheOnly")
        response_payload = build_domain_metadata_snapshot_payload(
            layers=layers,
            domains=domains,
            refresh=bool(refresh),
        )
        headers: Dict[str, str] = {
            "Cache-Control": "no-store",
            "X-Domain-Metadata-Source": "snapshot-batch",
            "X-Domain-Metadata-Entry-Count": str(len(response_payload.get("entries") or {})),
        }
        updated_at = response_payload.get("updatedAt")
        if isinstance(updated_at, str) and updated_at.strip():
            headers["X-Domain-Metadata-Updated-At"] = updated_at
            headers["Last-Modified"] = updated_at
        etag_basis = {
            "updatedAt": response_payload.get("updatedAt"),
            "keys": sorted((response_payload.get("entries") or {}).keys()),
        }
        etag = (
            'W/"'
            + hashlib.sha256(
                json.dumps(etag_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:24]
            + '"'
        )
        headers["ETag"] = etag
        if (request.headers.get("if-none-match") or "").strip() == etag:
            return Response(status_code=304, headers=headers)
        return JSONResponse(response_payload, headers=headers)

    @router.get("/domain-metadata/snapshot/cache", response_model=domain_metadata_snapshot_response_model)
    def get_domain_metadata_snapshot_cache(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        mdc = _runtime_attr(runtime, "mdc")
        domain_metadata_ui_cache_path = _runtime_attr(runtime, "_domain_metadata_ui_cache_path")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        warnings: List[str] = []
        cache_hit = False
        payload: Dict[str, Any] = {}

        try:
            raw = mdc.get_common_json_content(domain_metadata_ui_cache_path())
        except Exception as exc:
            logger.warning("Failed to read persisted UI domain metadata cache: %s", exc)
            raw = None
            warnings.append(f"Read failed: {exc}")

        if isinstance(raw, dict):
            try:
                parsed = domain_metadata_snapshot_response_model(**raw)
                cache_hit = True
                payload = parsed.model_dump() if hasattr(parsed, "model_dump") else parsed.dict()
            except Exception as exc:
                logger.warning("Persisted UI cache payload was invalid. Returning empty snapshot. err=%s", exc)
                warnings.append(f"Invalid cache payload ignored: {exc}")

        if not payload:
            payload = {
                "version": 1,
                "updatedAt": None,
                "entries": {},
                "warnings": warnings or ["No persisted UI domain metadata snapshot found."],
            }
        elif warnings:
            payload["warnings"] = [*list(payload.get("warnings") or []), *warnings]

        return JSONResponse(
            payload,
            headers={
                "Cache-Control": "no-store",
                "X-Domain-Metadata-UI-Cache": "hit" if cache_hit else "miss",
                "X-Domain-Metadata-Entry-Count": str(len(payload.get("entries") or {})),
            },
        )

    @router.put("/domain-metadata/snapshot/cache", response_model=domain_metadata_snapshot_response_model)
    def put_domain_metadata_snapshot_cache(
        request: Request,
        payload: domain_metadata_snapshot_response_model,
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        utc_timestamp = _runtime_attr(runtime, "_utc_timestamp")
        mdc = _runtime_attr(runtime, "mdc")
        domain_metadata_ui_cache_path = _runtime_attr(runtime, "_domain_metadata_ui_cache_path")
        logger = _runtime_attr(runtime, "logger")
        emit_domain_metadata_snapshot_changed = _runtime_attr(runtime, "_emit_domain_metadata_snapshot_changed")
        extract_domain_metadata_targets_from_entries = _runtime_attr(
            runtime,
            "_extract_domain_metadata_targets_from_entries",
        )

        validate_auth(request)
        payload_out = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        if not str(payload_out.get("updatedAt") or "").strip():
            payload_out["updatedAt"] = utc_timestamp()
        try:
            mdc.save_common_json_content(payload_out, domain_metadata_ui_cache_path())
        except Exception as exc:
            logger.warning("Failed to persist UI domain metadata cache: %s", exc)
            raise HTTPException(status_code=503, detail=f"Failed to persist UI domain metadata cache: {exc}") from exc

        emit_domain_metadata_snapshot_changed(
            "ui-cache-write",
            extract_domain_metadata_targets_from_entries(payload_out.get("entries") or {}),
        )

        return JSONResponse(
            payload_out,
            headers={
                "Cache-Control": "no-store",
                "X-Domain-Metadata-UI-Cache": "written",
                "X-Domain-Metadata-Entry-Count": str(len(payload_out.get("entries") or {})),
            },
        )

    return router, {
        "domain_metadata": domain_metadata,
        "domain_metadata_snapshot": domain_metadata_snapshot,
        "get_domain_metadata_snapshot_cache": get_domain_metadata_snapshot_cache,
        "put_domain_metadata_snapshot_cache": put_domain_metadata_snapshot_cache,
    }
