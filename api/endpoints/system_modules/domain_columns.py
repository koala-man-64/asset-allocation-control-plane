import logging
import os
import sys
import threading
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Callable, Dict, List, Literal, Optional, TypeVar

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.foundation.blob_storage import BlobStorageClient
from asset_allocation_runtime_common.market_data.delta_core import get_delta_schema_columns
logger = logging.getLogger("asset-allocation.api.system.domain_columns")

_LAYER_CONTAINER_ENV = {
    "bronze": "AZURE_CONTAINER_BRONZE",
    "silver": "AZURE_CONTAINER_SILVER",
    "gold": "AZURE_CONTAINER_GOLD",
}
_RULE_DATA_PREFIXES: Dict[str, Dict[str, str]] = {
    "silver": {
        "market": "market-data/",
        "finance": "finance-data/",
        "earnings": "earnings-data/",
        "price-target": "price-target-data/",
    },
    "gold": {
        "market": "market/",
        "finance": "finance/",
        "earnings": "earnings/",
        "price-target": "targets/",
    },
}
_T = TypeVar("_T")


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
    return normalized if normalized in _LAYER_CONTAINER_ENV else None


def _fallback_normalize_domain(value: str) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"market", "finance", "earnings", "price-target"} else None


def _fallback_resolve_container(layer: str) -> str:
    env_key = _LAYER_CONTAINER_ENV.get(layer)
    if not env_key:
        raise HTTPException(status_code=400, detail=f"Unknown layer '{layer}'.")
    container = os.environ.get(env_key, "").strip()
    if not container:
        raise HTTPException(status_code=503, detail=f"Missing {env_key} for domain columns.")
    return container


class DomainColumnsResponse(BaseModel):
    layer: str
    domain: str
    columns: List[str] = Field(default_factory=list)
    found: bool = False
    promptRetrieve: bool = False
    source: Literal["common-file", "artifact"] = "common-file"
    cachePath: str
    updatedAt: Optional[str] = None


class DomainColumnsRefreshRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    sample_limit: int = Field(default=500, ge=1, le=5000)


_DOMAIN_COLUMNS_CACHE_FILE_DEFAULT = "metadata/domain-columns.json"
_DOMAIN_COLUMNS_READ_TIMEOUT_SECONDS_DEFAULT = 8.0
_DOMAIN_COLUMNS_REFRESH_TIMEOUT_SECONDS_DEFAULT = 25.0


def _domain_columns_cache_path() -> str:
    configured = (os.environ.get("DOMAIN_COLUMNS_CACHE_PATH") or "").strip()
    return configured or _DOMAIN_COLUMNS_CACHE_FILE_DEFAULT


def _parse_timeout_seconds_env(env_name: str, default_value: float) -> float:
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return float(default_value)
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Invalid %s=%s. Using default=%s", env_name, raw, default_value)
        return float(default_value)
    if value <= 0:
        return float(default_value)
    return value


def _domain_columns_read_timeout_seconds() -> float:
    return _parse_timeout_seconds_env(
        "DOMAIN_COLUMNS_READ_TIMEOUT_SECONDS",
        _DOMAIN_COLUMNS_READ_TIMEOUT_SECONDS_DEFAULT,
    )


def _domain_columns_refresh_timeout_seconds() -> float:
    return _parse_timeout_seconds_env(
        "DOMAIN_COLUMNS_REFRESH_TIMEOUT_SECONDS",
        _DOMAIN_COLUMNS_REFRESH_TIMEOUT_SECONDS_DEFAULT,
    )


def _run_with_timeout(fn: Callable[[], _T], *, timeout_seconds: float, timeout_message: str) -> _T:
    if timeout_seconds <= 0:
        return fn()

    done = threading.Event()
    state: Dict[str, Any] = {}

    def _worker() -> None:
        try:
            state["result"] = fn()
        except Exception as exc:
            state["error"] = exc
        finally:
            done.set()

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    if not done.wait(timeout_seconds):
        raise TimeoutError(timeout_message)

    if "error" in state:
        raise state["error"]
    return state["result"]


def _require_common_storage_for_domain_columns() -> None:
    if getattr(mdc, "common_storage_client", None) is None:
        raise HTTPException(
            status_code=503,
            detail="Common storage is unavailable (AZURE_CONTAINER_COMMON).",
        )


def _normalize_columns_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values:
        column = str(value or "").strip()
        if not column or column in seen:
            continue
        seen.add(column)
        normalized.append(column)
    return normalized


def _read_domain_columns_from_artifact(layer: str, domain: str) -> tuple[List[str], Optional[str], bool, Optional[str]]:
    artifact = domain_artifacts.load_domain_artifact(layer=layer, domain=domain)
    if not isinstance(artifact, dict):
        return [], None, False, None
    columns = _normalize_columns_list(artifact.get("columns"))
    updated_at = artifact.get("updatedAt") or artifact.get("computedAt")
    artifact_path = artifact.get("artifactPath")
    return (
        columns,
        str(updated_at) if isinstance(updated_at, str) else None,
        bool(columns),
        str(artifact_path) if isinstance(artifact_path, str) and artifact_path.strip() else None,
    )


def _domain_columns_cache_key(layer: str, domain: str) -> str:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    normalized_layer = normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = normalize_domain(domain) or str(domain or "").strip().lower()
    return f"{normalized_layer}/{normalized_domain}"


def _default_domain_columns_document() -> Dict[str, Any]:
    return {"version": 1, "updatedAt": None, "entries": {}}


def _load_domain_columns_document() -> Dict[str, Any]:
    path = _domain_columns_cache_path()
    payload = mdc.get_common_json_content(path)
    if not isinstance(payload, dict):
        return _default_domain_columns_document()

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    return payload


def _read_cached_domain_columns(layer: str, domain: str) -> tuple[List[str], Optional[str], bool]:
    key = _domain_columns_cache_key(layer, domain)
    payload = _load_domain_columns_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return [], None, False

    raw_entry = entries.get(key)
    if isinstance(raw_entry, list):
        columns = _normalize_columns_list(raw_entry)
        updated_at = payload.get("updatedAt")
        return columns, (str(updated_at) if isinstance(updated_at, str) else None), bool(columns)
    if not isinstance(raw_entry, dict):
        return [], None, False

    columns = _normalize_columns_list(raw_entry.get("columns"))
    updated_at = raw_entry.get("updatedAt")
    return columns, (str(updated_at) if isinstance(updated_at, str) else None), bool(columns)


def _write_cached_domain_columns(layer: str, domain: str, columns: List[str]) -> tuple[List[str], str]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    utc_timestamp = _system_attr("_utc_timestamp", None)

    normalized_columns = _normalize_columns_list(columns)
    if not normalized_columns:
        raise ValueError("No columns were discovered for cache update.")

    normalized_layer = normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = normalize_domain(domain) or str(domain or "").strip().lower()
    key = _domain_columns_cache_key(normalized_layer, normalized_domain)

    payload = _load_domain_columns_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        payload["entries"] = entries

    now = utc_timestamp() if callable(utc_timestamp) else datetime.now(timezone.utc).isoformat()
    entries[key] = {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "columns": normalized_columns,
        "updatedAt": now,
    }
    payload["version"] = 1
    payload["updatedAt"] = now
    mdc.save_common_json_content(payload, _domain_columns_cache_path())
    return normalized_columns, now


def _discover_first_delta_table_for_prefix(*, container: str, prefix: str) -> Optional[str]:
    normalized = f"{str(prefix or '').strip().strip('/')}/"
    if normalized == "/":
        return None

    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    marker = "/_delta_log/"
    for blob in client.container_client.list_blobs(name_starts_with=normalized):
        name = str(getattr(blob, "name", "") or "")
        if marker not in name:
            continue
        root = name.split(marker, 1)[0].strip("/")
        if root and root.startswith(normalized.rstrip("/")):
            return root
    return None


def _retrieve_domain_columns_from_schema(layer: str, domain: str) -> List[str]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)
    resolve_container = _system_attr("_resolve_container", _fallback_resolve_container)

    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    if normalized_layer not in {"silver", "gold"}:
        return []
    if not normalized_domain:
        return []

    prefix = _RULE_DATA_PREFIXES.get(normalized_layer, {}).get(normalized_domain)
    if not prefix:
        return []

    container = resolve_container(normalized_layer)
    first_table = _discover_first_delta_table_for_prefix(container=container, prefix=prefix)
    if not first_table:
        return []

    schema_columns = get_delta_schema_columns(container, first_table)
    return _normalize_columns_list(schema_columns or [])


def _retrieve_domain_columns(layer: str, domain: str, sample_limit: int) -> List[str]:
    normalize_layer = _system_attr("_normalize_layer", _fallback_normalize_layer)
    normalize_domain = _system_attr("_normalize_domain", _fallback_normalize_domain)

    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    if normalized_layer not in {"bronze", "silver", "gold"}:
        raise HTTPException(status_code=400, detail="layer must be bronze, silver, or gold.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")

    try:
        schema_columns = _retrieve_domain_columns_from_schema(normalized_layer, normalized_domain)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "Schema-first column retrieval failed; falling back to sampled retrieval. layer=%s domain=%s err=%s",
            normalized_layer,
            normalized_domain,
            exc,
        )
        schema_columns = []

    if schema_columns:
        return schema_columns

    try:
        from api.data_service import DataService
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Data service unavailable: {exc}") from exc

    try:
        rows = DataService.get_data(
            layer=normalized_layer,
            domain=normalized_domain,
            ticker=None,
            limit=int(sample_limit),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Domain columns retrieval failed: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve domain columns: {exc}") from exc

    for row in rows or []:
        if isinstance(row, dict) and row:
            return _normalize_columns_list(list(row.keys()))
    return []


_domain_columns_cache_path_impl = _domain_columns_cache_path
_domain_columns_cache_path = _compat_export("_domain_columns_cache_path", _domain_columns_cache_path_impl)
_parse_timeout_seconds_env_impl = _parse_timeout_seconds_env
_parse_timeout_seconds_env = _compat_export("_parse_timeout_seconds_env", _parse_timeout_seconds_env_impl)
_domain_columns_read_timeout_seconds_impl = _domain_columns_read_timeout_seconds
_domain_columns_read_timeout_seconds = _compat_export(
    "_domain_columns_read_timeout_seconds",
    _domain_columns_read_timeout_seconds_impl,
)
_domain_columns_refresh_timeout_seconds_impl = _domain_columns_refresh_timeout_seconds
_domain_columns_refresh_timeout_seconds = _compat_export(
    "_domain_columns_refresh_timeout_seconds",
    _domain_columns_refresh_timeout_seconds_impl,
)
_run_with_timeout_impl = _run_with_timeout
_run_with_timeout = _compat_export("_run_with_timeout", _run_with_timeout_impl)
_require_common_storage_for_domain_columns_impl = _require_common_storage_for_domain_columns
_require_common_storage_for_domain_columns = _compat_export(
    "_require_common_storage_for_domain_columns",
    _require_common_storage_for_domain_columns_impl,
)
_normalize_columns_list_impl = _normalize_columns_list
_normalize_columns_list = _compat_export("_normalize_columns_list", _normalize_columns_list_impl)
_read_domain_columns_from_artifact_impl = _read_domain_columns_from_artifact
_read_domain_columns_from_artifact = _compat_export(
    "_read_domain_columns_from_artifact",
    _read_domain_columns_from_artifact_impl,
)
_domain_columns_cache_key_impl = _domain_columns_cache_key
_domain_columns_cache_key = _compat_export("_domain_columns_cache_key", _domain_columns_cache_key_impl)
_default_domain_columns_document_impl = _default_domain_columns_document
_default_domain_columns_document = _compat_export(
    "_default_domain_columns_document",
    _default_domain_columns_document_impl,
)
_load_domain_columns_document_impl = _load_domain_columns_document
_load_domain_columns_document = _compat_export("_load_domain_columns_document", _load_domain_columns_document_impl)
_read_cached_domain_columns_impl = _read_cached_domain_columns
_read_cached_domain_columns = _compat_export("_read_cached_domain_columns", _read_cached_domain_columns_impl)
_write_cached_domain_columns_impl = _write_cached_domain_columns
_write_cached_domain_columns = _compat_export("_write_cached_domain_columns", _write_cached_domain_columns_impl)
_discover_first_delta_table_for_prefix_impl = _discover_first_delta_table_for_prefix
_discover_first_delta_table_for_prefix = _compat_export(
    "_discover_first_delta_table_for_prefix",
    _discover_first_delta_table_for_prefix_impl,
)
_retrieve_domain_columns_from_schema_impl = _retrieve_domain_columns_from_schema
_retrieve_domain_columns_from_schema = _compat_export(
    "_retrieve_domain_columns_from_schema",
    _retrieve_domain_columns_from_schema_impl,
)
_retrieve_domain_columns_impl = _retrieve_domain_columns
_retrieve_domain_columns = _compat_export("_retrieve_domain_columns", _retrieve_domain_columns_impl)


def build_router(
    *,
    runtime: ModuleType,
    domain_columns_response_model: Any,
    domain_columns_refresh_request_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/domain-columns", response_model=domain_columns_response_model)
    def get_domain_columns(
        request: Request,
        layer: str = Query(..., description="Medallion layer key (bronze|silver|gold)"),
        domain: str = Query(..., description="Domain key (market|finance|earnings|price-target)"),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        read_domain_columns_from_artifact = _runtime_attr(runtime, "_read_domain_columns_from_artifact")
        domain_artifacts = _runtime_attr(runtime, "domain_artifacts")
        require_common_storage_for_domain_columns = _runtime_attr(
            runtime,
            "_require_common_storage_for_domain_columns",
        )
        domain_columns_read_timeout_seconds = _runtime_attr(runtime, "_domain_columns_read_timeout_seconds")
        run_with_timeout = _runtime_attr(runtime, "_run_with_timeout")
        read_cached_domain_columns = _runtime_attr(runtime, "_read_cached_domain_columns")
        domain_columns_cache_path = _runtime_attr(runtime, "_domain_columns_cache_path")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        normalized_layer = normalize_layer(layer)
        normalized_domain = normalize_domain(domain)
        if not normalized_layer:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not normalized_domain:
            raise HTTPException(status_code=400, detail="domain is required.")

        try:
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = read_domain_columns_from_artifact(
                normalized_layer,
                normalized_domain,
            )
        except Exception as exc:
            logger.warning(
                "Domain columns artifact read failed: layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = [], None, False, None

        if artifact_found:
            return JSONResponse(
                {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "columns": artifact_columns,
                    "found": True,
                    "promptRetrieve": False,
                    "source": "artifact",
                    "cachePath": artifact_path
                    or domain_artifacts.domain_artifact_path(layer=normalized_layer, domain=normalized_domain),
                    "updatedAt": artifact_updated_at,
                },
                headers={"Cache-Control": "no-store"},
            )

        require_common_storage_for_domain_columns()

        read_timeout = domain_columns_read_timeout_seconds()
        try:
            columns, updated_at, found = run_with_timeout(
                lambda: read_cached_domain_columns(normalized_layer, normalized_domain),
                timeout_seconds=read_timeout,
                timeout_message=(
                    f"Domain columns cache read timed out after {read_timeout:.1f}s for "
                    f"{normalized_layer}/{normalized_domain}."
                ),
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Domain columns cache read failed: layer=%s domain=%s",
                normalized_layer,
                normalized_domain,
            )
            raise HTTPException(status_code=503, detail=f"Domain columns cache unavailable: {exc}") from exc

        return JSONResponse(
            {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "columns": columns,
                "found": found,
                "promptRetrieve": not found,
                "source": "common-file",
                "cachePath": domain_columns_cache_path(),
                "updatedAt": updated_at,
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/domain-columns/refresh", response_model=domain_columns_response_model)
    def refresh_domain_columns(payload: domain_columns_refresh_request_model, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        normalize_layer = _runtime_attr(runtime, "_normalize_layer")
        normalize_domain = _runtime_attr(runtime, "_normalize_domain")
        read_domain_columns_from_artifact = _runtime_attr(runtime, "_read_domain_columns_from_artifact")
        domain_artifacts = _runtime_attr(runtime, "domain_artifacts")
        require_common_storage_for_domain_columns = _runtime_attr(
            runtime,
            "_require_common_storage_for_domain_columns",
        )
        domain_columns_refresh_timeout_seconds = _runtime_attr(
            runtime,
            "_domain_columns_refresh_timeout_seconds",
        )
        run_with_timeout = _runtime_attr(runtime, "_run_with_timeout")
        retrieve_domain_columns = _runtime_attr(runtime, "_retrieve_domain_columns")
        write_cached_domain_columns = _runtime_attr(runtime, "_write_cached_domain_columns")
        domain_columns_cache_path = _runtime_attr(runtime, "_domain_columns_cache_path")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        normalized_layer = normalize_layer(payload.layer)
        normalized_domain = normalize_domain(payload.domain)
        if not normalized_layer:
            raise HTTPException(status_code=400, detail="layer is required.")
        if not normalized_domain:
            raise HTTPException(status_code=400, detail="domain is required.")

        try:
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = read_domain_columns_from_artifact(
                normalized_layer,
                normalized_domain,
            )
        except Exception as exc:
            logger.warning(
                "Domain columns artifact refresh read failed: layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            artifact_columns, artifact_updated_at, artifact_found, artifact_path = [], None, False, None

        if artifact_found:
            return JSONResponse(
                {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "columns": artifact_columns,
                    "found": True,
                    "promptRetrieve": False,
                    "source": "artifact",
                    "cachePath": artifact_path
                    or domain_artifacts.domain_artifact_path(layer=normalized_layer, domain=normalized_domain),
                    "updatedAt": artifact_updated_at,
                },
                headers={"Cache-Control": "no-store"},
            )

        require_common_storage_for_domain_columns()

        refresh_timeout = domain_columns_refresh_timeout_seconds()
        try:
            columns = run_with_timeout(
                lambda: retrieve_domain_columns(normalized_layer, normalized_domain, int(payload.sample_limit)),
                timeout_seconds=refresh_timeout,
                timeout_message=(
                    f"Domain columns retrieval timed out after {refresh_timeout:.1f}s for "
                    f"{normalized_layer}/{normalized_domain}."
                ),
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Domain columns refresh retrieval failed: layer=%s domain=%s",
                normalized_layer,
                normalized_domain,
            )
            raise HTTPException(status_code=503, detail=f"Domain columns retrieval unavailable: {exc}") from exc

        if not columns:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No columns discovered for {normalized_layer}/{normalized_domain}. "
                    "Verify data exists and retry refresh."
                ),
            )

        try:
            cached_columns, updated_at = run_with_timeout(
                lambda: write_cached_domain_columns(normalized_layer, normalized_domain, columns),
                timeout_seconds=refresh_timeout,
                timeout_message=(
                    f"Domain columns cache write timed out after {refresh_timeout:.1f}s for "
                    f"{normalized_layer}/{normalized_domain}."
                ),
            )
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Common storage is unavailable for column cache updates: {exc}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception(
                "Domain columns cache update failed: layer=%s domain=%s",
                normalized_layer,
                normalized_domain,
            )
            raise HTTPException(status_code=500, detail=f"Failed to update domain columns cache: {exc}") from exc

        return JSONResponse(
            {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "columns": cached_columns,
                "found": True,
                "promptRetrieve": False,
                "source": "common-file",
                "cachePath": domain_columns_cache_path(),
                "updatedAt": updated_at,
            },
            headers={"Cache-Control": "no-store"},
        )

    return router, {
        "get_domain_columns": get_domain_columns,
        "refresh_domain_columns": refresh_domain_columns,
    }
