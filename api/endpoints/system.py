import logging
import json
import math
import os
import re
import sys
import threading
import time
import uuid
from io import BytesIO
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Literal, Tuple, TypeVar, Sequence

import httpx
from anyio import from_thread
from fastapi import APIRouter, HTTPException, Request

from api.endpoints.system_modules import container_apps as system_container_apps_routes
from api.endpoints.system_modules import domain_columns as system_domain_columns_routes
from api.endpoints.system_modules import domain_metadata as system_domain_metadata_routes
from api.endpoints.system_modules import jobs as system_jobs_routes
from api.endpoints.system_modules import purge as system_purge_routes
from api.endpoints.system_modules import runtime_ops as system_runtime_ops_routes
from api.endpoints.system_modules import status_read
from api.service.dependencies import (
    get_auth_manager,
    get_settings,
    get_system_health_cache,
    validate_auth,
)
from api.service.realtime import manager as realtime_manager
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.control_plane import collect_jobs_and_executions
from monitoring.domain_metadata import collect_domain_metadata
from monitoring.log_analytics import AzureLogAnalyticsClient, extract_first_table_rows
from monitoring.system_health import collect_system_health_snapshot
from monitoring.ttl_cache import TtlCache
from core import bronze_bucketing
from core import config as cfg
from core import core as mdc
from core import delta_core
from core import domain_artifacts
from core import domain_metadata_snapshots
from core import layer_bucketing
from core.blob_storage import BlobStorageClient
from core.debug_symbols import (
    delete_debug_symbols_state,
    read_debug_symbols_state,
    replace_debug_symbols_state,
)
from core.delta_core import load_delta
from core.delta_core import get_delta_schema_columns
from core.domain_metadata_snapshots import build_snapshot_miss_payload
from core.finance_contracts import SILVER_FINANCE_SUBDOMAINS
from core.pipeline import DataPaths
from core.postgres import PostgresError
from core.purge_rules import (
    PurgeRule,
    claim_purge_rule_for_run,
    complete_purge_rule_execution,
    create_purge_rule,
    is_percent_operator,
    list_due_purge_rules,
    normalize_purge_rule_operator,
)
from core.runtime_config import (
    DEFAULT_ENV_OVERRIDE_KEYS,
    delete_runtime_config,
    list_runtime_config,
    normalize_env_override,
    upsert_runtime_config,
)

# Preserve the historical import surface while route assembly is moved into submodules.
_LEGACY_EXPORTS = (
    json,
    math,
    time,
    deepcopy,
    timedelta,
    httpx,
    get_system_health_cache,
    validate_auth,
    ArmConfig,
    AzureArmClient,
    collect_jobs_and_executions,
    collect_domain_metadata,
    AzureLogAnalyticsClient,
    extract_first_table_rows,
    collect_system_health_snapshot,
    TtlCache,
    domain_artifacts,
    delete_debug_symbols_state,
    read_debug_symbols_state,
    replace_debug_symbols_state,
    get_delta_schema_columns,
    build_snapshot_miss_payload,
    DEFAULT_ENV_OVERRIDE_KEYS,
    delete_runtime_config,
    list_runtime_config,
    normalize_env_override,
    upsert_runtime_config,
)

logger = logging.getLogger("asset-allocation.api.system")

router = APIRouter()


def _system_runtime():
    return sys.modules[__name__]


def _reject_removed_query_params(request: Request, *names: str) -> None:
    removed = [name for name in names if name in request.query_params]
    if removed:
        joined = ", ".join(sorted(removed))
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported query parameter(s): {joined}. Use the canonical request contract instead.",
        )




REALTIME_TOPIC_BACKTESTS = "backtests"
REALTIME_TOPIC_SYSTEM_HEALTH = "system-health"
REALTIME_TOPIC_JOBS = "jobs"
REALTIME_TOPIC_CONTAINER_APPS = "container-apps"
REALTIME_TOPIC_RUNTIME_CONFIG = "runtime-config"
REALTIME_TOPIC_DEBUG_SYMBOLS = "debug-symbols"
_ACTIVE_JOB_EXECUTION_STATUS_TOKENS = frozenset(
    {"running", "processing", "inprogress", "starting", "queued", "waiting", "scheduling"}
)

_PURGE_OPERATIONS: Dict[str, Dict[str, Any]] = {}
_PURGE_OPERATIONS_LOCK = threading.Lock()
_PURGE_BLACKLIST_UPDATE_LOCK = threading.Lock()
_PURGE_RULE_AUDIT_INTERVAL_MINUTES = 60 * 24 * 365
_DEFAULT_PURGE_SYMBOL_MAX_WORKERS = 8
_MAX_PURGE_SYMBOL_MAX_WORKERS = 32
_DEFAULT_PURGE_PREVIEW_LOAD_MAX_WORKERS = 8
_MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS = 32
_DEFAULT_PURGE_SCOPE_MAX_WORKERS = 8
_MAX_PURGE_SCOPE_MAX_WORKERS = 32
_DEFAULT_PURGE_SYMBOL_TARGET_MAX_WORKERS = 8
_MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS = 32
_DEFAULT_PURGE_SYMBOL_LAYER_MAX_WORKERS = 3
_MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS = 3
_T = TypeVar("_T")


def _emit_realtime(topic: str, event_type: str, payload: Optional[Dict[str, Any]] = None) -> None:
    """
    Emit websocket events from sync FastAPI endpoints.

    Endpoints in this module are mostly sync (`def`) and run in AnyIO worker threads.
    `from_thread.run` bridges to the app event loop so connected websocket clients receive updates.
    """
    message = {
        "type": event_type,
        "payload": payload or {},
        "emittedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        from_thread.run(realtime_manager.broadcast, topic, message)
    except RuntimeError:
        logger.debug(
            "Realtime emit skipped (no AnyIO worker context): topic=%s type=%s",
            topic,
            event_type,
        )
    except Exception:
        logger.exception("Realtime emit failed: topic=%s type=%s", topic, event_type)






def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _get_actor(request: Request) -> Optional[str]:
    settings = get_settings(request)
    if settings.anonymous_local_auth_enabled:
        return None
    auth = get_auth_manager(request)
    ctx = auth.authenticate_headers(dict(request.headers))
    if ctx.subject:
        return ctx.subject
    for key in ("preferred_username", "email", "upn"):
        value = ctx.claims.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _job_control_context(request: Request) -> Dict[str, str]:
    actor = _get_actor(request)
    request_id = request.headers.get("x-request-id")
    context: Dict[str, str] = {}
    if actor:
        context["actor"] = actor
    if request_id:
        context["requestId"] = request_id.strip()
    return context


def _split_csv(raw: Optional[str]) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]






_status_read_router, _status_read_exports = status_read.build_router(
    runtime=_system_runtime(),
    symbol_sync_state_response_model=status_read.SymbolSyncStateResponse,
    system_status_view_response_model=status_read.SystemStatusViewResponse,
)
router.include_router(_status_read_router)

# Preserve the legacy import surface for tests while moving route assembly out of this module.
system_health = _status_read_exports["system_health"]
get_symbol_sync_state_endpoint = _status_read_exports["get_symbol_sync_state_endpoint"]
system_status_view = _status_read_exports["system_status_view"]
system_lineage = _status_read_exports["system_lineage"]

SymbolSyncStateResponse = status_read.SymbolSyncStateResponse
SystemStatusViewSources = status_read.SystemStatusViewSources
SystemStatusViewResponse = status_read.SystemStatusViewResponse
_sanitize_system_health_json_value = status_read._sanitize_system_health_json_value
_resolve_system_health_payload = status_read._resolve_system_health_payload
_extract_arm_error_message = status_read._extract_arm_error_message
_normalize_job_name_key = status_read._normalize_job_name_key
_status_view_domain_job_names = status_read._status_view_domain_job_names
_merge_live_job_resources = status_read._merge_live_job_resources
_same_job_run = status_read._same_job_run
_merge_live_job_runs = status_read._merge_live_job_runs
_overlay_live_domain_job_runtime = status_read._overlay_live_domain_job_runtime
build_system_status_view = status_read.build_system_status_view


_domain_metadata_router, _domain_metadata_exports = system_domain_metadata_routes.build_router(
    runtime=_system_runtime(),
    domain_metadata_response_model=system_domain_metadata_routes.DomainMetadataResponse,
    domain_metadata_snapshot_response_model=system_domain_metadata_routes.DomainMetadataSnapshotResponse,
)
router.include_router(_domain_metadata_router)

domain_metadata = _domain_metadata_exports["domain_metadata"]
domain_metadata_snapshot = _domain_metadata_exports["domain_metadata_snapshot"]
get_domain_metadata_snapshot_cache = _domain_metadata_exports["get_domain_metadata_snapshot_cache"]
put_domain_metadata_snapshot_cache = _domain_metadata_exports["put_domain_metadata_snapshot_cache"]

DomainDateRange = system_domain_metadata_routes.DomainDateRange
DomainMetadataResponse = system_domain_metadata_routes.DomainMetadataResponse
DomainMetadataSnapshotResponse = system_domain_metadata_routes.DomainMetadataSnapshotResponse
_normalize_domain_metadata_targets = system_domain_metadata_routes._normalize_domain_metadata_targets
_extract_domain_metadata_targets_from_entries = system_domain_metadata_routes._extract_domain_metadata_targets_from_entries
_emit_domain_metadata_snapshot_changed = system_domain_metadata_routes._emit_domain_metadata_snapshot_changed
_domain_metadata_cache_path = system_domain_metadata_routes._domain_metadata_cache_path
_domain_metadata_ui_cache_path = system_domain_metadata_routes._domain_metadata_ui_cache_path
_domain_metadata_snapshot_cache_ttl_seconds = system_domain_metadata_routes._domain_metadata_snapshot_cache_ttl_seconds
_cache_domain_metadata_document = system_domain_metadata_routes._cache_domain_metadata_document
_invalidate_domain_metadata_document_cache = system_domain_metadata_routes._invalidate_domain_metadata_document_cache
_domain_metadata_cache_key = system_domain_metadata_routes._domain_metadata_cache_key
_default_domain_metadata_document = system_domain_metadata_routes._default_domain_metadata_document
_load_domain_metadata_document = system_domain_metadata_routes._load_domain_metadata_document
_read_cached_domain_metadata_snapshot = system_domain_metadata_routes._read_cached_domain_metadata_snapshot
_write_cached_domain_metadata_snapshot = system_domain_metadata_routes._write_cached_domain_metadata_snapshot
_refresh_domain_metadata_snapshot = system_domain_metadata_routes._refresh_domain_metadata_snapshot
_extract_cached_domain_metadata_snapshots = system_domain_metadata_routes._extract_cached_domain_metadata_snapshots
_parse_domain_metadata_filter = system_domain_metadata_routes._parse_domain_metadata_filter
_build_domain_metadata_snapshot_payload = system_domain_metadata_routes._build_domain_metadata_snapshot_payload



_domain_columns_router, _domain_columns_exports = system_domain_columns_routes.build_router(
    runtime=_system_runtime(),
    domain_columns_response_model=system_domain_columns_routes.DomainColumnsResponse,
    domain_columns_refresh_request_model=system_domain_columns_routes.DomainColumnsRefreshRequest,
)
router.include_router(_domain_columns_router)

get_domain_columns = _domain_columns_exports["get_domain_columns"]
refresh_domain_columns = _domain_columns_exports["refresh_domain_columns"]

DomainColumnsResponse = system_domain_columns_routes.DomainColumnsResponse
DomainColumnsRefreshRequest = system_domain_columns_routes.DomainColumnsRefreshRequest
_domain_columns_cache_path = system_domain_columns_routes._domain_columns_cache_path
_parse_timeout_seconds_env = system_domain_columns_routes._parse_timeout_seconds_env
_domain_columns_read_timeout_seconds = system_domain_columns_routes._domain_columns_read_timeout_seconds
_domain_columns_refresh_timeout_seconds = system_domain_columns_routes._domain_columns_refresh_timeout_seconds
_run_with_timeout = system_domain_columns_routes._run_with_timeout
_require_common_storage_for_domain_columns = system_domain_columns_routes._require_common_storage_for_domain_columns
_normalize_columns_list = system_domain_columns_routes._normalize_columns_list
_read_domain_columns_from_artifact = system_domain_columns_routes._read_domain_columns_from_artifact
_domain_columns_cache_key = system_domain_columns_routes._domain_columns_cache_key
_default_domain_columns_document = system_domain_columns_routes._default_domain_columns_document
_load_domain_columns_document = system_domain_columns_routes._load_domain_columns_document
_read_cached_domain_columns = system_domain_columns_routes._read_cached_domain_columns
_write_cached_domain_columns = system_domain_columns_routes._write_cached_domain_columns
_discover_first_delta_table_for_prefix = system_domain_columns_routes._discover_first_delta_table_for_prefix
_retrieve_domain_columns_from_schema = system_domain_columns_routes._retrieve_domain_columns_from_schema
_retrieve_domain_columns = system_domain_columns_routes._retrieve_domain_columns




_purge_router, _purge_exports = system_purge_routes.build_router(
    runtime=_system_runtime(),
    domain_lists_response_model=system_purge_routes.DomainListsResponse,
    purge_request_model=system_purge_routes.PurgeRequest,
    domain_list_reset_request_model=system_purge_routes.DomainListResetRequest,
    domain_checkpoint_reset_request_model=system_purge_routes.DomainCheckpointResetRequest,
    purge_candidates_request_model=system_purge_routes.PurgeCandidatesRequest,
    purge_symbol_request_model=system_purge_routes.PurgeSymbolRequest,
    purge_symbols_batch_request_model=system_purge_routes.PurgeSymbolsBatchRequest,
    purge_rule_create_request_model=system_purge_routes.PurgeRuleCreateRequest,
    purge_rule_update_request_model=system_purge_routes.PurgeRuleUpdateRequest,
    purge_rule_preview_request_model=system_purge_routes.PurgeRulePreviewRequest,
)
router.include_router(_purge_router)

list_purge_rule_operators = _purge_exports["list_purge_rule_operators"]
list_purge_rules_endpoint = _purge_exports["list_purge_rules_endpoint"]
create_purge_rule_endpoint = _purge_exports["create_purge_rule_endpoint"]
update_purge_rule_endpoint = _purge_exports["update_purge_rule_endpoint"]
delete_purge_rule_endpoint = _purge_exports["delete_purge_rule_endpoint"]
preview_purge_rule = _purge_exports["preview_purge_rule"]
run_purge_rule_now = _purge_exports["run_purge_rule_now"]
purge_data = _purge_exports["purge_data"]
get_domain_lists = _purge_exports["get_domain_lists"]
reset_domain_lists = _purge_exports["reset_domain_lists"]
reset_domain_checkpoints = _purge_exports["reset_domain_checkpoints"]
get_purge_candidates = _purge_exports["get_purge_candidates"]
create_purge_candidates_operation = _purge_exports["create_purge_candidates_operation"]
get_blacklist_symbols_for_purge = _purge_exports["get_blacklist_symbols_for_purge"]
purge_symbols = _purge_exports["purge_symbols"]
purge_symbol = _purge_exports["purge_symbol"]
get_purge_operation = _purge_exports["get_purge_operation"]

PurgeRequest = system_purge_routes.PurgeRequest
DomainListResetRequest = system_purge_routes.DomainListResetRequest
DomainCheckpointResetRequest = system_purge_routes.DomainCheckpointResetRequest
DomainListFileResponse = system_purge_routes.DomainListFileResponse
DomainListsResponse = system_purge_routes.DomainListsResponse
DomainCheckpointTargetResponse = system_purge_routes.DomainCheckpointTargetResponse
DomainCheckpointResetResponse = system_purge_routes.DomainCheckpointResetResponse
PurgeCandidatesRequest = system_purge_routes.PurgeCandidatesRequest
PurgeSymbolRequest = system_purge_routes.PurgeSymbolRequest
PurgeRuleAuditRequest = system_purge_routes.PurgeRuleAuditRequest
PurgeSymbolsBatchRequest = system_purge_routes.PurgeSymbolsBatchRequest
PurgeRuleCreateRequest = system_purge_routes.PurgeRuleCreateRequest
PurgeRuleUpdateRequest = system_purge_routes.PurgeRuleUpdateRequest
PurgeRulePreviewRequest = system_purge_routes.PurgeRulePreviewRequest

def _require_postgres_dsn(request: Request) -> str:
    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")
    return dsn


def _rule_normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


def _serialize_purge_rule(rule: PurgeRule) -> Dict[str, Any]:
    return {
        "id": rule.id,
        "name": rule.name,
        "layer": rule.layer,
        "domain": rule.domain,
        "columnName": rule.column_name,
        "operator": rule.operator,
        "threshold": rule.threshold,
        "runIntervalMinutes": rule.run_interval_minutes,
        "nextRunAt": _iso(rule.next_run_at),
        "lastRunAt": _iso(rule.last_run_at),
        "lastStatus": rule.last_status,
        "lastError": rule.last_error,
        "lastMatchCount": rule.last_match_count,
        "lastPurgeCount": rule.last_purge_count,
        "createdAt": _iso(rule.created_at),
        "updatedAt": _iso(rule.updated_at),
        "createdBy": rule.created_by,
        "updatedBy": rule.updated_by,
    }


def _resolve_purge_rule_table(layer: str, domain: str) -> tuple[str, str]:
    prefix = _RULE_DATA_PREFIXES.get(layer, {}).get(domain)
    if not prefix:
        raise HTTPException(status_code=400, detail=f"Unsupported purge layer/domain: {layer}/{domain}.")
    container = _resolve_container(layer)
    return container, prefix


def _discover_delta_tables_for_prefix(*, container: str, prefix: str) -> List[str]:
    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    normalized = f"{str(prefix or '').strip().strip('/')}/"
    if normalized == "/":
        return []
    roots: set[str] = set()
    for blob_name in client.list_files(name_starts_with=normalized):
        text = str(blob_name or "")
        marker = "/_delta_log/"
        if marker not in text:
            continue
        root = text.split(marker, 1)[0].strip("/")
        if root and root.startswith(normalized.rstrip("/")):
            roots.add(root)
    return sorted(roots)


def _load_rule_frame(layer: str, domain: str) -> pd.DataFrame:
    container, prefix = _resolve_purge_rule_table(layer, domain)
    table_paths = _discover_delta_tables_for_prefix(container=container, prefix=prefix)
    if not table_paths:
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    worker_count = _resolve_purge_preview_load_workers(len(table_paths))
    loaded_by_path: Dict[str, pd.DataFrame] = {}
    if worker_count <= 1:
        for table_path in table_paths:
            try:
                df = load_delta(container=container, path=table_path)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            loaded_by_path[table_path] = df
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-preview-load") as executor:
            future_to_path = {
                executor.submit(load_delta, container=container, path=table_path): table_path for table_path in table_paths
            }
            for future in as_completed(future_to_path):
                table_path = future_to_path[future]
                try:
                    df = future.result()
                except Exception:
                    continue
                if df is None or df.empty:
                    continue
                loaded_by_path[table_path] = df

    # Preserve deterministic ordering regardless of parallel completion order.
    for table_path in table_paths:
        df = loaded_by_path.get(table_path)
        if df is None or df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _resolve_rule_symbol_column(df: pd.DataFrame) -> str:
    for column in df.columns:
        if _rule_normalize_column_name(column) in {"symbol", "ticker"}:
            return str(column)
    raise HTTPException(status_code=400, detail="Dataset does not contain symbol/ticker column.")


def _resolve_rule_value_column(df: pd.DataFrame, raw_column_name: str) -> str:
    target = _rule_normalize_column_name(raw_column_name)
    for column in df.columns:
        if _rule_normalize_column_name(column) == target:
            return str(column)
    raise HTTPException(
        status_code=400,
        detail=f"Column '{raw_column_name}' does not exist in the selected dataset.",
    )


def _resolve_rule_date_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["date", "obsdate", "obs_date", "timestamp", "datetime", "asof", "as_of_date", "tradingdate"]
    normalized_to_name: Dict[str, str] = {_rule_normalize_column_name(column): str(column) for column in df.columns}
    for candidate in candidates:
        column = normalized_to_name.get(_rule_normalize_column_name(candidate))
        if column:
            return column
    return None


def _collect_rule_symbol_values(rule: PurgeRule) -> List[tuple[str, float]]:
    layer = _rule_normalize_column_name(rule.layer)
    domain = rule.domain
    operator = rule.operator
    df = _load_rule_frame(layer, domain)

    if df is None or df.empty:
        return []

    symbol_column = _resolve_rule_symbol_column(df)
    value_column = _resolve_rule_value_column(df, rule.column_name)
    normalized_values = pd.to_numeric(df[value_column], errors="coerce")
    symbols = df[symbol_column].astype("string").str.upper().str.strip()

    work = pd.DataFrame(
        {
            "symbol": symbols,
            "value": normalized_values,
        }
    )
    work = work.dropna(subset=["symbol", "value"]).copy()
    if work.empty:
        return []

    date_column = _resolve_rule_date_column(df)
    if date_column:
        work["date"] = pd.to_datetime(df[date_column], errors="coerce")
        work = work.dropna(subset=["date"]).sort_values("date")
        selected = work.groupby("symbol", as_index=False).tail(1)
    else:
        selected = work.groupby("symbol", as_index=False)["value"].mean()

    selected["value"] = pd.to_numeric(selected["value"], errors="coerce")
    selected = selected.dropna(subset=["value"])
    if selected.empty:
        return []

    symbol_values = {
        str(row["symbol"]): float(row["value"])
        for _, row in selected.iterrows()
        if str(row["symbol"]).strip()
    }
    if not symbol_values:
        return []

    if is_percent_operator(operator):
        percentile = rule.threshold
        values = pd.Series(list(symbol_values.values()), dtype=float)
        if values.empty:
            return []
        if operator == "bottom_percent":
            cutoff = values.quantile(percentile / 100.0)
            return [
                (symbol, value)
                for symbol, value in symbol_values.items()
                if value <= cutoff
            ]
        cutoff = values.quantile(1.0 - (percentile / 100.0))
        return [
            (symbol, value)
            for symbol, value in symbol_values.items()
            if value >= cutoff
        ]

    ops: Dict[str, Any] = {
        "gt": lambda lhs, rhs: lhs > rhs,
        "gte": lambda lhs, rhs: lhs >= rhs,
        "lt": lambda lhs, rhs: lhs < rhs,
        "lte": lambda lhs, rhs: lhs <= rhs,
        "eq": lambda lhs, rhs: lhs == rhs,
        "ne": lambda lhs, rhs: lhs != rhs,
    }
    comparator = ops.get(operator)
    if comparator is None:
        raise HTTPException(status_code=400, detail=f"Unsupported operator '{operator}'.")

    return [
        (symbol, value)
        for symbol, value in symbol_values.items()
        if comparator(value, float(rule.threshold))
    ]


_CANDIDATE_AGGREGATION_ALIASES: Dict[str, str] = {
    "average": "avg",
    "mean": "avg",
    "std": "stddev",
    "stdev": "stddev",
    "std_dev": "stddev",
    "standard_deviation": "stddev",
}
_SUPPORTED_CANDIDATE_AGGREGATIONS = {"min", "max", "avg", "stddev"}


def _normalize_candidate_aggregation(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    resolved = _CANDIDATE_AGGREGATION_ALIASES.get(normalized, normalized)
    if resolved not in _SUPPORTED_CANDIDATE_AGGREGATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported aggregation '{value}'. Supported: avg, min, max, stddev.",
        )
    return resolved


def _aggregate_series(values: pd.Series, aggregation: str) -> float:
    if aggregation == "min":
        return float(values.min())
    if aggregation == "max":
        return float(values.max())
    if aggregation == "stddev":
        # Use population stddev so a single-row window is deterministic (0.0).
        return float(values.std(ddof=0))
    return float(values.mean())


def _collect_purge_candidates(
    layer: str,
    domain: str,
    column: str,
    operator: str,
    raw_value: float,
    as_of: Optional[str] = None,
    min_rows: int = 1,
    recent_rows: int = 1,
    aggregation: str = "avg",
    limit: Optional[int] = None,
    offset: int = 0,
) -> tuple[List[Dict[str, Any]], int, int, int]:
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    if not normalized_layer or not normalized_domain:
        raise HTTPException(status_code=400, detail="layer and domain are required.")

    operator = normalize_purge_rule_operator(operator)
    threshold = float(raw_value)
    if not pd.notna(threshold) or not pd.api.types.is_number(threshold):
        raise HTTPException(status_code=400, detail="value must be a finite number.")
    recent_rows = int(recent_rows)
    if recent_rows < 1:
        raise HTTPException(status_code=400, detail="recent_rows must be >= 1.")
    resolved_aggregation = _normalize_candidate_aggregation(aggregation)

    df = _load_rule_frame(normalized_layer, normalized_domain)

    if df is None or df.empty:
        return [], 0, 0, 0

    symbol_column = _resolve_rule_symbol_column(df)
    value_column = _resolve_rule_value_column(df, column)
    rows = pd.to_numeric(df[value_column], errors="coerce")
    work = pd.DataFrame(
        {
            "symbol": df[symbol_column].astype("string").str.upper().str.strip(),
            "value": rows,
        }
    )

    date_column = _resolve_rule_date_column(df)
    if date_column:
        work["asOf"] = pd.to_datetime(df[date_column], errors="coerce")
        work = work.dropna(subset=["symbol", "value", "asOf"]).copy()
        if as_of:
            as_of_dt = pd.to_datetime(as_of, errors="coerce")
            if pd.isna(as_of_dt):
                raise HTTPException(status_code=400, detail=f"Invalid as_of value '{as_of}'.")
            work = work.loc[work["asOf"] <= as_of_dt]

        if work.empty:
            return [], 0, 0, 0

        work = work.sort_values(["symbol", "asOf"]).reset_index(drop=True)
        windowed = work.groupby("symbol", as_index=False, group_keys=False).tail(recent_rows)
        rows_per_symbol = windowed.groupby("symbol", as_index=False).size().rename(columns={"size": "rowsContributing"})
        latest = (
            windowed.groupby("symbol", as_index=False)
            .agg(
                value=("value", lambda series: _aggregate_series(series.astype(float), resolved_aggregation)),
                asOf=("asOf", "max"),
            )
            .merge(rows_per_symbol, on="symbol", how="left")
        )
    else:
        work = work.dropna(subset=["symbol", "value"]).copy()
        if work.empty:
            return [], 0, 0, 0

        windowed = work.groupby("symbol", as_index=False, group_keys=False).tail(recent_rows)
        latest = windowed.groupby("symbol", as_index=False).agg(
            value=("value", lambda series: _aggregate_series(series.astype(float), resolved_aggregation)),
            rowsContributing=("value", "size"),
        )
        latest["asOf"] = None

    latest["value"] = pd.to_numeric(latest["value"], errors="coerce")
    latest = latest.dropna(subset=["symbol", "value"])
    if latest.empty:
        return [], len(df), 0, 0

    if is_percent_operator(operator):
        if not (1 <= threshold <= 100):
            raise HTTPException(status_code=400, detail="Percent threshold must be between 1 and 100.")
        values = latest["value"].astype(float)
        if values.empty:
            return [], len(df), 0, 0

        if operator == "bottom_percent":
            cutoff = float(values.quantile(threshold / 100.0))
            latest = latest.loc[latest["value"] <= cutoff]
        else:
            cutoff = float(values.quantile(1.0 - (threshold / 100.0)))
            latest = latest.loc[latest["value"] >= cutoff]
    else:
        ops: Dict[str, Any] = {
            "gt": lambda lhs, rhs: lhs > rhs,
            "gte": lambda lhs, rhs: lhs >= rhs,
            "lt": lambda lhs, rhs: lhs < rhs,
            "lte": lambda lhs, rhs: lhs <= rhs,
            "eq": lambda lhs, rhs: lhs == rhs,
            "ne": lambda lhs, rhs: lhs != rhs,
        }
        comparator = ops.get(operator)
        if comparator is None:
            raise HTTPException(status_code=400, detail=f"Unsupported operator '{operator}'.")
        latest = latest.loc[latest.apply(lambda row: bool(comparator(float(row["value"]), threshold)), axis=1)]

    if latest.empty:
        return [], len(df), 0, 0

    latest = latest.loc[latest["rowsContributing"] >= int(min_rows)]
    if latest.empty:
        return [], len(df), 0, 0

    latest["rowsContributing"] = pd.to_numeric(latest["rowsContributing"], errors="coerce").fillna(0).astype(int)
    latest = latest.sort_values("value", ascending=False).reset_index(drop=True)

    matched_value_total = int(latest["rowsContributing"].sum()) if "rowsContributing" in latest else 0
    total = int(len(latest))
    if limit is None:
        window = latest.iloc[offset:]
    else:
        window = latest.iloc[offset : offset + int(limit)]

    matches: List[Dict[str, Any]] = []
    for _, row in window.iterrows():
        matched_value = row["value"]
        as_of_value = row.get("asOf")
        matches.append(
            {
                "symbol": str(row["symbol"]),
                "matchedValue": float(matched_value),
                "rowsContributing": int(row["rowsContributing"]),
                "latestAsOf": _iso(as_of_value.to_pydatetime()) if pd.notna(as_of_value) else None,
            }
        )

    return matches, len(df), total, matched_value_total


def _build_purge_candidates_response(
    *,
    layer: str,
    domain: str,
    column: str,
    operator: str,
    value: Optional[float],
    percentile: Optional[float],
    as_of: Optional[str],
    recent_rows: int,
    aggregation: str,
    limit: Optional[int],
    offset: int,
    min_rows: int,
) -> Dict[str, Any]:
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    resolved_column = str(column or "").strip()
    if not normalized_layer:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")
    if not resolved_column:
        raise HTTPException(status_code=400, detail="column is required.")

    normalized_operator = normalize_purge_rule_operator(operator)
    normalized_aggregation = _normalize_candidate_aggregation(aggregation)
    raw_value = percentile if is_percent_operator(normalized_operator) else value
    if raw_value is None:
        raise HTTPException(
            status_code=400,
            detail="value is required for numeric operators; percentile is required for top/bottom percent operators.",
        )
    if is_percent_operator(normalized_operator) and percentile is None:
        raw_value = value
        if raw_value is None:
            raise HTTPException(status_code=400, detail="percentile is required for percent operators.")

    candidate_layer = "silver" if normalized_layer == "bronze" else normalized_layer
    matches, total_rows, matched, contrib = _collect_purge_candidates(
        layer=candidate_layer,
        domain=normalized_domain,
        column=resolved_column,
        operator=normalized_operator,
        raw_value=float(raw_value),
        as_of=as_of,
        min_rows=min_rows,
        recent_rows=recent_rows,
        aggregation=normalized_aggregation,
        limit=limit,
        offset=offset,
    )

    criteria = {
        "requestedLayer": normalized_layer,
        "resolvedLayer": candidate_layer,
        "domain": normalized_domain,
        "column": resolved_column,
        "operator": normalized_operator,
        "value": float(raw_value),
        "asOf": as_of,
        "minRows": min_rows,
        "recentRows": recent_rows,
        "aggregation": normalized_aggregation,
    }
    expression = _build_purge_expression(
        resolved_column,
        normalized_operator,
        float(raw_value),
        recent_rows=recent_rows,
        aggregation=normalized_aggregation,
    )
    return {
        "criteria": criteria,
        "expression": expression,
        "summary": {
            "totalRowsScanned": total_rows,
            "symbolsMatched": matched,
            "rowsContributing": contrib,
            "estimatedDeletionTargets": matched,
        },
        "symbols": matches,
        "offset": offset,
        "limit": limit if limit is not None else len(matches),
        "total": matched,
        "hasMore": bool(limit is not None and (offset + len(matches) < matched)),
        "note": (
            "Bronze preview uses silver dataset for ranking; bronze-wide criteria are supported for runtime purge targets only."
            if normalized_layer == "bronze"
            else None
        ),
    }


def _build_purge_expression(
    column: str,
    operator: str,
    value: float,
    *,
    recent_rows: int = 1,
    aggregation: str = "avg",
) -> str:
    operator = normalize_purge_rule_operator(operator)
    display_value = float(value)
    resolved_aggregation = _normalize_candidate_aggregation(aggregation)
    metric = (
        str(column)
        if int(recent_rows) == 1 and resolved_aggregation == "avg"
        else f"{resolved_aggregation}({column}) over last {int(recent_rows)} rows"
    )
    if operator == "gt":
        return f"{metric} > {display_value:g}"
    if operator == "gte":
        return f"{metric} >= {display_value:g}"
    if operator == "lt":
        return f"{metric} < {display_value:g}"
    if operator == "lte":
        return f"{metric} <= {display_value:g}"
    if operator == "eq":
        return f"{metric} == {display_value:g}"
    if operator == "ne":
        return f"{metric} != {display_value:g}"
    if operator == "top_percent":
        return f"top {display_value:g}% by {metric}"
    if operator == "bottom_percent":
        return f"bottom {display_value:g}% by {metric}"
    return f"{metric} {operator} {display_value:g}"


def _persist_purge_symbols_audit_rule(
    *,
    dsn: str,
    audit_rule: PurgeRuleAuditRequest,
    actor: Optional[str],
) -> PurgeRule:
    normalized_layer = _normalize_layer(audit_rule.layer)
    normalized_domain = _normalize_domain(audit_rule.domain)
    if not normalized_layer or not normalized_domain:
        raise HTTPException(status_code=400, detail="audit_rule.layer and audit_rule.domain are required.")

    resolved_column = str(audit_rule.column_name or "").strip()
    if not resolved_column:
        raise HTTPException(status_code=400, detail="audit_rule.column_name is required.")

    normalized_operator = normalize_purge_rule_operator(audit_rule.operator)
    threshold = float(audit_rule.threshold)
    if not pd.notna(threshold) or threshold in {float("inf"), float("-inf")}:
        raise HTTPException(status_code=400, detail="audit_rule.threshold must be a finite number.")
    if is_percent_operator(normalized_operator) and not (0 <= threshold <= 100):
        raise HTTPException(
            status_code=400,
            detail="audit_rule.threshold must be between 0 and 100 for percentile operators.",
        )

    recent_rows = int(audit_rule.recent_rows or 1)
    normalized_aggregation = _normalize_candidate_aggregation(audit_rule.aggregation or "avg")
    expression = str(audit_rule.expression or "").strip() or _build_purge_expression(
        resolved_column,
        normalized_operator,
        threshold,
        recent_rows=recent_rows,
        aggregation=normalized_aggregation,
    )

    details: List[str] = []
    if audit_rule.matched_symbol_count is not None:
        details.append(f"matched={int(audit_rule.matched_symbol_count)}")
    if audit_rule.selected_symbol_count is not None:
        details.append(f"selected={int(audit_rule.selected_symbol_count)}")
    detail_suffix = f" ({', '.join(details)})" if details else ""
    audit_name = f"audit {normalized_layer}/{normalized_domain}: {expression}{detail_suffix}"

    try:
        return create_purge_rule(
            dsn=dsn,
            name=audit_name,
            layer=normalized_layer,
            domain=normalized_domain,
            column_name=resolved_column,
            operator=normalized_operator,
            threshold=threshold,
            run_interval_minutes=_PURGE_RULE_AUDIT_INTERVAL_MINUTES,
            actor=actor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid audit_rule payload: {exc}") from exc
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to persist audit purge rule: {exc}") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Failed to persist audit purge rule: layer=%s domain=%s column=%s operator=%s",
            normalized_layer,
            normalized_domain,
            resolved_column,
            normalized_operator,
        )
        raise HTTPException(status_code=500, detail=f"Failed to persist audit purge rule: {exc}") from exc


def _normalize_candidate_symbols(symbols: List[str]) -> List[str]:
    seen = set()
    normalized: List[str] = []
    for symbol in symbols:
        normalized_symbol = _normalize_purge_symbol(symbol)
        if normalized_symbol in seen:
            continue
        seen.add(normalized_symbol)
        normalized.append(normalized_symbol)
    if not normalized:
        raise HTTPException(status_code=400, detail="At least one unique symbol is required.")
    return normalized


def _resolve_purge_symbol_workers(symbol_count: int) -> int:
    if symbol_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SYMBOL_MAX_WORKERS, symbol_count)
    raw = str(os.environ.get("PURGE_SYMBOL_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SYMBOL_MAX_WORKERS))
    return min(symbol_count, bounded)


def _resolve_purge_preview_load_workers(table_count: int) -> int:
    if table_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_PREVIEW_LOAD_MAX_WORKERS, table_count)
    raw = str(os.environ.get("PURGE_PREVIEW_LOAD_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS))
    return min(table_count, bounded)


def _resolve_purge_scope_workers(target_count: int) -> int:
    if target_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SCOPE_MAX_WORKERS, target_count)
    raw = str(os.environ.get("PURGE_SCOPE_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SCOPE_MAX_WORKERS))
    return min(target_count, bounded)


def _resolve_purge_symbol_target_workers(target_count: int) -> int:
    if target_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SYMBOL_TARGET_MAX_WORKERS, target_count)
    raw = str(os.environ.get("PURGE_SYMBOL_TARGET_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS))
    return min(target_count, bounded)


def _resolve_purge_symbol_layer_workers(layer_count: int) -> int:
    if layer_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SYMBOL_LAYER_MAX_WORKERS, layer_count)
    raw = str(os.environ.get("PURGE_SYMBOL_LAYER_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS))
    return min(layer_count, bounded)


def _run_symbol_cleanup_tasks(
    tasks: List[Tuple[Dict[str, Any], Callable[[], int]]], *, worker_count: int, thread_name_prefix: str
) -> List[Dict[str, Any]]:
    if not tasks:
        return []

    results_by_index: Dict[int, Dict[str, Any]] = {}
    if worker_count <= 1:
        for idx, (base, work) in enumerate(tasks):
            deleted = int(work())
            item = dict(base)
            item["deleted"] = deleted
            results_by_index[idx] = item
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=thread_name_prefix) as executor:
            future_to_index: Dict[Any, Tuple[int, Dict[str, Any]]] = {
                executor.submit(work): (idx, base) for idx, (base, work) in enumerate(tasks)
            }
            for future in as_completed(future_to_index):
                idx, base = future_to_index[future]
                deleted = int(future.result())
                item = dict(base)
                item["deleted"] = deleted
                results_by_index[idx] = item

    return [results_by_index[idx] for idx in range(len(tasks))]


def _build_purge_symbols_summary(
    *,
    symbols: List[str],
    scope_note: Optional[str],
    dry_run: bool,
    succeeded: int,
    failed: int,
    skipped: int,
    total_deleted: int,
    symbol_results: List[Dict[str, Any]],
    in_progress: int = 0,
) -> Dict[str, Any]:
    requested = len(symbols)
    completed = int(succeeded) + int(failed) + int(skipped)
    pending = max(0, requested - completed - max(0, int(in_progress)))
    progress_pct = float((completed / requested) * 100.0) if requested > 0 else 100.0
    return {
        "scope": "symbols",
        "dryRun": bool(dry_run),
        "scopeNote": scope_note,
        "requestedSymbols": symbols,
        "requestedSymbolCount": requested,
        "completed": completed,
        "pending": pending,
        "inProgress": max(0, int(in_progress)),
        "progressPct": round(progress_pct, 2),
        "succeeded": int(succeeded),
        "failed": int(failed),
        "skipped": int(skipped),
        "totalDeleted": int(total_deleted),
        "symbolResults": list(symbol_results),
    }


def _create_purge_symbols_operation(
    symbols: List[str],
    actor: Optional[str],
    *,
    scope_note: Optional[str],
    dry_run: bool,
    audit_rule_id: Optional[int] = None,
) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    initial_summary = _build_purge_symbols_summary(
        symbols=symbols,
        scope_note=scope_note,
        dry_run=bool(dry_run),
        succeeded=0,
        failed=0,
        skipped=0,
        total_deleted=0,
        symbol_results=[],
        in_progress=0,
    )
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": "symbols",
            "requestedBy": actor,
            "symbols": symbols,
            "symbolCount": len(symbols),
            "scopeNote": scope_note,
            "dryRun": bool(dry_run),
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": initial_summary,
            "error": None,
            "auditRuleId": int(audit_rule_id) if audit_rule_id else None,
        }
    return operation_id


def _execute_purge_symbols_operation(
    operation_id: str,
    symbols: List[str],
    *,
    dry_run: bool,
    scope_note: Optional[str],
) -> None:
    symbol_results: List[Dict[str, Any]] = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_deleted = 0

    def _publish_progress(*, in_progress: int) -> None:
        summary = _build_purge_symbols_summary(
            symbols=symbols,
            scope_note=scope_note,
            dry_run=bool(dry_run),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            total_deleted=total_deleted,
            symbol_results=symbol_results,
            in_progress=in_progress,
        )
        _update_purge_operation(
            operation_id,
            {"status": "running", "result": summary},
        )

    _publish_progress(in_progress=0)

    if dry_run:
        for index, symbol in enumerate(symbols, start=1):
            symbol_results.append(
                {
                    "symbol": symbol,
                    "status": "skipped",
                    "deleted": 0,
                    "dryRun": True,
                }
            )
            skipped += 1
            _publish_progress(in_progress=0)
            logger.info(
                "Purge-symbols dry-run progress: operation=%s completed=%s/%s",
                operation_id,
                index,
                len(symbols),
            )
    else:
        worker_count = _resolve_purge_symbol_workers(len(symbols))
        logger.info(
            "Purge-symbols operation started: operation=%s symbols=%s workers=%s",
            operation_id,
            len(symbols),
            worker_count,
        )
        in_progress = len(symbols)
        _publish_progress(in_progress=in_progress)
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-symbols") as executor:
            future_to_symbol = {
                executor.submit(
                    _run_purge_symbol_operation,
                    PurgeSymbolRequest(symbol=symbol, confirm=True),
                ): symbol
                for symbol in symbols
            }
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                in_progress = max(0, in_progress - 1)
                try:
                    result = future.result()
                    deleted = int(result.get("totalDeleted") or 0)
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "status": "succeeded",
                            "deleted": deleted,
                            "targets": result.get("targets") or [],
                        }
                    )
                    total_deleted += deleted
                    succeeded += 1
                except HTTPException as exc:
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "status": "failed",
                            "deleted": 0,
                            "error": str(exc.detail),
                        }
                    )
                    failed += 1
                except Exception as exc:
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "status": "failed",
                            "deleted": 0,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    failed += 1
                _publish_progress(in_progress=in_progress)
                logger.info(
                    "Purge-symbols progress: operation=%s completed=%s/%s succeeded=%s failed=%s in_progress=%s",
                    operation_id,
                    len(symbol_results),
                    len(symbols),
                    succeeded,
                    failed,
                    in_progress,
                )

    summary = _build_purge_symbols_summary(
        symbols=symbols,
        scope_note=scope_note,
        dry_run=bool(dry_run),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        total_deleted=total_deleted,
        symbol_results=symbol_results,
        in_progress=0,
    )
    status = "failed" if failed > 0 else "succeeded"

    logger.info(
        "Purge-symbols operation finished: operation=%s symbols=%s succeeded=%s failed=%s skipped=%s dry_run=%s",
        operation_id,
        len(symbols),
        succeeded,
        failed,
        skipped,
        bool(dry_run),
    )

    if status == "succeeded":
        _update_purge_operation(
            operation_id,
            {"status": "succeeded", "result": summary, "completedAt": _utc_timestamp()},
        )
    else:
        operation_error = "One or more symbols failed."
        _update_purge_operation(
            operation_id,
            {"status": "failed", "result": summary, "error": operation_error, "completedAt": _utc_timestamp()},
        )


def _execute_purge_rule(rule: PurgeRule, *, actor: Optional[str]) -> Dict[str, Any]:
    symbol_values = _collect_rule_symbol_values(rule)
    matches = sorted(symbol_values, key=lambda item: str(item[0]))
    matched_symbols = [symbol for symbol, _ in matches]
    matched_count = len(matched_symbols)
    purged_count = 0
    failed: List[str] = []
    if not matched_symbols:
        return {
            "ruleId": rule.id,
            "ruleName": rule.name,
            "matchedCount": matched_count,
            "purgedCount": purged_count,
            "symbols": [],
            "failedSymbols": [],
        }

    for symbol, metric in matches:
        try:
            payload = PurgeSymbolRequest(symbol=symbol, confirm=True)
            result = _run_purge_symbol_operation(payload)
            purged_count += int(result.get("totalDeleted") or 0)
        except HTTPException as exc:
            failed.append(f"{symbol}: {exc.detail}")
        except Exception as exc:
            failed.append(f"{symbol}: {type(exc).__name__}: {exc}")

    status = "failed" if failed else "succeeded"
    logger.info(
        "Purge rule executed: id=%s name=%s actor=%s matched=%s purged=%s status=%s",
        rule.id,
        rule.name,
        actor or "-",
        matched_count,
        purged_count,
        status,
    )
    return {
        "ruleId": rule.id,
        "ruleName": rule.name,
        "matchedCount": matched_count,
        "purgedCount": purged_count,
        "symbols": matched_symbols,
        "failedSymbols": failed,
    }
_FINANCE_BRONZE_TABLE_TYPES: List[Tuple[str, str]] = [
    # Bronze finance raw files are written to title-cased folders with spaces.
    # Keep these names aligned with tasks/finance_data/bronze_finance_data.py::REPORTS.
    ("Balance Sheet", "quarterly_balance-sheet"),
    ("Income Statement", "quarterly_financials"),
    ("Cash Flow", "quarterly_cash-flow"),
    ("Valuation", "quarterly_valuation_measures"),
]

_FINANCE_BRONZE_FOLDER_ALIASES: Dict[str, Tuple[str, ...]] = {
    "Balance Sheet": ("Balance Sheet", "balance_sheet"),
    "Income Statement": ("Income Statement", "income_statement"),
    "Cash Flow": ("Cash Flow", "cash_flow"),
    "Valuation": ("Valuation", "valuation"),
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


def _normalize_purge_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="symbol is required.")
    return normalized


def _market_symbol(symbol: str) -> str:
    return _normalize_purge_symbol(symbol).replace(".", "-")


def _symbol_variants(symbol: str) -> List[str]:
    normalized = _normalize_purge_symbol(symbol)
    market_symbol = normalized.replace(".", "-")
    variants = [normalized]
    if market_symbol != normalized:
        variants.append(market_symbol)
    return variants


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _create_purge_operation(
    payload: PurgeRequest,
    actor: Optional[str],
) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": payload.scope,
            "layer": payload.layer,
            "domain": payload.domain,
            "requestedBy": actor,
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": None,
            "error": None,
        }
    return operation_id


def _create_purge_candidates_operation(payload: PurgeCandidatesRequest, actor: Optional[str]) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": "candidate-preview",
            "layer": payload.layer,
            "domain": payload.domain,
            "requestedBy": actor,
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": None,
            "error": None,
        }
    return operation_id


def _execute_purge_candidates_operation(operation_id: str, payload: PurgeCandidatesRequest) -> None:
    started = datetime.now(timezone.utc)
    try:
        result = _build_purge_candidates_response(
            layer=payload.layer,
            domain=payload.domain,
            column=payload.column,
            operator=payload.operator,
            value=payload.value,
            percentile=payload.percentile,
            as_of=payload.as_of,
            recent_rows=payload.recent_rows,
            aggregation=payload.aggregation,
            limit=payload.limit,
            offset=payload.offset,
            min_rows=payload.min_rows,
        )
        duration_ms = max(0, int((datetime.now(timezone.utc) - started).total_seconds() * 1000))
        summary = result.get("summary") if isinstance(result, dict) else {}
        logger.info(
            "Purge-candidates operation succeeded: operation=%s layer=%s domain=%s durationMs=%s totalRowsScanned=%s symbolsMatched=%s",
            operation_id,
            payload.layer,
            payload.domain,
            duration_ms,
            (summary or {}).get("totalRowsScanned"),
            (summary or {}).get("symbolsMatched"),
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "succeeded",
                "completedAt": _utc_timestamp(),
                "result": result,
                "error": None,
            },
        )
    except HTTPException as exc:
        detail = str(exc.detail) if exc.detail is not None else "Purge candidates failed."
        logger.warning(
            "Purge-candidates operation failed: operation=%s layer=%s domain=%s detail=%s",
            operation_id,
            payload.layer,
            payload.domain,
            detail,
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "completedAt": _utc_timestamp(),
                "error": detail,
            },
        )
    except Exception as exc:
        logger.exception(
            "Purge-candidates operation failed: operation=%s layer=%s domain=%s",
            operation_id,
            payload.layer,
            payload.domain,
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "completedAt": _utc_timestamp(),
                "error": f"{type(exc).__name__}: {exc}",
            },
        )


def _get_purge_operation(operation_id: str) -> Optional[Dict[str, Any]]:
    with _PURGE_OPERATIONS_LOCK:
        operation = _PURGE_OPERATIONS.get(operation_id)
        return dict(operation) if operation else None


def _update_purge_operation(operation_id: str, patch: Dict[str, Any]) -> bool:
    with _PURGE_OPERATIONS_LOCK:
        operation = _PURGE_OPERATIONS.get(operation_id)
        if not operation:
            return False
        operation.update(patch)
        operation["updatedAt"] = _utc_timestamp()
        return True


def _normalize_layer(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).strip().lower()


def _normalize_domain(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = str(value).strip().lower().replace("_", "-").replace(" ", "-")
    if cleaned == "targets":
        return "price-target"
    return cleaned


_LAYER_CONTAINER_ENV = {
    "bronze": "AZURE_CONTAINER_BRONZE",
    "silver": "AZURE_CONTAINER_SILVER",
    "gold": "AZURE_CONTAINER_GOLD",
    "platinum": "AZURE_CONTAINER_PLATINUM",
}

_DOMAIN_PREFIXES: Dict[str, Dict[str, List[str]]] = {
    "bronze": {
        "market": ["market-data/"],
        "finance": ["finance-data/"],
        "earnings": ["earnings-data/"],
        "price-target": ["price-target-data/"],
    },
    "silver": {
        "market": ["market-data/"],
        "finance": ["finance-data/"],
        "earnings": ["earnings-data/"],
        "price-target": ["price-target-data/"],
    },
    "gold": {
        "market": ["market/"],
        "finance": ["finance/"],
        "earnings": ["earnings/"],
        "price-target": ["targets/"],
    },
    "platinum": {
        "platinum": ["platinum/"],
    },
}

_SILVER_JOB_CHECKPOINT_KEYS: Dict[str, Tuple[str, str]] = {
    "market": ("bronze_market_data", "silver_market_data"),
    "finance": ("bronze_finance_data", "silver_finance_data"),
    "earnings": ("bronze_earnings_data", "silver_earnings_data"),
    "price-target": ("bronze_price_target_data", "silver_price_target_data"),
}

_GOLD_JOB_WATERMARK_KEYS: Dict[str, str] = {
    "market": "gold_market_features",
    "finance": "gold_finance_features",
    "earnings": "gold_earnings_features",
    "price-target": "gold_price_target_features",
}


def _resolve_container(layer: str) -> str:
    env_key = _LAYER_CONTAINER_ENV.get(layer)
    if not env_key:
        raise HTTPException(status_code=400, detail=f"Unknown layer '{layer}'.")
    container = os.environ.get(env_key, "").strip()
    if not container:
        raise HTTPException(status_code=503, detail=f"Missing {env_key} for purge.")
    return container


def _targets_for_layer_domain(layer: str, domain: str) -> List[Tuple[str, str]]:
    prefixes = _DOMAIN_PREFIXES.get(layer, {}).get(domain, [])
    if not prefixes:
        raise HTTPException(status_code=400, detail=f"Unknown domain '{domain}' for layer '{layer}'.")
    container = _resolve_container(layer)
    return [(container, prefix) for prefix in prefixes]


def _delete_blob_if_exists(client: BlobStorageClient, path: str) -> int:
    if client.file_exists(path):
        client.delete_file(path)
        return 1
    return 0


def _delete_prefix_if_exists(client: BlobStorageClient, path: str) -> int:
    return int(client.delete_prefix(path))


def _bronze_blacklist_paths() -> List[str]:
    earnings_prefix = getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data") or "earnings-data"
    return [
        "market-data/blacklist.csv",
        "finance-data/blacklist.csv",
        f"{earnings_prefix}/blacklist.csv",
        "price-target-data/blacklist.csv",
    ]


def _resolve_domain_list_paths(layer: str, domain: str) -> List[Dict[str, str]]:
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    prefixes = _DOMAIN_PREFIXES.get(layer_norm, {}).get(domain_norm, [])
    if not prefixes:
        raise HTTPException(status_code=400, detail=f"Unknown domain '{domain_norm}' for layer '{layer_norm}'.")

    paths: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for prefix in prefixes:
        base = str(prefix or "").strip().strip("/")
        if not base:
            continue
        for list_type in ("whitelist", "blacklist"):
            path = f"{base}/{list_type}.csv"
            dedupe_key = (list_type, path)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            paths.append({"listType": list_type, "path": path})

    if not paths:
        raise HTTPException(
            status_code=400,
            detail=f"No blacklist/whitelist list paths are configured for layer '{layer_norm}' domain '{domain_norm}'.",
        )
    return paths


def _load_domain_list_file_preview(
    client: BlobStorageClient,
    *,
    list_type: str,
    path: str,
    limit: int,
) -> Dict[str, Any]:
    exists = bool(client.file_exists(path))
    warning: Optional[str] = None
    symbols: List[str] = []

    if exists:
        try:
            loaded_symbols = mdc.load_ticker_list(path, client=client) or []
            symbols = _normalize_symbol_candidates(loaded_symbols)
        except Exception as exc:
            warning = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Domain list load failed: container=%s path=%s error=%s",
                client.container_name,
                path,
                warning,
            )

    truncated = len(symbols) > limit
    preview_symbols = symbols[:limit]
    result: Dict[str, Any] = {
        "listType": list_type,
        "path": path,
        "exists": exists,
        "symbolCount": len(symbols),
        "symbols": preview_symbols,
        "truncated": truncated,
    }
    if warning:
        result["warning"] = warning
    return result


def _reset_domain_lists(client: BlobStorageClient, *, layer: str, domain: str) -> Dict[str, Any]:
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    list_paths = _resolve_domain_list_paths(layer_norm, domain_norm)
    empty_symbols = pd.DataFrame(columns=["Symbol"])
    targets: List[Dict[str, Any]] = []
    for item in list_paths:
        list_type = str(item["listType"]).strip().lower()
        path = str(item["path"]).strip()
        existed = bool(client.file_exists(path))
        try:
            mdc.store_csv(empty_symbols, path, client=client)
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to reset {list_type} list for {layer_norm}/{domain_norm}: {exc}",
            ) from exc
        targets.append({"listType": list_type, "path": path, "status": "reset", "existed": existed})

    return {
        "layer": layer_norm,
        "domain": domain_norm,
        "container": client.container_name,
        "resetCount": len(targets),
        "targets": targets,
        "updatedAt": _utc_timestamp(),
    }


def _reset_domain_checkpoints(*, layer: str, domain: str) -> Dict[str, Any]:
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    if domain_norm not in _DOMAIN_PREFIXES.get(layer_norm, {}):
        raise HTTPException(status_code=400, detail=f"Unknown domain '{domain_norm}' for layer '{layer_norm}'.")

    scope_targets: List[Dict[str, Optional[str]]] = [
        {
            "layer": layer_norm,
            "domain": domain_norm,
            "container": None,
            "prefix": None,
        }
    ]
    raw_targets = [
        *_build_silver_checkpoint_reset_targets(scope_targets),
        *_build_gold_checkpoint_reset_targets(scope_targets),
    ]

    deduped_targets: List[Dict[str, Optional[str]]] = []
    seen: set[Tuple[str, str, str]] = set()
    for target in raw_targets:
        container = str(target.get("container") or "").strip()
        prefix = str(target.get("prefix") or "").strip()
        operation = str(target.get("operation") or "reset-checkpoint").strip() or "reset-checkpoint"
        if not container or not prefix:
            continue
        dedupe_key = (container, prefix, operation)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped_targets.append(
            {
                "container": container,
                "prefix": prefix,
                "operation": operation,
            }
        )

    if not deduped_targets:
        return {
            "layer": layer_norm,
            "domain": domain_norm,
            "container": None,
            "resetCount": 0,
            "deletedCount": 0,
            "targets": [],
            "updatedAt": _utc_timestamp(),
            "note": "No checkpoint gates are configured for this layer/domain.",
        }

    clients: Dict[str, BlobStorageClient] = {}
    results: List[Dict[str, Any]] = []
    deleted_count = 0
    for target in deduped_targets:
        container = str(target["container"])
        prefix = str(target["prefix"])
        operation = str(target["operation"])
        client = clients.get(container)
        if client is None:
            client = BlobStorageClient(container_name=container, ensure_container_exists=False)
            clients[container] = client

        try:
            existed = bool(client.file_exists(prefix))
            deleted = False
            if existed:
                client.delete_file(prefix)
                deleted = True
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to reset checkpoint {operation} for {layer_norm}/{domain_norm}: {exc}",
            ) from exc

        results.append(
            {
                "operation": operation,
                "path": prefix,
                "status": "reset",
                "existed": existed,
                "deleted": deleted,
            }
        )
        if deleted:
            deleted_count += 1

    return {
        "layer": layer_norm,
        "domain": domain_norm,
        "container": str(deduped_targets[0]["container"]),
        "resetCount": len(results),
        "deletedCount": deleted_count,
        "targets": results,
        "updatedAt": _utc_timestamp(),
    }


def _normalize_symbol_candidates(symbols: Sequence[Any]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for raw in symbols:
        try:
            symbol = _normalize_purge_symbol(str(raw or ""))
        except HTTPException:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _load_symbols_from_bronze_blacklists(client: BlobStorageClient) -> Dict[str, Any]:
    merged: List[str] = []
    sources: List[Dict[str, Any]] = []
    for path in _bronze_blacklist_paths():
        loaded: Sequence[Any] = []
        warning: Optional[str] = None
        try:
            loaded = mdc.load_ticker_list(path, client=client) or []
        except Exception as exc:
            warning = f"{type(exc).__name__}: {exc}"
            logger.warning("Blacklist load failed: container=%s path=%s error=%s", client.container_name, path, warning)

        normalized = _normalize_symbol_candidates(loaded)
        merged.extend(normalized)
        source_info: Dict[str, Any] = {
            "path": path,
            "symbolCount": len(normalized),
        }
        if warning:
            source_info["warning"] = warning
        sources.append(source_info)

    symbols = _normalize_symbol_candidates(merged)
    return {
        "container": client.container_name,
        "symbolCount": len(symbols),
        "symbols": symbols,
        "sources": sources,
    }


def _append_symbol_to_bronze_blacklists(client: BlobStorageClient, symbol: str) -> Dict[str, Any]:
    normalized_symbol = _normalize_purge_symbol(symbol)
    blacklist_paths = _bronze_blacklist_paths()

    for path in blacklist_paths:
        mdc.update_csv_set(path, normalized_symbol, client=client)

    return {"updated": len(blacklist_paths), "paths": blacklist_paths}


def _remove_symbol_from_alpha26_bucket(
    *,
    client: BlobStorageClient,
    domain: str,
    symbol: str,
) -> int:
    bucket = bronze_bucketing.bucket_letter(symbol)
    bucket_path = bronze_bucketing.active_bucket_blob_path_for_domain(domain, bucket)
    raw = mdc.read_raw_bytes(bucket_path, client=client)
    if not raw:
        return 0
    df = pd.read_parquet(BytesIO(raw))
    if df is None or df.empty or "symbol" not in df.columns:
        return 0
    symbol_mask = df["symbol"].astype(str).str.upper() == symbol
    removed = int(symbol_mask.sum())
    if removed <= 0:
        return 0
    filtered = df.loc[~symbol_mask].copy()
    payload = filtered.to_parquet(index=False, compression=bronze_bucketing.alpha26_codec())
    mdc.store_raw_bytes(payload, bucket_path, client=client)
    return removed


def _remove_symbol_from_delta_bucket(
    *,
    container: str,
    path: str,
    symbol: str,
) -> int:
    try:
        df = load_delta(container, path)
    except Exception:
        return 0
    if df is None or df.empty:
        return 0

    symbol_column = None
    for candidate in ("symbol", "Symbol", "ticker", "Ticker"):
        if candidate in df.columns:
            symbol_column = candidate
            break
    if not symbol_column:
        return 0

    mask = df[symbol_column].astype(str).str.upper() == symbol
    removed = int(mask.sum())
    if removed <= 0:
        return 0

    filtered = df.loc[~mask].reset_index(drop=True)
    delta_core.store_delta(filtered, container, path, mode="overwrite")
    return removed


def _remove_symbol_from_bronze_storage(client: BlobStorageClient, symbol: str) -> List[Dict[str, Any]]:
    normalized_symbol = _normalize_purge_symbol(symbol)
    bronze_bucketing.bronze_layout_mode()
    alpha26_tasks: List[Tuple[Dict[str, Any], Callable[[], int]]] = []
    alpha26_domains = (
        "market",
        "finance",
        "earnings",
        "price-target",
    )
    for domain in alpha26_domains:
        bucket_path = bronze_bucketing.active_bucket_blob_path_for_domain(
            domain,
            bronze_bucketing.bucket_letter(normalized_symbol),
        )
        alpha26_tasks.append(
            (
                {
                    "layer": "bronze",
                    "domain": domain,
                    "container": client.container_name,
                    "path": bucket_path,
                    "operation": "row_delete",
                },
                lambda d=domain: _remove_symbol_from_alpha26_bucket(
                    client=client,
                    domain=d,
                    symbol=normalized_symbol,
                ),
            )
        )
    worker_count = _resolve_purge_symbol_target_workers(len(alpha26_tasks))
    return _run_symbol_cleanup_tasks(
        alpha26_tasks,
        worker_count=worker_count,
        thread_name_prefix="purge-symbol-bronze-alpha26",
    )


def _remove_symbol_from_layer_storage(
    client: BlobStorageClient,
    container: str,
    symbol: str,
    layer: Literal["silver", "gold"],
) -> List[Dict[str, Any]]:
    normalized_symbol = _normalize_purge_symbol(symbol)
    bucket = layer_bucketing.bucket_letter(normalized_symbol)
    alpha26_tasks: List[Tuple[Dict[str, Any], Callable[[], int]]] = []
    if layer == "silver":
        layer_bucketing.silver_layout_mode()
        alpha26_tasks.extend(
            [
                (
                    {
                        "layer": layer,
                        "domain": "market",
                        "container": container,
                        "path": DataPaths.get_silver_market_bucket_path(bucket),
                        "operation": "row_delete",
                    },
                    lambda path=DataPaths.get_silver_market_bucket_path(bucket): _remove_symbol_from_delta_bucket(
                        container=container,
                        path=path,
                        symbol=normalized_symbol,
                    ),
                ),
                (
                    {
                        "layer": layer,
                        "domain": "earnings",
                        "container": container,
                        "path": DataPaths.get_silver_earnings_bucket_path(bucket),
                        "operation": "row_delete",
                    },
                    lambda path=DataPaths.get_silver_earnings_bucket_path(bucket): _remove_symbol_from_delta_bucket(
                        container=container,
                        path=path,
                        symbol=normalized_symbol,
                    ),
                ),
                (
                    {
                        "layer": layer,
                        "domain": "price-target",
                        "container": container,
                        "path": DataPaths.get_silver_price_target_bucket_path(bucket),
                        "operation": "row_delete",
                    },
                    lambda path=DataPaths.get_silver_price_target_bucket_path(bucket): _remove_symbol_from_delta_bucket(
                        container=container,
                        path=path,
                        symbol=normalized_symbol,
                    ),
                ),
            ]
        )
        for sub_domain in SILVER_FINANCE_SUBDOMAINS:
            finance_bucket_path = DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
            alpha26_tasks.append(
                (
                    {
                        "layer": layer,
                        "domain": "finance",
                        "container": container,
                        "path": finance_bucket_path,
                        "operation": "row_delete",
                    },
                    lambda path=finance_bucket_path: _remove_symbol_from_delta_bucket(
                        container=container,
                        path=path,
                        symbol=normalized_symbol,
                    ),
                )
            )
        worker_count = _resolve_purge_symbol_target_workers(len(alpha26_tasks))
        return _run_symbol_cleanup_tasks(
            alpha26_tasks,
            worker_count=worker_count,
            thread_name_prefix="purge-symbol-silver-alpha26",
        )

    layer_bucketing.gold_layout_mode()
    alpha26_tasks.extend(
        [
            (
                {
                    "layer": layer,
                    "domain": "market",
                    "container": container,
                    "path": DataPaths.get_gold_market_bucket_path(bucket),
                    "operation": "row_delete",
                },
                lambda path=DataPaths.get_gold_market_bucket_path(bucket): _remove_symbol_from_delta_bucket(
                    container=container,
                    path=path,
                    symbol=normalized_symbol,
                ),
            ),
            (
                {
                    "layer": layer,
                    "domain": "earnings",
                    "container": container,
                    "path": DataPaths.get_gold_earnings_bucket_path(bucket),
                    "operation": "row_delete",
                },
                lambda path=DataPaths.get_gold_earnings_bucket_path(bucket): _remove_symbol_from_delta_bucket(
                    container=container,
                    path=path,
                    symbol=normalized_symbol,
                ),
            ),
            (
                {
                    "layer": layer,
                    "domain": "price-target",
                    "container": container,
                    "path": DataPaths.get_gold_price_targets_bucket_path(bucket),
                    "operation": "row_delete",
                },
                lambda path=DataPaths.get_gold_price_targets_bucket_path(bucket): _remove_symbol_from_delta_bucket(
                    container=container,
                    path=path,
                    symbol=normalized_symbol,
                ),
            ),
        ]
    )
    finance_bucket_path = DataPaths.get_gold_finance_alpha26_bucket_path(bucket)
    alpha26_tasks.append(
        (
            {
                "layer": layer,
                "domain": "finance",
                "container": container,
                "path": finance_bucket_path,
                "operation": "row_delete",
            },
            lambda path=finance_bucket_path: _remove_symbol_from_delta_bucket(
                container=container,
                path=path,
                symbol=normalized_symbol,
            ),
        )
    )
    worker_count = _resolve_purge_symbol_target_workers(len(alpha26_tasks))
    return _run_symbol_cleanup_tasks(
        alpha26_tasks,
        worker_count=worker_count,
        thread_name_prefix="purge-symbol-gold-alpha26",
    )


def _resolve_purge_targets(scope: str, layer: Optional[str], domain: Optional[str]) -> List[Dict[str, Optional[str]]]:
    scope = scope.strip().lower()
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)

    targets: List[Dict[str, Optional[str]]] = []

    if scope == "layer-domain":
        if not layer_norm or not domain_norm:
            raise HTTPException(status_code=400, detail="layer and domain are required for scope 'layer-domain'.")
        for container, prefix in _targets_for_layer_domain(layer_norm, domain_norm):
            targets.append({"layer": layer_norm, "domain": domain_norm, "container": container, "prefix": prefix})
    elif scope == "layer":
        if not layer_norm:
            raise HTTPException(status_code=400, detail="layer is required for scope 'layer'.")
        container = _resolve_container(layer_norm)
        targets.append({"layer": layer_norm, "domain": None, "container": container, "prefix": None})
    elif scope == "domain":
        if not domain_norm:
            raise HTTPException(status_code=400, detail="domain is required for scope 'domain'.")
        for layer_name in _DOMAIN_PREFIXES.keys():
            if domain_norm not in _DOMAIN_PREFIXES.get(layer_name, {}):
                continue
            for container, prefix in _targets_for_layer_domain(layer_name, domain_norm):
                targets.append({"layer": layer_name, "domain": domain_norm, "container": container, "prefix": prefix})
        if not targets:
            raise HTTPException(status_code=400, detail=f"No targets found for domain '{domain_norm}'.")
    else:
        raise HTTPException(status_code=400, detail=f"Unknown scope '{scope}'.")

    return targets


def _watermark_blob_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/{cleaned}.json"


def _run_checkpoint_blob_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/runs/{cleaned}.json"


def _collect_domains_for_layer(
    targets: List[Dict[str, Optional[str]]],
    *,
    layer: str,
    supported_domains: Sequence[str],
) -> List[str]:
    domains: set[str] = set()
    include_all_domains = False

    for target in targets:
        target_layer = _normalize_layer(str(target.get("layer") or ""))
        if target_layer != layer:
            continue

        raw_domain = target.get("domain")
        target_domain = _normalize_domain(str(raw_domain or "")) if raw_domain is not None else None
        if not target_domain:
            include_all_domains = True
            continue
        if target_domain in supported_domains:
            domains.add(target_domain)

    if include_all_domains:
        domains.update(supported_domains)
    return [name for name in supported_domains if name in domains]


def _build_silver_checkpoint_reset_targets(targets: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    domains = _collect_domains_for_layer(
        targets,
        layer="silver",
        supported_domains=list(_SILVER_JOB_CHECKPOINT_KEYS.keys()),
    )
    if not domains:
        return []

    common_container = str(getattr(cfg, "AZURE_CONTAINER_COMMON", "") or "").strip()
    if not common_container:
        raise HTTPException(status_code=503, detail="Missing AZURE_CONTAINER_COMMON for silver checkpoint reset.")

    checkpoint_targets: List[Dict[str, Optional[str]]] = []
    for domain in domains:
        bronze_watermark_key, silver_run_key = _SILVER_JOB_CHECKPOINT_KEYS[domain]
        checkpoint_targets.append(
            {
                "layer": "common",
                "domain": domain,
                "container": common_container,
                "prefix": _watermark_blob_path(bronze_watermark_key),
                "operation": "reset-watermark",
            }
        )
        checkpoint_targets.append(
            {
                "layer": "common",
                "domain": domain,
                "container": common_container,
                "prefix": _run_checkpoint_blob_path(silver_run_key),
                "operation": "reset-run-checkpoint",
            }
        )

    return checkpoint_targets


def _build_gold_checkpoint_reset_targets(targets: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    domains = _collect_domains_for_layer(
        targets,
        layer="gold",
        supported_domains=list(_GOLD_JOB_WATERMARK_KEYS.keys()),
    )
    if not domains:
        return []

    common_container = str(getattr(cfg, "AZURE_CONTAINER_COMMON", "") or "").strip()
    if not common_container:
        raise HTTPException(status_code=503, detail="Missing AZURE_CONTAINER_COMMON for gold checkpoint reset.")

    checkpoint_targets: List[Dict[str, Optional[str]]] = []
    for domain in domains:
        watermark_key = _GOLD_JOB_WATERMARK_KEYS[domain]
        checkpoint_targets.append(
            {
                "layer": "common",
                "domain": domain,
                "container": common_container,
                "prefix": _watermark_blob_path(watermark_key),
                "operation": "reset-watermark",
            }
        )

    return checkpoint_targets


def _collect_purged_domain_metadata_targets(
    targets: List[Dict[str, Optional[str]]],
) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for target in targets:
        layer = _normalize_layer(str(target.get("layer") or ""))
        if not layer or layer not in _DOMAIN_PREFIXES:
            continue

        supported_domains = _DOMAIN_PREFIXES.get(layer, {})
        raw_domain = target.get("domain")
        domain_candidates = [raw_domain] if raw_domain is not None else list(supported_domains.keys())
        container = str(target.get("container") or "").strip()

        for raw_domain_name in domain_candidates:
            domain = _normalize_domain(str(raw_domain_name or "")) if raw_domain_name is not None else ""
            if not domain or domain not in supported_domains:
                continue
            dedupe_key = (layer, domain)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            collected.append(
                {
                    "layer": layer,
                    "domain": domain,
                    "container": container,
                }
            )

    return collected


def _mark_purged_domain_metadata_snapshots(targets: List[Dict[str, str]]) -> None:
    if not targets:
        return

    for target in targets:
        domain_metadata_snapshots.mark_domain_metadata_snapshot_purged(
            layer=str(target.get("layer") or ""),
            domain=str(target.get("domain") or ""),
            container=str(target.get("container") or "").strip() or None,
        )

    _invalidate_domain_metadata_document_cache()
    _emit_domain_metadata_snapshot_changed("purge", targets)


def _run_purge_operation(payload: PurgeRequest) -> Dict[str, Any]:
    targets = _resolve_purge_targets(payload.scope, payload.layer, payload.domain)
    metadata_targets = _collect_purged_domain_metadata_targets(targets)
    targets = [
        *targets,
        *_build_silver_checkpoint_reset_targets(targets),
        *_build_gold_checkpoint_reset_targets(targets),
    ]

    worker_count = _resolve_purge_scope_workers(len(targets))
    planned_by_index: Dict[int, Tuple[BlobStorageClient, Dict[str, Optional[str]]]] = {}
    any_data = False
    if worker_count <= 1:
        for idx, target in enumerate(targets):
            container = str(target["container"] or "")
            prefix = target.get("prefix")
            try:
                client = BlobStorageClient(container_name=container, ensure_container_exists=False)
                has_data = client.has_blobs(prefix)
            except Exception as exc:
                logger.exception(
                    "Purge preflight failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                    container,
                    prefix,
                    payload.scope,
                    target.get("layer"),
                    target.get("domain"),
                )
                raise HTTPException(
                    status_code=502, detail=f"Purge preflight failed for {container}:{prefix}: {exc}"
                ) from exc
            target["hasData"] = bool(has_data)
            planned_by_index[idx] = (client, target)
            any_data = any_data or bool(has_data)
    else:
        def _preflight_target(idx: int, target: Dict[str, Optional[str]]) -> Tuple[int, BlobStorageClient, bool]:
            container = str(target["container"] or "")
            prefix = target.get("prefix")
            client = BlobStorageClient(container_name=container, ensure_container_exists=False)
            has_data = bool(client.has_blobs(prefix))
            return idx, client, has_data

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-preflight") as executor:
            future_to_target: Dict[Any, Tuple[int, Dict[str, Optional[str]]]] = {
                executor.submit(_preflight_target, idx, target): (idx, target) for idx, target in enumerate(targets)
            }
            for future in as_completed(future_to_target):
                idx, target = future_to_target[future]
                container = str(target.get("container") or "")
                prefix = target.get("prefix")
                try:
                    _, client, has_data = future.result()
                except Exception as exc:
                    logger.exception(
                        "Purge preflight failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                        container,
                        prefix,
                        payload.scope,
                        target.get("layer"),
                        target.get("domain"),
                    )
                    raise HTTPException(
                        status_code=502, detail=f"Purge preflight failed for {container}:{prefix}: {exc}"
                    ) from exc
                target["hasData"] = bool(has_data)
                planned_by_index[idx] = (client, target)
                any_data = any_data or bool(has_data)

    planned = [planned_by_index[idx] for idx in sorted(planned_by_index.keys())]

    if not any_data:
        raise HTTPException(status_code=409, detail="Nothing to purge for the selected scope.")

    results: List[Dict[str, Any]] = []
    total_deleted = 0

    if worker_count <= 1:
        for client, target in planned:
            if not target.get("hasData"):
                continue
            container = str(target["container"] or "")
            prefix = target.get("prefix")
            try:
                deleted = client.delete_prefix(prefix)
            except Exception as exc:
                logger.exception(
                    "Purge failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                    container,
                    prefix,
                    payload.scope,
                    target.get("layer"),
                    target.get("domain"),
                )
                raise HTTPException(status_code=502, detail=f"Purge failed for {container}:{prefix}: {exc}") from exc

            result: Dict[str, Any] = {
                "container": container,
                "prefix": prefix,
                "layer": target.get("layer"),
                "domain": target.get("domain"),
                "deleted": deleted,
            }
            if target.get("operation"):
                result["operation"] = target.get("operation")
            results.append(result)
            total_deleted += int(deleted or 0)
    else:
        delete_results_by_index: Dict[int, Dict[str, Any]] = {}

        def _delete_target(
            idx: int, client: BlobStorageClient, target: Dict[str, Optional[str]]
        ) -> Tuple[int, Dict[str, Any]]:
            container = str(target.get("container") or "")
            prefix = target.get("prefix")
            deleted = client.delete_prefix(prefix)
            result: Dict[str, Any] = {
                "container": container,
                "prefix": prefix,
                "layer": target.get("layer"),
                "domain": target.get("domain"),
                "deleted": deleted,
            }
            if target.get("operation"):
                result["operation"] = target.get("operation")
            return idx, result

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-delete") as executor:
            future_to_target: Dict[Any, Tuple[int, Dict[str, Optional[str]]]] = {}
            for idx, (client, target) in enumerate(planned):
                if not target.get("hasData"):
                    continue
                future = executor.submit(_delete_target, idx, client, target)
                future_to_target[future] = (idx, target)

            for future in as_completed(future_to_target):
                _, target = future_to_target[future]
                container = str(target.get("container") or "")
                prefix = target.get("prefix")
                try:
                    idx, result = future.result()
                except Exception as exc:
                    logger.exception(
                        "Purge failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                        container,
                        prefix,
                        payload.scope,
                        target.get("layer"),
                        target.get("domain"),
                    )
                    raise HTTPException(status_code=502, detail=f"Purge failed for {container}:{prefix}: {exc}") from exc
                delete_results_by_index[idx] = result

        for idx in sorted(delete_results_by_index.keys()):
            result = delete_results_by_index[idx]
            results.append(result)
            total_deleted += int(result.get("deleted") or 0)

    try:
        _mark_purged_domain_metadata_snapshots(metadata_targets)
    except Exception as exc:
        logger.exception(
            "Purge metadata refresh failed: scope=%s layer=%s domain=%s targets=%s",
            payload.scope,
            payload.layer,
            payload.domain,
            len(metadata_targets),
        )
        raise HTTPException(
            status_code=502,
            detail=f"Purge completed but metadata refresh failed: {type(exc).__name__}: {exc}",
        ) from exc

    logger.warning(
        "Purge completed: scope=%s layer=%s domain=%s targets=%s deleted=%s",
        payload.scope,
        payload.layer,
        payload.domain,
        len(results),
        total_deleted,
    )

    return {
        "scope": payload.scope,
        "layer": payload.layer,
        "domain": payload.domain,
        "totalDeleted": total_deleted,
        "targets": results,
    }


def _run_purge_symbol_operation(
    payload: PurgeSymbolRequest,
    *,
    update_blacklist: bool = True,
) -> Dict[str, Any]:
    normalized_symbol = _normalize_purge_symbol(payload.symbol)

    container_bronze = _resolve_container("bronze")
    container_silver = _resolve_container("silver")
    container_gold = _resolve_container("gold")

    bronze_client = BlobStorageClient(container_name=container_bronze, ensure_container_exists=False)
    silver_client = BlobStorageClient(container_name=container_silver, ensure_container_exists=False)
    gold_client = BlobStorageClient(container_name=container_gold, ensure_container_exists=False)

    results: List[Dict[str, Any]] = []
    total_deleted = 0

    if update_blacklist:
        with _PURGE_BLACKLIST_UPDATE_LOCK:
            blacklist_update = _append_symbol_to_bronze_blacklists(bronze_client, normalized_symbol)
        results.append(
            {
                "operation": "blacklist",
                "layer": "bronze",
                "domain": "all",
                "container": container_bronze,
                "status": "updated",
                "paths": blacklist_update["paths"],
                "updated": blacklist_update["updated"],
            }
        )

    layer_work: Dict[str, Callable[[], List[Dict[str, Any]]]] = {
        "bronze": lambda: _remove_symbol_from_bronze_storage(bronze_client, normalized_symbol),
        "silver": lambda: _remove_symbol_from_layer_storage(
            client=silver_client,
            container=container_silver,
            symbol=normalized_symbol,
            layer="silver",
        ),
        "gold": lambda: _remove_symbol_from_layer_storage(
            client=gold_client,
            container=container_gold,
            symbol=normalized_symbol,
            layer="gold",
        ),
    }
    layer_order = ["bronze", "silver", "gold"]
    layer_results: Dict[str, List[Dict[str, Any]]] = {}
    layer_worker_count = _resolve_purge_symbol_layer_workers(len(layer_order))
    if layer_worker_count <= 1:
        for layer_name in layer_order:
            layer_results[layer_name] = layer_work[layer_name]()
    else:
        with ThreadPoolExecutor(max_workers=layer_worker_count, thread_name_prefix="purge-symbol-layers") as executor:
            future_to_layer = {executor.submit(layer_work[layer_name]): layer_name for layer_name in layer_order}
            for future in as_completed(future_to_layer):
                layer_name = future_to_layer[future]
                layer_results[layer_name] = future.result()

    for layer_name in layer_order:
        for outcome in layer_results.get(layer_name, []):
            total_deleted += int(outcome.get("deleted") or 0)
            results.append(outcome)

    logger.warning(
        "Purge-symbol completed: symbol=%s bronze=%s silver=%s gold=%s",
        normalized_symbol,
        container_bronze,
        container_silver,
        container_gold,
    )

    return {
        "symbol": normalized_symbol,
        "symbolVariants": _symbol_variants(normalized_symbol),
        "totalDeleted": total_deleted,
        "targets": results,
    }


def _execute_purge_operation(operation_id: str, payload: PurgeRequest) -> None:
    try:
        result = _run_purge_operation(payload)
        _update_purge_operation(
            operation_id,
            {"status": "succeeded", "result": result, "completedAt": _utc_timestamp()},
        )
    except HTTPException as exc:
        logger.exception(
            "Purge operation failed: operation=%s scope=%s layer=%s domain=%s",
            operation_id,
            payload.scope,
            payload.layer,
            payload.domain,
        )
        _update_purge_operation(
            operation_id,
            {"status": "failed", "error": str(exc.detail), "completedAt": _utc_timestamp()},
        )
    except Exception as exc:
        logger.exception(
            "Purge operation crashed: operation=%s scope=%s layer=%s domain=%s",
            operation_id,
            payload.scope,
            payload.layer,
            payload.domain,
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "completedAt": _utc_timestamp(),
            },
        )


def _create_purge_symbol_operation(
    payload: PurgeSymbolRequest,
    actor: Optional[str],
) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": "symbol",
            "symbol": payload.symbol,
            "requestedBy": actor,
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": None,
            "error": None,
        }
    return operation_id


def _execute_purge_symbol_operation(operation_id: str, payload: PurgeSymbolRequest) -> None:
    try:
        result = _run_purge_symbol_operation(payload)
        _update_purge_operation(
            operation_id,
            {"status": "succeeded", "result": result, "completedAt": _utc_timestamp()},
        )
    except HTTPException as exc:
        logger.exception("Purge-symbol operation failed: operation=%s symbol=%s", operation_id, payload.symbol)
        _update_purge_operation(
            operation_id,
            {"status": "failed", "error": str(exc.detail), "completedAt": _utc_timestamp()},
        )
    except Exception as exc:
        logger.exception("Purge-symbol operation crashed: operation=%s symbol=%s", operation_id, payload.symbol)
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "completedAt": _utc_timestamp(),
            },
        )


def _run_due_purge_rules(dsn: str, *, actor: Optional[str]) -> Dict[str, Any]:
    due_rules = list_due_purge_rules(dsn=dsn)
    now = datetime.now(timezone.utc)
    result = {
        "checked": len(due_rules),
        "executed": 0,
        "succeeded": 0,
        "failed": 0,
    }

    for rule in due_rules:
        try:
            if not claim_purge_rule_for_run(
                dsn=dsn,
                rule_id=rule.id,
                now=now,
                require_due=True,
                actor=actor,
            ):
                continue
        except Exception:
            logger.exception("Failed to claim purge rule for execution: id=%s", rule.id)
            result["failed"] += 1
            continue

        try:
            execution = _execute_purge_rule(rule=rule, actor=actor)
            failed_symbols = execution.get("failedSymbols") or []
            status = "failed" if failed_symbols else "succeeded"
            complete_purge_rule_execution(
                dsn=dsn,
                rule_id=rule.id,
                status=status,
                error=None if not failed_symbols else "; ".join(failed_symbols),
                matched_count=int(execution.get("matchedCount") or 0),
                purged_count=int(execution.get("purgedCount") or 0),
                run_interval_minutes=rule.run_interval_minutes,
                actor=actor,
                now=now,
            )
            result["executed"] += 1
            if status == "succeeded":
                result["succeeded"] += 1
            else:
                result["failed"] += 1
        except Exception as exc:
            logger.exception("Purge rule execution failed: id=%s name=%s", rule.id, rule.name)
            try:
                complete_purge_rule_execution(
                    dsn=dsn,
                    rule_id=rule.id,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    matched_count=None,
                    purged_count=None,
                    run_interval_minutes=rule.run_interval_minutes,
                    actor=actor,
                    now=now,
                )
            except Exception:
                logger.exception("Failed to persist purge-rule failure status: id=%s", rule.id)
            result["failed"] += 1

    return result


def run_due_purge_rules(*, dsn: Optional[str], actor: Optional[str] = "system") -> Dict[str, Any]:
    if not dsn:
        raise ValueError("POSTGRES_DSN is not configured.")
    return _run_due_purge_rules(dsn=dsn, actor=actor)


RUNTIME_CONFIG_CATALOG: Dict[str, Dict[str, str]] = {
    "SYMBOLS_REFRESH_INTERVAL_HOURS": {
        "description": "Refresh symbol universe from NASDAQ/Alpha Vantage when older than this many hours (0 disables refresh).",
        "example": "24",
    },
    "DEBUG_SYMBOLS": {
        "description": "Comma-separated or JSON-array symbol allowlist applied when debug filtering is configured.",
        "example": "AAPL,MSFT,NVDA",
    },
    "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN": {
        "description": "Alpha Vantage API rate limit per minute (integer).",
        "example": "300",
    },
    "ALPHA_VANTAGE_TIMEOUT_SECONDS": {
        "description": "Alpha Vantage request timeout (float seconds).",
        "example": "15",
    },
    "ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS": {
        "description": "Max wait time for API-side Alpha Vantage rate-limit queue before returning throttle (float seconds).",
        "example": "600",
    },
    "ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS": {
        "description": "Cooldown after Alpha Vantage throttle signals; outbound requests are paused for this duration (minimum 60 seconds).",
        "example": "60",
    },
    "ALPHA_VANTAGE_GATEWAY_RETRY_ATTEMPTS": {
        "description": "How many client-side retries Alpha Vantage jobs should attempt after gateway 504/timeouts (integer, includes the initial attempt).",
        "example": "3",
    },
    "ALPHA_VANTAGE_GATEWAY_RETRY_BASE_SECONDS": {
        "description": "Initial client-side backoff after Alpha Vantage gateway 504/timeouts before retrying (float seconds).",
        "example": "120",
    },
    "ALPHA_VANTAGE_GATEWAY_RETRY_MAX_SECONDS": {
        "description": "Maximum client-side backoff cap after Alpha Vantage gateway 504/timeouts (float seconds).",
        "example": "300",
    },
    "ALPHA_VANTAGE_MAX_WORKERS": {
        "description": "Alpha Vantage concurrency (max worker threads) for ingestion jobs (integer).",
        "example": "32",
    },
    "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS": {
        "description": "How many days earnings data is considered fresh before re-fetch (integer).",
        "example": "7",
    },
    "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON": {
        "description": "How far ahead to retain Alpha Vantage earnings calendar rows (3month|6month|12month).",
        "example": "12month",
    },
    "ALPHA_VANTAGE_FINANCE_FRESH_DAYS": {
        "description": "How many days finance statement data is considered fresh before re-fetch (integer).",
        "example": "28",
    },
    "MASSIVE_TIMEOUT_SECONDS": {
        "description": "Massive request timeout (float seconds) for API gateway and ETL callers.",
        "example": "30",
    },
    "MASSIVE_MAX_WORKERS": {
        "description": "Massive concurrency (max worker threads) for market/finance bronze ingestion jobs.",
        "example": "32",
    },
    "MASSIVE_FINANCE_FRESH_DAYS": {
        "description": "How many days finance statement data is considered fresh before re-fetch (integer).",
        "example": "28",
    },
    "TRIGGER_NEXT_JOB_NAME": {
        "description": "Optional downstream job name to trigger on success.",
        "example": "silver-market-job",
    },
    "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS": {
        "description": "Downstream trigger retry attempts (integer).",
        "example": "3",
    },
    "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS": {
        "description": "Downstream trigger retry base delay (float seconds).",
        "example": "1.0",
    },
    "FINANCE_PIPELINE_SHARED_LOCK_NAME": {
        "description": "Shared distributed lock key used to serialize Bronze/Silver finance jobs.",
        "example": "finance-pipeline-shared",
    },
    "BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS": {
        "description": "How long Bronze finance waits for the shared finance lock before skipping/failing (float seconds).",
        "example": "0",
    },
    "SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS": {
        "description": "How long Silver finance waits for the shared finance lock before failing (float seconds).",
        "example": "3600",
    },
    "SYSTEM_HEALTH_TTL_SECONDS": {
        "description": "System health cache TTL for the API (float seconds).",
        "example": "300",
    },
    "SYSTEM_HEALTH_MAX_AGE_SECONDS": {
        "description": "Max staleness window before marking layers stale (integer seconds).",
        "example": "129600",
    },
    "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON": {
        "description": (
            "JSON object of per-domain freshness overrides. "
            "Keys support layer.domain, layer:domain, domain, layer.*, and *."
        ),
        "example": '{"silver.market":{"maxAgeSeconds":43200},"gold.*":{"maxAgeSeconds":172800}}',
    },
    "SYSTEM_HEALTH_MARKERS_CONTAINER": {
        "description": "Container name holding marker blobs (defaults to AZURE_CONTAINER_COMMON).",
        "example": "common",
    },
    "SYSTEM_HEALTH_MARKERS_PREFIX": {
        "description": "Prefix path for marker blobs inside marker container.",
        "example": "system/health_markers",
    },
    "SYSTEM_HEALTH_VERBOSE_IDS": {
        "description": "Comma-separated list of alert IDs/components to include in verbose mode.",
        "example": "AzureMonitorMetrics,AzureLogAnalytics",
    },
    "SYSTEM_HEALTH_ARM_API_VERSION": {
        "description": "Azure ARM API version for Container Apps Job queries (string).",
        "example": "2024-03-01",
    },
    "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS": {
        "description": "Timeout for Azure ARM calls made by system health (float seconds).",
        "example": "5",
    },
    "SYSTEM_HEALTH_ARM_CONTAINERAPPS": {
        "description": "Comma-separated list of Container App names to probe via ARM.",
        "example": "asset-allocation-api,asset-allocation-ui",
    },
    "SYSTEM_HEALTH_ARM_JOBS": {
        "description": "Comma-separated list of Container App Job names to probe via ARM.",
        "example": "silver-market-job,gold-finance-job",
    },
    "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB": {
        "description": "How many recent job executions to pull per job during system-health probes (integer).",
        "example": "10",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION": {
        "description": "Azure Monitor Metrics API version.",
        "example": "2018-01-01",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES": {
        "description": "Timespan window (minutes) for Azure Monitor Metrics queries (integer).",
        "example": "15",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL": {
        "description": "Metrics query interval (ISO8601 duration string).",
        "example": "PT1M",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION": {
        "description": "Metrics aggregation (e.g., Average, Total).",
        "example": "Average",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS": {
        "description": "Comma-separated metric names to query for Container Apps.",
        "example": "UsageNanoCores,WorkingSetBytes",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS": {
        "description": "Comma-separated metric names to query for Container Apps Jobs.",
        "example": "UsageNanoCores,UsageBytes",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON": {
        "description": "JSON object mapping metric name to thresholds (warn_above/error_above/etc).",
        "example": '{"CpuUsage":{"warn_above":80,"error_above":95}}',
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID": {
        "description": "Log Analytics workspace ID for system health queries.",
        "example": "00000000-0000-0000-0000-000000000000",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS": {
        "description": "Timeout for Log Analytics queries made by system health (float seconds).",
        "example": "5",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES": {
        "description": "Timespan window (minutes) for Log Analytics queries (integer).",
        "example": "15",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON": {
        "description": "JSON array of Log Analytics query specs used by system health (KQL templates).",
        "example": '[{"resourceType":"Microsoft.App/jobs","name":"job_errors_15m","query":"ContainerAppConsoleLogs_CL|...","warnAbove":1,"errorAbove":10,"unit":"count"}]',
    },
    "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_LOOKBACK_HOURS": {
        "description": "Lookback window (hours) for Bronze symbol-count jump detection in system health.",
        "example": "168",
    },
    "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON": {
        "description": "JSON object of Bronze job symbol-count jump thresholds keyed by job name or *.",
        "example": '{"*":{"warnFactor":3.0,"errorFactor":10.0,"minPreviousSymbols":100,"minCurrentSymbols":1000}}',
    },
    "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION": {
        "description": "Azure Resource Health API version.",
        "example": "2022-10-01",
    },
    "DOMAIN_METADATA_MAX_SCANNED_BLOBS": {
        "description": "Limit for blob scanning when computing domain metadata (integer).",
        "example": "200000",
    },
    "DOMAIN_METADATA_CACHE_PATH": {
        "description": "Common-container JSON file path used to persist per-layer/domain metadata snapshots.",
        "example": "metadata/domain-metadata.json",
    },
    "DOMAIN_METADATA_UI_CACHE_PATH": {
        "description": "Common-container JSON file path used to persist UI-hydrated domain metadata snapshots.",
        "example": "metadata/ui-cache/domain-metadata-snapshot.json",
    },
    "DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS": {
        "description": "In-process TTL (seconds) for the parsed domain metadata snapshot document.",
        "example": "30",
    },
}




_runtime_ops_router, _runtime_ops_exports = system_runtime_ops_routes.build_router(
    runtime=_system_runtime(),
    runtime_config_upsert_request_model=system_runtime_ops_routes.RuntimeConfigUpsertRequest,
    debug_symbols_update_request_model=system_runtime_ops_routes.DebugSymbolsUpdateRequest,
)
router.include_router(_runtime_ops_router)

get_runtime_config_catalog = _runtime_ops_exports["get_runtime_config_catalog"]
get_runtime_config = _runtime_ops_exports["get_runtime_config"]
set_runtime_config = _runtime_ops_exports["set_runtime_config"]
remove_runtime_config = _runtime_ops_exports["remove_runtime_config"]
get_debug_symbols = _runtime_ops_exports["get_debug_symbols"]
set_debug_symbols = _runtime_ops_exports["set_debug_symbols"]
remove_debug_symbols = _runtime_ops_exports["remove_debug_symbols"]

RuntimeConfigUpsertRequest = system_runtime_ops_routes.RuntimeConfigUpsertRequest
DebugSymbolsUpdateRequest = system_runtime_ops_routes.DebugSymbolsUpdateRequest

_container_apps_router, _container_apps_exports = system_container_apps_routes.build_router(
    runtime=_system_runtime(),
)
router.include_router(_container_apps_router)

list_container_apps = _container_apps_exports["list_container_apps"]
get_container_app_logs = _container_apps_exports["get_container_app_logs"]
start_container_app = _container_apps_exports["start_container_app"]
stop_container_app = _container_apps_exports["stop_container_app"]

_normalize_container_app_name = system_container_apps_routes._normalize_container_app_name
_container_app_allowlist = system_container_apps_routes._container_app_allowlist
_container_app_health_url_overrides = system_container_apps_routes._container_app_health_url_overrides
_container_app_default_health_path = system_container_apps_routes._container_app_default_health_path
_resolve_container_app_health_url = system_container_apps_routes._resolve_container_app_health_url
_probe_container_app_health = system_container_apps_routes._probe_container_app_health
_resource_status_from_provisioning_state = system_container_apps_routes._resource_status_from_provisioning_state
_worse_status = system_container_apps_routes._worse_status
_extract_container_app_properties = system_container_apps_routes._extract_container_app_properties

_jobs_router, _jobs_exports = system_jobs_routes.build_router(
    runtime=_system_runtime(),
)
router.include_router(_jobs_router)

trigger_job_run = _jobs_exports["trigger_job_run"]
suspend_job = _jobs_exports["suspend_job"]
stop_job = _jobs_exports["stop_job"]
resume_job = _jobs_exports["resume_job"]
get_job_logs = _jobs_exports["get_job_logs"]

_normalize_job_execution_status_token = system_jobs_routes._normalize_job_execution_status_token
_is_active_job_execution_status = system_jobs_routes._is_active_job_execution_status
_is_active_job_execution = system_jobs_routes._is_active_job_execution
_select_anchored_job_executions = system_jobs_routes._select_anchored_job_executions
_coalesce_log_row_string = system_jobs_routes._coalesce_log_row_string
_extract_console_log_entries = system_jobs_routes._extract_console_log_entries
_extract_log_lines = system_jobs_routes._extract_log_lines

def _is_truthy(raw: Optional[str]) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _escape_kql_literal(value: str) -> str:
    return str(value or "").replace("'", "''")


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None




