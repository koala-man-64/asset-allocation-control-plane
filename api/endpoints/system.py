import logging
import json
import math
import os
import re
import sys
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypeVar

import httpx
from anyio import from_thread
from fastapi import APIRouter, HTTPException, Request

from api.endpoints.system_modules import compat as system_compat
from api.endpoints.system_modules import container_apps as system_container_apps_routes
from api.endpoints.system_modules import domain_columns as system_domain_columns_routes
from api.endpoints.system_modules import domain_metadata as system_domain_metadata_routes
from api.endpoints.system_modules import jobs as system_jobs_routes
from api.endpoints.system_modules import purge as system_purge_routes
from api.endpoints.system_modules import runtime_ops as system_runtime_ops_routes
from api.endpoints.system_modules import purge_runtime as system_purge_runtime
from api.endpoints.system_modules import symbol_enrichment as system_symbol_enrichment_routes
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
from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import domain_artifacts
from asset_allocation_runtime_common.market_data import domain_metadata_snapshots
from asset_allocation_runtime_common.foundation.debug_symbols import (
    delete_debug_symbols_state,
    read_debug_symbols_state,
    replace_debug_symbols_state,
)
from asset_allocation_runtime_common.market_data.delta_core import get_delta_schema_columns
from asset_allocation_runtime_common.market_data.domain_metadata_snapshots import build_snapshot_miss_payload
from asset_allocation_runtime_common.foundation.runtime_config import (
    DEFAULT_ENV_OVERRIDE_KEYS,
    delete_runtime_config,
    list_runtime_config,
    normalize_env_override,
    upsert_runtime_config,
)
from core.symbol_enrichment_repository import (
    enqueue_symbol_cleanup_run,
    get_symbol_enrichment_summary,
    get_symbol_enrichment_symbol_detail,
    list_symbol_cleanup_runs,
    list_symbol_enrichment_symbols,
    upsert_symbol_profile_overrides,
)

# Preserve the historical import surface while route assembly is moved into submodules.
_LEGACY_EXPORTS = (
    json,
    math,
    os,
    re,
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
    mdc,
    domain_metadata_snapshots,
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

_PURGE_OPERATIONS = system_purge_runtime._PURGE_OPERATIONS
_PURGE_OPERATIONS_LOCK = system_purge_runtime._PURGE_OPERATIONS_LOCK
_PURGE_BLACKLIST_UPDATE_LOCK = system_purge_runtime._PURGE_BLACKLIST_UPDATE_LOCK
_PURGE_RULE_AUDIT_INTERVAL_MINUTES = system_purge_runtime._PURGE_RULE_AUDIT_INTERVAL_MINUTES
_DEFAULT_PURGE_SYMBOL_MAX_WORKERS = system_purge_runtime._DEFAULT_PURGE_SYMBOL_MAX_WORKERS
_MAX_PURGE_SYMBOL_MAX_WORKERS = system_purge_runtime._MAX_PURGE_SYMBOL_MAX_WORKERS
_DEFAULT_PURGE_PREVIEW_LOAD_MAX_WORKERS = system_purge_runtime._DEFAULT_PURGE_PREVIEW_LOAD_MAX_WORKERS
_MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS = system_purge_runtime._MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS
_DEFAULT_PURGE_SCOPE_MAX_WORKERS = system_purge_runtime._DEFAULT_PURGE_SCOPE_MAX_WORKERS
_MAX_PURGE_SCOPE_MAX_WORKERS = system_purge_runtime._MAX_PURGE_SCOPE_MAX_WORKERS
_DEFAULT_PURGE_SYMBOL_TARGET_MAX_WORKERS = system_purge_runtime._DEFAULT_PURGE_SYMBOL_TARGET_MAX_WORKERS
_MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS = system_purge_runtime._MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS
_DEFAULT_PURGE_SYMBOL_LAYER_MAX_WORKERS = system_purge_runtime._DEFAULT_PURGE_SYMBOL_LAYER_MAX_WORKERS
_MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS = system_purge_runtime._MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS
_CANDIDATE_AGGREGATION_ALIASES = system_purge_runtime._CANDIDATE_AGGREGATION_ALIASES
_SUPPORTED_CANDIDATE_AGGREGATIONS = system_purge_runtime._SUPPORTED_CANDIDATE_AGGREGATIONS
_RULE_DATA_PREFIXES = system_purge_runtime._RULE_DATA_PREFIXES
_LAYER_CONTAINER_ENV = system_purge_runtime._LAYER_CONTAINER_ENV
_DOMAIN_PREFIXES = system_purge_runtime._DOMAIN_PREFIXES
_SILVER_JOB_CHECKPOINT_KEYS = system_purge_runtime._SILVER_JOB_CHECKPOINT_KEYS
_GOLD_JOB_WATERMARK_KEYS = system_purge_runtime._GOLD_JOB_WATERMARK_KEYS
_iso = system_purge_runtime._iso
_require_postgres_dsn = system_purge_runtime._require_postgres_dsn
_rule_normalize_column_name = system_purge_runtime._rule_normalize_column_name
_serialize_purge_rule = system_purge_runtime._serialize_purge_rule
_resolve_purge_rule_table = system_purge_runtime._resolve_purge_rule_table
_discover_delta_tables_for_prefix = system_purge_runtime._discover_delta_tables_for_prefix
_load_rule_frame = system_purge_runtime._load_rule_frame
_resolve_rule_symbol_column = system_purge_runtime._resolve_rule_symbol_column
_resolve_rule_value_column = system_purge_runtime._resolve_rule_value_column
_resolve_rule_date_column = system_purge_runtime._resolve_rule_date_column
_collect_rule_symbol_values = system_purge_runtime._collect_rule_symbol_values
_normalize_candidate_aggregation = system_purge_runtime._normalize_candidate_aggregation
_aggregate_series = system_purge_runtime._aggregate_series
_collect_purge_candidates = system_purge_runtime._collect_purge_candidates
_build_purge_candidates_response = system_purge_runtime._build_purge_candidates_response
_build_purge_expression = system_purge_runtime._build_purge_expression
_persist_purge_symbols_audit_rule = system_purge_runtime._persist_purge_symbols_audit_rule
_normalize_candidate_symbols = system_purge_runtime._normalize_candidate_symbols
_resolve_purge_symbol_workers = system_purge_runtime._resolve_purge_symbol_workers
_resolve_purge_preview_load_workers = system_purge_runtime._resolve_purge_preview_load_workers
_resolve_purge_scope_workers = system_purge_runtime._resolve_purge_scope_workers
_resolve_purge_symbol_target_workers = system_purge_runtime._resolve_purge_symbol_target_workers
_resolve_purge_symbol_layer_workers = system_purge_runtime._resolve_purge_symbol_layer_workers
_run_symbol_cleanup_tasks = system_purge_runtime._run_symbol_cleanup_tasks
_build_purge_symbols_summary = system_purge_runtime._build_purge_symbols_summary
_create_purge_symbols_operation = system_purge_runtime._create_purge_symbols_operation
_execute_purge_symbols_operation = system_purge_runtime._execute_purge_symbols_operation
_execute_purge_rule = system_purge_runtime._execute_purge_rule
_normalize_purge_symbol = system_purge_runtime._normalize_purge_symbol
_market_symbol = system_purge_runtime._market_symbol
_symbol_variants = system_purge_runtime._symbol_variants
_utc_timestamp = system_purge_runtime._utc_timestamp
_create_purge_operation = system_purge_runtime._create_purge_operation
_create_purge_candidates_operation = system_purge_runtime._create_purge_candidates_operation
_execute_purge_candidates_operation = system_purge_runtime._execute_purge_candidates_operation
_get_purge_operation = system_purge_runtime._get_purge_operation
_update_purge_operation = system_purge_runtime._update_purge_operation
_normalize_layer = system_purge_runtime._normalize_layer
_normalize_domain = system_purge_runtime._normalize_domain
_resolve_container = system_purge_runtime._resolve_container
_targets_for_layer_domain = system_purge_runtime._targets_for_layer_domain
_delete_blob_if_exists = system_purge_runtime._delete_blob_if_exists
_delete_prefix_if_exists = system_purge_runtime._delete_prefix_if_exists
_bronze_blacklist_paths = system_purge_runtime._bronze_blacklist_paths
_resolve_domain_list_paths = system_purge_runtime._resolve_domain_list_paths
_load_domain_list_file_preview = system_purge_runtime._load_domain_list_file_preview
_reset_domain_lists = system_purge_runtime._reset_domain_lists
_reset_domain_checkpoints = system_purge_runtime._reset_domain_checkpoints
_normalize_symbol_candidates = system_purge_runtime._normalize_symbol_candidates
_load_symbols_from_bronze_blacklists = system_purge_runtime._load_symbols_from_bronze_blacklists
_append_symbol_to_bronze_blacklists = system_purge_runtime._append_symbol_to_bronze_blacklists
_remove_symbol_from_alpha26_bucket = system_purge_runtime._remove_symbol_from_alpha26_bucket
_remove_symbol_from_delta_bucket = system_purge_runtime._remove_symbol_from_delta_bucket
_remove_symbol_from_bronze_storage = system_purge_runtime._remove_symbol_from_bronze_storage
_remove_symbol_from_layer_storage = system_purge_runtime._remove_symbol_from_layer_storage
_resolve_purge_targets = system_purge_runtime._resolve_purge_targets
_watermark_blob_path = system_purge_runtime._watermark_blob_path
_run_checkpoint_blob_path = system_purge_runtime._run_checkpoint_blob_path
_collect_domains_for_layer = system_purge_runtime._collect_domains_for_layer
_build_silver_checkpoint_reset_targets = system_purge_runtime._build_silver_checkpoint_reset_targets
_build_gold_checkpoint_reset_targets = system_purge_runtime._build_gold_checkpoint_reset_targets
_collect_purged_domain_metadata_targets = system_purge_runtime._collect_purged_domain_metadata_targets
_mark_purged_domain_metadata_snapshots = system_purge_runtime._mark_purged_domain_metadata_snapshots
_run_purge_operation = system_purge_runtime._run_purge_operation
_run_purge_symbol_operation = system_purge_runtime._run_purge_symbol_operation
_execute_purge_operation = system_purge_runtime._execute_purge_operation
_create_purge_symbol_operation = system_purge_runtime._create_purge_symbol_operation
_execute_purge_symbol_operation = system_purge_runtime._execute_purge_symbol_operation
_run_due_purge_rules = system_purge_runtime._run_due_purge_rules
run_due_purge_rules = system_purge_runtime.run_due_purge_rules
BlobStorageClient = system_purge_runtime.BlobStorageClient
PostgresError = system_purge_runtime.PostgresError
PurgeRule = system_purge_runtime.PurgeRule
SILVER_FINANCE_SUBDOMAINS = system_purge_runtime.SILVER_FINANCE_SUBDOMAINS
DataPaths = system_purge_runtime.DataPaths
bronze_bucketing = system_purge_runtime.bronze_bucketing
cfg = system_purge_runtime.cfg
claim_purge_rule_for_run = system_purge_runtime.claim_purge_rule_for_run
complete_purge_rule_execution = system_purge_runtime.complete_purge_rule_execution
create_purge_rule = system_purge_runtime.create_purge_rule
delete_purge_rule_row = system_purge_runtime.delete_purge_rule_row
delta_core = system_purge_runtime.delta_core
get_purge_rule = system_purge_runtime.get_purge_rule
is_percent_operator = system_purge_runtime.is_percent_operator
layer_bucketing = system_purge_runtime.layer_bucketing
list_due_purge_rules = system_purge_runtime.list_due_purge_rules
list_purge_rules = system_purge_runtime.list_purge_rules
load_delta = system_purge_runtime.load_delta
normalize_purge_rule_operator = system_purge_runtime.normalize_purge_rule_operator
supported_purge_rule_operators = system_purge_runtime.supported_purge_rule_operators
threading = system_purge_runtime.threading
update_purge_rule = system_purge_runtime.update_purge_rule
_escape_kql_literal = system_compat._escape_kql_literal
_parse_dt = system_compat._parse_dt




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


_symbol_enrichment_router, _symbol_enrichment_exports = system_symbol_enrichment_routes.build_router(
    runtime=_system_runtime(),
)
router.include_router(_symbol_enrichment_router)

get_symbol_enrichment_summary_endpoint = _symbol_enrichment_exports["get_symbol_enrichment_summary_endpoint"]
list_symbol_enrichment_runs_endpoint = _symbol_enrichment_exports["list_symbol_enrichment_runs_endpoint"]
list_symbol_enrichment_symbols_endpoint = _symbol_enrichment_exports["list_symbol_enrichment_symbols_endpoint"]
get_symbol_enrichment_symbol_detail_endpoint = _symbol_enrichment_exports["get_symbol_enrichment_symbol_detail_endpoint"]
enqueue_symbol_enrichment_endpoint = _symbol_enrichment_exports["enqueue_symbol_enrichment_endpoint"]
update_symbol_profile_overrides_endpoint = _symbol_enrichment_exports["update_symbol_profile_overrides_endpoint"]





