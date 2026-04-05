from __future__ import annotations

import ast
from pathlib import Path


_DISALLOWED_FACADE_NAMES = {
    "DebugSymbolsUpdateRequest",
    "DomainColumnsRefreshRequest",
    "DomainColumnsResponse",
    "DomainDateRange",
    "DomainMetadataResponse",
    "DomainMetadataSnapshotResponse",
    "RuntimeConfigUpsertRequest",
    "SymbolSyncStateResponse",
    "SystemStatusViewResponse",
    "SystemStatusViewSources",
    "_build_domain_metadata_snapshot_payload",
    "_cache_domain_metadata_document",
    "_container_app_allowlist",
    "_container_app_default_health_path",
    "_container_app_health_url_overrides",
    "_discover_first_delta_table_for_prefix",
    "_domain_columns_cache_key",
    "_domain_columns_cache_path",
    "_domain_columns_read_timeout_seconds",
    "_domain_columns_refresh_timeout_seconds",
    "_domain_metadata_cache_key",
    "_domain_metadata_cache_path",
    "_domain_metadata_snapshot_cache_ttl_seconds",
    "_domain_metadata_ui_cache_path",
    "_emit_domain_metadata_snapshot_changed",
    "_extract_cached_domain_metadata_snapshots",
    "_extract_console_log_entries",
    "_extract_container_app_properties",
    "_extract_domain_metadata_targets_from_entries",
    "_extract_log_lines",
    "_invalidate_domain_metadata_document_cache",
    "_load_domain_columns_document",
    "_load_domain_metadata_document",
    "_merge_live_job_resources",
    "_merge_live_job_runs",
    "_normalize_columns_list",
    "_normalize_container_app_name",
    "_normalize_domain_metadata_targets",
    "_normalize_job_execution_status_token",
    "_normalize_job_name_key",
    "_overlay_live_domain_job_runtime",
    "_parse_domain_metadata_filter",
    "_parse_timeout_seconds_env",
    "_probe_container_app_health",
    "_read_cached_domain_columns",
    "_read_cached_domain_metadata_snapshot",
    "_read_domain_columns_from_artifact",
    "_refresh_domain_metadata_snapshot",
    "_resolve_container_app_health_url",
    "_resolve_system_health_payload",
    "_resource_status_from_provisioning_state",
    "_retrieve_domain_columns",
    "_retrieve_domain_columns_from_schema",
    "_run_with_timeout",
    "_same_job_run",
    "_sanitize_system_health_json_value",
    "_select_anchored_job_executions",
    "_status_view_domain_job_names",
    "_worse_status",
    "_write_cached_domain_columns",
    "_write_cached_domain_metadata_snapshot",
    "build_system_status_view",
}


def _system_module_path() -> Path:
    return Path(__file__).resolve().parents[2] / "api" / "endpoints" / "system.py"


def test_system_facade_defines_no_top_level_classes() -> None:
    module = ast.parse(_system_module_path().read_text(encoding="utf-8"))
    top_level_classes = [node.name for node in module.body if isinstance(node, ast.ClassDef)]
    assert top_level_classes == []


def test_system_facade_no_longer_owns_migrated_helper_blocks() -> None:
    module = ast.parse(_system_module_path().read_text(encoding="utf-8"))
    owned_names = {
        node.name
        for node in module.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }
    unexpected = sorted(owned_names & _DISALLOWED_FACADE_NAMES)
    assert unexpected == []
