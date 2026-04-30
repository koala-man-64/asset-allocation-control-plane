from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from monitoring.azure_blob_store import AzureBlobStore, AzureBlobStoreConfig, LastModifiedProbeResult
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.backtest_health import (
    build_backtest_operational_alerts,
    collect_backtest_operational_summary,
)
from monitoring.control_plane import (
    ResourceHealthItem,
    collect_container_apps,
    collect_jobs_and_executions,
    resolve_container_app_job_names,
)
from monitoring.log_analytics import (
    AzureLogAnalyticsClient,
    collect_log_analytics_signals,
    parse_log_analytics_queries_json,
)
from monitoring.monitor_metrics import (
    DEFAULT_MONITOR_METRICS_API_VERSION,
    collect_monitor_metrics,
    parse_metric_thresholds_json,
)
from monitoring.resource_health import DEFAULT_RESOURCE_HEALTH_API_VERSION
from monitoring.system_health_modules.alerts import (
    _alert_id,
    _bronze_finance_zero_write_alerts,
    _bronze_symbol_jump_alerts,
    _job_failure_reason_alerts,
    _layer_alerts,
    _load_bronze_symbol_jump_threshold_overrides,
    _resolve_bronze_symbol_jump_threshold,
    _slug,
)
from monitoring.system_health_modules.env_config import (
    BronzeSymbolJumpThreshold,
    FreshnessPolicy,
    JobScheduleMetadata,
    MarkerProbeConfig,
    _env_float_or_default,
    _env_has_value,
    _env_int_or_default,
    _env_or_default,
    _is_test_mode,
    _parse_bool,
    _require_env,
    _require_float,
    _require_int,
    _split_csv,
)
from monitoring.system_health_modules.freshness import (
    DomainSpec,
    DomainTimestampResolution,
    LayerProbeSpec,
    _collect_job_names_for_layers,
    _compute_layer_status,
    _default_layer_specs,
    _domain_name_from_delta_path,
    _domain_name_from_marker_path,
    _load_freshness_overrides,
    _load_job_schedule_metadata,
    _marker_blob_name,
    _marker_probe_config,
    _normalize_probe_result,
    _overall_from_layers,
    _probe_container_last_modified,
    _probe_marker_last_modified,
    _resolve_domain_schedule,
    _resolve_last_updated_with_marker_probes,
    _resolve_freshness_policy,
)
from monitoring.system_health_modules.job_queries import (
    _enrich_recent_job_retry_symbol_metadata,
    _query_job_system_log_messages,
    _query_recent_bronze_finance_ingest_summaries,
    _query_recent_bronze_symbol_counts,
)
from monitoring.system_health_modules.signals import (
    _append_job_usage_percent_signals,
    _append_signal_details,
    _iso,
    _newer_execution,
    _parse_iso_start_time,
    _utc_now,
    _worse_resource_status,
    build_backtest_operational_signals,
    collect_resource_health_signals,
)
from monitoring.system_health_modules.snapshot import (
    _describe_cron,
    _derive_job_name,
    _get_domain_description,
    _make_container_portal_url,
    _make_folder_portal_url,
    _make_job_portal_url,
    collect_system_health_snapshot,
)
from core.intraday_monitor_repository import get_intraday_health_summary

logger = logging.getLogger("asset_allocation.monitoring.system_health")
DEFAULT_ARM_API_VERSION = ArmConfig(subscription_id="", resource_group="").api_version
DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX = "system/health_markers"
DEFAULT_SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS = 5.0
DEFAULT_SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB = 3
DEFAULT_SYSTEM_HEALTH_MAX_AGE_SECONDS = 129600
DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES = 15
DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL = "PT1M"
DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION = "Average"
DEFAULT_SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS = 5.0
DEFAULT_SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES = 15
DEFAULT_SYSTEM_HEALTH_CONTAINERAPP_MONITOR_METRIC_NAMES = ("UsageNanoCores", "WorkingSetBytes")
DEFAULT_SYSTEM_HEALTH_JOB_MONITOR_METRIC_NAMES = ("UsageNanoCores", "UsageBytes")

# Preserve the historical import surface while the implementation lives in system_health_modules.
_LEGACY_EXPORTS = (
    hashlib,
    json,
    os,
    re,
    datetime,
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    AzureBlobStore,
    AzureBlobStoreConfig,
    LastModifiedProbeResult,
    AzureArmClient,
    collect_backtest_operational_summary,
    build_backtest_operational_alerts,
    ResourceHealthItem,
    collect_container_apps,
    collect_jobs_and_executions,
    resolve_container_app_job_names,
    AzureLogAnalyticsClient,
    collect_log_analytics_signals,
    parse_log_analytics_queries_json,
    DEFAULT_MONITOR_METRICS_API_VERSION,
    collect_monitor_metrics,
    parse_metric_thresholds_json,
    DEFAULT_RESOURCE_HEALTH_API_VERSION,
    DEFAULT_SYSTEM_HEALTH_MAX_AGE_SECONDS,
    DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES,
    DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL,
    DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION,
    DEFAULT_SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS,
    DEFAULT_SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES,
    _alert_id,
    _bronze_finance_zero_write_alerts,
    _bronze_symbol_jump_alerts,
    _job_failure_reason_alerts,
    _layer_alerts,
    _load_bronze_symbol_jump_threshold_overrides,
    _resolve_bronze_symbol_jump_threshold,
    _slug,
    BronzeSymbolJumpThreshold,
    FreshnessPolicy,
    JobScheduleMetadata,
    MarkerProbeConfig,
    _env_float_or_default,
    _env_has_value,
    _env_int_or_default,
    _env_or_default,
    _is_test_mode,
    _parse_bool,
    _require_env,
    _require_float,
    _require_int,
    _split_csv,
    DomainSpec,
    DomainTimestampResolution,
    LayerProbeSpec,
    _collect_job_names_for_layers,
    _compute_layer_status,
    _default_layer_specs,
    _domain_name_from_delta_path,
    _domain_name_from_marker_path,
    _load_freshness_overrides,
    _load_job_schedule_metadata,
    _marker_blob_name,
    _marker_probe_config,
    _normalize_probe_result,
    _overall_from_layers,
    _probe_container_last_modified,
    _probe_marker_last_modified,
    _resolve_domain_schedule,
    _resolve_last_updated_with_marker_probes,
    _resolve_freshness_policy,
    _enrich_recent_job_retry_symbol_metadata,
    _query_job_system_log_messages,
    _query_recent_bronze_finance_ingest_summaries,
    _query_recent_bronze_symbol_counts,
    _append_job_usage_percent_signals,
    _append_signal_details,
    _iso,
    _newer_execution,
    _parse_iso_start_time,
    _utc_now,
    _worse_resource_status,
    build_backtest_operational_signals,
    collect_resource_health_signals,
    _describe_cron,
    _derive_job_name,
    _get_domain_description,
    _make_container_portal_url,
    _make_folder_portal_url,
    _make_job_portal_url,
    collect_system_health_snapshot,
    get_intraday_health_summary,
)
