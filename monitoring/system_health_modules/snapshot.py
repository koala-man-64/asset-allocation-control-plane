from __future__ import annotations

import logging
import os
from datetime import datetime
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, List, Optional

logger = logging.getLogger("asset_allocation.monitoring.system_health")


def _runtime_module() -> ModuleType:
    return import_module("monitoring.system_health")


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def _make_container_portal_url(sub_id: str, rg: str, account: str, container: str) -> Optional[str]:
    if not all([sub_id, rg, account, container]):
        return None

    storage_id = (
        f"/subscriptions/{sub_id}/resourceGroups/{rg}"
        f"/providers/Microsoft.Storage/storageAccounts/{account}"
    )
    return (
        "https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade"
        f"/~/overview/storageAccountId/{storage_id.replace('/', '%2F')}/path/{container}"
    )


def _get_domain_description(layer_name: str, name: str) -> str:
    layer_name_clean = layer_name.lower()
    name_clean = name.lower()

    if "market" in name_clean:
        if "bronze" in layer_name_clean:
            return "Raw historical OHLCV files"
        if "silver" in layer_name_clean:
            return "Standardized daily OHLCV tables"
        if "gold" in layer_name_clean:
            return "Entity-resolved market features"
        return "Historical price and volume data"

    if "finance" in name_clean:
        if "bronze" in layer_name_clean:
            return "Raw financial statements"
        if "silver" in layer_name_clean:
            return "Standardized financial tables"
        if "gold" in layer_name_clean:
            return "Financial ratios & growth metrics"
        return "Fundamental financial data"

    if "earnings" in name_clean:
        if "bronze" in layer_name_clean:
            return "Raw earnings calendar/surprises"
        if "silver" in layer_name_clean:
            return "Standardized earnings history"
        if "gold" in layer_name_clean:
            return "Earnings surprise metrics"
        return "Earnings data"

    if "target" in name_clean:
        if "bronze" in layer_name_clean:
            return "Raw analyst price targets"
        if "silver" in layer_name_clean:
            return "Standardized consensus targets"
        if "gold" in layer_name_clean:
            return "Consensus upside/downside metrics"
        return "Analyst price targets"

    if "regime" in name_clean:
        if "gold" in layer_name_clean:
            return "Market-wide regime monitor outputs"
        return "Regime monitor outputs"

    return ""


def _describe_cron(expression: str) -> str:
    mapping = {
        "0 12 * * *": "Daily at 12:00 PM UTC",
        "0 12 * * 1-5": "Weekdays at 12:00 PM UTC",
        "30 12 * * *": "Daily at 12:30 PM UTC",
        "0 14-22 * * *": "Daily, hourly 2:00-10:00 PM UTC",
        "0 14-22 * * 1-5": "Weekdays, hourly 2:00-10:00 PM UTC",
        "30 14-22 * * *": "Daily, hourly 2:30-10:30 PM UTC",
        "30 14-23 * * *": "Daily, hourly 2:30-11:30 PM UTC",
        "30 0 * * *": "Daily at 12:30 AM UTC",
        "30 1 * * *": "Daily at 1:30 AM UTC",
        "0 4 * * 1-5": "Weekdays at 4:00 AM UTC",
        "0 10 * * 1-5": "Weekdays at 10:00 AM UTC",
        "0 16 * * 1-5": "Weekdays at 4:00 PM UTC",
        "0 22 * * *": "Daily at 10:00 PM UTC",
        "0 22 * * 1-5": "Weekdays at 10:00 PM UTC",
        "30 22 * * *": "Daily at 10:30 PM UTC",
        "0 23 * * *": "Daily at 11:00 PM UTC",
        "0 23 * * 1-5": "Weekdays at 11:00 PM UTC",
        "30 23 * * *": "Daily at 11:30 PM UTC",
        "0 5 * * *": "Daily at 5:00 AM UTC",
        "0 0 * * *": "Daily at Midnight UTC",
    }
    return mapping.get(expression, expression)


def _derive_job_name(layer_name: str, domain_clean: str) -> str:
    layer_name_clean = layer_name.lower()
    domain_name_clean = domain_clean.lower()
    if layer_name_clean == "platinum":
        return ""
    return f"{layer_name_clean}-{domain_name_clean}-job"


def _make_job_portal_url(sub_id: str, rg: str, job_name: str) -> Optional[str]:
    if not all([sub_id, rg, job_name]):
        return None
    return (
        f"https://portal.azure.com/#resource/subscriptions/{sub_id}"
        f"/resourceGroups/{rg}/providers/Microsoft.App/jobs/{job_name}/overview"
    )


def _make_folder_portal_url(sub_id: str, rg: str, account: str, container: str, folder_path: str) -> Optional[str]:
    if not all([sub_id, rg, account, container, folder_path]):
        return None

    storage_id = (
        f"/subscriptions/{sub_id}/resourceGroups/{rg}"
        f"/providers/Microsoft.Storage/storageAccounts/{account}"
    )
    full_path = f"{container}/{folder_path}".strip("/")
    return (
        "https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade"
        f"/~/overview/storageAccountId/{storage_id.replace('/', '%2F')}/path/{full_path.replace('/', '%2F')}"
    )


def collect_system_health_snapshot(
    *,
    now: Optional[datetime] = None,
    include_resource_ids: bool = False,
) -> Dict[str, Any]:
    runtime = _runtime_module()

    iso = _runtime_attr(runtime, "_iso")
    utc_now = _runtime_attr(runtime, "_utc_now")
    is_test_mode = _runtime_attr(runtime, "_is_test_mode")
    default_layer_specs = _runtime_attr(runtime, "_default_layer_specs")
    load_freshness_overrides = _runtime_attr(runtime, "_load_freshness_overrides")
    marker_probe_config = _runtime_attr(runtime, "_marker_probe_config")
    collect_job_names_for_layers = _runtime_attr(runtime, "_collect_job_names_for_layers")
    load_job_schedule_metadata = _runtime_attr(runtime, "_load_job_schedule_metadata")
    domain_name_from_marker_path = _runtime_attr(runtime, "_domain_name_from_marker_path")
    domain_name_from_delta_path = _runtime_attr(runtime, "_domain_name_from_delta_path")
    resolve_domain_schedule = _runtime_attr(runtime, "_resolve_domain_schedule")
    resolve_freshness_policy = _runtime_attr(runtime, "_resolve_freshness_policy")
    resolve_last_updated_with_marker_probes = _runtime_attr(runtime, "_resolve_last_updated_with_marker_probes")
    compute_layer_status = _runtime_attr(runtime, "_compute_layer_status")
    layer_alerts = _runtime_attr(runtime, "_layer_alerts")
    overall_from_layers = _runtime_attr(runtime, "_overall_from_layers")
    logger_runtime = _runtime_attr(runtime, "logger")
    azure_blob_store_config = _runtime_attr(runtime, "AzureBlobStoreConfig")
    azure_blob_store = _runtime_attr(runtime, "AzureBlobStore")

    now = now or utc_now()
    if is_test_mode():
        logger_runtime.info("System health running in test mode (returning empty payload).")
        return {"overall": "healthy", "dataLayers": [], "recentJobs": [], "alerts": []}

    logger_runtime.info("Collecting system health: include_resource_ids=%s", include_resource_ids)

    cfg = azure_blob_store_config.from_env()
    store = azure_blob_store(cfg)

    layers: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []
    resources: List[Dict[str, Any]] = []
    job_runs: List[Dict[str, Any]] = []
    statuses: List[str] = []

    sub_id = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "").strip()
    rg = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "").strip()
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "").strip()

    layer_specs = default_layer_specs()
    freshness_overrides = load_freshness_overrides()
    marker_cfg = marker_probe_config()
    layer_job_names = collect_job_names_for_layers(layer_specs)
    job_schedule_metadata = load_job_schedule_metadata(
        subscription_id=sub_id,
        resource_group=rg,
        job_names=layer_job_names,
    )

    for spec in layer_specs:
        layer_last_updated: Optional[datetime] = None
        had_layer_error = False
        domain_items: List[Dict[str, Any]] = []

        container_attr = getattr(spec, "container_name", None)
        if callable(container_attr):
            container = container_attr()
        else:
            container = str(container_attr or "")

        for domain_spec in spec.marker_blobs:
            blob_name = domain_spec.path
            domain_path = os.path.dirname(blob_name) or blob_name
            name_clean = domain_name_from_marker_path(blob_name)
            job_name = _derive_job_name(spec.name, name_clean)
            job_url = _make_job_portal_url(sub_id, rg, job_name)
            folder_url = _make_folder_portal_url(sub_id, rg, storage_account, container, domain_path)
            domain_cron, domain_frequency = resolve_domain_schedule(
                job_name=job_name,
                default_cron=domain_spec.cron,
                default_trigger_type=domain_spec.trigger_type,
                job_schedule_metadata=job_schedule_metadata,
            )
            policy = resolve_freshness_policy(
                layer_name=spec.name,
                domain_name=name_clean,
                default_max_age_seconds=spec.max_age_seconds,
                overrides=freshness_overrides,
            )
            probe_resolution = resolve_last_updated_with_marker_probes(
                layer_name=spec.name,
                domain_name=name_clean,
                store=store,
                marker_cfg=marker_cfg,
            )
            last_modified = probe_resolution.last_updated
            if probe_resolution.status == "error":
                had_layer_error = True
                domain_items.append(
                    {
                        "name": name_clean,
                        "description": _get_domain_description(spec.name, name_clean),
                        "type": "blob",
                        "path": blob_name,
                        "maxAgeSeconds": policy.max_age_seconds,
                        "cron": domain_cron,
                        "frequency": domain_frequency,
                        "lastUpdated": None,
                        "status": "error",
                        "portalUrl": folder_url,
                        "jobUrl": job_url,
                        "jobName": job_name,
                        "freshnessSource": probe_resolution.source,
                        "freshnessPolicySource": policy.source,
                        "warnings": probe_resolution.warnings,
                    }
                )
                continue

            if spec.name.lower() == "platinum" and last_modified is None:
                status = "healthy"
            else:
                status = compute_layer_status(
                    now,
                    last_modified,
                    max_age_seconds=policy.max_age_seconds,
                    had_error=False,
                )
            domain_items.append(
                {
                    "name": name_clean,
                    "description": _get_domain_description(spec.name, name_clean),
                    "type": "blob",
                    "path": blob_name,
                    "maxAgeSeconds": policy.max_age_seconds,
                    "cron": domain_cron,
                    "frequency": domain_frequency,
                    "lastUpdated": iso(last_modified),
                    "status": status,
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                    "freshnessSource": probe_resolution.source,
                    "freshnessPolicySource": policy.source,
                    "warnings": probe_resolution.warnings,
                }
            )

        for domain_spec in spec.delta_tables:
            table_path = domain_spec.path
            name_clean = domain_name_from_delta_path(table_path)
            job_name = _derive_job_name(spec.name, name_clean)
            job_url = _make_job_portal_url(sub_id, rg, job_name)
            folder_url = _make_folder_portal_url(sub_id, rg, storage_account, container, table_path)
            domain_cron, domain_frequency = resolve_domain_schedule(
                job_name=job_name,
                default_cron=domain_spec.cron,
                default_trigger_type=domain_spec.trigger_type,
                job_schedule_metadata=job_schedule_metadata,
            )
            policy = resolve_freshness_policy(
                layer_name=spec.name,
                domain_name=name_clean,
                default_max_age_seconds=spec.max_age_seconds,
                overrides=freshness_overrides,
            )

            delta_version: Optional[int] = None
            try:
                delta_version, _ = store.get_delta_table_last_modified(
                    container=container,
                    table_path=table_path,
                )
            except Exception as exc:
                logger_runtime.info(
                    "Skipping delta version probe for table=%s container=%s: %s",
                    table_path,
                    container,
                    exc,
                )
                delta_version = None

            probe_resolution = resolve_last_updated_with_marker_probes(
                layer_name=spec.name,
                domain_name=name_clean,
                store=store,
                marker_cfg=marker_cfg,
            )
            last_modified = probe_resolution.last_updated
            if probe_resolution.status == "error":
                had_layer_error = True
                domain_items.append(
                    {
                        "name": name_clean,
                        "description": "",
                        "type": "delta",
                        "path": table_path,
                        "maxAgeSeconds": policy.max_age_seconds,
                        "cron": domain_cron,
                        "frequency": domain_frequency,
                        "lastUpdated": None,
                        "status": "error",
                        "version": None,
                        "portalUrl": folder_url,
                        "jobUrl": job_url,
                        "jobName": job_name,
                        "freshnessSource": probe_resolution.source,
                        "freshnessPolicySource": policy.source,
                        "warnings": probe_resolution.warnings,
                    }
                )
                continue

            status = compute_layer_status(
                now,
                last_modified,
                max_age_seconds=policy.max_age_seconds,
                had_error=False,
            )
            domain_items.append(
                {
                    "name": name_clean,
                    "description": _get_domain_description(spec.name, name_clean),
                    "type": "delta",
                    "path": table_path,
                    "maxAgeSeconds": policy.max_age_seconds,
                    "cron": domain_cron,
                    "frequency": domain_frequency,
                    "lastUpdated": iso(last_modified),
                    "status": status,
                    "version": delta_version if delta_version is not None else None,
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                    "freshnessSource": probe_resolution.source,
                    "freshnessPolicySource": policy.source,
                    "warnings": probe_resolution.warnings,
                }
            )

        valid_times = [
            datetime.fromisoformat(item["lastUpdated"])
            for item in domain_items
            if item["lastUpdated"] and item["status"] != "error"
        ]
        layer_last_updated = max(valid_times) if valid_times else None

        layer_statuses = [item["status"] for item in domain_items]
        if "error" in layer_statuses:
            layer_status = "error"
        elif "stale" in layer_statuses:
            layer_status = "stale"
        elif (
            spec.name.lower() == "platinum"
            and not had_layer_error
            and domain_items
            and layer_last_updated is None
            and all(str(item.get("status") or "").lower() == "healthy" for item in domain_items)
        ):
            layer_status = "healthy"
        else:
            layer_status = compute_layer_status(
                now,
                layer_last_updated,
                max_age_seconds=spec.max_age_seconds,
                had_error=had_layer_error,
            )

        statuses.append(layer_status)

        portal_url = _make_container_portal_url(sub_id, rg, storage_account, container)
        unique_frequencies = sorted(
            {
                str(item.get("frequency") or "").strip()
                for item in domain_items
                if item.get("frequency")
            }
        )
        refresh_frequency = unique_frequencies[0] if len(unique_frequencies) == 1 else "Multiple schedules"

        logger_runtime.info(
            "Layer probe complete: layer=%s status=%s domains=%s",
            spec.name,
            layer_status,
            len(domain_items),
        )
        layers.append(
            {
                "name": spec.name,
                "description": spec.description,
                "lastUpdated": iso(layer_last_updated),
                "status": layer_status,
                "maxAgeSeconds": spec.max_age_seconds,
                "refreshFrequency": refresh_frequency,
                "portalUrl": portal_url,
                "domains": domain_items,
            }
        )
        alerts.extend(
            layer_alerts(
                now,
                layer_name=spec.name,
                status=layer_status,
                last_updated=layer_last_updated,
                error=None,
            )
        )

    subscription_id = sub_id
    resource_group = rg
    app_names = _runtime_attr(runtime, "_split_csv")(os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS", ""))
    job_names = _runtime_attr(runtime, "_split_csv")(os.environ.get("SYSTEM_HEALTH_ARM_JOBS", ""))
    logger_runtime.info(
        "System health ARM probe config: subscription_set=%s resource_group_set=%s apps=%s jobs=%s "
        "arm_api_version_set=%s arm_timeout_set=%s resource_health_api_version_set=%s monitor_metrics_api_version_set=%s "
        "log_analytics_workspace_set=%s job_exec_limit_set=%s",
        bool(subscription_id),
        bool(resource_group),
        len(app_names),
        len(job_names),
        _runtime_attr(runtime, "_env_has_value")("SYSTEM_HEALTH_ARM_API_VERSION"),
        _runtime_attr(runtime, "_env_has_value")("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS"),
        _runtime_attr(runtime, "_env_has_value")("SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION"),
        _runtime_attr(runtime, "_env_has_value")("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION"),
        _runtime_attr(runtime, "_env_has_value")("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID"),
        _runtime_attr(runtime, "_env_has_value")("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB"),
    )
    if subscription_id and resource_group and (app_names or job_names):
        _collect_control_plane_snapshot(
            runtime=runtime,
            now=now,
            include_resource_ids=include_resource_ids,
            subscription_id=subscription_id,
            resource_group=resource_group,
            app_names=app_names,
            job_names=job_names,
            statuses=statuses,
            alerts=alerts,
            resources=resources,
            job_runs=job_runs,
        )
    else:
        logger_runtime.warning(
            "System health ARM probes not configured: subscription_set=%s resource_group_set=%s apps=%s jobs=%s",
            bool(subscription_id),
            bool(resource_group),
            len(app_names),
            len(job_names),
        )

    overall = overall_from_layers(statuses)
    payload: Dict[str, Any] = {"overall": overall, "dataLayers": layers, "recentJobs": job_runs, "alerts": alerts}
    if resources:
        payload["resources"] = resources
    logger_runtime.info(
        "System health summary: overall=%s layers=%s alerts=%s resources=%s jobs=%s",
        overall,
        len(layers),
        len(alerts),
        len(resources),
        len(job_runs),
    )
    return payload


def _collect_control_plane_snapshot(
    *,
    runtime: ModuleType,
    now: datetime,
    include_resource_ids: bool,
    subscription_id: str,
    resource_group: str,
    app_names,
    job_names,
    statuses: List[str],
    alerts: List[Dict[str, Any]],
    resources: List[Dict[str, Any]],
    job_runs: List[Dict[str, Any]],
) -> None:
    iso = _runtime_attr(runtime, "_iso")
    env_or_default = _runtime_attr(runtime, "_env_or_default")
    env_float_or_default = _runtime_attr(runtime, "_env_float_or_default")
    env_int_or_default = _runtime_attr(runtime, "_env_int_or_default")
    append_job_usage_percent_signals = _runtime_attr(runtime, "_append_job_usage_percent_signals")
    append_signal_details = _runtime_attr(runtime, "_append_signal_details")
    worse_resource_status = _runtime_attr(runtime, "_worse_resource_status")
    newer_execution = _runtime_attr(runtime, "_newer_execution")
    enrich_recent_job_retry_symbol_metadata = _runtime_attr(runtime, "_enrich_recent_job_retry_symbol_metadata")
    job_failure_reason_alerts = _runtime_attr(runtime, "_job_failure_reason_alerts")
    bronze_symbol_jump_alerts = _runtime_attr(runtime, "_bronze_symbol_jump_alerts")
    bronze_finance_zero_write_alerts = _runtime_attr(runtime, "_bronze_finance_zero_write_alerts")
    alert_id = _runtime_attr(runtime, "_alert_id")
    arm_config_cls = _runtime_attr(runtime, "ArmConfig")
    azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
    azure_log_analytics_client = _runtime_attr(runtime, "AzureLogAnalyticsClient")
    collect_container_apps = _runtime_attr(runtime, "collect_container_apps")
    collect_jobs_and_executions = _runtime_attr(runtime, "collect_jobs_and_executions")
    collect_monitor_metrics = _runtime_attr(runtime, "collect_monitor_metrics")
    collect_log_analytics_signals = _runtime_attr(runtime, "collect_log_analytics_signals")
    parse_metric_thresholds_json = _runtime_attr(runtime, "parse_metric_thresholds_json")
    parse_log_analytics_queries_json = _runtime_attr(runtime, "parse_log_analytics_queries_json")
    default_arm_api_version = _runtime_attr(runtime, "DEFAULT_ARM_API_VERSION")
    default_arm_timeout_seconds = _runtime_attr(runtime, "DEFAULT_SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    default_job_executions_per_job = _runtime_attr(runtime, "DEFAULT_SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB")
    default_monitor_metrics_timespan_minutes = _runtime_attr(
        runtime,
        "DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES",
    )
    default_monitor_metrics_interval = _runtime_attr(
        runtime,
        "DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL",
    )
    default_monitor_metrics_aggregation = _runtime_attr(
        runtime,
        "DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION",
    )
    default_containerapp_metric_names = _runtime_attr(runtime, "DEFAULT_SYSTEM_HEALTH_CONTAINERAPP_MONITOR_METRIC_NAMES")
    default_job_metric_names = _runtime_attr(runtime, "DEFAULT_SYSTEM_HEALTH_JOB_MONITOR_METRIC_NAMES")
    default_monitor_metrics_api_version = _runtime_attr(runtime, "DEFAULT_MONITOR_METRICS_API_VERSION")
    default_resource_health_api_version = _runtime_attr(runtime, "DEFAULT_RESOURCE_HEALTH_API_VERSION")
    default_log_analytics_timeout_seconds = _runtime_attr(
        runtime,
        "DEFAULT_SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS",
    )
    default_log_analytics_timespan_minutes = _runtime_attr(
        runtime,
        "DEFAULT_SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES",
    )
    resource_health_item_cls = _runtime_attr(runtime, "ResourceHealthItem")
    logger_runtime = _runtime_attr(runtime, "logger")

    api_version = env_or_default("SYSTEM_HEALTH_ARM_API_VERSION", default_arm_api_version)
    timeout_seconds = env_float_or_default(
        "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS",
        default_arm_timeout_seconds,
        min_value=0.5,
        max_value=30.0,
    )
    resource_health_api_version = env_or_default(
        "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION",
        default_resource_health_api_version,
    )
    containerapp_metric_names = _runtime_attr(runtime, "_split_csv")(os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS")) or (list(default_containerapp_metric_names) if app_names else [])
    job_metric_names = _runtime_attr(runtime, "_split_csv")(os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS")) or (list(default_job_metric_names) if job_names else [])
    monitor_metrics_enabled = bool(containerapp_metric_names or job_metric_names)
    monitor_metrics_api_version = (
        env_or_default("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION", default_monitor_metrics_api_version)
        if monitor_metrics_enabled
        else default_monitor_metrics_api_version
    )
    monitor_metrics_timespan_minutes = (
        env_int_or_default(
            "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES",
            default_monitor_metrics_timespan_minutes,
            min_value=1,
            max_value=24 * 60,
        )
        if monitor_metrics_enabled
        else 0
    )
    monitor_metrics_interval = (
        env_or_default("SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL", default_monitor_metrics_interval)
        if monitor_metrics_enabled
        else ""
    )
    monitor_metrics_aggregation = (
        env_or_default("SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION", default_monitor_metrics_aggregation)
        if monitor_metrics_enabled
        else ""
    )
    monitor_metrics_thresholds = {}
    raw_thresholds = (os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON") or "").strip()
    if raw_thresholds:
        try:
            monitor_metrics_thresholds = parse_metric_thresholds_json(raw_thresholds)
        except Exception as exc:
            alerts.append({"id": alert_id(severity="warning", title="Monitor metrics thresholds invalid", component="AzureMonitorMetrics"), "severity": "warning", "title": "Monitor metrics thresholds invalid", "component": "AzureMonitorMetrics", "timestamp": iso(now), "message": f"SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON parse error: {exc}"})

    log_analytics_workspace_id = (os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID") or "").strip()
    log_analytics_enabled = bool(log_analytics_workspace_id)
    log_analytics_timeout_seconds = (
        env_float_or_default(
            "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS",
            default_log_analytics_timeout_seconds,
            min_value=0.5,
            max_value=30.0,
        )
        if log_analytics_enabled
        else 0.0
    )
    log_analytics_timespan_minutes = (
        env_int_or_default(
            "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES",
            default_log_analytics_timespan_minutes,
            min_value=1,
            max_value=24 * 60,
        )
        if log_analytics_enabled
        else 0
    )
    log_analytics_queries = []
    raw_queries = (os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON") or "").strip()
    if raw_queries:
        try:
            log_analytics_queries = parse_log_analytics_queries_json(raw_queries)
        except Exception as exc:
            alerts.append({"id": alert_id(severity="warning", title="Log Analytics queries invalid", component="AzureLogAnalytics"), "severity": "warning", "title": "Log Analytics queries invalid", "component": "AzureLogAnalytics", "timestamp": iso(now), "message": f"SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON parse error: {exc}"})

    arm_cfg = arm_config_cls(subscription_id=subscription_id, resource_group=resource_group, api_version=api_version, timeout_seconds=timeout_seconds)
    checked_iso = iso(now)
    try:
        with azure_arm_client(arm_cfg) as arm:
            log_client: Optional[Any] = None
            if log_analytics_enabled:
                log_client = azure_log_analytics_client(timeout_seconds=log_analytics_timeout_seconds)

            def _enrich_resource(item: Any, *, metric_names) -> Any:
                status = item.status
                details = item.details
                signals: List[Dict[str, Any]] = list(item.signals)
                if monitor_metrics_enabled and metric_names and item.azure_id:
                    metric_signals, metric_status = collect_monitor_metrics(
                        arm,
                        resource_id=item.azure_id,
                        metric_names=metric_names,
                        end_time=now,
                        timespan_minutes=monitor_metrics_timespan_minutes,
                        interval=monitor_metrics_interval,
                        aggregation=monitor_metrics_aggregation,
                        api_version=monitor_metrics_api_version,
                        thresholds=monitor_metrics_thresholds,
                    )
                    if metric_signals:
                        metric_signals = append_job_usage_percent_signals(item, metric_signals)
                        signals.extend(metric_signals)
                        status = worse_resource_status(status, metric_status)
                        details = append_signal_details(details, metric_signals)
                if log_client is not None and item.azure_id:
                    log_signals, log_status = collect_log_analytics_signals(
                        log_client,
                        workspace_id=log_analytics_workspace_id,
                        specs=log_analytics_queries,
                        resource_type=item.resource_type,
                        resource_name=item.name,
                        resource_id=item.azure_id,
                        end_time=now,
                        timespan_minutes=log_analytics_timespan_minutes,
                    )
                    if log_signals:
                        signals.extend(log_signals)
                        status = worse_resource_status(status, log_status)
                        details = append_signal_details(details, log_signals)
                return resource_health_item_cls(
                    name=item.name,
                    resource_type=item.resource_type,
                    status=status,
                    last_checked=item.last_checked,
                    details=details,
                    azure_id=item.azure_id,
                    running_state=item.running_state,
                    last_modified_at=item.last_modified_at,
                    signals=tuple(signals),
                )

            def _record_resource(item: Any, *, title: str) -> None:
                resources.append(item.to_dict(include_ids=include_resource_ids))
                if item.status in {"warning", "error"}:
                    statuses.append("stale" if item.status == "warning" else "error")
                    alerts.append(
                        {
                            "id": alert_id(severity="warning" if item.status == "warning" else "error", title=title, component=item.name),
                            "severity": "warning" if item.status == "warning" else "error",
                            "title": title,
                            "component": item.name,
                            "timestamp": checked_iso,
                            "message": f"{item.resource_type}: {item.details}",
                        }
                    )

            try:
                if app_names:
                    logger_runtime.info("Collecting Azure container app health: count=%s", len(app_names))
                    app_resources = collect_container_apps(
                        arm,
                        app_names=app_names,
                        last_checked_iso=checked_iso,
                        include_ids=include_resource_ids,
                        resource_health_enabled=True,
                        resource_health_api_version=resource_health_api_version,
                    )
                    for item in app_resources:
                        _record_resource(_enrich_resource(item, metric_names=containerapp_metric_names), title="Azure resource health")

                if job_names:
                    logger_runtime.info("Collecting Azure job health: count=%s", len(job_names))
                    max_executions_per_job = env_int_or_default(
                        "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB",
                        default_job_executions_per_job,
                        min_value=1,
                        max_value=25,
                    )
                    job_resources, runs = collect_jobs_and_executions(
                        arm,
                        job_names=job_names,
                        last_checked_iso=checked_iso,
                        include_ids=include_resource_ids,
                        max_executions_per_job=max_executions_per_job,
                        resource_health_enabled=True,
                        resource_health_api_version=resource_health_api_version,
                    )
                    run_counts: Dict[str, int] = {}
                    for run in runs:
                        run_job_name = str(run.get("jobName") or "").strip()
                        if run_job_name:
                            run_counts[run_job_name] = run_counts.get(run_job_name, 0) + 1
                    jobs_without_runs = [name for name in job_names if run_counts.get(name, 0) == 0]
                    logger_runtime.info(
                        "Azure job execution summary: configured_jobs=%s resources=%s runs=%s max_per_job=%s jobs_without_runs=%s",
                        len(job_names),
                        len(job_resources),
                        len(runs),
                        max_executions_per_job,
                        len(jobs_without_runs),
                    )
                    if jobs_without_runs:
                        logger_runtime.warning("Azure job execution summary missing runs: jobs=%s", ",".join(jobs_without_runs[:20]))
                    for item in job_resources:
                        _record_resource(_enrich_resource(item, metric_names=job_metric_names), title="Azure job health")
                    enrich_recent_job_retry_symbol_metadata(runs=runs, log_client=log_client, workspace_id=log_analytics_workspace_id)
                    job_runs.extend(runs)
                    latest_by_job: Dict[str, Dict[str, Any]] = {}
                    for run in runs:
                        run_job_name = str(run.get("jobName") or "").strip()
                        if run_job_name and newer_execution(run, latest_by_job.get(run_job_name)):
                            latest_by_job[run_job_name] = run
                    for run in latest_by_job.values():
                        if run.get("status") != "failed":
                            continue
                        run_job_name = str(run.get("jobName") or "job")
                        statuses.append("error")
                        reason_alerts = job_failure_reason_alerts(run=run, checked_iso=checked_iso, log_client=log_client, workspace_id=log_analytics_workspace_id)
                        if reason_alerts:
                            alerts.extend(reason_alerts)
                        else:
                            start_time = str(run.get("startTime") or "")
                            message = "Latest execution reported failed." if not start_time else f"Latest execution reported failed (startTime={start_time})."
                            alerts.append({"id": alert_id(severity="error", title="Job execution failed", component=run_job_name), "severity": "error", "title": "Job execution failed", "component": run_job_name, "timestamp": checked_iso, "message": message})
                    jump_alerts = bronze_symbol_jump_alerts(job_names=job_names, checked_iso=checked_iso, log_client=log_client, workspace_id=log_analytics_workspace_id)
                    if jump_alerts:
                        alerts.extend(jump_alerts)
                        statuses.extend("error" if alert.get("severity") == "error" else "stale" for alert in jump_alerts)
                    zero_write_alerts = bronze_finance_zero_write_alerts(job_names=job_names, checked_iso=checked_iso, log_client=log_client, workspace_id=log_analytics_workspace_id)
                    if zero_write_alerts:
                        alerts.extend(zero_write_alerts)
                        statuses.extend("error" if alert.get("severity") == "error" else "stale" for alert in zero_write_alerts)
            finally:
                if log_client is not None:
                    log_client.close()
    except Exception as exc:
        logger_runtime.exception(
            "Azure control-plane probes failed: subscription_set=%s resource_group_set=%s apps=%s jobs=%s error=%s",
            bool(subscription_id),
            bool(resource_group),
            len(app_names),
            len(job_names),
            exc,
        )
        alerts.append(
            {
                "id": alert_id(severity="warning", title="Azure monitoring unavailable", component="AzureControlPlane"),
                "severity": "warning",
                "title": "Azure monitoring unavailable",
                "component": "AzureControlPlane",
                "timestamp": iso(now),
                "message": f"Control-plane probe error: {exc}",
            }
        )
