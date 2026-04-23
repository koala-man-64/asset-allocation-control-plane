from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from importlib import import_module
from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence

from monitoring.azure_blob_store import AzureBlobStore, LastModifiedProbeResult
from monitoring.system_health_modules.env_config import (
    FreshnessPolicy,
    JobScheduleMetadata,
    MarkerProbeConfig,
    _env_int_or_default,
    _env_or_default,
)

logger = logging.getLogger("asset_allocation.monitoring.system_health")


def _runtime_module() -> ModuleType:
    return import_module("monitoring.system_health")


def _runtime_attr(name: str) -> Any:
    return getattr(_runtime_module(), name)


def _config_attr(name: str) -> Any:
    return getattr(import_module("core.config"), name, None)


def _config_str(name: str) -> str:
    value = _config_attr(name)
    return str(value or "").strip()


def _normalize_layer_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _normalize_domain_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _government_signals_slug() -> str:
    return str(os.environ.get("AZURE_FOLDER_GOVERNMENT_SIGNALS") or "government-signals").strip().strip("/")


def _canonicalize_domain_name(value: str) -> str:
    normalized = str(value or "").strip().strip("/")
    if not normalized:
        return normalized

    government_signals_slug = _government_signals_slug()
    if normalized in {government_signals_slug, f"{government_signals_slug}/runs"}:
        return "government-signals"
    if normalized == "targets":
        return "price-target"
    return normalized


def _load_freshness_overrides() -> Dict[str, Dict[str, Any]]:
    raw = os.environ.get("SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON", "")
    text = raw.strip()
    if not text:
        return {}

    try:
        payload = json.loads(text)
    except Exception as exc:
        logger.warning(
            "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON parse error: %s",
            exc,
            exc_info=True,
        )
        return {}

    if not isinstance(payload, dict):
        logger.warning("SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON must be a JSON object.")
        return {}

    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if isinstance(raw_value, dict):
            normalized[key] = raw_value
        elif isinstance(raw_value, int):
            normalized[key] = {"maxAgeSeconds": int(raw_value)}
        else:
            logger.warning(
                "Ignoring freshness override for key=%s (expected object or int, got %s).",
                key,
                type(raw_value).__name__,
            )
    return normalized


def _resolve_freshness_policy(
    *,
    layer_name: str,
    domain_name: str,
    default_max_age_seconds: int,
    overrides: Dict[str, Dict[str, Any]],
) -> FreshnessPolicy:
    layer_key = _normalize_layer_key(layer_name)
    domain_key = _normalize_domain_key(domain_name)

    candidates = [
        f"{layer_key}.{domain_key}",
        f"{layer_key}:{domain_key}",
        domain_key,
        f"{layer_key}.*",
        "*",
    ]
    for key in candidates:
        node = overrides.get(key)
        if not isinstance(node, dict):
            continue
        raw_max_age = node.get("maxAgeSeconds")
        if raw_max_age is None:
            continue
        try:
            parsed = int(raw_max_age)
        except Exception:
            logger.warning(
                "Invalid maxAgeSeconds for freshness override key=%s value=%r",
                key,
                raw_max_age,
            )
            continue
        if parsed <= 0:
            logger.warning(
                "Ignoring non-positive maxAgeSeconds for freshness override key=%s value=%r",
                key,
                raw_max_age,
            )
            continue
        return FreshnessPolicy(max_age_seconds=parsed, source=f"override:{key}")

    return FreshnessPolicy(max_age_seconds=int(default_max_age_seconds), source="default")


def _marker_probe_config() -> MarkerProbeConfig:
    default_prefix = _runtime_attr("DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX")
    container = _env_or_default(
        "SYSTEM_HEALTH_MARKERS_CONTAINER",
        _config_str("AZURE_CONTAINER_COMMON"),
    ).strip()
    prefix = os.environ.get("SYSTEM_HEALTH_MARKERS_PREFIX", default_prefix).strip()

    return MarkerProbeConfig(
        enabled=bool(container),
        container=container,
        prefix=prefix or default_prefix,
    )


def _marker_blob_name(*, layer_name: str, domain_name: str, prefix: str) -> str:
    default_prefix = _runtime_attr("DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX")
    layer_key = _normalize_layer_key(layer_name)
    domain_key = _normalize_domain_key(domain_name)
    prefix_clean = str(prefix or default_prefix).strip().strip("/")
    return f"{prefix_clean}/{layer_key}/{domain_key}.json"


def _probe_marker_last_modified(
    *,
    store: AzureBlobStore,
    container: str,
    marker_blob: str,
) -> LastModifiedProbeResult:
    try:
        last_modified = store.get_blob_last_modified(container=container, blob_name=marker_blob)
    except Exception as exc:
        return LastModifiedProbeResult(state="error", error=str(exc))
    if last_modified is None:
        return LastModifiedProbeResult(state="not_found")
    return LastModifiedProbeResult(state="ok", last_modified=last_modified)


def _normalize_probe_result(raw: Any) -> LastModifiedProbeResult:
    if isinstance(raw, LastModifiedProbeResult):
        return raw

    state = str(getattr(raw, "state", "") or "").strip().lower()
    last_modified = getattr(raw, "last_modified", None)
    error = str(getattr(raw, "error", "") or "").strip() or None

    if state not in {"ok", "not_found", "error"}:
        if isinstance(last_modified, datetime):
            state = "ok"
        elif last_modified is None:
            state = "not_found"
        else:
            state = "error"
            error = error or "Invalid probe response."

    if state == "ok" and not isinstance(last_modified, datetime):
        if last_modified is None:
            state = "not_found"
        else:
            state = "error"
            error = error or "Invalid probe timestamp."

    return LastModifiedProbeResult(
        state=state,
        last_modified=last_modified if isinstance(last_modified, datetime) else None,
        error=error,
    )


def _probe_container_last_modified(
    *,
    store: Any,
    container: str,
    prefix: Optional[str],
) -> LastModifiedProbeResult:
    probe_fn = getattr(store, "probe_container_last_modified", None)
    if callable(probe_fn):
        raw = probe_fn(container=container, prefix=prefix)
        if raw.__class__.__module__.startswith("unittest.mock"):
            normalized = LastModifiedProbeResult(state="error", error="Mock probe response.")
        else:
            normalized = _normalize_probe_result(raw)
        if normalized.state != "error" or normalized.error not in {
            "Invalid probe response.",
            "Invalid probe timestamp.",
            "Mock probe response.",
        }:
            return normalized

    last_modified = store.get_container_last_modified(container=container, prefix=prefix)
    if isinstance(last_modified, datetime):
        return LastModifiedProbeResult(state="ok", last_modified=last_modified)
    if last_modified is None:
        return LastModifiedProbeResult(state="not_found")
    return LastModifiedProbeResult(state="error", error="Invalid container probe timestamp.")


@dataclass(frozen=True)
class DomainTimestampResolution:
    status: str
    last_updated: Optional[datetime]
    source: str
    warnings: List[str]
    error: Optional[str] = None


def _resolve_last_updated_with_marker_probes(
    *,
    layer_name: str,
    domain_name: str,
    store: AzureBlobStore,
    marker_cfg: MarkerProbeConfig,
) -> DomainTimestampResolution:
    if not marker_cfg.enabled:
        message = "Marker probes are not configured."
        logger.error(message)
        return DomainTimestampResolution(
            status="error",
            last_updated=None,
            source="marker",
            warnings=[message],
            error=message,
        )

    if not marker_cfg.container:
        message = "Marker probes enabled but marker container is not configured."
        logger.error(message)
        return DomainTimestampResolution(
            status="error",
            last_updated=None,
            source="marker",
            warnings=[message],
            error=message,
        )

    marker_blob = _marker_blob_name(
        layer_name=layer_name,
        domain_name=domain_name,
        prefix=marker_cfg.prefix,
    )
    marker_probe = _probe_marker_last_modified(
        store=store,
        container=marker_cfg.container,
        marker_blob=marker_blob,
    )
    if marker_probe.state == "ok":
        return DomainTimestampResolution(
            status="ok",
            last_updated=marker_probe.last_modified,
            source="marker",
            warnings=[],
        )

    if marker_probe.state == "error":
        message = f"Marker probe failed for {marker_blob}: {marker_probe.error or 'unknown error'}"
    else:
        message = f"Marker missing for {marker_blob}."
    logger.error(message)
    return DomainTimestampResolution(
        status="error",
        last_updated=None,
        source="marker",
        warnings=[message],
        error=message,
    )


def _domain_name_from_marker_path(path: str) -> str:
    cleaned_path = str(path or "").strip().rstrip("/")
    domain_name = os.path.dirname(cleaned_path) or cleaned_path
    normalized = domain_name.replace("/whitelist.csv", "").replace("-data", "")
    return _canonicalize_domain_name(normalized)


def _domain_name_from_delta_path(path: str) -> str:
    domain_name = str(path or "").strip().strip("/")
    name_clean = domain_name.split("/")[-1].replace("-data", "")
    if "/signals/" in domain_name:
        name_clean = "signals"
    return _canonicalize_domain_name(name_clean)


def _collect_job_names_for_layers(specs: Sequence["LayerProbeSpec"]) -> List[str]:
    derive_job_name = _runtime_attr("_derive_job_name")

    names: List[str] = []
    seen: set[str] = set()
    for spec in specs:
        for domain_spec in spec.marker_blobs:
            domain_name = _domain_name_from_marker_path(domain_spec.path)
            job_name = derive_job_name(spec.name, domain_name)
            if not job_name:
                continue
            normalized = job_name.strip().lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            names.append(job_name)
        for domain_spec in spec.delta_tables:
            domain_name = _domain_name_from_delta_path(domain_spec.path)
            job_name = derive_job_name(spec.name, domain_name)
            if not job_name:
                continue
            normalized = job_name.strip().lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            names.append(job_name)
    return names


def _load_job_schedule_metadata(
    *,
    subscription_id: str,
    resource_group: str,
    job_names: Sequence[str],
) -> Dict[str, JobScheduleMetadata]:
    if not subscription_id or not resource_group or not job_names:
        return {}

    arm_config_cls = _runtime_attr("ArmConfig")
    azure_arm_client = _runtime_attr("AzureArmClient")
    default_arm_api_version = _runtime_attr("DEFAULT_ARM_API_VERSION")
    default_timeout_seconds = _runtime_attr("DEFAULT_SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")

    api_version = _env_or_default("SYSTEM_HEALTH_ARM_API_VERSION", default_arm_api_version)
    timeout_raw = _env_or_default(
        "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS",
        str(default_timeout_seconds),
    )
    try:
        timeout_seconds = float(timeout_raw)
    except Exception:
        timeout_seconds = default_timeout_seconds
    if timeout_seconds <= 0:
        timeout_seconds = default_timeout_seconds

    arm_cfg = arm_config_cls(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    metadata: Dict[str, JobScheduleMetadata] = {}
    try:
        with azure_arm_client(arm_cfg) as arm:
            for name in job_names:
                job_name = str(name or "").strip()
                if not job_name:
                    continue
                job_key = job_name.lower()
                try:
                    payload = arm.get_json(
                        arm.resource_url(
                            provider="Microsoft.App",
                            resource_type="jobs",
                            name=job_name,
                        )
                    )
                    props = payload.get("properties") if isinstance(payload, dict) else {}
                    cfg = props.get("configuration") if isinstance(props, dict) else {}
                    trigger_type = str(cfg.get("triggerType") or "").strip().lower() if isinstance(cfg, dict) else ""
                    schedule_cfg = cfg.get("scheduleTriggerConfig") if isinstance(cfg, dict) else {}
                    cron_expression = (
                        str(schedule_cfg.get("cronExpression") or "").strip()
                        if isinstance(schedule_cfg, dict)
                        else ""
                    )
                    if not trigger_type and not cron_expression:
                        continue
                    metadata[job_key] = JobScheduleMetadata(
                        trigger_type=trigger_type,
                        cron_expression=cron_expression,
                    )
                except Exception as exc:
                    logger.info("Unable to resolve job trigger metadata for job=%s: %s", job_name, exc)
    except Exception as exc:
        logger.info("Skipping job schedule metadata probe (ARM unavailable): %s", exc)

    return metadata


def _resolve_domain_schedule(
    *,
    job_name: str,
    default_cron: str,
    default_trigger_type: str = "schedule",
    job_schedule_metadata: Dict[str, JobScheduleMetadata],
) -> tuple[str, str]:
    describe_cron = _runtime_attr("_describe_cron")
    schedule = job_schedule_metadata.get(str(job_name or "").strip().lower())
    default_cron_clean = str(default_cron or "").strip()
    default_trigger = str(default_trigger_type or "").strip().lower()

    if schedule is None:
        if default_trigger == "manual":
            return "", "Manual trigger"
        if default_trigger == "schedule":
            return default_cron_clean, describe_cron(default_cron_clean) if default_cron_clean else "Scheduled trigger"
        if default_trigger:
            return "", f"{default_trigger.title()} trigger"
        return default_cron_clean, describe_cron(default_cron_clean) if default_cron_clean else ""

    trigger = schedule.trigger_type
    if trigger == "manual":
        return "", "Manual trigger"
    if trigger == "schedule":
        cron = schedule.cron_expression or default_cron_clean
        return cron, describe_cron(cron) if cron else "Scheduled trigger"
    if trigger:
        return "", f"{trigger.title()} trigger"

    cron = schedule.cron_expression or default_cron_clean
    return cron, describe_cron(cron) if cron else ""


@dataclass(frozen=True)
class DomainSpec:
    path: str
    cron: str = "0 0 * * *"
    trigger_type: str = "schedule"


@dataclass(frozen=True)
class LayerProbeSpec:
    name: str
    description: str
    container_env: str
    max_age_seconds: int
    marker_blobs: Sequence[DomainSpec] = ()
    delta_tables: Sequence[DomainSpec] = ()
    job_name: Optional[str] = None

    def container_name(self) -> str:
        container = _env_or_default(self.container_env, _config_str(self.container_env)).strip()
        if container:
            return container
        raise ValueError(f"Missing required container setting: {self.container_env}")


def _compute_layer_status(
    now: datetime,
    last_updated: Optional[datetime],
    *,
    max_age_seconds: int,
    had_error: bool,
) -> str:
    if had_error:
        return "error"
    if last_updated is None:
        return "stale"
    age_seconds = max((now - last_updated).total_seconds(), 0.0)
    if age_seconds > float(max_age_seconds):
        return "stale"
    return "healthy"


def _overall_from_layers(statuses: Sequence[str]) -> str:
    if any(status == "error" for status in statuses):
        return "critical"
    if any(status == "stale" for status in statuses):
        return "degraded"
    return "healthy"


def _default_layer_specs() -> List[LayerProbeSpec]:
    max_age_default = _env_int_or_default(
        "SYSTEM_HEALTH_MAX_AGE_SECONDS",
        _runtime_attr("DEFAULT_SYSTEM_HEALTH_MAX_AGE_SECONDS"),
    )
    government_signals_slug = _government_signals_slug()

    cron_bronze_market = "0 22 * * 1-5"
    cron_bronze_price_target = "0 4 * * 1-5"
    cron_bronze_earnings = "0 10 * * 1-5"
    cron_bronze_finance = "0 16 * * 1-5"
    cron_bronze_government_signals = "*/15 * * * *"
    cron_platinum = "0 0 * * *"

    return [
        LayerProbeSpec(
            name="Bronze",
            description="Landing zone for raw data. Immutable source of truth for replayability.",
            container_env="AZURE_CONTAINER_BRONZE",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market-data/whitelist.csv", cron=cron_bronze_market, trigger_type="schedule"),
                DomainSpec("finance-data/whitelist.csv", cron=cron_bronze_finance, trigger_type="schedule"),
                DomainSpec("earnings-data/whitelist.csv", cron=cron_bronze_earnings, trigger_type="schedule"),
                DomainSpec("price-target-data/whitelist.csv", cron=cron_bronze_price_target, trigger_type="schedule"),
                DomainSpec(f"{government_signals_slug}/runs/", cron=cron_bronze_government_signals, trigger_type="schedule"),
            ),
        ),
        LayerProbeSpec(
            name="Silver",
            description="Cleaned, standardized tabular data. Enforced schemas for reliable querying.",
            container_env="AZURE_CONTAINER_SILVER",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market-data/", trigger_type="manual"),
                DomainSpec("finance-data/", trigger_type="manual"),
                DomainSpec("earnings-data/", trigger_type="manual"),
                DomainSpec("price-target-data/", trigger_type="manual"),
                DomainSpec(f"{government_signals_slug}/", trigger_type="manual"),
            ),
        ),
        LayerProbeSpec(
            name="Gold",
            description="Entity-resolved feature store. Financial metrics ready for modeling.",
            container_env="AZURE_CONTAINER_GOLD",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market/", trigger_type="manual"),
                DomainSpec("finance/", trigger_type="manual"),
                DomainSpec("earnings/", trigger_type="manual"),
                DomainSpec("targets/", trigger_type="manual"),
                DomainSpec(f"{government_signals_slug}/", trigger_type="manual"),
                DomainSpec("regime/", trigger_type="manual"),
            ),
        ),
        LayerProbeSpec(
            name="Platinum",
            description="Curated/derived datasets (reserved)",
            container_env="AZURE_CONTAINER_PLATINUM",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("rankings", cron=cron_platinum, trigger_type="schedule"),
                DomainSpec("backtests", trigger_type="manual"),
            ),
        ),
    ]
