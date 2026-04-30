from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from monitoring.arm_client import AzureArmClient
from monitoring.resource_health import DEFAULT_RESOURCE_HEALTH_API_VERSION, get_current_availability

logger = logging.getLogger("asset_allocation.monitoring.control_plane")

_ACTIVE_EXECUTION_STATUS_TOKENS = frozenset(
    {"running", "processing", "inprogress", "starting", "queued", "waiting", "scheduling"}
)
_CONTAINER_APP_RESOURCE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?$")
_MEMORY_QUANTITY_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]*)\s*$")
_MEMORY_UNIT_FACTORS = {
    "": 1,
    "b": 1,
    "k": 1_000,
    "kb": 1_000,
    "m": 1_000_000,
    "mb": 1_000_000,
    "g": 1_000_000_000,
    "gb": 1_000_000_000,
    "t": 1_000_000_000_000,
    "tb": 1_000_000_000_000,
    "p": 1_000_000_000_000_000,
    "pb": 1_000_000_000_000_000,
    "ki": 1024,
    "kib": 1024,
    "mi": 1024**2,
    "mib": 1024**2,
    "gi": 1024**3,
    "gib": 1024**3,
    "ti": 1024**4,
    "tib": 1024**4,
    "pi": 1024**5,
    "pib": 1024**5,
}


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _duration_seconds(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if not start or not end:
        return None
    seconds = int((end - start).total_seconds())
    return seconds if seconds >= 0 else None


def _normalize_job_status_token(raw: str) -> str:
    return "".join(ch for ch in (raw or "").strip().lower() if ch.isalnum())


def _is_active_execution_status(raw: str) -> bool:
    return _normalize_job_status_token(raw) in _ACTIVE_EXECUTION_STATUS_TOKENS


def _dedupe_resource_names(names: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in names:
        name = str(raw or "").strip()
        if not name or not _CONTAINER_APP_RESOURCE_NAME_RE.fullmatch(name):
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def discover_container_app_job_names(arm: AzureArmClient) -> List[str]:
    """
    List Container App Jobs in the configured ARM resource group.

    An explicit SYSTEM_HEALTH_ARM_JOBS value can still narrow the monitored set, but
    the default should be resource-group discovery so new jobs are available without
    an application config edit.
    """
    names: List[str] = []
    seen: set[str] = set()
    url = arm.resource_collection_url(provider="Microsoft.App", resource_type="jobs")

    while url:
        payload = arm.get_json(url)
        values = payload.get("value") if isinstance(payload.get("value"), list) else []
        for item in values:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name or not _CONTAINER_APP_RESOURCE_NAME_RE.fullmatch(name):
                continue
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            names.append(name)
        next_link = payload.get("nextLink")
        url = str(next_link or "").strip()

    return sorted(names, key=str.lower)


def resolve_container_app_job_names(
    arm: AzureArmClient,
    *,
    configured_job_names: Sequence[str],
) -> List[str]:
    configured = _dedupe_resource_names(configured_job_names)
    if configured:
        return configured
    return discover_container_app_job_names(arm)


def _include_anchored_active_execution(
    executions: Sequence[Dict[str, Any]], *, limit: int
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []

    selected = list(executions[:limit])
    if not selected:
        return selected

    active_execution = next(
        (
            item
            for item in executions
            if _map_job_execution_status(
                str(
                    (
                        item.get("properties")
                        if isinstance(item.get("properties"), dict)
                        else {}
                    ).get("status")
                    or ""
                ),
                end_time=str(
                    (
                        item.get("properties")
                        if isinstance(item.get("properties"), dict)
                        else {}
                    ).get("endTime")
                    or ""
                ),
            )
            == "running"
        ),
        None,
    )
    if active_execution is None or active_execution in selected:
        return selected

    return [active_execution, *selected[: max(0, limit - 1)]]


def _map_job_execution_status(raw: str, *, end_time: Optional[str] = None) -> str:
    status = _normalize_job_status_token(raw)
    has_end_time = bool((end_time or "").strip())
    if status in {"succeeded", "success", "completed", "complete"}:
        return "success"
    if status in {"succeededwithwarnings", "completedwithwarnings", "warning"}:
        return "warning"
    if status in {"failed", "error", "failure", "terminated", "terminatedwitherror"}:
        return "failed"
    if has_end_time and status in _ACTIVE_EXECUTION_STATUS_TOKENS:
        # Azure job executions can surface a terminal endTime while leaving status as Running in the ARM response.
        # Treat the execution as completed so dashboards do not report a finished job as still active.
        return "success"
    if status in _ACTIVE_EXECUTION_STATUS_TOKENS:
        return "running"
    if status in {"stopped", "canceled", "cancelled", "canceling", "cancellationrequested"}:
        return "failed"
    return "pending"


def _normalize_execution_start_time(value: Optional[datetime], fallback: str) -> str:
    if value is None:
        return fallback
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _job_type_from_name(job_name: str) -> str:
    text = (job_name or "").lower()
    if "backtest" in text:
        return "backtest"
    if "risk" in text:
        return "risk-calc"
    if "attribution" in text:
        return "attribution"
    if "rank" in text or "signal" in text or "portfolio" in text:
        return "portfolio-build"
    return "data-ingest"


def _resource_status_from_provisioning_state(state: str, *, has_ready_signal: bool = True) -> Tuple[str, str]:
    raw = (state or "").strip()
    normalized = raw.lower()
    if normalized == "succeeded":
        return ("healthy" if has_ready_signal else "warning"), raw or "Succeeded"
    if normalized in {"failed", "canceled", "cancelled"}:
        return "error", raw or "Failed"
    if normalized in {"creating", "updating", "deleting", "inprogress"}:
        return "warning", raw or "InProgress"
    if not raw:
        return "unknown", "Unknown"
    return "warning", raw


def _combine_status(primary: str, secondary: str) -> str:
    status_order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if status_order.get(secondary, 0) > status_order.get(primary, 0) else primary


def _extract_resource_last_modified_at(payload: Dict[str, Any]) -> Optional[str]:
    system_data = payload.get("systemData") if isinstance(payload.get("systemData"), dict) else {}
    last_modified = str(system_data.get("lastModifiedAt") or "").strip()
    return last_modified or None


def _parse_positive_float(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed > 0 else None

    text = str(value or "").strip()
    if not text:
        return None

    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _parse_memory_bytes(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed > 0 else None

    text = str(value or "").strip()
    if not text:
        return None

    match = _MEMORY_QUANTITY_RE.match(text)
    if not match:
        return None

    try:
        magnitude = float(match.group("value"))
    except ValueError:
        return None
    if magnitude <= 0:
        return None

    unit = match.group("unit").strip().lower()
    factor = _MEMORY_UNIT_FACTORS.get(unit)
    if factor is None:
        return None
    return magnitude * factor


def _extract_job_resource_limits(job_props: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    template = job_props.get("template") if isinstance(job_props.get("template"), dict) else {}
    containers = template.get("containers") if isinstance(template.get("containers"), list) else []

    cpu_limit_cores = 0.0
    memory_limit_bytes = 0.0
    has_cpu_limit = False
    has_memory_limit = False

    for container in containers:
        if not isinstance(container, dict):
            continue
        resources = container.get("resources") if isinstance(container.get("resources"), dict) else {}

        cpu = _parse_positive_float(resources.get("cpu"))
        if cpu is not None:
            cpu_limit_cores += cpu
            has_cpu_limit = True

        memory = _parse_memory_bytes(resources.get("memory"))
        if memory is not None:
            memory_limit_bytes += memory
            has_memory_limit = True

    return (
        cpu_limit_cores if has_cpu_limit else None,
        memory_limit_bytes if has_memory_limit else None,
    )


@dataclass(frozen=True)
class ResourceHealthItem:
    name: str
    resource_type: str
    status: str  # healthy|warning|error|unknown
    last_checked: str
    details: str
    azure_id: Optional[str] = None
    running_state: Optional[str] = None
    last_modified_at: Optional[str] = None
    signals: Tuple[Dict[str, Any], ...] = ()
    cpu_limit_cores: Optional[float] = None
    memory_limit_bytes: Optional[float] = None

    def to_dict(self, *, include_ids: bool) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "resourceType": self.resource_type,
            "status": self.status,
            "lastChecked": self.last_checked,
            "details": self.details,
        }
        if include_ids and self.azure_id:
            payload["azureId"] = self.azure_id
        if self.running_state:
            payload["runningState"] = self.running_state
        if self.last_modified_at:
            payload["lastModifiedAt"] = self.last_modified_at
        if self.signals:
            payload["signals"] = list(self.signals)
        return payload


def collect_container_apps(
    arm: AzureArmClient,
    *,
    app_names: Sequence[str],
    last_checked_iso: str,
    include_ids: bool,
    resource_health_enabled: bool = False,
    resource_health_api_version: str = DEFAULT_RESOURCE_HEALTH_API_VERSION,
) -> List[ResourceHealthItem]:
    items: List[ResourceHealthItem] = []
    for name in app_names:
        url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=name)
        try:
            payload = arm.get_json(url)
            props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
            provisioning_state = str(props.get("provisioningState") or "")
            latest_ready = str(props.get("latestReadyRevisionName") or "")
            status, state_text = _resource_status_from_provisioning_state(
                provisioning_state, has_ready_signal=bool(latest_ready)
            )
            resource_id = str(payload.get("id") or "") or None
            last_modified_at = _extract_resource_last_modified_at(payload if isinstance(payload, dict) else {})
            details = f"provisioningState={state_text}"
            if latest_ready:
                details += f", latestReadyRevision={latest_ready}"

            if resource_health_enabled and resource_id:
                signal, availability_status = get_current_availability(
                    arm, resource_id=resource_id, api_version=resource_health_api_version
                )
                if signal is not None:
                    status = _combine_status(status, availability_status)
                    details += f", {signal.to_details_fragment()}"
            items.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/containerApps",
                    status=status,
                    last_checked=last_checked_iso,
                    details=details,
                    azure_id=resource_id,
                    last_modified_at=last_modified_at,
                )
            )
        except Exception as exc:
            items.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/containerApps",
                    status="error",
                    last_checked=last_checked_iso,
                    details=f"probe_error={exc}",
                    azure_id=None,
                )
            )
    return items


def collect_jobs_and_executions(
    arm: AzureArmClient,
    *,
    job_names: Sequence[str],
    last_checked_iso: str,
    include_ids: bool,
    max_executions_per_job: int = 3,
    resource_health_enabled: bool = False,
    resource_health_api_version: str = DEFAULT_RESOURCE_HEALTH_API_VERSION,
) -> Tuple[List[ResourceHealthItem], List[Dict[str, Any]]]:
    resources: List[ResourceHealthItem] = []
    runs: List[Dict[str, Any]] = []
    last_checked_dt = _parse_dt(last_checked_iso) or datetime.now(timezone.utc)

    for name in job_names:
        job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=name)
        try:
            job_payload = arm.get_json(job_url)
            job_props = job_payload.get("properties") if isinstance(job_payload.get("properties"), dict) else {}
            provisioning_state = str(job_props.get("provisioningState") or "")
            status, state_text = _resource_status_from_provisioning_state(provisioning_state, has_ready_signal=True)
            cpu_limit_cores, memory_limit_bytes = _extract_job_resource_limits(job_props)

            resource_id = str(job_payload.get("id") or "") or None
            last_modified_at = _extract_resource_last_modified_at(
                job_payload if isinstance(job_payload, dict) else {}
            )
            details = f"provisioningState={state_text}"
            running_state_raw = str(job_props.get("runningState") or "").strip()
            running_state = running_state_raw or None
            if running_state:
                details += f", runningState={running_state}"
            if resource_health_enabled and resource_id:
                signal, availability_status = get_current_availability(
                    arm, resource_id=resource_id, api_version=resource_health_api_version
                )
                if signal is not None:
                    status = _combine_status(status, availability_status)
                    details += f", {signal.to_details_fragment()}"
            resources.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/jobs",
                    status=status,
                    last_checked=last_checked_iso,
                    details=details,
                    azure_id=resource_id,
                    running_state=running_state,
                    last_modified_at=last_modified_at,
                    cpu_limit_cores=cpu_limit_cores,
                    memory_limit_bytes=memory_limit_bytes,
                )
            )
        except Exception as exc:
            resources.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/jobs",
                    status="error",
                    last_checked=last_checked_iso,
                    details=f"probe_error={exc}",
                    azure_id=None,
                )
            )
            continue

        executions_url = f"{job_url}/executions"
        try:
            exec_payload = arm.get_json(executions_url)
            values = exec_payload.get("value") if isinstance(exec_payload.get("value"), list) else []
            executions: List[Dict[str, Any]] = [item for item in values if isinstance(item, dict)]

            def _execution_start_ts(execution: Dict[str, Any]) -> float:
                props = execution.get("properties") if isinstance(execution.get("properties"), dict) else {}
                start_dt = _parse_dt(str(props.get("startTime") or ""))
                if start_dt:
                    return float(start_dt.timestamp())
                if _is_active_execution_status(str(props.get("status") or "")):
                    return float(last_checked_dt.timestamp())
                return 0.0

            # Ensure we sample the most recent executions for each job regardless of API ordering.
            executions.sort(key=_execution_start_ts, reverse=True)
            sampled = _include_anchored_active_execution(
                executions, limit=max_executions_per_job
            )
            logger.info(
                "Job execution listing: job=%s total=%s sampled=%s",
                name,
                len(executions),
                len(sampled),
            )
            if not sampled:
                logger.warning(
                    "Job execution listing returned no runs: job=%s url=%s",
                    name,
                    executions_url,
                )
            for item in sampled:
                if not isinstance(item, dict):
                    continue
                props = item.get("properties") if isinstance(item.get("properties"), dict) else {}
                raw_status = str(props.get("status") or "")
                start_time = str(props.get("startTime") or "")
                end_time = str(props.get("endTime") or "")

                start_dt = _parse_dt(start_time)
                end_dt = _parse_dt(end_time)
                duration = _duration_seconds(start_dt, end_dt)
                normalized_start_time = _normalize_execution_start_time(start_dt, start_time or last_checked_iso)

                runs.append(
                    {
                        "jobName": name,
                        "jobType": _job_type_from_name(name),
                        "status": _map_job_execution_status(raw_status, end_time=end_time),
                        "statusCode": raw_status or None,
                        "executionName": str(item.get("name") or "") or None,
                        "executionId": str(item.get("id") or "") or None,
                        "startTime": normalized_start_time,
                        "endTime": end_time or None,
                        "duration": duration,
                        "triggeredBy": "azure",
                    }
                )
        except Exception as exc:
            # Executions are best-effort; rely on job resource status + alerts in aggregation.
            logger.warning(
                "Failed to list job executions: job=%s url=%s error=%s",
                name,
                executions_url,
                exc,
                exc_info=True,
            )
            continue

    runs.sort(key=lambda r: r.get("startTime", ""), reverse=True)
    return resources, runs
