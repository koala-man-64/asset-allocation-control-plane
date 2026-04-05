from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Sequence

from monitoring.log_analytics import AzureLogAnalyticsClient
from monitoring.system_health_modules.env_config import BronzeSymbolJumpThreshold, _parse_bool, _require_int
from monitoring.system_health_modules.job_queries import (
    _query_job_system_log_messages,
    _query_recent_bronze_finance_ingest_summaries,
    _query_recent_bronze_symbol_counts,
)
from monitoring.system_health_modules.signals import _iso, _parse_iso_start_time

logger = logging.getLogger("asset_allocation.monitoring.system_health")


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower()).strip("-")
    return cleaned[:80] if cleaned else "alert"


def _alert_id(*, severity: str, title: str, component: str) -> str:
    raw = f"{severity}|{title}|{component}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:10]
    return f"{_slug(component)}--{_slug(title)}--{digest}"


def _layer_alerts(
    now,
    *,
    layer_name: str,
    status: str,
    last_updated,
    error: Optional[str],
) -> List[Dict[str, Any]]:
    if status == "healthy":
        return []

    timestamp = _iso(now)
    if status == "error":
        return [
            {
                "id": _alert_id(severity="error", title="Layer probe error", component=layer_name),
                "severity": "error",
                "title": "Layer probe error",
                "component": layer_name,
                "timestamp": timestamp,
                "message": error or "Layer probe failed.",
            }
        ]

    last_text = _iso(last_updated) if last_updated else "unknown"
    return [
        {
            "id": _alert_id(severity="warning", title="Layer stale", component=layer_name),
            "severity": "warning",
            "title": "Layer stale",
            "component": layer_name,
            "timestamp": timestamp,
            "message": f"{layer_name} appears stale (lastUpdated={last_text}).",
        }
    ]


def _load_bronze_symbol_jump_threshold_overrides() -> Dict[str, Dict[str, Any]]:
    raw = os.environ.get("SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON", "")
    text = raw.strip()
    if not text:
        return {}

    try:
        payload = json.loads(text)
    except Exception as exc:
        logger.warning(
            "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON parse error: %s",
            exc,
            exc_info=True,
        )
        return {}

    if not isinstance(payload, dict):
        logger.warning("SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON must be a JSON object.")
        return {}

    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if not isinstance(raw_value, dict):
            logger.warning(
                "Ignoring bronze symbol jump threshold for key=%s (expected object, got %s).",
                key,
                type(raw_value).__name__,
            )
            continue
        normalized[key] = raw_value
    return normalized


def _resolve_bronze_symbol_jump_threshold(
    job_name: str,
    overrides: Dict[str, Dict[str, Any]],
) -> Optional[BronzeSymbolJumpThreshold]:
    candidates = [str(job_name or "").strip(), "*"]
    node: Optional[Dict[str, Any]] = None
    for key in candidates:
        current = overrides.get(key)
        if isinstance(current, dict):
            node = current
            break
    if not node:
        return None

    enabled = node.get("enabled")
    if enabled is not None:
        try:
            if isinstance(enabled, str):
                if not _parse_bool(enabled):
                    return None
            elif not bool(enabled):
                return None
        except Exception:
            return None

    try:
        warn_factor = float(node.get("warnFactor", 0))
        error_factor = float(node.get("errorFactor", 0))
        min_previous = int(node.get("minPreviousSymbols", 1))
        min_current = int(node.get("minCurrentSymbols", 1))
    except Exception:
        logger.warning("Ignoring bronze symbol jump threshold for job=%s due to invalid numeric values.", job_name)
        return None

    if warn_factor <= 1.0 and error_factor <= 1.0:
        logger.warning(
            "Ignoring bronze symbol jump threshold for job=%s because warn/error factors must exceed 1.0.",
            job_name,
        )
        return None
    if error_factor > 0 and warn_factor > error_factor:
        warn_factor = error_factor
    return BronzeSymbolJumpThreshold(
        warn_factor=warn_factor if warn_factor > 1.0 else error_factor,
        error_factor=error_factor if error_factor > 1.0 else warn_factor,
        min_previous_symbols=max(min_previous, 1),
        min_current_symbols=max(min_current, 1),
    )


def _job_failure_reason_alerts(
    *,
    run: Dict[str, Any],
    checked_iso: str,
    log_client: Optional[AzureLogAnalyticsClient],
    workspace_id: str,
) -> List[Dict[str, Any]]:
    if log_client is None or not workspace_id:
        return []
    if run.get("status") != "failed":
        return []

    job_name = str(run.get("jobName") or "").strip()
    if not job_name:
        return []
    execution_name = str(run.get("executionName") or "").strip() or None
    start_time = _parse_iso_start_time(str(run.get("startTime") or ""))
    end_time = _parse_iso_start_time(str(run.get("endTime") or ""))

    try:
        messages = _query_job_system_log_messages(
            log_client,
            workspace_id=workspace_id,
            job_name=job_name,
            execution_name=execution_name,
            start_time=start_time,
            end_time=end_time,
        )
    except Exception as exc:
        logger.warning("Job failure reason probe failed for job=%s: %s", job_name, exc, exc_info=True)
        return []

    normalized = "\n".join(message.lower() for message in messages)
    alerts: List[Dict[str, Any]] = []
    if "exit code 137" in normalized or "exit code '137'" in normalized:
        alerts.append(
            {
                "id": _alert_id(
                    severity="error",
                    title="Job terminated with exit 137",
                    component=job_name,
                ),
                "severity": "error",
                "title": "Job terminated with exit 137",
                "component": job_name,
                "timestamp": checked_iso,
                "message": "Latest execution was terminated with exit code 137.",
            }
        )
    if "backofflimitexceeded" in normalized:
        alerts.append(
            {
                "id": _alert_id(
                    severity="error",
                    title="Job hit BackoffLimitExceeded",
                    component=job_name,
                ),
                "severity": "error",
                "title": "Job hit BackoffLimitExceeded",
                "component": job_name,
                "timestamp": checked_iso,
                "message": "Latest execution exhausted retries and hit BackoffLimitExceeded.",
            }
        )
    return alerts


def _bronze_symbol_jump_alerts(
    *,
    job_names: Sequence[str],
    checked_iso: str,
    log_client: Optional[AzureLogAnalyticsClient],
    workspace_id: str,
) -> List[Dict[str, Any]]:
    if log_client is None or not workspace_id:
        return []

    try:
        lookback_hours = _require_int(
            "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_LOOKBACK_HOURS",
            min_value=1,
            max_value=24 * 365,
        )
    except ValueError:
        lookback_hours = 24 * 7

    overrides = _load_bronze_symbol_jump_threshold_overrides()
    if not overrides:
        return []

    alerts: List[Dict[str, Any]] = []
    for job_name in job_names:
        threshold = _resolve_bronze_symbol_jump_threshold(job_name, overrides)
        if threshold is None:
            continue
        if not str(job_name or "").startswith("bronze-"):
            continue
        try:
            runs = _query_recent_bronze_symbol_counts(
                log_client,
                workspace_id=workspace_id,
                job_name=job_name,
                lookback_hours=lookback_hours,
            )
        except Exception as exc:
            logger.warning("Bronze symbol jump probe failed for job=%s: %s", job_name, exc, exc_info=True)
            continue
        if len(runs) < 2:
            continue

        current = runs[0]
        previous = runs[1]
        current_count = int(current.get("symbolCount") or 0)
        previous_count = int(previous.get("symbolCount") or 0)
        if previous_count < threshold.min_previous_symbols or current_count < threshold.min_current_symbols:
            continue
        ratio = float(current_count) / float(previous_count)
        if ratio < threshold.warn_factor:
            continue
        severity = "error" if ratio >= threshold.error_factor else "warning"
        alerts.append(
            {
                "id": _alert_id(
                    severity=severity,
                    title="Bronze symbol count jump",
                    component=job_name,
                ),
                "severity": severity,
                "title": "Bronze symbol count jump",
                "component": job_name,
                "timestamp": checked_iso,
                "message": (
                    f"Latest Bronze symbol count jumped from {previous_count} to {current_count} "
                    f"({ratio:.2f}x; previous={previous.get('timeGenerated')}, current={current.get('timeGenerated')})."
                ),
            }
        )
    return alerts


def _bronze_finance_zero_write_alerts(
    *,
    job_names: Sequence[str],
    checked_iso: str,
    log_client: Optional[AzureLogAnalyticsClient],
    workspace_id: str,
) -> List[Dict[str, Any]]:
    if log_client is None or not workspace_id:
        return []

    try:
        lookback_hours = _require_int(
            "SYSTEM_HEALTH_BRONZE_FINANCE_ZERO_WRITE_LOOKBACK_HOURS",
            min_value=1,
            max_value=24 * 365,
        )
    except ValueError:
        lookback_hours = 24 * 7

    alerts: List[Dict[str, Any]] = []
    for job_name in job_names:
        if str(job_name or "").strip().lower() != "bronze-finance-job":
            continue
        try:
            summaries = _query_recent_bronze_finance_ingest_summaries(
                log_client,
                workspace_id=workspace_id,
                job_name=job_name,
                lookback_hours=lookback_hours,
            )
        except Exception as exc:
            logger.warning("Bronze finance zero-write probe failed for job=%s: %s", job_name, exc, exc_info=True)
            continue
        if not summaries:
            continue
        latest = summaries[0]
        processed = int(latest.get("processed") or 0)
        written = int(latest.get("written") or 0)
        if processed <= 0 or written != 0:
            continue
        alerts.append(
            {
                "id": _alert_id(
                    severity="error",
                    title="Bronze finance wrote zero rows",
                    component=job_name,
                ),
                "severity": "error",
                "title": "Bronze finance wrote zero rows",
                "component": job_name,
                "timestamp": checked_iso,
                "message": (
                    f"Latest Bronze finance run processed {processed} symbol(s) but wrote {written} row(s). "
                    f"summary={latest.get('message') or 'n/a'}"
                ),
            }
        )
    return alerts
