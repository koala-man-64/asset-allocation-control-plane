from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from monitoring.arm_client import AzureArmClient


DEFAULT_MONITOR_METRICS_API_VERSION = "2018-01-01"
METRICS_PROVIDER_PATH = "/providers/microsoft.insights/metrics"


@dataclass(frozen=True)
class MetricThreshold:
    warn_above: Optional[float] = None
    error_above: Optional[float] = None
    warn_below: Optional[float] = None
    error_below: Optional[float] = None


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_key(name: str) -> str:
    return (name or "").strip().lower()


def parse_metric_thresholds_json(raw: str) -> Dict[str, MetricThreshold]:
    """
    Expected format:
      {"CpuUsage":{"warn_above":80,"error_above":95},"ErrorCount":{"warn_above":1,"error_above":10}}
    Keys are matched case-insensitively.
    """
    text = (raw or "").strip()
    if not text:
        return {}
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON must be a JSON object.")

    out: Dict[str, MetricThreshold] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not key.strip():
            continue
        if not isinstance(value, dict):
            continue
        out[_normalize_key(key)] = MetricThreshold(
            warn_above=float(value["warn_above"]) if "warn_above" in value and value["warn_above"] is not None else None,
            error_above=float(value["error_above"]) if "error_above" in value and value["error_above"] is not None else None,
            warn_below=float(value["warn_below"]) if "warn_below" in value and value["warn_below"] is not None else None,
            error_below=float(value["error_below"]) if "error_below" in value and value["error_below"] is not None else None,
        )
    return out


def _status_for_value(value: Optional[float], threshold: Optional[MetricThreshold]) -> str:
    if value is None:
        return "unknown"
    if threshold is None:
        return "unknown"

    if threshold.error_above is not None and value >= threshold.error_above:
        return "error"
    if threshold.warn_above is not None and value >= threshold.warn_above:
        return "warning"
    if threshold.error_below is not None and value <= threshold.error_below:
        return "error"
    if threshold.warn_below is not None and value <= threshold.warn_below:
        return "warning"
    return "healthy"


def _worse_status(primary: str, secondary: str) -> str:
    status_order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if status_order.get(secondary, 0) > status_order.get(primary, 0) else primary


def _extract_latest(metric: Dict[str, Any], *, aggregation: str) -> Tuple[Optional[float], Optional[str]]:
    agg_key = (aggregation or "Average").strip().lower()
    if not agg_key:
        agg_key = "average"

    timeseries = metric.get("timeseries") if isinstance(metric.get("timeseries"), list) else []
    for series in timeseries:
        if not isinstance(series, dict):
            continue
        points = series.get("data") if isinstance(series.get("data"), list) else []
        for point in reversed(points):
            if not isinstance(point, dict):
                continue
            ts = point.get("timeStamp")
            val = point.get(agg_key)
            if val is None:
                continue
            try:
                return float(val), str(ts or "")
            except (TypeError, ValueError):
                continue
    return None, None


def collect_monitor_metrics(
    arm: AzureArmClient,
    *,
    resource_id: str,
    metric_names: Sequence[str],
    end_time: datetime,
    timespan_minutes: int = 15,
    interval: str = "PT1M",
    aggregation: str = "Average",
    api_version: str = DEFAULT_MONITOR_METRICS_API_VERSION,
    thresholds: Mapping[str, MetricThreshold] = (),
) -> Tuple[List[Dict[str, Any]], str]:
    rid = (resource_id or "").strip()
    names = [n.strip() for n in metric_names if (n or "").strip()]
    if not rid.startswith("/") or not names:
        return [], "unknown"

    end = _utc(end_time)
    start = end - timedelta(minutes=max(timespan_minutes, 1))
    timespan = f"{start.isoformat()}/{end.isoformat()}"

    url = f"https://management.azure.com{rid.rstrip('/')}{METRICS_PROVIDER_PATH}"
    params = {
        "api-version": api_version,
        "metricnames": ",".join(names),
        "timespan": timespan,
        "interval": interval,
        "aggregation": aggregation,
    }

    try:
        payload = arm.get_json(url, params=params)
    except Exception:
        return [], "unknown"

    signals: List[Dict[str, Any]] = []
    worst = "unknown"

    values = payload.get("value") if isinstance(payload.get("value"), list) else []
    for metric in values:
        if not isinstance(metric, dict):
            continue
        name_obj = metric.get("name") if isinstance(metric.get("name"), dict) else {}
        metric_name = str(name_obj.get("value") or name_obj.get("localizedValue") or "").strip() or "metric"
        unit = str(metric.get("unit") or "") or ""
        value, timestamp = _extract_latest(metric, aggregation=aggregation)

        threshold = thresholds.get(_normalize_key(metric_name))
        status = _status_for_value(value, threshold)
        worst = _worse_status(worst, status)

        signals.append(
            {
                "name": metric_name,
                "value": value,
                "unit": unit,
                "timestamp": timestamp or end.isoformat(),
                "status": status,
                "source": "metrics",
            }
        )

    return signals, worst

