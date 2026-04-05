from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from monitoring.control_plane import ResourceHealthItem


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _worse_resource_status(primary: str, secondary: str) -> str:
    status_order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if status_order.get(secondary, 0) > status_order.get(primary, 0) else primary


def _append_signal_details(details: str, signals: Sequence[Dict[str, Any]]) -> str:
    fragments: List[str] = []
    for signal in signals:
        if signal.get("status") not in {"warning", "error"}:
            continue
        name = str(signal.get("name") or "").strip() or "signal"
        value = signal.get("value")
        unit = str(signal.get("unit") or "").strip()
        if value is None:
            fragments.append(f"{name}=unknown")
        else:
            text = f"{name}={value}"
            if unit:
                text += f" {unit}"
            fragments.append(text)
    if not fragments:
        return details
    suffix = "; ".join(fragments[:6])
    return f"{details}, signals[{suffix}]"


def _normalize_signal_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _signal_numeric_value(signal: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(signal, dict):
        return None
    value = signal.get("value")
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    return float(value)


def _find_preferred_signal(
    signals: Sequence[Dict[str, Any]],
    *preferred_names: str,
) -> Optional[Dict[str, Any]]:
    normalized_signals = [
        (signal, _normalize_signal_name(signal.get("name")))
        for signal in signals
        if isinstance(signal, dict)
    ]
    if not normalized_signals:
        return None

    for preferred_name in preferred_names:
        candidate = _normalize_signal_name(preferred_name)
        if not candidate:
            continue
        for signal, signal_name in normalized_signals:
            if signal_name == candidate:
                return signal
        for signal, signal_name in normalized_signals:
            if signal_name and (candidate in signal_name or signal_name in candidate):
                return signal
    return None


def _build_percent_signal(name: str, base_signal: Dict[str, Any], value: float) -> Dict[str, Any]:
    bounded_value = max(0.0, value)
    return {
        "name": name,
        "value": bounded_value,
        "unit": "Percent",
        "timestamp": str(base_signal.get("timestamp") or _iso(_utc_now())),
        "status": "unknown",
        "source": str(base_signal.get("source") or "metrics"),
    }


def _append_job_usage_percent_signals(
    item: ResourceHealthItem,
    signals: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    enriched_signals = [signal for signal in signals if isinstance(signal, dict)]
    if item.resource_type != "Microsoft.App/jobs" or not enriched_signals:
        return enriched_signals

    signal_names = {_normalize_signal_name(signal.get("name")) for signal in enriched_signals}

    if item.cpu_limit_cores and "cpupercent" not in signal_names:
        cpu_signal = _find_preferred_signal(enriched_signals, "UsageNanoCores")
        cpu_value = _signal_numeric_value(cpu_signal)
        if cpu_signal is not None and cpu_value is not None:
            cpu_percent = (cpu_value / (item.cpu_limit_cores * 1_000_000_000.0)) * 100.0
            enriched_signals.append(_build_percent_signal("CpuPercent", cpu_signal, cpu_percent))

    if item.memory_limit_bytes and "memorypercent" not in signal_names:
        memory_signal = _find_preferred_signal(
            enriched_signals,
            "UsageBytes",
            "MemoryWorkingSetBytes",
            "WorkingSetBytes",
            "MemoryBytes",
        )
        memory_value = _signal_numeric_value(memory_signal)
        if memory_signal is not None and memory_value is not None:
            memory_percent = (memory_value / item.memory_limit_bytes) * 100.0
            enriched_signals.append(_build_percent_signal("MemoryPercent", memory_signal, memory_percent))

    return enriched_signals


def _parse_iso_start_time(value: Optional[str]) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _newer_execution(current: Dict[str, Any], existing: Optional[Dict[str, Any]]) -> bool:
    if existing is None:
        return True

    current_time = _parse_iso_start_time(str(current.get("startTime") or ""))
    existing_time = _parse_iso_start_time(str(existing.get("startTime") or ""))

    if current_time and existing_time:
        return current_time > existing_time
    if current_time and not existing_time:
        return True
    return str(current.get("startTime") or "") > str(existing.get("startTime") or "")


def collect_resource_health_signals(*_args: Any, **_kwargs: Any) -> List[Dict[str, Any]]:
    """
    Compatibility shim for tests expecting a resource health collector in this module.

    The current system health flow enriches resources inline; return an empty list by default.
    """
    return []
