from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

from core.backtest_job_control import resolve_backtest_job_name
from core.backtest_repository import BacktestRepository

logger = logging.getLogger("asset_allocation.monitoring.backtest_health")


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw_value = str(os.environ.get(name) or "").strip()
    if not raw_value:
        return default
    try:
        return max(minimum, int(raw_value))
    except Exception:
        return default


def collect_backtest_operational_summary(dsn: str | None = None) -> dict[str, Any]:
    job_name = resolve_backtest_job_name()
    queue_dispatch_grace_seconds = _env_int("BACKTEST_QUEUE_DISPATCH_GRACE_SECONDS", 120)
    heartbeat_timeout_seconds = _env_int("BACKTEST_HEARTBEAT_TIMEOUT_SECONDS", 1800)
    duration_window_hours = _env_int("BACKTEST_DURATION_WINDOW_HOURS", 24, minimum=1)
    summary = BacktestRepository(dsn).get_operational_summary(
        queue_dispatch_grace_seconds=queue_dispatch_grace_seconds,
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        duration_window_hours=duration_window_hours,
    )
    summary["jobName"] = job_name
    summary["queueDispatchGraceSeconds"] = queue_dispatch_grace_seconds
    summary["heartbeatTimeoutSeconds"] = heartbeat_timeout_seconds
    return summary


def build_backtest_operational_alerts(
    *,
    summary: Dict[str, Any],
    checked_iso: str,
    component: str,
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    stale_heartbeat_count = int(summary.get("staleHeartbeatCount") or 0)
    dispatch_failure_count = int(summary.get("dispatchFailureCount") or 0)
    oldest_queued_age_seconds = float(summary.get("oldestQueuedAgeSeconds") or 0.0)
    if stale_heartbeat_count > 0:
        alerts.append(
            {
                "severity": "error",
                "title": "Backtest stale heartbeat",
                "component": component,
                "timestamp": checked_iso,
                "message": f"{stale_heartbeat_count} running backtest run(s) exceeded the heartbeat timeout.",
            }
        )
    if dispatch_failure_count > 0:
        alerts.append(
            {
                "severity": "warning",
                "title": "Backtest dispatch backlog",
                "component": component,
                "timestamp": checked_iso,
                "message": f"{dispatch_failure_count} queued run(s) are older than the dispatch grace window and still lack an execution.",
            }
        )
    if oldest_queued_age_seconds > max(300.0, float(summary.get("queueDispatchGraceSeconds") or 120)):
        alerts.append(
            {
                "severity": "warning",
                "title": "Backtest queue aged",
                "component": component,
                "timestamp": checked_iso,
                "message": f"Oldest queued backtest age is {oldest_queued_age_seconds:.0f}s.",
            }
        )
    return alerts
