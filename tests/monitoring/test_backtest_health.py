from __future__ import annotations

from datetime import datetime, timezone

import pytest

from monitoring import backtest_health
from monitoring.system_health_modules.signals import build_backtest_operational_signals


def test_collect_backtest_operational_summary_uses_repo_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, int] = {}

    class _FakeRepo:
        def __init__(self, dsn: str | None = None) -> None:
            assert dsn == "postgresql://test:test@localhost:5432/asset_allocation"

        def get_operational_summary(
            self,
            *,
            queue_dispatch_grace_seconds: int,
            heartbeat_timeout_seconds: int,
            duration_window_hours: int,
        ) -> dict[str, object]:
            seen["queue_dispatch_grace_seconds"] = queue_dispatch_grace_seconds
            seen["heartbeat_timeout_seconds"] = heartbeat_timeout_seconds
            seen["duration_window_hours"] = duration_window_hours
            return {
                "queuedCount": 3,
                "oldestQueuedAgeSeconds": 420.0,
                "dispatchFailureCount": 1,
                "runningCount": 2,
                "staleHeartbeatCount": 1,
                "durationP95Seconds": 95.0,
            }

    monkeypatch.setenv("BACKTEST_QUEUE_DISPATCH_GRACE_SECONDS", "150")
    monkeypatch.setenv("BACKTEST_HEARTBEAT_TIMEOUT_SECONDS", "900")
    monkeypatch.setenv("BACKTEST_DURATION_WINDOW_HOURS", "12")
    monkeypatch.setattr(backtest_health, "BacktestRepository", _FakeRepo)
    monkeypatch.setattr(backtest_health, "resolve_backtest_job_name", lambda: "backtests-job")

    summary = backtest_health.collect_backtest_operational_summary(
        "postgresql://test:test@localhost:5432/asset_allocation"
    )

    assert seen == {
        "queue_dispatch_grace_seconds": 150,
        "heartbeat_timeout_seconds": 900,
        "duration_window_hours": 12,
    }
    assert summary["jobName"] == "backtests-job"
    assert summary["dispatchFailureCount"] == 1


def test_build_backtest_operational_signals_marks_warning_and_error() -> None:
    signals = build_backtest_operational_signals(
        summary={
            "queuedCount": 3,
            "oldestQueuedAgeSeconds": 420.0,
            "runningCount": 2,
            "staleHeartbeatCount": 1,
            "dispatchFailureCount": 1,
            "durationP95Seconds": 95.0,
        },
        checked_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
    )

    signals_by_name = {signal["name"]: signal for signal in signals}
    assert signals_by_name["BacktestQueuedCount"]["value"] == 3.0
    assert signals_by_name["BacktestOldestQueuedAgeSeconds"]["status"] == "warning"
    assert signals_by_name["BacktestDispatchFailureCount"]["status"] == "warning"
    assert signals_by_name["BacktestStaleHeartbeatCount"]["status"] == "error"
