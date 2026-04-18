from __future__ import annotations

from datetime import datetime, timezone

from core.backtest_reconcile import reconcile_backtest_runs


class _FakeRepo:
    def __init__(
        self,
        *,
        queued_without_execution: list[dict] | None = None,
        queued_with_execution: list[dict] | None = None,
        stale_running: list[dict] | None = None,
    ) -> None:
        self.queued_without_execution = queued_without_execution or []
        self.queued_with_execution = queued_with_execution or []
        self.stale_running = stale_running or []
        self.execution_names: dict[str, str] = {}
        self.failed: dict[str, str] = {}

    def list_queued_runs_without_execution(self, *, older_than_seconds: int, limit: int) -> list[dict]:
        return self.queued_without_execution[:limit]

    def list_queued_runs_with_execution(self, *, older_than_seconds: int, limit: int) -> list[dict]:
        return self.queued_with_execution[:limit]

    def list_stale_running_runs(self, *, heartbeat_timeout_seconds: int, limit: int) -> list[dict]:
        return self.stale_running[:limit]

    def set_execution_name(self, run_id: str, execution_name: str) -> None:
        self.execution_names[run_id] = execution_name

    def fail_run(self, run_id: str, *, error: str) -> None:
        self.failed[run_id] = error


def _run(run_id: str, *, status: str, execution_name: str | None = None) -> dict:
    return {
        "run_id": run_id,
        "status": status,
        "execution_name": execution_name,
        "strategy_name": "mom-spy-res",
        "attempt_count": 1,
        "submitted_at": datetime(2026, 4, 17, 12, 0, tzinfo=timezone.utc),
        "started_at": datetime(2026, 4, 17, 12, 1, tzinfo=timezone.utc),
        "heartbeat_at": datetime(2026, 4, 17, 12, 2, tzinfo=timezone.utc),
    }


def test_reconcile_dispatches_queued_run_without_execution(monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_ACA_JOB_NAME", "backtests-job")
    repo = _FakeRepo(queued_without_execution=[_run("run-1", status="queued")])

    payload = reconcile_backtest_runs(
        "postgresql://ignored",
        repo=repo,  # type: ignore[arg-type]
        trigger_job=lambda job_name: {"status": "queued", "executionName": "backtests-job-exec-001"},
        get_execution=lambda job_name, execution_name: None,
        utcnow=lambda: datetime(2026, 4, 17, 12, 10, tzinfo=timezone.utc),
    )

    assert payload.dispatchedCount == 1
    assert payload.dispatchedRunIds == ["run-1"]
    assert repo.execution_names == {"run-1": "backtests-job-exec-001"}


def test_reconcile_skips_queued_run_with_active_execution(monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_ACA_JOB_NAME", "backtests-job")
    repo = _FakeRepo(queued_with_execution=[_run("run-2", status="queued", execution_name="exec-live")])

    payload = reconcile_backtest_runs(
        "postgresql://ignored",
        repo=repo,  # type: ignore[arg-type]
        trigger_job=lambda job_name: {"status": "queued", "executionName": "should-not-run"},
        get_execution=lambda job_name, execution_name: {"status": "running", "statusCode": "Running"},
        utcnow=lambda: datetime(2026, 4, 17, 12, 10, tzinfo=timezone.utc),
    )

    assert payload.dispatchedCount == 0
    assert payload.skippedActiveCount == 1
    assert repo.execution_names == {}


def test_reconcile_fails_stale_running_run_without_active_execution(monkeypatch) -> None:
    monkeypatch.setenv("BACKTEST_ACA_JOB_NAME", "backtests-job")
    repo = _FakeRepo(stale_running=[_run("run-3", status="running", execution_name="exec-dead")])

    payload = reconcile_backtest_runs(
        "postgresql://ignored",
        repo=repo,  # type: ignore[arg-type]
        trigger_job=lambda job_name: {"status": "queued", "executionName": "unused"},
        get_execution=lambda job_name, execution_name: {"status": "failed", "statusCode": "Failed"},
        utcnow=lambda: datetime(2026, 4, 17, 12, 40, tzinfo=timezone.utc),
    )

    assert payload.failedStaleRunningCount == 1
    assert payload.failedRunIds == ["run-3"]
    assert repo.failed["run-3"].startswith("Backtest worker heartbeat timed out")
