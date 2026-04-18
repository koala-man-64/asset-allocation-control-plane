from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable

from asset_allocation_contracts.backtest import BacktestReconcileResponse

from core.backtest_job_control import get_job_execution, resolve_backtest_job_name, trigger_backtest_job
from core.backtest_repository import BacktestRepository

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning("Invalid integer override for %s: %s", name, raw)
        return max(minimum, default)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_or_none(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    return None


def _queue_age_ms(run: dict[str, Any], now: datetime) -> int | None:
    submitted_at = run.get("submitted_at")
    if not isinstance(submitted_at, datetime):
        return None
    return max(0, int((now - submitted_at).total_seconds() * 1000))


def _log_reconcile_event(event: str, *, now: datetime, run: dict[str, Any], **fields: Any) -> None:
    payload = {
        "event": event,
        "run_id": run.get("run_id"),
        "execution_name": run.get("execution_name"),
        "strategy_name": run.get("strategy_name"),
        "attempt_count": run.get("attempt_count"),
        "queue_age_ms": _queue_age_ms(run, now),
        "phase": fields.pop("phase", "reconcile"),
        "submitted_at": _iso_or_none(run.get("submitted_at")),
        "started_at": _iso_or_none(run.get("started_at")),
        "heartbeat_at": _iso_or_none(run.get("heartbeat_at")),
        **fields,
    }
    logger.info("backtest_lifecycle_event %s", json.dumps(payload, sort_keys=True, default=str))


def reconcile_backtest_runs(
    dsn: str,
    *,
    repo: BacktestRepository | None = None,
    trigger_job: Callable[[str], dict[str, Any]] = trigger_backtest_job,
    get_execution: Callable[[str, str], dict[str, Any] | None] = get_job_execution,
    utcnow: Callable[[], datetime] = _utc_now,
) -> BacktestReconcileResponse:
    repository = repo or BacktestRepository(dsn)
    now = utcnow()
    job_name = resolve_backtest_job_name()

    dispatch_grace_seconds = _env_int("BACKTEST_QUEUE_DISPATCH_GRACE_SECONDS", 120)
    redispatch_grace_seconds = _env_int("BACKTEST_QUEUE_REDISPATCH_GRACE_SECONDS", 300)
    heartbeat_timeout_seconds = _env_int("BACKTEST_HEARTBEAT_TIMEOUT_SECONDS", 1800)
    dispatch_budget = _env_int("BACKTEST_RECONCILE_MAX_DISPATCH_PER_PASS", 10, minimum=1)

    dispatched_run_ids: list[str] = []
    dispatch_failed_run_ids: list[str] = []
    failed_run_ids: list[str] = []
    skipped_active_count = 0
    dispatch_attempts = 0

    def _dispatch(run: dict[str, Any], *, reason: str) -> None:
        nonlocal dispatch_attempts
        if dispatch_attempts >= dispatch_budget:
            return
        dispatch_attempts += 1
        try:
            job_response = trigger_job(job_name)
        except Exception as exc:
            dispatch_failed_run_ids.append(str(run["run_id"]))
            _log_reconcile_event(
                "reconcile_dispatch_failed",
                now=now,
                run=run,
                phase="dispatch",
                failure_reason=str(exc),
                dispatch_reason=reason,
            )
            return
        execution_name = str(job_response.get("executionName") or "").strip() or None
        if execution_name:
            repository.set_execution_name(str(run["run_id"]), execution_name)
            run["execution_name"] = execution_name
        dispatched_run_ids.append(str(run["run_id"]))
        _log_reconcile_event(
            "reconcile_dispatched",
            now=now,
            run=run,
            phase="dispatch",
            dispatch_reason=reason,
            new_execution_name=execution_name,
        )

    queued_without_execution = repository.list_queued_runs_without_execution(
        older_than_seconds=dispatch_grace_seconds,
        limit=dispatch_budget,
    )
    for run in queued_without_execution:
        if dispatch_attempts >= dispatch_budget:
            break
        _dispatch(run, reason="queued_without_execution")

    remaining_budget = max(0, dispatch_budget - dispatch_attempts)
    if remaining_budget > 0:
        queued_with_execution = repository.list_queued_runs_with_execution(
            older_than_seconds=redispatch_grace_seconds,
            limit=remaining_budget,
        )
        for run in queued_with_execution:
            if dispatch_attempts >= dispatch_budget:
                break
            execution_name = str(run.get("execution_name") or "").strip()
            execution = None
            if execution_name:
                execution = get_execution(job_name, execution_name)
            if execution and str(execution.get("status") or "") == "running":
                skipped_active_count += 1
                _log_reconcile_event(
                    "reconcile_skip_active",
                    now=now,
                    run=run,
                    phase="dispatch",
                    execution_status=str(execution.get("statusCode") or execution.get("status") or ""),
                )
                continue
            _dispatch(run, reason="queued_with_inactive_execution")

    stale_running_runs = repository.list_stale_running_runs(
        heartbeat_timeout_seconds=heartbeat_timeout_seconds,
        limit=max(dispatch_budget, 50),
    )
    stale_running_failure = "Backtest worker heartbeat timed out and no active Azure execution was found."
    for run in stale_running_runs:
        execution_name = str(run.get("execution_name") or "").strip()
        execution = None
        if execution_name:
            execution = get_execution(job_name, execution_name)
        if execution and str(execution.get("status") or "") == "running":
            skipped_active_count += 1
            _log_reconcile_event(
                "reconcile_stale_running_active",
                now=now,
                run=run,
                phase="stale_running",
                execution_status=str(execution.get("statusCode") or execution.get("status") or ""),
            )
            continue
        repository.fail_run(str(run["run_id"]), error=stale_running_failure)
        failed_run_ids.append(str(run["run_id"]))
        _log_reconcile_event(
            "reconcile_stale_running_failed",
            now=now,
            run=run,
            phase="stale_running",
            failure_reason=stale_running_failure,
            execution_status=str(execution.get("statusCode") or execution.get("status") or "") if execution else None,
        )

    no_action_count = 0
    if not dispatched_run_ids and not dispatch_failed_run_ids and not failed_run_ids and skipped_active_count == 0:
        no_action_count = 1

    return BacktestReconcileResponse(
        dispatchedCount=len(dispatched_run_ids),
        dispatchFailedCount=len(dispatch_failed_run_ids),
        failedStaleRunningCount=len(failed_run_ids),
        skippedActiveCount=skipped_active_count,
        noActionCount=no_action_count,
        dispatchedRunIds=dispatched_run_ids,
        dispatchFailedRunIds=dispatch_failed_run_ids,
        failedRunIds=failed_run_ids,
    )
