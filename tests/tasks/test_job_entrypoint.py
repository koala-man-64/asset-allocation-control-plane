from __future__ import annotations

import pytest

from tasks.common import job_entrypoint


def test_run_logged_job_logs_runtime_context_and_callback_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    callback_calls: list[str] = []

    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("TRIGGER_NEXT_JOB_NAME", "silver-market-job,gold-market-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "exec-123")
    monkeypatch.setattr(job_entrypoint.mdc, "write_line", messages.append)
    monkeypatch.setattr(job_entrypoint.mdc, "write_warning", messages.append)
    monkeypatch.setattr(job_entrypoint.mdc, "write_error", messages.append)

    def _callback() -> None:
        callback_calls.append("called")

    result = job_entrypoint.run_logged_job(
        job_name="bronze-market-job",
        run=lambda: 0,
        on_success=(_callback,),
    )

    assert result == 0
    assert callback_calls == ["called"]
    assert any("Job runtime context:" in message for message in messages)
    assert any("Job body starting: job=bronze-market-job" in message for message in messages)
    assert any("Running success callbacks: job=bronze-market-job callback_count=1" in message for message in messages)
    assert any("Success callback starting: job=bronze-market-job callback=_callback index=1/1" in message for message in messages)
    assert any("Success callback completed: job=bronze-market-job callback=_callback index=1/1" in message for message in messages)
    assert any("Job completed successfully: job=bronze-market-job exit_code=0" in message for message in messages)


def test_run_logged_job_logs_callback_skip_for_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []

    monkeypatch.setattr(job_entrypoint.mdc, "write_line", messages.append)
    monkeypatch.setattr(job_entrypoint.mdc, "write_warning", messages.append)
    monkeypatch.setattr(job_entrypoint.mdc, "write_error", messages.append)

    result = job_entrypoint.run_logged_job(
        job_name="silver-market-job",
        run=lambda: 2,
        on_success=(lambda: None,),
    )

    assert result == 2
    assert any("Skipping success callbacks: job=silver-market-job exit_code=2 callback_count=1" in message for message in messages)
    assert any("Job completed with failures: job=silver-market-job exit_code=2" in message for message in messages)
