from tasks.common.job_status import (
    JOB_EXIT_CODE_FAILURE,
    JOB_EXIT_CODE_SUCCESS,
    JOB_STATUS_FAILED,
    JOB_STATUS_SUCCEEDED,
    JOB_STATUS_SUCCEEDED_WITH_WARNINGS,
    resolve_job_run_status,
)


def test_resolve_job_run_status_returns_warning_status_with_success_exit_code() -> None:
    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=3)

    assert status == JOB_STATUS_SUCCEEDED_WITH_WARNINGS
    assert exit_code == JOB_EXIT_CODE_SUCCESS


def test_resolve_job_run_status_returns_failed_status_when_errors_exist() -> None:
    status, exit_code = resolve_job_run_status(failed_count=1, warning_count=10)

    assert status == JOB_STATUS_FAILED
    assert exit_code == JOB_EXIT_CODE_FAILURE


def test_resolve_job_run_status_returns_success_status_without_failures_or_warnings() -> None:
    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=0)

    assert status == JOB_STATUS_SUCCEEDED
    assert exit_code == JOB_EXIT_CODE_SUCCESS
