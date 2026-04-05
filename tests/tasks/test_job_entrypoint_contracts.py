from __future__ import annotations

from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[2]

_ENTRYPOINT_LOCK_CASES: list[tuple[str, list[str]]] = [
    ("tasks/market_data/bronze_market_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    ("tasks/earnings_data/bronze_earnings_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    (
        "tasks/finance_data/bronze_finance_data.py",
        [
            "job_lock_factory = mdc.JobLock",
            'with job_lock_factory(job_name, conflict_policy="fail")',
            'conflict_policy="wait_then_fail"',
        ],
    ),
    ("tasks/price_target_data/bronze_price_target_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    ("tasks/market_data/silver_market_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    ("tasks/earnings_data/silver_earnings_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    (
        "tasks/finance_data/silver_finance_data.py",
        [
            'with mdc.JobLock(shared_lock_name, conflict_policy="wait_then_fail"',
            'with mdc.JobLock(job_name, conflict_policy="fail")',
        ],
    ),
    ("tasks/price_target_data/silver_price_target_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    (
        "tasks/market_data/gold_market_data.py",
        ['with mdc.JobLock(job_name, conflict_policy="wait_then_fail", wait_timeout_seconds=90)'],
    ),
    ("tasks/earnings_data/gold_earnings_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    ("tasks/finance_data/gold_finance_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    ("tasks/price_target_data/gold_price_target_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
    ("tasks/regime_data/gold_regime_data.py", ['with mdc.JobLock(job_name, conflict_policy="fail")']),
]


@pytest.mark.parametrize(
    ("relative_path", "expected_snippets"),
    _ENTRYPOINT_LOCK_CASES,
    ids=[Path(relative_path).stem for relative_path, _ in _ENTRYPOINT_LOCK_CASES],
)
def test_stateful_job_entrypoints_use_expected_locks(
    relative_path: str,
    expected_snippets: list[str],
) -> None:
    source = (_REPO_ROOT / relative_path).read_text(encoding="utf-8")

    for snippet in expected_snippets:
        assert snippet in source
