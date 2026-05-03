from __future__ import annotations

import pytest

from core import results_freshness
from core.results_freshness import _ranking_dirty_window


def test_ranking_dirty_window_returns_none_for_identical_state() -> None:
    state = {
        "strategy": {"name": "alpha", "version": 1},
        "ranking": {"name": "quality", "version": 2},
        "universe": {"name": "default", "version": 3},
        "domains": {
            "market": {
                "fingerprint": "fp-1",
                "affectedAsOfStart": "2026-03-01",
                "affectedAsOfEnd": "2026-03-03",
            }
        },
    }

    assert _ranking_dirty_window(state, state) == (None, None)


def test_ranking_dirty_window_uses_changed_domain_window_for_lineage_delta() -> None:
    previous_state = {
        "strategy": {"name": "alpha", "version": 1},
        "ranking": {"name": "quality", "version": 2},
        "universe": {"name": "default", "version": 3},
        "domains": {
            "market": {
                "fingerprint": "fp-1",
                "affectedAsOfStart": "2026-03-01",
                "affectedAsOfEnd": "2026-03-03",
            },
            "finance": {
                "fingerprint": "fp-2",
                "affectedAsOfStart": "2026-03-02",
                "affectedAsOfEnd": "2026-03-04",
            },
        },
    }
    current_state = {
        **previous_state,
        "domains": {
            **previous_state["domains"],
            "finance": {
                "fingerprint": "fp-3",
                "affectedAsOfStart": "2026-03-10",
                "affectedAsOfEnd": "2026-03-12",
            },
        },
    }

    dirty_start, dirty_end = _ranking_dirty_window(previous_state, current_state)

    assert dirty_start.isoformat() == "2026-03-10"
    assert dirty_end.isoformat() == "2026-03-12"


def test_ranking_dirty_window_forces_full_window_when_structural_inputs_change() -> None:
    previous_state = {
        "strategy": {"name": "alpha", "version": 1},
        "ranking": {"name": "quality", "version": 2},
        "universe": {"name": "default", "version": 3},
        "domains": {
            "market": {
                "fingerprint": "fp-1",
                "affectedAsOfStart": "2026-03-01",
                "affectedAsOfEnd": "2026-03-03",
            },
            "finance": {
                "fingerprint": "fp-2",
                "affectedAsOfStart": "2026-03-05",
                "affectedAsOfEnd": "2026-03-07",
            },
        },
    }
    current_state = {
        **previous_state,
        "ranking": {"name": "quality", "version": 3},
    }

    dirty_start, dirty_end = _ranking_dirty_window(previous_state, current_state)

    assert dirty_start.isoformat() == "2026-03-01"
    assert dirty_end.isoformat() == "2026-03-07"


class _EmptyStrategyRepository:
    def __init__(self, _dsn: str) -> None:
        pass

    def list_strategies(self) -> list[dict[str, object]]:
        return []


class _UnusedBacktestRepository:
    def __init__(self, _dsn: str) -> None:
        pass


class _SignalUpdateCursor:
    def __init__(self, rowcounts: list[int]) -> None:
        self._rowcounts = list(rowcounts)
        self.rowcount = 0
        self.execute_calls: list[tuple[str, object]] = []

    def __enter__(self) -> "_SignalUpdateCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))
        self.rowcount = self._rowcounts.pop(0) if self._rowcounts else 0


class _SignalUpdateConnection:
    def __init__(self, cursor: _SignalUpdateCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_SignalUpdateConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    def cursor(self) -> _SignalUpdateCursor:
        return self._cursor


def _stub_reconcile_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(results_freshness, "_load_domain_inputs", lambda: {})
    monkeypatch.setattr(results_freshness, "StrategyRepository", _EmptyStrategyRepository)
    monkeypatch.setattr(results_freshness, "BacktestRepository", _UnusedBacktestRepository)
    monkeypatch.setattr(results_freshness, "_list_canonical_targets", lambda _dsn: [])


def test_reconcile_dry_run_counts_due_publication_signals_without_claiming(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_reconcile_dependencies(monkeypatch)
    monkeypatch.setattr(
        results_freshness,
        "_list_due_publication_reconcile_signals",
        lambda _dsn: [{"job_key": "regime", "source_fingerprint": "fp-1"}],
    )
    monkeypatch.setattr(
        results_freshness,
        "_claim_publication_reconcile_signals",
        lambda *_args, **_kwargs: pytest.fail("dry run must not claim durable signals"),
    )

    result = results_freshness._reconcile_results_freshness_locked("postgresql://test", dry_run=True)

    assert result["publicationSignalsProcessedCount"] == 1
    assert result["publicationSignalsErrorCount"] == 0
    assert result["errorCount"] == 0


def test_reconcile_completes_claimed_publication_signals_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _stub_reconcile_dependencies(monkeypatch)
    completed: list[list[dict[str, object]]] = []
    signal = {"job_key": "regime", "source_fingerprint": "fp-1", "claim_token": "claim-1"}
    monkeypatch.setattr(
        results_freshness,
        "_claim_publication_reconcile_signals",
        lambda _dsn, execution_name=None: [signal],
    )
    monkeypatch.setattr(
        results_freshness,
        "_complete_publication_reconcile_signals",
        lambda _dsn, signals: completed.append(list(signals)),
    )

    result = results_freshness._reconcile_results_freshness_locked("postgresql://test", dry_run=False)

    assert result["publicationSignalsProcessedCount"] == 1
    assert result["publicationSignalsErrorCount"] == 0
    assert completed == [[signal]]


def test_reconcile_retries_claimed_publication_signals_when_reconcile_has_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_reconcile_dependencies(monkeypatch)
    signal = {"job_key": "regime", "source_fingerprint": "fp-1", "claim_token": "claim-1"}
    failures: list[tuple[list[dict[str, object]], str]] = []

    class _FailingStrategyRepository:
        def __init__(self, _dsn: str) -> None:
            pass

        def list_strategies(self) -> list[dict[str, object]]:
            raise RuntimeError("strategy catalog failed")

    monkeypatch.setattr(results_freshness, "StrategyRepository", _FailingStrategyRepository)
    monkeypatch.setattr(
        results_freshness,
        "_claim_publication_reconcile_signals",
        lambda _dsn, execution_name=None: [signal],
    )
    monkeypatch.setattr(
        results_freshness,
        "_fail_publication_reconcile_signals",
        lambda _dsn, signals, *, error: failures.append((list(signals), error)),
    )

    result = results_freshness._reconcile_results_freshness_locked("postgresql://test", dry_run=False)

    assert result["publicationSignalsProcessedCount"] == 0
    assert result["publicationSignalsErrorCount"] == 1
    assert result["errorCount"] == 2
    assert failures == [([signal], "ranking:list_strategies:strategy catalog failed")]


def test_publication_signal_completion_requires_claimed_row(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _SignalUpdateCursor([0])
    monkeypatch.setattr(results_freshness, "connect", lambda _dsn: _SignalUpdateConnection(cursor))

    with pytest.raises(RuntimeError, match="completion lost its claim"):
        results_freshness._complete_publication_reconcile_signals(
            "postgresql://test",
            [{"job_key": "regime", "source_fingerprint": "fp-1", "claim_token": "claim-1"}],
        )


def test_publication_signal_retry_marking_requires_claimed_row(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _SignalUpdateCursor([0])
    monkeypatch.setattr(results_freshness, "connect", lambda _dsn: _SignalUpdateConnection(cursor))

    with pytest.raises(RuntimeError, match="retry marking lost its claim"):
        results_freshness._fail_publication_reconcile_signals(
            "postgresql://test",
            [{"job_key": "regime", "source_fingerprint": "fp-1", "claim_token": "claim-1", "attempt_count": 1}],
            error="reconcile failed",
        )
