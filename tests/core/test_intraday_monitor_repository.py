from __future__ import annotations

from core import intraday_monitor_repository as repo


class _FakeCursor:
    def __init__(self, *, fetchone_results: list[tuple[object, ...]] | None = None) -> None:
        self.fetchone_results = list(fetchone_results or [])
        self.execute_calls: list[tuple[str, object]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_get_intraday_health_summary_counts_only_session_open_due_runs(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_results=[
            (3, 2),
            (1,),
            (0,),
            (4,),
            (None,),
        ]
    )
    connection = _FakeConnection(cursor)
    due_calls: list[object] = []

    monkeypatch.setattr(repo, "connect", lambda _dsn: connection)
    monkeypatch.setattr(
        repo,
        "_list_currently_due_watchlists",
        lambda conn: due_calls.append(conn) or [("watch-open", 2, "us_equities_regular")],
    )
    monkeypatch.setattr(repo, "list_intraday_monitor_runs", lambda dsn, **kwargs: [])
    monkeypatch.setattr(repo, "list_intraday_refresh_batches", lambda dsn, **kwargs: [])

    summary = repo.get_intraday_health_summary("postgresql://user:pass@localhost/db")

    assert summary["watchlistCount"] == 3
    assert summary["enabledWatchlistCount"] == 2
    assert summary["dueRunBacklogCount"] == 2
    assert summary["failedRunCount"] == 0
    assert summary["staleSymbolCount"] == 4
    assert summary["refreshBatchBacklogAgeSeconds"] == 0.0
    assert len(due_calls) == 1
