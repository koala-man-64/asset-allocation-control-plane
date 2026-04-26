from __future__ import annotations

from core import intraday_monitor_repository as repo


class _FakeCursor:
    def __init__(
        self,
        *,
        fetchone_results: list[tuple[object, ...]] | None = None,
        fetchall_results: list[list[tuple[object, ...]]] | None = None,
    ) -> None:
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.execute_calls: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, object]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def executemany(self, sql: str, params=None) -> None:
        self.executemany_calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []


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


def test_append_intraday_watchlist_symbols_adds_symbols_and_queues_non_force_refresh_run(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_results=[("watch-1", True)],
        fetchall_results=[[("AAPL",), ("MSFT",)]],
    )
    connection = _FakeConnection(cursor)
    queued_run = repo.IntradayMonitorRunSummary(
        runId="run-1",
        watchlistId="watch-1",
        triggerKind="manual",
        status="queued",
        forceRefresh=False,
        symbolCount=3,
    )

    monkeypatch.setattr(repo, "connect", lambda _dsn: connection)
    monkeypatch.setattr(repo, "_assert_symbols_exist", lambda conn, symbols: None)
    monkeypatch.setattr(
        repo,
        "get_intraday_watchlist",
        lambda dsn, watchlist_id: repo.IntradayWatchlistDetail(
            watchlistId=watchlist_id,
            name="Tech Momentum",
            enabled=True,
            symbolCount=3,
            symbols=["AAPL", "MSFT", "NVDA"],
        ),
    )
    monkeypatch.setattr(repo, "get_intraday_monitor_run", lambda dsn, run_id: queued_run)

    result = repo.append_intraday_watchlist_symbols(
        "postgresql://user:pass@localhost/db",
        watchlist_id="watch-1",
        payload=repo.IntradayWatchlistSymbolAppendRequest(
            symbols=["msft", " nvda ", "AAPL"],
            reason=" operator add ",
        ),
        actor="operator@example.com",
        request_id="req-1",
    )

    assert result.addedSymbols == ["NVDA"]
    assert result.alreadyPresentSymbols == ["MSFT", "AAPL"]
    assert result.queuedRun is not None
    assert result.queuedRun.forceRefresh is False
    assert cursor.executemany_calls[0][1] == [("watch-1", "NVDA")]
    run_insert = next(call for call in cursor.execute_calls if repo._MONITOR_RUNS_TABLE in call[0])
    assert run_insert[1][3] is False
    audit_insert = next(call for call in cursor.execute_calls if repo._WATCHLIST_EVENTS_TABLE in call[0])
    assert audit_insert[1][2:] == (
        "operator@example.com",
        "req-1",
        "operator add",
        '["NVDA"]',
        '["MSFT","AAPL"]',
        2,
        3,
        '{"queueRun":true,"queuedRunId":"' + run_insert[1][0] + '","runSkippedReason":null}',
    )
