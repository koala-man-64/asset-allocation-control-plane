from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from asset_allocation_runtime_common.shared_core import gold_sync_contracts as sync


class _FakeCursor:
    def __init__(
        self,
        *,
        fetchall_rows=None,
        fetchone_rows=None,
        fail_on_execute: bool = False,
        rowcount_map: dict[str, int] | None = None,
    ) -> None:
        self.fetchall_rows = list(fetchall_rows or [])
        self.fetchone_rows = list(fetchone_rows or [])
        self.fail_on_execute = fail_on_execute
        self.rowcount_map = dict(rowcount_map or {})
        self.executed: list[tuple[str, object]] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))
        self.rowcount = 0
        for pattern, value in self.rowcount_map.items():
            if pattern in sql:
                self.rowcount = int(value)
                break
        if self.fail_on_execute:
            raise RuntimeError("boom")

    def fetchall(self):
        return list(self.fetchall_rows)

    def fetchone(self):
        if not self.fetchone_rows:
            return None
        return self.fetchone_rows.pop(0)

    def copy(self, _statement: str):
        class _FakeCopy:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, exc_type, exc, tb) -> None:
                return None

            def write_row(self_inner, _row) -> None:
                return None

        return _FakeCopy()


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_load_domain_sync_state_returns_bucket_map(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(
        fetchall_rows=[
            ("a", 101.0, "success", 12, 2, "2026-03-07T01:02:03Z", None),
            ("b", 99.0, "failed", 0, 0, "2026-03-06T01:02:03Z", "bad"),
        ]
    )
    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))

    state = sync.load_domain_sync_state("postgresql://test", domain="market")

    assert state["A"]["source_commit"] == 101.0
    assert state["A"]["status"] == "success"
    assert state["B"]["error"] == "bad"


def test_bucket_sync_is_current_requires_successful_matching_commit() -> None:
    assert (
        sync.bucket_sync_is_current(
            {"A": {"source_commit": 100.0, "status": "success"}},
            bucket="A",
            source_commit=100.0,
        )
        is True
    )
    assert (
        sync.bucket_sync_is_current(
            {"A": {"source_commit": 99.0, "status": "success"}},
            bucket="A",
            source_commit=100.0,
        )
        is False
    )
    assert (
        sync.bucket_sync_is_current(
            {"A": {"source_commit": 100.0, "status": "failed"}},
            bucket="A",
            source_commit=100.0,
        )
        is False
    )


def test_sync_gold_bucket_stages_rows_deletes_missing_scope_rows_and_updates_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        rowcount_map={
            "DELETE FROM gold.market_data AS target": 1,
            "INSERT INTO gold.market_data AS target": 1,
        }
    )
    copied: dict[str, object] = {}
    messages: list[str] = []

    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(sync.mdc, "write_line", lambda msg: messages.append(str(msg)))

    def _fake_copy_rows(cur, *, table, columns, rows) -> None:
        copied["cursor"] = cur
        copied["table"] = table
        copied["columns"] = list(columns)
        copied["rows"] = list(rows)

    monkeypatch.setattr(sync, "copy_rows", _fake_copy_rows)

    result = sync.sync_gold_bucket(
        domain="market",
        bucket="a",
        frame=pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-02")],
                "symbol": ["aapl"],
                "close": [101.5],
                "range": [2.25],
                "volume": [1000],
            }
        ),
        scope_symbols=["MSFT"],
        source_commit=123.0,
        dsn="postgresql://test",
    )

    assert result.status == "ok"
    assert result.bucket == "A"
    assert result.row_count == 1
    assert result.symbol_count == 1
    assert result.scope_symbol_count == 2
    assert result.min_key == date(2026, 1, 2)
    assert copied["table"] == "pg_temp.gold_sync_stage"
    assert '"range"' in copied["columns"]
    assert copied["rows"][0][0] == date(2026, 1, 2)
    assert copied["rows"][0][1] == "AAPL"
    assert any("CREATE TEMP TABLE gold_sync_stage" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.market_data AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.market_data AS target" in sql for sql, _params in cursor.executed)
    assert any("IS DISTINCT FROM" in sql for sql, _params in cursor.executed)
    assert all("LOCK TABLE" not in sql and "pg_advisory_lock" not in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO core.gold_sync_state" in sql for sql, _params in cursor.executed)
    assert any(
        "postgres_gold_sync_apply_stats domain=market bucket=A staged_rows=1 "
        "deleted_rows=1 upserted_rows=1 unchanged_rows=0 scope_symbols=2"
        in message
        for message in messages
    )


def test_sync_gold_bucket_chunks_streams_multiple_prepared_frames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        rowcount_map={
            "DELETE FROM gold.market_data AS target": 1,
            "INSERT INTO gold.market_data AS target": 2,
        }
    )
    copied_batches: list[list[tuple[object, ...]]] = []

    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))

    def _fake_copy_rows(cur, *, table, columns, rows) -> None:
        assert cur is cursor
        assert table == "pg_temp.gold_sync_stage"
        assert '"symbol"' in columns
        copied_batches.append(list(rows))

    monkeypatch.setattr(sync, "copy_rows", _fake_copy_rows)

    result = sync.sync_gold_bucket_chunks(
        domain="market",
        bucket="a",
        frames=[
            pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-01-02")],
                    "symbol": ["aapl"],
                    "close": [101.5],
                }
            ),
            pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-01-03")],
                    "symbol": ["msft"],
                    "close": [202.5],
                }
            ),
        ],
        scope_symbols=["OLD"],
        source_commit=123.0,
        dsn="postgresql://test",
    )

    assert result.status == "ok"
    assert result.bucket == "A"
    assert result.row_count == 2
    assert result.symbol_count == 2
    assert result.scope_symbol_count == 3
    assert len(copied_batches) == 2
    assert copied_batches[0][0][1] == "AAPL"
    assert copied_batches[1][0][1] == "MSFT"
    delete_sql, delete_params = next(
        (sql, params) for sql, params in cursor.executed if "DELETE FROM gold.market_data AS target" in sql
    )
    assert "pg_temp.gold_sync_stage AS stage" in delete_sql
    assert delete_params == (["AAPL", "MSFT", "OLD"],)


def test_sync_gold_bucket_records_failure_state(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, object] = {}
    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(_FakeCursor(fail_on_execute=True)))
    monkeypatch.setattr(sync, "_record_failed_sync_state", lambda *args, **kwargs: recorded.update(kwargs))

    with pytest.raises(sync.PostgresError, match="Gold Postgres sync failed"):
        sync.sync_gold_bucket(
            domain="finance",
            bucket="A",
            frame=pd.DataFrame({"date": [pd.Timestamp("2026-01-02")], "symbol": ["AAPL"]}),
            scope_symbols=["AAPL"],
            source_commit=321.0,
            dsn="postgresql://test",
        )

    assert recorded["domain"] == "finance"
    assert recorded["bucket"] == "A"
    assert recorded["source_commit"] == 321.0


def test_sync_gold_bucket_retries_transient_read_only_delete_failures_until_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages: list[str] = []
    retry_sleeps: list[float] = []
    connect_calls = {"count": 0}

    class ReadOnlySqlTransaction(RuntimeError):
        pass

    class _RetryCursor(_FakeCursor):
        def __init__(self, *, fail_delete: bool) -> None:
            super().__init__()
            self.fail_delete = fail_delete

        def execute(self, sql: str, params=None) -> None:
            self.executed.append((sql, params))
            if "DELETE FROM" in sql and self.fail_delete:
                raise ReadOnlySqlTransaction("cannot execute DELETE in a read-only transaction")
            self.rowcount = 0

    def _fake_connect(_dsn: str):
        connect_calls["count"] = int(connect_calls["count"]) + 1
        return _FakeConnection(_RetryCursor(fail_delete=connect_calls["count"] == 1))

    monkeypatch.setattr(sync, "connect", _fake_connect)
    monkeypatch.setattr(sync.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(sync.time, "sleep", lambda seconds: retry_sleeps.append(float(seconds)))

    result = sync.sync_gold_bucket(
        domain="market",
        bucket="A",
        frame=pd.DataFrame({"date": [pd.Timestamp("2026-01-02")], "symbol": ["AAPL"]}),
        scope_symbols=["AAPL"],
        source_commit=123.0,
        dsn="postgresql://test",
    )

    assert result.status == "ok"
    assert connect_calls["count"] == 2
    assert retry_sleeps == [2.0]
    assert any("postgres_gold_sync_retry domain=market bucket=A attempt=1 next_attempt=2" in msg for msg in messages)


def test_sync_gold_bucket_classifies_read_only_delete_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []
    recorded: dict[str, object] = {}

    class ReadOnlySqlTransaction(RuntimeError):
        pass

    class _ReadOnlyCursor(_FakeCursor):
        def execute(self, sql: str, params=None) -> None:
            self.executed.append((sql, params))
            if "DELETE FROM" in sql:
                raise ReadOnlySqlTransaction("cannot execute DELETE in a read-only transaction")
            self.rowcount = 0

    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(_ReadOnlyCursor()))
    monkeypatch.setattr(sync, "_record_failed_sync_state", lambda *args, **kwargs: recorded.update(kwargs))
    monkeypatch.setattr(sync.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(sync.time, "sleep", lambda _seconds: None)

    with pytest.raises(sync.PostgresError, match="stage=delete_missing category=read_only_transaction"):
        sync.sync_gold_bucket(
            domain="market",
            bucket="A",
            frame=pd.DataFrame({"date": [pd.Timestamp("2026-01-02")], "symbol": ["AAPL"]}),
            scope_symbols=["AAPL"],
            source_commit=123.0,
            dsn="postgresql://test",
        )

    assert recorded["error"] == (
        "stage=delete_missing category=read_only_transaction "
        "error_class=ReadOnlySqlTransaction transient=true "
        "detail=cannot execute DELETE in a read-only transaction"
    )
    assert any(
        "postgres_gold_sync_failure domain=market bucket=A stage=delete_missing "
        "category=read_only_transaction transient=true error_class=ReadOnlySqlTransaction"
        in message
        for message in messages
    )


def test_sync_gold_bucket_detects_read_only_session_before_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []
    recorded: dict[str, object] = {}

    def _fake_connect(_dsn: str):
        return _FakeConnection(_FakeCursor(fetchone_rows=[("on",), ("off",), (False,)]))

    monkeypatch.setattr(sync, "connect", _fake_connect)
    monkeypatch.setattr(sync, "_record_failed_sync_state", lambda *args, **kwargs: recorded.update(kwargs))
    monkeypatch.setattr(sync.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(sync.time, "sleep", lambda _seconds: None)

    with pytest.raises(sync.PostgresError, match="stage=verify_write_target category=read_only_transaction"):
        sync.sync_gold_bucket(
            domain="market",
            bucket="A",
            frame=pd.DataFrame({"date": [pd.Timestamp("2026-01-02")], "symbol": ["AAPL"]}),
            scope_symbols=["AAPL"],
            source_commit=123.0,
            dsn="postgresql://test",
        )

    assert recorded["error"] == (
        "stage=verify_write_target category=read_only_transaction "
        "error_class=PostgresWriteTargetUnavailableError transient=true "
        "detail=Postgres write target unavailable: transaction_read_only=on "
        "default_transaction_read_only=off pg_is_in_recovery=false"
    )
    assert any(
        "postgres_gold_sync_failure domain=market bucket=A stage=verify_write_target "
        "category=read_only_transaction transient=true error_class=PostgresWriteTargetUnavailableError"
        in message
        for message in messages
    )


def test_sync_gold_bucket_chunks_retries_with_callable_frame_supplier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retry_sleeps: list[float] = []
    frame_supplier_calls = {"count": 0}
    connect_calls = {"count": 0}

    class ReadOnlySqlTransaction(RuntimeError):
        pass

    class _RetryCursor(_FakeCursor):
        def __init__(self, *, fail_copy: bool) -> None:
            super().__init__()
            self.fail_copy = fail_copy

        def execute(self, sql: str, params=None) -> None:
            self.executed.append((sql, params))
            self.rowcount = 0

        def copy(self, _statement: str):
            class _FailingCopy:
                def __init__(self, *, fail_copy: bool) -> None:
                    self.fail_copy = fail_copy
                    self.write_calls = 0

                def __enter__(self_inner):
                    return self_inner

                def __exit__(self_inner, exc_type, exc, tb) -> None:
                    return None

                def write_row(self_inner, _row) -> None:
                    self_inner.write_calls += 1
                    if self_inner.fail_copy and self_inner.write_calls == 1:
                        raise ReadOnlySqlTransaction("cannot execute DELETE in a read-only transaction")

            return _FailingCopy(fail_copy=self.fail_copy)

    def _fake_connect(_dsn: str):
        connect_calls["count"] = int(connect_calls["count"]) + 1
        return _FakeConnection(_RetryCursor(fail_copy=connect_calls["count"] == 1))

    def _frame_supplier():
        frame_supplier_calls["count"] = int(frame_supplier_calls["count"]) + 1
        return [
            pd.DataFrame({"date": [pd.Timestamp("2026-01-02")], "symbol": ["AAPL"], "close": [101.5]}),
            pd.DataFrame({"date": [pd.Timestamp("2026-01-03")], "symbol": ["MSFT"], "close": [202.5]}),
        ]

    monkeypatch.setattr(sync, "connect", _fake_connect)
    monkeypatch.setattr(sync.time, "sleep", lambda seconds: retry_sleeps.append(float(seconds)))

    result = sync.sync_gold_bucket_chunks(
        domain="market",
        bucket="A",
        frames=_frame_supplier,
        scope_symbols=["AAPL", "MSFT"],
        source_commit=123.0,
        dsn="postgresql://test",
    )

    assert result.status == "ok"
    assert result.row_count == 2
    assert result.symbol_count == 2
    assert connect_calls["count"] == 2
    assert frame_supplier_calls["count"] == 2
    assert retry_sleeps == [2.0]


def test_sync_gold_bucket_empty_frame_deletes_stale_scope_rows_and_records_zero_row_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        rowcount_map={
            "DELETE FROM gold.finance_data AS target": 2,
            "INSERT INTO gold.finance_data AS target": 0,
        }
    )
    messages: list[str] = []
    copied_tables: list[str] = []

    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(sync.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(
        sync,
        "copy_rows",
        lambda _cur, *, table, columns, rows: copied_tables.append(str(table)),
    )

    result = sync.sync_gold_bucket(
        domain="finance",
        bucket="A",
        frame=pd.DataFrame({"date": [], "symbol": []}),
        scope_symbols=["AAPL", "MSFT"],
        source_commit=321.0,
        dsn="postgresql://test",
    )

    assert result.status == "ok"
    assert result.row_count == 0
    assert result.symbol_count == 0
    assert result.scope_symbol_count == 2
    assert copied_tables == []
    assert any("DELETE FROM gold.finance_data AS target" in sql for sql, _params in cursor.executed)
    assert any(
        "postgres_gold_sync_apply_stats domain=finance bucket=A staged_rows=0 "
        "deleted_rows=2 upserted_rows=0 unchanged_rows=0 scope_symbols=2"
        in message
        for message in messages
    )


def test_validate_sync_target_schema_raises_for_missing_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("gold.finance_data",)],
        fetchall_rows=[("date",), ("symbol",), ("market_cap",), ("pe_ratio",)],
    )
    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))

    with pytest.raises(sync.PostgresError, match="missing columns=.*price_to_book"):
        sync.validate_sync_target_schema(
            "postgresql://test",
            domain="finance",
            remediation_hint="Run finance schema migrations.",
        )


def test_validate_sync_target_schema_raises_when_target_table_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(fetchone_rows=[(None,)])
    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))

    with pytest.raises(sync.PostgresError, match="target table does not exist"):
        sync.validate_sync_target_schema(
            "postgresql://test",
            domain="finance",
            remediation_hint="Run finance schema migrations.",
        )


def test_prepare_frame_preserves_earnings_calendar_text_and_dates() -> None:
    config = sync.get_sync_config("earnings")
    prepared = sync._prepare_frame(  # type: ignore[attr-defined]
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-02-28")],
                "symbol": ["aapl"],
                "next_earnings_date": [pd.Timestamp("2026-03-01")],
                "next_earnings_fiscal_date_ending": [pd.Timestamp("2025-12-31")],
                "next_earnings_time_of_day": [" post-market "],
                "has_upcoming_earnings": [1],
                "is_scheduled_earnings_day": [0],
            }
        ),
        config=config,
    )

    row = prepared.iloc[0]
    assert row["symbol"] == "AAPL"
    assert row["next_earnings_date"] == date(2026, 3, 1)
    assert row["next_earnings_fiscal_date_ending"] == date(2025, 12, 31)
    assert row["next_earnings_time_of_day"] == "post-market"


def test_market_sync_config_includes_market_structure_columns() -> None:
    config = sync.get_sync_config("market")
    prepared = sync._prepare_frame(  # type: ignore[attr-defined]
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-02-28")],
                "symbol": ["aapl"],
                "dividend_amount": [0.25],
                "split_coefficient": [2.0],
                "is_dividend_day": [1],
                "is_split_day": [1],
                "donchian_high_20d": [110.5],
                "sr_support_1_touches": [2],
                "fib_swing_direction": [-1],
                "fib_level_618": [98.2],
                "fib_in_value_zone": [1],
            }
        ),
        config=config,
    )

    row = prepared.iloc[0]
    assert "dividend_amount" in config.columns
    assert "split_coefficient" in config.columns
    assert "donchian_high_20d" in config.columns
    assert "fib_level_618" in config.columns
    assert "is_dividend_day" in config.integer_columns
    assert "is_split_day" in config.integer_columns
    assert "sr_support_1_touches" in config.integer_columns
    assert "fib_swing_direction" in config.integer_columns
    assert float(row["dividend_amount"]) == 0.25
    assert float(row["split_coefficient"]) == 2.0
    assert int(row["is_dividend_day"]) == 1
    assert int(row["is_split_day"]) == 1
    assert int(row["sr_support_1_touches"]) == 2
    assert int(row["fib_swing_direction"]) == -1
    assert int(row["fib_in_value_zone"]) == 1


def test_finance_sync_config_includes_wide_ratio_columns() -> None:
    config = sync.get_sync_config("finance")
    prepared = sync._prepare_frame(  # type: ignore[attr-defined]
        pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-02-28")],
                "symbol": ["aapl"],
                "market_cap": [1_000_000.0],
                "pe_ratio": [20.0],
                "price_to_book": [5.2],
                "current_ratio": [1.8],
                "ev_to_ebitda": [12.4],
                "free_cash_flow": [123456.0],
            }
        ),
        config=config,
    )

    row = prepared.iloc[0]
    assert "price_to_book" in config.columns
    assert "current_ratio" in config.columns
    assert "ev_to_ebitda" in config.columns
    assert "free_cash_flow" in config.columns
    assert row["symbol"] == "AAPL"
    assert row["price_to_book"] == pytest.approx(5.2)
    assert row["current_ratio"] == pytest.approx(1.8)
