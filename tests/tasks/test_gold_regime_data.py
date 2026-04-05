from __future__ import annotations

from typing import Any
from datetime import datetime, timezone

import pandas as pd
import pytest

from core.market_symbols import REGIME_REQUIRED_MARKET_SYMBOLS
from tasks.regime_data import gold_regime_data as regime_job


class _FakeCursor:
    def __init__(self, *, fetchone_rows=None, rowcount_map: dict[str, int] | None = None) -> None:
        self.fetchone_rows = list(fetchone_rows or [])
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

    def fetchone(self):
        if not self.fetchone_rows:
            return None
        return self.fetchone_rows.pop(0)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_regime_job_uses_shared_required_market_symbol_contract() -> None:
    assert regime_job.REGIME_REQUIRED_MARKET_SYMBOLS == REGIME_REQUIRED_MARKET_SYMBOLS


def test_validate_required_market_series_reports_missing_symbols() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["SPY", "SPY", "^VIX"],
            "date": ["2026-03-03", "2026-03-04", "2026-03-04"],
            "close": [580.0, 582.0, 21.5],
            "return_1d": [0.01, 0.003, None],
            "return_20d": [0.04, 0.05, None],
        }
    )

    normalized = regime_job._normalize_market_series(frame)

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(normalized)

    message = str(excinfo.value)
    assert "missing required regime symbols" in message
    assert REGIME_REQUIRED_MARKET_SYMBOLS[-1] in message
    assert "coverage=" in message


def test_validate_required_market_series_logs_fast_fail_context(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = regime_job._normalize_market_series(
        pd.DataFrame(
            {
                "symbol": ["SPY", "^VIX"],
                "date": ["2026-03-03", "2026-03-03"],
                "close": [580.0, 21.5],
                "return_1d": [0.01, None],
                "return_20d": [0.04, None],
            }
        )
    )
    errors: list[str] = []

    monkeypatch.setattr(regime_job, "_summarize_market_sync_state", lambda _dsn: "market_sync_state=empty")
    monkeypatch.setattr(regime_job.mdc, "write_error", lambda msg: errors.append(str(msg)))

    with pytest.raises(ValueError) as excinfo:
        regime_job._validate_required_market_series(frame, dsn="postgresql://example")

    message = str(excinfo.value)
    assert "Gold regime fast-fail" in message
    assert "market_sync_state=empty" in message
    assert "gold-market-job" in message
    assert errors == [message]


def test_assert_complete_regime_inputs_reports_non_overlapping_series() -> None:
    market_series = regime_job._validate_required_market_series(
        regime_job._normalize_market_series(
            pd.DataFrame(
                {
                    "symbol": ["SPY", "SPY", "^VIX", "^VIX3M"],
                    "date": ["2026-03-03", "2026-03-04", "2026-03-05", "2026-03-05"],
                    "close": [580.0, 582.0, 21.5, 22.0],
                    "return_1d": [0.01, 0.003, None, None],
                    "return_20d": [0.04, 0.05, None, None],
                }
            )
        )
    )

    inputs = regime_job._build_inputs_daily(
        market_series,
        computed_at=datetime(2026, 3, 9, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError) as excinfo:
        regime_job._assert_complete_regime_inputs(inputs, market_series=market_series)

    message = str(excinfo.value)
    assert "no complete SPY/^VIX/^VIX3M rows" in message
    assert "inputs_range=" in message
    assert "coverage=" in message


def test_write_storage_outputs_refreshes_persisted_metadata_snapshots(monkeypatch: pytest.MonkeyPatch) -> None:
    parquet_paths: list[str] = []
    saved_artifact: dict[str, object] = {}
    snapshot_updates: list[dict[str, object]] = []

    class _FakeClient:
        def write_parquet(self, path: str, frame: pd.DataFrame) -> None:
            parquet_paths.append(path)
            assert isinstance(frame, pd.DataFrame)

    monkeypatch.setattr(regime_job.mdc, "get_storage_client", lambda _container: _FakeClient())
    monkeypatch.setattr(regime_job, "computed_at_iso", lambda: "2026-03-21T12:00:00+00:00")
    monkeypatch.setattr(
        "core.domain_artifacts.mdc.save_json_content",
        lambda payload, path, client=None: saved_artifact.update({"payload": payload, "path": path, "client": client}),
    )
    monkeypatch.setattr(
        "core.domain_artifacts.domain_metadata_snapshots.update_domain_metadata_snapshots_from_artifact",
        lambda **kwargs: snapshot_updates.append(kwargs),
    )

    inputs = pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "symbol": ["SPY"]})
    history = pd.DataFrame({"as_of_date": [pd.Timestamp("2026-03-20")], "regime_code": ["risk_on"]})
    latest = history.copy()
    transitions = pd.DataFrame({"effective_from_date": [pd.Timestamp("2026-03-20")]})

    regime_job._write_storage_outputs(
        gold_container="gold",
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
    )

    assert parquet_paths == [
        "regime/inputs.parquet",
        "regime/history.parquet",
        "regime/latest.parquet",
        "regime/transitions.parquet",
    ]
    assert saved_artifact["path"] == "regime/_metadata/domain.json"
    assert saved_artifact["payload"]["artifactPath"] == "regime/_metadata/domain.json"
    assert saved_artifact["payload"]["rootPath"] == "regime"
    assert len(snapshot_updates) == 1
    assert snapshot_updates[0]["layer"] == "gold"
    assert snapshot_updates[0]["domain"] == "regime"
    assert snapshot_updates[0]["artifact"]["artifactPath"] == "regime/_metadata/domain.json"


def test_replace_postgres_tables_uses_staged_apply_for_all_regime_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("off",), (False,)],
        rowcount_map={
            "DELETE FROM gold.regime_inputs_daily AS target": 1,
            "INSERT INTO gold.regime_inputs_daily AS target": 1,
            "DELETE FROM gold.regime_history AS target": 2,
            "INSERT INTO gold.regime_history AS target": 2,
            "DELETE FROM gold.regime_latest AS target": 1,
            "INSERT INTO gold.regime_latest AS target": 1,
            "DELETE FROM gold.regime_transitions AS target": 1,
            "INSERT INTO gold.regime_transitions AS target": 1,
        },
    )
    messages: list[str] = []
    copied_tables: list[str] = []

    monkeypatch.setattr(regime_job, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(regime_job.mdc, "write_line", lambda msg: messages.append(str(msg)))

    def _fake_copy_rows(_cur: Any, *, table: str, columns, rows) -> None:
        copied_tables.append(str(table))
        assert rows is not None

    monkeypatch.setattr(regime_job, "copy_rows", _fake_copy_rows)

    inputs = pd.DataFrame(
        {
            "as_of_date": [pd.Timestamp("2026-03-20")],
            "spy_close": [580.0],
            "return_1d": [0.01],
            "return_20d": [0.05],
            "rvol_10d_ann": [18.0],
            "vix_spot_close": [21.5],
            "vix3m_close": [22.1],
            "vix_slope": [0.6],
            "trend_state": ["up"],
            "curve_state": ["contango"],
            "vix_gt_32_streak": [0],
            "inputs_complete_flag": [True],
            "computed_at": [pd.Timestamp("2026-03-20T12:00:00Z")],
        }
    )
    history = pd.DataFrame(
        {
            "as_of_date": [pd.Timestamp("2026-03-20"), pd.Timestamp("2026-03-21")],
            "effective_from_date": [pd.Timestamp("2026-03-20"), pd.Timestamp("2026-03-20")],
            "model_name": ["default-regime", "default-regime"],
            "model_version": [1, 1],
            "regime_code": ["risk_on", "risk_on"],
            "regime_status": ["active", "active"],
            "matched_rule_id": ["rule-1", "rule-1"],
            "halt_flag": [False, False],
            "halt_reason": [pd.NA, pd.NA],
            "spy_return_20d": [0.05, 0.06],
            "rvol_10d_ann": [18.0, 17.5],
            "vix_spot_close": [21.5, 20.8],
            "vix3m_close": [22.1, 21.6],
            "vix_slope": [0.6, 0.8],
            "trend_state": ["up", "up"],
            "curve_state": ["contango", "contango"],
            "vix_gt_32_streak": [0, 0],
            "computed_at": [pd.Timestamp("2026-03-20T12:00:00Z"), pd.Timestamp("2026-03-21T12:00:00Z")],
        }
    )
    latest = history.tail(1).copy()
    transitions = pd.DataFrame(
        {
            "model_name": ["default-regime"],
            "model_version": [1],
            "effective_from_date": [pd.Timestamp("2026-03-20")],
            "prior_regime_code": [pd.NA],
            "new_regime_code": ["risk_on"],
            "trigger_rule_id": ["rule-1"],
            "computed_at": [pd.Timestamp("2026-03-20T12:00:00Z")],
        }
    )

    regime_job._replace_postgres_tables(
        "postgresql://test",
        inputs=inputs,
        history=history,
        latest=latest,
        transitions=transitions,
        active_models=[("default-regime", 1)],
    )

    assert copied_tables == [
        "pg_temp.regime_active_models_scope",
        "pg_temp.regime_stage_regime_inputs_daily",
        "pg_temp.regime_stage_regime_history",
        "pg_temp.regime_stage_regime_latest",
        "pg_temp.regime_stage_regime_transitions",
    ]
    assert all("TRUNCATE TABLE gold.regime_inputs_daily" not in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_history AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_latest AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_transitions AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_history AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_latest AS target" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO gold.regime_transitions AS target" in sql for sql, _params in cursor.executed)
    assert any("IS DISTINCT FROM" in sql for sql, _params in cursor.executed)
    history_delete_sql = next(
        sql for sql, _params in cursor.executed if "DELETE FROM gold.regime_history AS target" in sql
    )
    assert "pg_temp.regime_active_models_scope AS scope" in history_delete_sql
    assert "pg_temp.regime_stage_regime_history AS stage" in history_delete_sql
    assert all("pg_advisory_lock" not in sql and "LOCK TABLE" not in sql for sql, _params in cursor.executed)
    assert sum("gold_regime_postgres_apply_stats" in message for message in messages) == 4


def test_replace_postgres_tables_deletes_scoped_model_rows_even_when_stage_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(
        fetchone_rows=[("off",), (False,)],
        rowcount_map={
            "DELETE FROM gold.regime_inputs_daily AS target": 3,
            "DELETE FROM gold.regime_history AS target": 2,
            "DELETE FROM gold.regime_latest AS target": 1,
            "DELETE FROM gold.regime_transitions AS target": 1,
        },
    )
    messages: list[str] = []
    copied_tables: list[str] = []

    monkeypatch.setattr(regime_job, "connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(regime_job.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(
        regime_job,
        "copy_rows",
        lambda _cur, *, table, columns, rows: copied_tables.append(str(table)),
    )

    regime_job._replace_postgres_tables(
        "postgresql://test",
        inputs=pd.DataFrame(columns=regime_job._INPUTS_COLUMNS),
        history=pd.DataFrame(columns=regime_job._HISTORY_COLUMNS),
        latest=pd.DataFrame(columns=regime_job._HISTORY_COLUMNS),
        transitions=pd.DataFrame(columns=regime_job._TRANSITIONS_COLUMNS),
        active_models=[("default-regime", 1)],
    )

    assert copied_tables == ["pg_temp.regime_active_models_scope"]
    assert any("DELETE FROM gold.regime_inputs_daily AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_history AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_latest AS target" in sql for sql, _params in cursor.executed)
    assert any("DELETE FROM gold.regime_transitions AS target" in sql for sql, _params in cursor.executed)
    assert any(
        "gold_regime_postgres_apply_stats table=gold.regime_history staged_rows=0 "
        "deleted_rows=2 upserted_rows=0 unchanged_rows=0 scope=active_models scope_models=1"
        in message
        for message in messages
    )
