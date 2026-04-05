import logging

import pandas as pd
import pytest

from core import delta_core


def _patch_delta_core_for_unit(monkeypatch, tmp_path):
    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(tmp_path / "table"))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})


def test_store_delta_drops_index_artifacts_before_write(monkeypatch, tmp_path):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    captured = {}

    monkeypatch.setattr(
        delta_core,
        "_get_existing_delta_schema_columns",
        lambda _uri, _storage_options: ["a", "index_level_0"],
    )

    def fake_write_deltalake(_uri, df, **kwargs):
        captured["df"] = df.copy()
        captured.update(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

    delta_core.store_delta(
        pd.DataFrame({"a": [1], "index_level_0": [5]}),
        container="container",
        path="silver/test",
        mode="overwrite",
    )

    assert captured["mode"] == "overwrite"
    assert list(captured["df"].columns) == ["a"]


def test_store_delta_resets_non_range_index_before_write(monkeypatch, tmp_path):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    captured = {}

    def fake_write_deltalake(_uri, df, **kwargs):
        captured["df"] = df.copy()
        captured.update(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

    df = pd.DataFrame({"a": [10, 20]})
    df.index = pd.Index([3, 4])

    delta_core.store_delta(
        df,
        container="container",
        path="silver/test",
        mode="overwrite",
    )

    assert isinstance(captured["df"].index, pd.RangeIndex)
    assert captured["df"].index.start == 0
    assert captured["df"].index.step == 1


def test_store_delta_triggers_schema_mismatch_diagnostics(monkeypatch, tmp_path):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    called = {"count": 0}

    def fake_write_deltalake(_uri, _df, **_kwargs):
        raise Exception("Cannot cast schema, number of fields does not match: 35 vs 30")

    def fake_log_mismatch(_df, _container, _path):
        called["count"] += 1

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)
    monkeypatch.setattr(delta_core, "_log_delta_schema_mismatch", fake_log_mismatch)

    with pytest.raises(Exception, match="Cannot cast schema"):
        delta_core.store_delta(
            pd.DataFrame({"a": [1]}),
            container="container",
            path="gold/test",
            mode="overwrite",
        )

    assert called["count"] == 1


def test_log_delta_schema_mismatch_emits_missing_extra_and_hint(monkeypatch, caplog):
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: "dummy-uri")
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})
    monkeypatch.setattr(
        delta_core,
        "_get_existing_delta_schema_columns",
        lambda _uri, _storage_options: ["date", "symbol", "drawdown"],
    )

    df = pd.DataFrame(columns=["date", "symbol", "drawdown_1y"])
    logger_name = delta_core.logger.name

    with caplog.at_level(logging.ERROR, logger=logger_name):
        delta_core._log_delta_schema_mismatch(df, container="market-data", path="gold/AAPL")

    assert "missing_in_df=['drawdown']" in caplog.text
    assert "extra_in_df=['drawdown_1y']" in caplog.text
    assert "existing table has 'drawdown' but DataFrame has 'drawdown_1y'" in caplog.text


def test_store_delta_logs_prewrite_column_comparison_warning(monkeypatch, tmp_path, caplog):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    monkeypatch.setattr(
        delta_core,
        "_get_existing_delta_schema_columns",
        lambda _uri, _storage_options: ["a", "b"],
    )
    monkeypatch.setattr(delta_core, "write_deltalake", lambda *_args, **_kwargs: None)

    logger_name = delta_core.logger.name
    with caplog.at_level(logging.INFO, logger=logger_name):
        delta_core.store_delta(
            pd.DataFrame({"a": [1], "c": [2]}),
            container="container",
            path="gold/test",
            mode="overwrite",
        )

    matching = [
        record
        for record in caplog.records
        if "Pre-write Delta column check for gold/test" in record.getMessage()
    ]
    assert matching
    assert any(record.levelno == logging.WARNING for record in matching)


def test_store_delta_logs_all_null_column_profiles(monkeypatch, tmp_path, caplog):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    monkeypatch.setattr(delta_core, "write_deltalake", lambda *_args, **_kwargs: None)

    logger_name = delta_core.logger.name
    with caplog.at_level(logging.WARNING, logger=logger_name):
        delta_core.store_delta(
            pd.DataFrame(
                {
                    "a": [1.0],
                    "all_null_text": pd.Series([pd.NA], dtype="string"),
                }
            ),
            container="container",
            path="gold/test",
            mode="overwrite",
        )

    assert "Pre-write Delta all-null columns for gold/test" in caplog.text
    assert "all_null_text(dtype=string)" in caplog.text


def test_store_delta_incompatible_rename_preserves_existing_table_schema(monkeypatch, tmp_path):
    table_dir = tmp_path / "price_targets_gold"
    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(table_dir))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})

    from deltalake import DeltaTable

    df_old = pd.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "obs_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "tp_mean_est": [100.0, 101.0],
        }
    )

    delta_core.store_delta(df_old, container="price-target-data", path="gold/AAPL", mode="overwrite")
    old_cols = [f.name for f in DeltaTable(str(table_dir)).schema().fields]
    assert "ticker" in old_cols
    assert "symbol" not in old_cols

    df_new = df_old.rename(columns={"ticker": "symbol"})
    with pytest.raises(Exception):
        delta_core.store_delta(
            df_new,
            container="price-target-data",
            path="gold/AAPL",
            mode="overwrite",
        )

    persisted_cols = [f.name for f in DeltaTable(str(table_dir)).schema().fields]
    assert "ticker" in persisted_cols
    assert "symbol" not in persisted_cols
