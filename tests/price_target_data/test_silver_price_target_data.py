import pandas as pd
import pytest

from core.pipeline import DataPaths
from tasks.price_target_data import silver_price_target_data as silver


def test_process_blob_skips_unchanged_without_loading_source_or_history(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}
    calls = {"read_raw_bytes": 0, "load_delta": 0, "get_delta_schema_columns": 0}

    monkeypatch.setattr(silver, "check_blob_unchanged", lambda _blob, _prior: (True, {"etag": "abc123"}))

    def _read_raw_bytes(*_args, **_kwargs):
        calls["read_raw_bytes"] += 1
        return b"ignored"

    def _load_delta(*_args, **_kwargs):
        calls["load_delta"] += 1
        return None

    def _get_delta_schema_columns(*_args, **_kwargs):
        calls["get_delta_schema_columns"] += 1
        return None

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", _read_raw_bytes)
    monkeypatch.setattr(silver.delta_core, "load_delta", _load_delta)
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", _get_delta_schema_columns)

    status = silver.process_blob(blob, watermarks={blob_name: {"etag": "abc123"}})

    assert status == "skipped_unchanged"
    assert calls == {"read_raw_bytes": 0, "load_delta": 0, "get_delta_schema_columns": 0}


def test_process_alpha26_bucket_blob_accepts_string_last_modified(monkeypatch):
    blob_name = "price-target-data/buckets/A.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": "2026-03-04T01:00:00Z",
    }
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: pd.DataFrame())

    status = silver.process_alpha26_bucket_blob(
        blob,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert status == "ok"
    assert watermarks[blob_name]["etag"] == "etag-a"
    assert watermarks[blob_name]["last_modified"] == "2026-03-04T01:00:00+00:00"


def test_process_blob_applies_backfill_start_cutoff(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}

    source = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2023-12-31"), pd.Timestamp("2024-01-02")],
            "tp_mean_est": [140.0, 150.0],
            "tp_std_dev_est": [7.0, 7.5],
            "tp_high_est": [155.0, 165.0],
            "tp_low_est": [125.0, 135.0],
            "tp_cnt_est": [9.0, 10.0],
            "tp_cnt_est_rev_up": [2.0, 3.0],
            "tp_cnt_est_rev_down": [1.0, 1.0],
        }
    )
    history = source.copy()
    history["obs_date"] = [pd.Timestamp("2023-12-30"), pd.Timestamp("2024-01-01")]
    history["symbol"] = "AAPL"

    captured: dict = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: source.copy())
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: history.copy())
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (pd.Timestamp("2024-01-01"), None))
    monkeypatch.setattr(silver.delta_core, "vacuum_delta_table", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda df, *_args, **_kwargs: captured.setdefault("df", df.copy()),
    )

    status = silver.process_blob(blob, watermarks={})

    assert status == "ok"
    assert "df" in captured
    assert pd.to_datetime(captured["df"]["obs_date"]).min().date().isoformat() >= "2024-01-01"


def test_process_blob_applies_price_target_precision_policy(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}
    today = pd.Timestamp.today().normalize()
    source = pd.DataFrame(
        {
            "obs_date": [today],
            "tp_mean_est": [100.005],
            "tp_std_dev_est": [1.23445],
            "tp_high_est": [120.005],
            "tp_low_est": [80.005],
            "tp_cnt_est": [10.125],
            "tp_cnt_est_rev_up": [2.0],
            "tp_cnt_est_rev_down": [1.0],
        }
    )

    captured: dict = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: source.copy())
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda df, *_args, **_kwargs: captured.setdefault("df", df.copy()),
    )

    status = silver.process_blob(blob, watermarks={})

    assert status == "ok"
    row = captured["df"].iloc[0]
    assert row["tp_mean_est"] == pytest.approx(100.01)
    assert row["tp_high_est"] == pytest.approx(120.01)
    assert row["tp_low_est"] == pytest.approx(80.01)
    assert row["tp_std_dev_est"] == pytest.approx(1.2345)
    assert row["tp_cnt_est"] == pytest.approx(10.125)


def test_process_blob_persists_watermark_signature_on_success(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": "2026-03-04T01:00:00Z",
    }
    source = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2026-03-04")],
            "tp_mean_est": [100.0],
            "tp_std_dev_est": [1.0],
            "tp_high_est": [110.0],
            "tp_low_est": [90.0],
            "tp_cnt_est": [10.0],
            "tp_cnt_est_rev_up": [2.0],
            "tp_cnt_est_rev_down": [1.0],
        }
    )
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: source.copy())
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.delta_core, "store_delta", lambda *_args, **_kwargs: None)

    status = silver.process_blob(blob, watermarks=watermarks)

    assert status == "ok"
    assert watermarks[blob_name]["etag"] == "etag-a"
    assert watermarks[blob_name]["last_modified"] == "2026-03-04T01:00:00+00:00"
    assert "updated_at" in watermarks[blob_name]


def test_write_alpha26_price_target_buckets_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_silver_price_target_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(_df: pd.DataFrame, _container: str, _path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path, _column_count = silver._write_alpha26_price_target_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_write_alpha26_price_target_buckets_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_silver_price_target_bucket_path("A")
    existing_cols = ["obs_date", "symbol", "tp_mean_est"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        silver.delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path, _column_count = silver._write_alpha26_price_target_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 1
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == existing_cols


def test_write_alpha26_price_target_buckets_partial_update_preserves_untouched_symbol_index(monkeypatch):
    captured_index: dict = {}
    captured_paths: list[str] = []

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["AAPL", "MSFT"], "bucket": ["A", "M"]}),
    )
    monkeypatch.setattr(
        silver.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "index",
    )
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: ["obs_date", "symbol"])
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda _df, _container, path, mode="overwrite", **_kwargs: captured_paths.append(path),
    )

    written_symbols, index_path, _column_count = silver._write_alpha26_price_target_buckets(
        {"A": [pd.DataFrame({"obs_date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})]},
        touched_buckets={"A"},
    )

    assert written_symbols == 2
    assert index_path == "index"
    assert captured_paths == [DataPaths.get_silver_price_target_bucket_path("A")]
    assert captured_index["symbol_to_bucket"] == {"AMZN": "A", "MSFT": "M"}


def test_write_alpha26_price_target_buckets_partial_update_fails_closed_without_prior_index(monkeypatch):
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(columns=["symbol", "bucket"]),
    )

    with pytest.raises(RuntimeError, match="incremental alpha26 write blocked"):
        silver._write_alpha26_price_target_buckets(
            {"A": [pd.DataFrame({"obs_date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})]},
            touched_buckets={"A"},
        )


def test_main_runs_price_target_reconciliation_and_records_metadata(monkeypatch):
    saved_last_success: dict = {}
    reconciliation_calls: list[list[dict]] = []

    def _save_last_success(_name: str, metadata=None):
        if metadata:
            saved_last_success.update(metadata)

    def _run_reconciliation(*, bronze_blob_list):
        reconciliation_calls.append(list(bronze_blob_list))
        return 2, 5

    monkeypatch.setattr(silver, "bronze_client", object())
    monkeypatch.setattr(
        silver.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda _domain, _client: [],
    )
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "save_last_success", _save_last_success)
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_run_price_target_reconciliation", _run_reconciliation)
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.mdc, "write_error", lambda *_args, **_kwargs: None)

    assert silver.main() == 0
    assert reconciliation_calls == [[]]
    assert saved_last_success.get("reconciled_orphans") == 2
    assert saved_last_success.get("reconciliation_deleted_blobs") == 5


def test_main_fails_closed_when_price_target_reconciliation_fails(monkeypatch):
    save_last_success_calls = {"count": 0}

    monkeypatch.setattr(silver, "bronze_client", object())
    monkeypatch.setattr(
        silver.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda _domain, _client: [],
    )
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda *_args, **_kwargs: save_last_success_calls.__setitem__("count", save_last_success_calls["count"] + 1),
    )
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(
        silver,
        "_run_price_target_reconciliation",
        lambda *, bronze_blob_list: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.mdc, "write_error", lambda *_args, **_kwargs: None)

    assert silver.main() == 1
    assert save_last_success_calls["count"] == 0
