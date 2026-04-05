import pandas as pd
import pytest
from unittest.mock import patch
from core.pipeline import DataPaths
from tasks.earnings_data import silver_earnings_data as silver


def test_process_file_success():
    """
    Verifies process_file:
    1. Reads Bronze raw bytes (mocked)
    2. Cleans/normalizes
    3. Merges with history (mocked)
    4. Writes back to Silver (mocked)
    """
    blob_name = "earnings-data/TEST.json"

    # Mock bronze data
    bronze_json = '[{"Date": "2023-01-01", "Reported EPS": 1.5}]'

    # Mock history
    mock_history = pd.DataFrame([{"Date": pd.Timestamp("2022-01-01"), "Reported EPS": 1.0, "Symbol": "TEST"}])

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=mock_history),
        patch("core.delta_core.store_delta") as mock_store,
    ):
        res = silver.process_file(blob_name)

        assert res is True
        mock_store.assert_called_once()
        df_saved = mock_store.call_args[0][0]

        # Should have 2 rows (old + new)
        assert len(df_saved) == 2
        assert "TEST" in df_saved["symbol"].values


def test_process_alpha26_bucket_blob_accepts_string_last_modified(monkeypatch):
    blob_name = "earnings-data/buckets/A.parquet"
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


def test_process_file_bad_json():
    blob_name = "earnings-data/BAD.json"
    with patch("core.core.read_raw_bytes", return_value=b"bad json"):
        res = silver.process_file(blob_name)
        assert res is False


def test_process_file_applies_backfill_start_cutoff():
    blob_name = "earnings-data/TEST.json"
    bronze_json = '[{"Date":"2023-12-31","Reported EPS":1.1},' '{"Date":"2024-01-10","Reported EPS":1.5}]'
    history = pd.DataFrame([{"Date": pd.Timestamp("2023-06-30"), "Reported EPS": 1.0, "Symbol": "TEST"}])

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=history),
        patch("core.delta_core.store_delta") as mock_store,
        patch(
            "tasks.earnings_data.silver_earnings_data.get_backfill_range",
            return_value=(pd.Timestamp("2024-01-01"), None),
        ),
        patch("core.delta_core.vacuum_delta_table", return_value=0),
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert pd.to_datetime(df_saved["date"]).min().date().isoformat() >= "2024-01-01"


def test_process_file_preserves_earnings_numeric_precision():
    blob_name = "earnings-data/TEST.json"
    bronze_json = '[{"Date":"2024-01-10","Reported EPS":1.234567}]'

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=None),
        patch("core.delta_core.store_delta") as mock_store,
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert df_saved.iloc[0]["reported_eps"] == pytest.approx(1.234567)


def test_process_file_keeps_history_when_scheduled_rows_are_present():
    blob_name = "earnings-data/TEST.json"
    bronze_json = (
        '[{"date":"2024-03-31","report_date":"2024-04-30","fiscal_date_ending":"2024-03-31",'
        '"reported_eps":1.5,"eps_estimate":1.4,"surprise":0.0714,"record_type":"actual","symbol":"TEST"},'
        '{"date":"2026-05-07","report_date":"2026-05-07","fiscal_date_ending":"2026-03-31",'
        '"reported_eps":null,"eps_estimate":1.7,"surprise":null,"record_type":"scheduled",'
        '"calendar_time_of_day":"post-market","calendar_currency":"USD","symbol":"TEST"}]'
    )
    history = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2023-12-31"),
                "symbol": "TEST",
                "report_date": pd.Timestamp("2024-01-30"),
                "fiscal_date_ending": pd.Timestamp("2023-12-31"),
                "reported_eps": 1.2,
                "eps_estimate": 1.0,
                "surprise": 0.2,
                "record_type": "actual",
            }
        ]
    )

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=history),
        patch("core.delta_core.store_delta") as mock_store,
        patch("tasks.earnings_data.silver_earnings_data.get_backfill_range", return_value=(None, None)),
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        saved_dates = sorted(pd.to_datetime(df_saved["date"]).dt.date.astype(str).tolist())
        assert saved_dates == ["2023-12-31", "2024-03-31", "2026-05-07"]
        scheduled = df_saved.loc[df_saved["record_type"] == "scheduled"].iloc[0]
        assert scheduled["calendar_time_of_day"] == "post-market"


def test_process_file_replaces_stale_scheduled_row_for_same_fiscal_period():
    blob_name = "earnings-data/TEST.json"
    bronze_json = (
        '[{"date":"2026-05-08","report_date":"2026-05-08","fiscal_date_ending":"2026-03-31",'
        '"reported_eps":null,"eps_estimate":1.8,"surprise":null,"record_type":"scheduled",'
        '"calendar_time_of_day":"post-market","calendar_currency":"USD","symbol":"TEST"}]'
    )
    history = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-05-01"),
                "symbol": "TEST",
                "report_date": pd.Timestamp("2026-05-01"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "reported_eps": None,
                "eps_estimate": 1.7,
                "surprise": None,
                "record_type": "scheduled",
                "calendar_time_of_day": "post-market",
                "calendar_currency": "USD",
            }
        ]
    )

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=history),
        patch("core.delta_core.store_delta") as mock_store,
        patch("tasks.earnings_data.silver_earnings_data.get_backfill_range", return_value=(None, None)),
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert len(df_saved) == 1
        row = df_saved.iloc[0]
        assert pd.to_datetime(row["report_date"]).date().isoformat() == "2026-05-08"
        assert pd.to_datetime(row["fiscal_date_ending"]).date().isoformat() == "2026-03-31"


def test_process_file_actual_replaces_scheduled_row_for_same_fiscal_period():
    blob_name = "earnings-data/TEST.json"
    bronze_json = (
        '[{"date":"2026-03-31","report_date":"2026-05-09","fiscal_date_ending":"2026-03-31",'
        '"reported_eps":1.9,"eps_estimate":1.8,"surprise":0.055,"record_type":"actual","symbol":"TEST"}]'
    )
    history = pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-05-08"),
                "symbol": "TEST",
                "report_date": pd.Timestamp("2026-05-08"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "reported_eps": None,
                "eps_estimate": 1.8,
                "surprise": None,
                "record_type": "scheduled",
                "calendar_time_of_day": "post-market",
                "calendar_currency": "USD",
            }
        ]
    )

    with (
        patch("core.core.read_raw_bytes", return_value=bronze_json.encode("utf-8")),
        patch("core.delta_core.load_delta", return_value=history),
        patch("core.delta_core.store_delta") as mock_store,
        patch("tasks.earnings_data.silver_earnings_data.get_backfill_range", return_value=(None, None)),
    ):
        assert silver.process_file(blob_name) is True
        df_saved = mock_store.call_args[0][0]
        assert len(df_saved) == 1
        row = df_saved.iloc[0]
        assert row["record_type"] == "actual"
        assert row["reported_eps"] == pytest.approx(1.9)
        assert pd.to_datetime(row["fiscal_date_ending"]).date().isoformat() == "2026-03-31"


def test_canonicalize_earnings_frame_keeps_calendar_time_of_day_as_string_dtype_when_all_null():
    df = pd.DataFrame(
        [
            {
                "date": "2026-03-31",
                "report_date": "2026-05-09",
                "fiscal_date_ending": "2026-03-31",
                "reported_eps": 1.9,
                "record_type": "actual",
                "symbol": "YALA",
            }
        ]
    )

    out = silver._canonicalize_earnings_frame(df)

    assert pd.api.types.is_string_dtype(out["calendar_time_of_day"])
    assert out["calendar_time_of_day"].isna().all()


def test_write_alpha26_earnings_buckets_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_silver_earnings_bucket_path("A")
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

    written_symbols, index_path, _column_count = silver._write_alpha26_earnings_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_write_alpha26_earnings_buckets_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_silver_earnings_bucket_path("A")
    existing_cols = ["date", "symbol", "reported_eps"]
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

    written_symbols, index_path, _column_count = silver._write_alpha26_earnings_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 1
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == existing_cols


def test_write_alpha26_earnings_buckets_partial_update_preserves_untouched_symbol_index(monkeypatch):
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
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: ["date", "symbol"])
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda _df, _container, path, mode="overwrite", **_kwargs: captured_paths.append(path),
    )

    written_symbols, index_path, _column_count = silver._write_alpha26_earnings_buckets(
        {"A": [pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})]},
        touched_buckets={"A"},
    )

    assert written_symbols == 2
    assert index_path == "index"
    assert captured_paths == [DataPaths.get_silver_earnings_bucket_path("A")]
    assert captured_index["symbol_to_bucket"] == {"AMZN": "A", "MSFT": "M"}


def test_write_alpha26_earnings_buckets_partial_update_fails_closed_without_prior_index(monkeypatch):
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(columns=["symbol", "bucket"]),
    )

    with pytest.raises(RuntimeError, match="incremental alpha26 write blocked"):
        silver._write_alpha26_earnings_buckets(
            {"A": [pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})]},
            touched_buckets={"A"},
        )
