import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from tasks.finance_data import silver_finance_data as silver


def test_read_finance_json_projects_only_balance_sheet_columns() -> None:
    payload = {
        "schema_version": 2,
        "provider": "massive",
        "report_type": "balance_sheet",
        "rows": [
            {
                "date": "2024-03-31",
                "timeframe": "quarterly",
                "total_assets": 1000.0,
                "current_assets": 250.0,
                "current_liabilities": 125.0,
                "long_term_debt": 300.0,
            }
        ],
    }

    out = silver._read_finance_json(
        json.dumps(payload).encode("utf-8"),
        ticker="AAPL",
        report_type="balance_sheet",
    )

    assert list(out.columns) == [
        "Date",
        "Symbol",
        "long_term_debt",
        "total_assets",
        "current_assets",
        "current_liabilities",
        "timeframe",
    ]
    assert out.loc[0, "Symbol"] == "AAPL"
    assert out.loc[0, "total_assets"] == 1000.0
    assert out.loc[0, "timeframe"] == "quarterly"


def test_read_finance_json_maps_income_statement_shares_outstanding_from_bronze_aliases() -> None:
    payload = {
        "status": "OK",
        "results": [
            {
                "period_end": "2024-03-31",
                "timeframe": "quarterly",
                "total_revenue": 1000.0,
                "gross_profit": 400.0,
                "net_income": 120.0,
                "basic_shares_outstanding": 50.0,
                "diluted_shares_outstanding": 55.0,
            }
        ],
    }

    out = silver._read_finance_json(
        json.dumps(payload).encode("utf-8"),
        ticker="AAPL",
        report_type="income_statement",
    )

    assert list(out.columns) == [
        "Date",
        "Symbol",
        "total_revenue",
        "gross_profit",
        "net_income",
        "shares_outstanding",
        "timeframe",
    ]
    assert out.loc[0, "shares_outstanding"] == 55.0


def test_read_finance_json_projects_requested_valuation_columns(monkeypatch) -> None:
    monkeypatch.setattr(
        silver.delta_core,
        "load_delta",
        lambda *_args, **_kwargs: pytest.fail("valuation parsing should not read market data"),
    )

    out = silver._read_finance_json(
        json.dumps(
            {
                "schema_version": 2,
                "provider": "massive",
                "report_type": "valuation",
                "as_of": "2024-03-31",
                "market_cap": 1000.0,
                "pe_ratio": 20.0,
            }
        ).encode("utf-8"),
        ticker="AAPL",
        report_type="valuation",
    )

    expected_columns = ["Date", "Symbol", *silver.SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN["valuation"][2:]]
    assert list(out.columns) == expected_columns
    assert len(out) == 1
    assert out.loc[0, "market_cap"] == 1000.0
    assert out.loc[0, "pe_ratio"] == 20.0
    assert pd.isna(out.loc[0, "price_to_book"])


def test_read_finance_json_decodes_raw_statement_payloads_and_filters_timeframes() -> None:
    payload = {
        "status": "OK",
        "request_id": "req-1",
        "results": [
            {"period_end": "2024-03-31", "timeframe": "trailing_twelve_months", "total_assets": 999.0},
            {"period_end": "2024-03-31", "timeframe": "quarterly", "total_assets": 1000.0},
            {"period_end": "2024-03-31", "timeframe": "quarterly", "total_assets": 1001.0},
            {"period_end": "2024-03-31", "timeframe": "annual", "total_assets": 1200.0},
            {"period_end": "2023-12-31", "timeframe": "annual", "total_assets": 900.0},
        ],
    }

    out = silver._read_finance_json(
        json.dumps(payload).encode("utf-8"),
        ticker="AAPL",
        report_type="balance_sheet",
    )

    assert list(out["Date"].dt.strftime("%Y-%m-%d")) == ["2023-12-31", "2024-03-31", "2024-03-31"]
    assert list(out["timeframe"]) == ["annual", "annual", "quarterly"]
    assert list(out["total_assets"]) == [900.0, 1200.0, 1001.0]


def test_read_finance_json_decodes_raw_valuation_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        silver.delta_core,
        "load_delta",
        lambda *_args, **_kwargs: pytest.fail("valuation parsing should not read market data"),
    )

    out = silver._read_finance_json(
        json.dumps(
            {
                "status": "OK",
                "results": [
                    {
                        "date": "2024-03-30",
                        "market_cap": 900.0,
                        "price_to_earnings": 18.0,
                        "price_to_book": 4.5,
                        "current": 1.25,
                    },
                    {
                        "date": "2024-03-31",
                        "market_cap": 1000.0,
                        "price_to_earnings": 20.0,
                        "price_to_book": 5.0,
                        "current": 1.5,
                        "quick": 1.1,
                        "cash": 0.6,
                        "ev_to_ebitda": 12.0,
                        "free_cash_flow": 12345.0,
                    },
                ],
            }
        ).encode("utf-8"),
        ticker="AAPL",
        report_type="valuation",
    )

    expected_columns = ["Date", "Symbol", *silver.SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN["valuation"][2:]]
    assert list(out.columns) == expected_columns
    assert out.loc[1, "market_cap"] == 1000.0
    assert out.loc[1, "pe_ratio"] == 20.0
    assert out.loc[1, "price_to_book"] == 5.0
    assert out.loc[1, "current_ratio"] == 1.5
    assert out.loc[1, "quick_ratio"] == 1.1
    assert out.loc[1, "cash_ratio"] == 0.6
    assert out.loc[1, "ev_to_ebitda"] == 12.0
    assert out.loc[1, "free_cash_flow"] == 12345.0


def test_read_finance_json_keeps_last_valuation_row_for_duplicate_dates() -> None:
    out = silver._read_finance_json(
        json.dumps(
            {
                "status": "OK",
                "results": [
                    {"date": "2024-03-31", "market_cap": 900.0, "price_to_earnings": 18.0},
                    {"date": "2024-03-31", "market_cap": 1000.0, "price_to_earnings": 20.0},
                ],
            }
        ).encode("utf-8"),
        ticker="AAPL",
        report_type="valuation",
    )

    assert len(out) == 1
    assert out.loc[0, "market_cap"] == 1000.0
    assert out.loc[0, "pe_ratio"] == 20.0


def test_read_finance_json_rejects_unsupported_payload() -> None:
    with pytest.raises(ValueError, match="Unsupported finance payload schema"):
        silver._read_finance_json(
            json.dumps({"status": "OK", "payload": []}).encode("utf-8"),
            ticker="AAPL",
            report_type="balance_sheet",
        )


def test_resample_daily_ffill_preserves_distinct_statement_timeframes() -> None:
    source = pd.DataFrame(
        [
            {"Date": "2024-03-31", "Symbol": "AAPL", "timeframe": "annual", "total_assets": 1200.0},
            {"Date": "2024-03-31", "Symbol": "AAPL", "timeframe": "quarterly", "total_assets": 1000.0},
        ]
    )

    out = silver.resample_daily_ffill(source, extend_to=pd.Timestamp("2024-04-02"))

    assert set(out["timeframe"]) == {"annual", "quarterly"}
    assert len(out[out["Date"] == pd.Timestamp("2024-04-02")]) == 2
    assert sorted(out[out["Date"] == pd.Timestamp("2024-04-02")]["total_assets"].tolist()) == [1000.0, 1200.0]


def test_process_alpha26_bucket_blob_processes_valuation_rows_into_valuation_bucket(monkeypatch) -> None:
    blob_name = "finance-data/buckets/A.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": datetime(2026, 3, 4, 1, 0, tzinfo=timezone.utc),
    }
    bucket_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "report_type": "valuation",
                "payload_json": json.dumps(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "date": "2026-03-04",
                                "market_cap": 100.0,
                                "price_to_earnings": 10.0,
                            }
                        ],
                    }
                ),
            }
        ]
    )
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(
        silver.mdc,
        "read_raw_bytes",
        lambda _name, client=None: bucket_df.to_parquet(index=False),
    )
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: pd.DataFrame())

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "ok"
    assert results[0].silver_path == "finance-data/valuation/buckets/A"
    assert blob_name in watermarks


def test_process_alpha26_bucket_blob_skips_empty_valuation_rows(monkeypatch) -> None:
    blob_name = "finance-data/buckets/A.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": datetime(2026, 3, 4, 1, 0, tzinfo=timezone.utc),
    }
    bucket_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "report_type": "valuation",
                "payload_json": json.dumps({"status": "OK", "results": []}),
            }
        ]
    )
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(
        silver.mdc,
        "read_raw_bytes",
        lambda _name, client=None: bucket_df.to_parquet(index=False),
    )

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert blob_name in watermarks


def test_process_alpha26_bucket_blob_marks_empty_non_valuation_rows_as_no_data(monkeypatch) -> None:
    blob_name = "finance-data/buckets/A.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": datetime(2026, 3, 4, 1, 0, tzinfo=timezone.utc),
    }
    bucket_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "report_type": "balance_sheet",
                "payload_json": json.dumps({"status": "OK", "results": []}),
            }
        ]
    )
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(
        silver.mdc,
        "read_raw_bytes",
        lambda _name, client=None: bucket_df.to_parquet(index=False),
    )

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "no_data"
    assert "Empty finance payload" in str(results[0].error)
    assert blob_name in watermarks


def test_process_alpha26_bucket_blob_accepts_string_last_modified(monkeypatch) -> None:
    blob_name = "finance-data/buckets/A.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": "2026-03-04T01:00:00Z",
    }
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: pd.DataFrame())

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert watermarks[blob_name]["etag"] == "etag-a"
    assert watermarks[blob_name]["last_modified"] == "2026-03-04T01:00:00+00:00"


def test_silver_finance_main_parallel_aggregates_failures_and_updates_watermarks(monkeypatch):
    blobs = [
        {
            "name": "finance-data/buckets/O.parquet",
            "etag": "etag-ok",
            "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
        },
        {
            "name": "finance-data/buckets/S.parquet",
            "etag": "etag-skip",
            "last_modified": datetime(2026, 1, 31, 0, 1, tzinfo=timezone.utc),
        },
        {
            "name": "finance-data/buckets/F.parquet",
            "etag": "etag-fail",
            "last_modified": datetime(2026, 1, 31, 0, 2, tzinfo=timezone.utc),
        },
    ]

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: (list(blobs), 0))
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(
        silver,
        "_write_alpha26_finance_silver_buckets",
        lambda _frames, **_kwargs: (0, "system/silver-index/finance/latest.parquet", None),
    )

    initial_watermarks = {"preexisting": {"etag": "keep"}}
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: dict(initial_watermarks))
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)

    saved = {}

    def fake_save_watermarks(key, items):
        saved["key"] = key
        saved["items"] = dict(items)

    monkeypatch.setattr(silver, "save_watermarks", fake_save_watermarks)

    def fake_process_alpha26(
        *,
        candidate_blobs,
        desired_end,
        backfill_start=None,
        watermarks=None,
        persist=True,
        alpha26_bucket_frames=None,
        flush_state=None,
    ):
        del desired_end, backfill_start, persist, alpha26_bucket_frames, flush_state
        results = []
        for blob in candidate_blobs:
            name = str(blob.get("name", ""))
            if name.endswith("/O.parquet"):
                watermarks[name] = {
                    "etag": "etag-ok",
                    "last_modified": "2026-01-31T00:00:00+00:00",
                    "updated_at": "2026-01-31T00:00:01+00:00",
                }
                results.append(
                    silver.BlobProcessResult(
                        blob_name=name,
                        silver_path="finance-data/balance_sheet/buckets/O",
                        ticker="OK",
                        status="ok",
                        rows_written=7,
                    )
                )
                continue
            if name.endswith("/S.parquet"):
                results.append(
                    silver.BlobProcessResult(
                        blob_name=name,
                        silver_path="finance-data/cash_flow/buckets/S",
                        ticker="SKIP",
                        status="skipped",
                    )
                )
                continue
            results.append(
                    silver.BlobProcessResult(
                        blob_name=name,
                        silver_path="finance-data/cash_flow/buckets/F",
                        ticker="FAIL",
                        status="failed",
                        error="simulated failure",
                )
            )
        return results, 0.01

    monkeypatch.setattr(silver, "_process_alpha26_candidate_blobs", fake_process_alpha26)

    exit_code = silver.main()

    assert exit_code == 1
    assert saved["key"] == "bronze_finance_data"
    assert saved["items"]["preexisting"] == {"etag": "keep"}
    assert saved["items"]["finance-data/buckets/O.parquet"]["etag"] == "etag-ok"
    assert "finance-data/buckets/F.parquet" not in saved["items"]


def test_silver_finance_main_succeeds_with_no_data_skips_and_records_skipped_no_data(monkeypatch):
    blobs = [
        {
            "name": "finance-data/buckets/A.parquet",
            "etag": "etag-a",
            "last_modified": datetime(2026, 3, 22, 0, 0, tzinfo=timezone.utc),
        }
    ]
    log_lines: list[str] = []
    saved_last_success = {}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda message: log_lines.append(str(message)))
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: (list(blobs), 0))
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-03-22"))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda key, when=None, metadata=None: saved_last_success.update(
            {"key": key, "when": when, "metadata": metadata}
        ),
    )

    def _fake_process(*, candidate_blobs, desired_end, backfill_start=None, watermarks=None, **_kwargs):
        del desired_end, backfill_start, _kwargs
        out = []
        for blob in candidate_blobs:
            watermarks[blob["name"]] = {
                "etag": blob["etag"],
                "last_modified": blob["last_modified"].isoformat(),
                "updated_at": "2026-03-22T00:00:01+00:00",
            }
            out.append(
                silver.BlobProcessResult(
                    blob_name=blob["name"],
                    silver_path="finance-data/balance_sheet/buckets/A",
                    ticker="AAPL",
                    status="skipped",
                    error=f"Empty finance payload: {blob['name']}",
                    reason="no_data",
                )
            )
        return out, 0.01

    monkeypatch.setattr(silver, "_process_alpha26_candidate_blobs", _fake_process)

    exit_code = silver.main()

    assert exit_code == 0
    assert saved_last_success["key"] == "silver_finance_data"
    assert saved_last_success["metadata"]["skipped"] == 1
    assert saved_last_success["metadata"]["skipped_no_data"] == 1
    assert saved_last_success["metadata"]["failed"] == 0
    assert any("skippedNoData=1" in line and "failed=0" in line for line in log_lines)


def test_silver_finance_main_processes_a_single_listing_pass(monkeypatch):
    bucket_blob = {
        "name": "finance-data/buckets/A.parquet",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
    }
    saved_last_success = {}
    saved_watermarks = {}
    list_calls = {"count": 0}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(
        silver,
        "_list_alpha26_finance_bucket_candidates",
        lambda: list_calls.__setitem__("count", list_calls["count"] + 1) or ([dict(bucket_blob)], 0),
    )
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)

    def _fake_process_alpha26_candidate_blobs(
        *,
        candidate_blobs,
        desired_end,
        backfill_start=None,
        watermarks=None,
        flush_state=None,
        **_kwargs,
    ):
        del desired_end, backfill_start, watermarks, _kwargs
        assert flush_state is not None
        flush_state.staged_rows = 1
        flush_state.flush_count = 1
        flush_state.written_symbols = 1
        flush_state.index_path = "index"
        flush_state.column_count = 14
        return (
            [
                silver.BlobProcessResult(
                    blob_name=candidate_blobs[0]["name"],
                    silver_path="finance-data/cash_flow/buckets/A",
                    ticker="A",
                    status="ok",
                    rows_written=1,
                    watermark_signature={
                        "etag": "etag-a",
                        "last_modified": bucket_blob["last_modified"].isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            ],
            0.01,
        )

    monkeypatch.setattr(
        silver,
        "_process_alpha26_candidate_blobs",
        _fake_process_alpha26_candidate_blobs,
    )
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda key, when=None, metadata=None: saved_last_success.update(
            {"key": key, "when": when, "metadata": metadata}
        ),
    )
    monkeypatch.setattr(
        silver,
        "save_watermarks",
        lambda key, items: saved_watermarks.update({"key": key, "items": dict(items)}),
    )

    exit_code = silver.main()
    assert exit_code == 0
    assert list_calls["count"] == 1
    assert saved_last_success["key"] == "silver_finance_data"
    assert saved_last_success["metadata"]["total_blobs"] == 1
    assert saved_last_success["metadata"]["candidates"] == 1
    assert saved_last_success["metadata"]["attempts"] == 1
    assert saved_last_success["metadata"]["skipped_checkpoint"] == 0
    assert saved_last_success["metadata"]["column_count"] == 14
    assert saved_watermarks["key"] == "bronze_finance_data"
    assert "source" not in saved_last_success["metadata"]
    assert "manifest_run_id" not in saved_last_success["metadata"]
    assert "manifest_path" not in saved_last_success["metadata"]
    assert "catchup_passes" not in saved_last_success["metadata"]
    assert "lag_candidate_count" not in saved_last_success["metadata"]
    assert "new_blobs_discovered_after_first_pass" not in saved_last_success["metadata"]


def test_write_alpha26_finance_silver_buckets_replaces_legacy_schema_with_contract(monkeypatch):
    existing_cols = ["date", "symbol", "shares_outstanding", "timeframe"]
    target_path = "finance-data/balance_sheet/buckets/A"
    captured: dict[str, object] = {}

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet",))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        silver.delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(
        df: pd.DataFrame,
        _container: str,
        path: str,
        mode: str = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode
        captured["schema_mode"] = schema_mode

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    assert captured["schema_mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == [
        "date",
        "symbol",
        "long_term_debt",
        "total_assets",
        "current_assets",
        "current_liabilities",
        "timeframe",
    ]


def test_write_alpha26_finance_silver_buckets_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = "finance-data/balance_sheet/buckets/C"
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet",))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("C",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", _fake_get_schema)

    def _fake_store(
        df: pd.DataFrame,
        _container: str,
        path: str,
        mode: str = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["path"] = path
        captured["mode"] = mode
        captured["schema_mode"] = schema_mode
        captured["df"] = df.copy()

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_write_alpha26_finance_silver_buckets_writes_sub_domain_indexes(monkeypatch):
    balance_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AAPL"]})
    cash_flow_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["MSFT"]})
    bucket_frames = {
        ("balance_sheet", "A"): [balance_df],
        ("cash_flow", "A"): [cash_flow_df],
    }
    index_calls: list[dict] = []

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet", "cash_flow"))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: ["date", "symbol"])
    monkeypatch.setattr(silver.delta_core, "store_delta", lambda *_args, **_kwargs: None)

    def _fake_index(**kwargs):
        index_calls.append(dict(kwargs))
        return "index"

    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", _fake_index)

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets(bucket_frames)
    assert written_symbols == 2
    assert index_path == "index"
    assert len(index_calls) == 3
    aggregate = [call for call in index_calls if call.get("sub_domain") is None][0]
    assert aggregate["symbol_to_bucket"] == {"AAPL": "A", "MSFT": "A"}
    sub_domains = sorted(call.get("sub_domain") for call in index_calls if call.get("sub_domain"))
    assert sub_domains == ["balance_sheet", "cash_flow"]


def test_write_alpha26_finance_silver_buckets_partial_update_preserves_untouched_sub_domains(monkeypatch):
    balance_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})
    bucket_frames = {
        ("balance_sheet", "A"): [balance_df],
    }
    index_calls: list[dict] = []

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet", "cash_flow"))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: ["date", "symbol"])
    monkeypatch.setattr(silver.delta_core, "store_delta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "bucket": ["A", "M"],
                "sub_domain": ["balance_sheet", "cash_flow"],
            }
        ),
    )

    def _fake_index(**kwargs):
        index_calls.append(dict(kwargs))
        return "index"

    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", _fake_index)
    monkeypatch.setattr(
        silver.domain_artifacts,
        "load_domain_artifact",
        lambda **kwargs: {"subDomain": kwargs.get("sub_domain"), "symbolCount": 1},
    )

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets(
        bucket_frames,
        touched_bucket_keys={("balance_sheet", "A")},
    )

    assert written_symbols == 2
    assert index_path == "index"
    aggregate = next(call for call in index_calls if call.get("sub_domain") is None)
    balance = next(call for call in index_calls if call.get("sub_domain") == "balance_sheet")
    assert aggregate["symbol_to_bucket"] == {"AMZN": "A", "MSFT": "M"}
    assert balance["symbol_to_bucket"] == {"AMZN": "A"}


def test_write_alpha26_finance_silver_buckets_recovers_missing_shared_index_from_persisted_tables(monkeypatch):
    balance_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})
    bucket_frames = {
        ("balance_sheet", "A"): [balance_df],
    }
    index_calls: list[dict] = []
    stored_paths: list[str] = []
    flush_state = silver._FinanceAlpha26FlushState()

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet", "cash_flow"))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(silver.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)

    def _fake_load_delta(_container: str, path: str, version=None, columns=None, **_kwargs):
        del version, _kwargs
        assert columns == ["symbol"]
        if path == "finance-data/balance_sheet/buckets/A":
            return pd.DataFrame({"symbol": ["AAPL"]})
        if path == "finance-data/cash_flow/buckets/M":
            return pd.DataFrame({"symbol": ["MSFT"]})
        return None

    def _fake_store(
        _df: pd.DataFrame,
        _container: str,
        path: str,
        mode: str = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        assert mode == "overwrite"
        assert schema_mode == "overwrite"
        stored_paths.append(path)

    def _fake_index(**kwargs):
        index_calls.append(dict(kwargs))
        sub_domain = kwargs.get("sub_domain")
        return "index-root" if sub_domain is None else f"index-{sub_domain}"

    monkeypatch.setattr(silver.delta_core, "load_delta", _fake_load_delta)
    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", _fake_index)
    monkeypatch.setattr(silver.domain_artifacts, "write_bucket_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(silver.domain_artifacts, "write_domain_artifact", lambda **_kwargs: {})
    monkeypatch.setattr(silver.domain_artifacts, "load_domain_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(silver.domain_artifacts, "extract_column_count", lambda _payload: 14)

    written_symbols, index_path, column_count = silver._write_alpha26_finance_silver_buckets(
        bucket_frames,
        touched_bucket_keys={("balance_sheet", "A")},
        recovery_state=flush_state,
    )

    assert written_symbols == 2
    assert index_path == "index-cash_flow"
    assert column_count == 14
    assert stored_paths == ["finance-data/balance_sheet/buckets/A"]
    aggregate = next(call for call in index_calls if call.get("sub_domain") is None)
    assert aggregate["symbol_to_bucket"] == {"AMZN": "A", "MSFT": "M"}
    assert sorted(call.get("sub_domain") for call in index_calls if call.get("sub_domain")) == [
        "balance_sheet",
        "cash_flow",
    ]
    assert flush_state.cached_symbol_maps == {
        "balance_sheet": {"AMZN": "A"},
        "cash_flow": {"MSFT": "M"},
    }
    assert flush_state.index_recovery_source == "persisted-silver-buckets"


def test_write_alpha26_finance_silver_buckets_bootstraps_missing_shared_index_from_staged_frames(monkeypatch):
    balance_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AAPL"]})
    bucket_frames = {
        ("balance_sheet", "A"): [balance_df],
    }
    index_calls: list[dict] = []
    stored_paths: list[str] = []
    flush_state = silver._FinanceAlpha26FlushState()

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet", "cash_flow"))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(silver.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: None)

    def _fake_store(
        _df: pd.DataFrame,
        _container: str,
        path: str,
        mode: str = "overwrite",
        schema_mode: str | None = None,
    ) -> None:
        assert mode == "overwrite"
        assert schema_mode == "overwrite"
        stored_paths.append(path)

    def _fake_index(**kwargs):
        index_calls.append(dict(kwargs))
        sub_domain = kwargs.get("sub_domain")
        return "index-root" if sub_domain is None else f"index-{sub_domain}"

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", _fake_index)
    monkeypatch.setattr(silver.domain_artifacts, "write_bucket_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(silver.domain_artifacts, "write_domain_artifact", lambda **_kwargs: {})
    monkeypatch.setattr(silver.domain_artifacts, "load_domain_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(silver.domain_artifacts, "extract_column_count", lambda _payload: 14)

    written_symbols, index_path, column_count = silver._write_alpha26_finance_silver_buckets(
        bucket_frames,
        touched_bucket_keys={("balance_sheet", "A")},
        recovery_state=flush_state,
    )

    assert written_symbols == 1
    assert index_path == "index-cash_flow"
    assert column_count == 14
    assert stored_paths == ["finance-data/balance_sheet/buckets/A"]
    aggregate = next(call for call in index_calls if call.get("sub_domain") is None)
    assert aggregate["symbol_to_bucket"] == {"AAPL": "A"}
    assert sorted(call.get("sub_domain") for call in index_calls if call.get("sub_domain")) == [
        "balance_sheet",
        "cash_flow",
    ]
    assert flush_state.cached_symbol_maps == {
        "balance_sheet": {"AAPL": "A"},
        "cash_flow": {},
    }
    assert flush_state.index_recovery_source == "staged-frames"


def test_process_alpha26_bucket_blob_does_not_skip_when_signature_matches_watermark(monkeypatch):
    blob_name = "finance-data/runs/run-123/buckets/M.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-m",
        "last_modified": datetime(2026, 3, 4, 1, 0, tzinfo=timezone.utc),
    }
    watermark_key = "finance-data/buckets/M.parquet"
    watermarks = {
        watermark_key: {
            "etag": "etag-m",
            "last_modified": "2026-03-04T01:00:00+00:00",
        }
    }
    bucket_df = pd.DataFrame(
        [
            {
                "symbol": "MSFT",
                "report_type": "balance_sheet",
                "payload_json": json.dumps(
                    {
                        "schema_version": 2,
                        "provider": "massive",
                        "report_type": "balance_sheet",
                        "rows": [{"date": "2024-01-01", "timeframe": "quarterly", "total_assets": 100.0}],
                    }
                ),
            }
        ]
    )
    captured_tickers: list[str] = []

    monkeypatch.setattr(
        silver.mdc,
        "read_raw_bytes",
        lambda _name, client=None: bucket_df.to_parquet(index=False),
    )
    monkeypatch.setattr(
        silver,
        "_read_finance_json",
        lambda _raw, ticker, report_type: pd.DataFrame({"Date": [pd.Timestamp("2024-01-01")], "Symbol": [ticker]}),
    )

    def _fake_process_finance_frame(**kwargs):
        captured_tickers.append(str(kwargs["ticker"]))
        return silver.BlobProcessResult(
            blob_name=kwargs["blob_name"],
            silver_path=kwargs["silver_path"],
            ticker=kwargs["ticker"],
            status="ok",
            rows_written=1,
        )

    monkeypatch.setattr(silver, "_process_finance_frame", _fake_process_finance_frame)

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "ok"
    assert captured_tickers == ["MSFT"]
    assert watermark_key in watermarks
    assert watermarks[watermark_key]["etag"] == "etag-m"
