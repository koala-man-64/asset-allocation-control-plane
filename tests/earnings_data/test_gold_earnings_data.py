import pytest
import pandas as pd
from asset_allocation_runtime_common.market_data import core as core_module
from asset_allocation_runtime_common.market_data import delta_core
from asset_allocation_runtime_common.market_data.pipeline import DataPaths
from asset_allocation_runtime_common.foundation.postgres import PostgresError
from tasks.earnings_data import gold_earnings_data as gold
from tasks.common.gold_output_contracts import GOLD_EARNINGS_OUTPUT_COLUMNS


def _capture_log_messages(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []
    monkeypatch.setattr(core_module, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_warning", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_error", lambda msg: messages.append(str(msg)))
    return messages


def test_build_job_config_reads_required_containers(monkeypatch):
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")

    cfg = gold._build_job_config()

    assert cfg.silver_container == "silver"
    assert cfg.gold_container == "gold"


def test_build_job_config_requires_silver_container(monkeypatch):
    monkeypatch.delenv("AZURE_CONTAINER_SILVER", raising=False)
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")

    with pytest.raises(ValueError, match="AZURE_CONTAINER_SILVER"):
        gold._build_job_config()


def test_build_job_config_requires_gold_container(monkeypatch):
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.delenv("AZURE_CONTAINER_GOLD", raising=False)

    with pytest.raises(ValueError, match="AZURE_CONTAINER_GOLD"):
        gold._build_job_config()


def test_compute_features():
    """
    Verifies compute_features:
    1. Snake-casing columns.
    2. Calculating surprise %.
    3. Resampling to daily with ffill.
    4. Identifying earnings days.
    """
    df_raw = pd.DataFrame({
        "Date": ["2023-01-01", "2023-04-01"],
        "Symbol": ["TEST", "TEST"],
        "Reported EPS": [1.1, 1.2],
        "EPS Estimate": [1.0, 1.0],
    })
    
    res = gold.compute_features(df_raw)
    
    # Check Result
    # 1. Columns snake_cased
    assert "reported_eps" in res.columns
    assert "surprise_pct" in res.columns
    
    # 2. Daily resampling
    # 2023-01-01 to 2023-04-01 is ~90 days
    assert len(res) >= 90 
    
    # 3. Validation of a specific date
    row_jan1 = res[res["date"] == pd.Timestamp("2023-01-01")].iloc[0]
    assert row_jan1["is_earnings_day"] == 1.0
    assert row_jan1["surprise_pct"] == (1.1 - 1.0) / 1.0 # 0.1
    
    row_jan2 = res[res["date"] == pd.Timestamp("2023-01-02")].iloc[0]
    assert row_jan2["is_earnings_day"] == 0.0
    # Should be ffilled
    assert row_jan2["reported_eps"] == 1.1

def test_compute_features_missing_cols():
    df_raw = pd.DataFrame({"Date": []})
    with pytest.raises(ValueError, match="Missing required columns"):
        gold.compute_features(df_raw)


def test_compute_features_adds_future_earnings_fields_without_polluting_actual_metrics(monkeypatch):
    monkeypatch.setattr(gold, "_utc_today", lambda: pd.Timestamp("2026-02-20"))
    df_raw = pd.DataFrame(
        {
            "date": ["2025-12-31", "2026-03-01"],
            "report_date": ["2026-01-30", "2026-03-01"],
            "fiscal_date_ending": ["2025-12-31", "2025-12-31"],
            "symbol": ["TEST", "TEST"],
            "reported_eps": [1.2, None],
            "eps_estimate": [1.0, 1.3],
            "record_type": ["actual", "scheduled"],
            "calendar_time_of_day": [None, "post-market"],
        }
    )

    out = gold.compute_features(df_raw)

    last = out.iloc[-1]
    pre_event = out.loc[out["date"] == pd.Timestamp("2026-02-28")].iloc[0]
    scheduled_day = out.loc[out["date"] == pd.Timestamp("2026-03-01")].iloc[0]

    assert last["date"] == pd.Timestamp("2026-03-01")
    assert pre_event["next_earnings_date"] == pd.Timestamp("2026-03-01")
    assert int(pre_event["days_until_next_earnings"]) == 1
    assert int(pre_event["has_upcoming_earnings"]) == 1
    assert pre_event["next_earnings_time_of_day"] == "post-market"
    assert scheduled_day["is_earnings_day"] == 0
    assert scheduled_day["is_scheduled_earnings_day"] == 1
    assert scheduled_day["last_earnings_date"] == pd.Timestamp("2025-12-31")


def test_compute_features_supports_scheduled_only_symbols(monkeypatch):
    monkeypatch.setattr(gold, "_utc_today", lambda: pd.Timestamp("2026-02-20"))
    df_raw = pd.DataFrame(
        {
            "date": ["2026-03-05"],
            "report_date": ["2026-03-05"],
            "fiscal_date_ending": ["2025-12-31"],
            "symbol": ["TEST"],
            "eps_estimate": [1.8],
            "record_type": ["scheduled"],
            "calendar_time_of_day": ["pre-market"],
        }
    )

    out = gold.compute_features(df_raw)

    assert out["date"].min() == pd.Timestamp("2026-02-20")
    assert out["date"].max() == pd.Timestamp("2026-03-05")
    first = out.iloc[0]
    assert pd.isna(first["reported_eps"])
    assert pd.isna(first["last_earnings_date"])
    assert int(first["has_upcoming_earnings"]) == 1
    assert first["next_earnings_time_of_day"] == "pre-market"


def test_run_alpha26_earnings_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_earnings_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core, "get_delta_last_commit", lambda *_args, **_kwargs: None)

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(_df: pd.DataFrame, _container: str, _path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["mode"] = mode

    monkeypatch.setattr(delta_core, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(delta_core, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_earnings_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 0
    assert skipped_unchanged == 0
    assert skipped_missing_source == 1
    assert failed == 0
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_run_alpha26_earnings_gold_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_gold_earnings_bucket_path("A")
    existing_cols = ["date", "symbol", "surprise"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core, "get_delta_last_commit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(delta_core, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_earnings_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert skipped_missing_source == 1
    assert failed == 0
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 1
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == existing_cols


def test_run_alpha26_earnings_gold_projects_contract_before_write(monkeypatch):
    target_path = DataPaths.get_gold_earnings_bucket_path("A")
    captured: dict[str, object] = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_earnings_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["AAPL"],
                "record_type": ["actual"],
            }
        )
        if path == DataPaths.get_silver_earnings_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda _df: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["aapl"],
                "reported_eps": [1.23],
                "eps_estimate": [1.11],
                "calendar_time_of_day": [pd.NA],
                "calendar_currency": [pd.NA],
                "next_earnings_time_of_day": ["post-market"],
                "has_upcoming_earnings": [1],
                "is_scheduled_earnings_day": [0],
            }
        ),
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(delta_core, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_earnings_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 0
    assert watermarks_dirty is True
    assert alpha26_symbols == 1
    assert index_path == "index"
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    assert list(captured["df"].columns) == list(GOLD_EARNINGS_OUTPUT_COLUMNS)
    assert "calendar_time_of_day" not in captured["df"].columns
    assert "calendar_currency" not in captured["df"].columns
    assert captured["df"].loc[0, "symbol"] == "AAPL"


def test_run_alpha26_earnings_gold_blocks_publication_when_bucket_fails(monkeypatch):
    index_calls = {"count": 0}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: index_calls.__setitem__("count", int(index_calls["count"]) + 1) or "index",
    )
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_earnings_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["AAPL"],
                "record_type": ["actual"],
            }
        )
        if path == DataPaths.get_silver_earnings_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(gold, "compute_features", lambda _df: (_ for _ in ()).throw(ValueError("boom")))
    monkeypatch.setattr(delta_core, "store_delta", lambda *_args, **_kwargs: None)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_earnings_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 0
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path is None
    assert index_calls["count"] == 0


def test_run_alpha26_earnings_gold_logs_structured_failure_counter_for_postgres_sync(monkeypatch):
    messages = _capture_log_messages(monkeypatch)

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.domain_artifacts, "write_bucket_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_earnings_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["AAPL"],
                "record_type": ["actual"],
            }
        )
        if path == DataPaths.get_silver_earnings_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda _df: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["AAPL"],
                "reported_eps": [1.23],
                "eps_estimate": [1.11],
                "calendar_time_of_day": [pd.NA],
                "calendar_currency": [pd.NA],
                "next_earnings_time_of_day": ["post-market"],
                "has_upcoming_earnings": [1],
                "is_scheduled_earnings_day": [0],
            }
        ),
    )
    monkeypatch.setattr(delta_core, "store_delta", lambda *_args, **_kwargs: None)

    failure = PostgresError("Gold Postgres sync failed")
    setattr(failure, "failure_stage", "delete_missing")
    setattr(failure, "failure_category", "read_only_transaction")
    setattr(failure, "failure_error_class", "ReadOnlySqlTransaction")
    setattr(failure, "failure_transient", True)
    monkeypatch.setattr(gold, "sync_gold_bucket", lambda **_kwargs: (_ for _ in ()).throw(failure))

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_earnings_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 0
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert alpha26_symbols == 0
    assert index_path is None
    assert any(
        "gold_earnings_failure_counter stage=bucket_write failure_source=delete_missing "
        "failure_category=read_only_transaction bucket=A ticker=n/a "
        "exception_type=ReadOnlySqlTransaction transient=true counter_value=1 "
        "failed_symbols=0 failed_buckets=1 failed_finalization=0" in message
        for message in messages
    )


def test_run_alpha26_earnings_gold_uses_checkpoint_helper_and_final_log_contract(monkeypatch):
    messages = _capture_log_messages(monkeypatch)
    domain_artifact_calls: list[dict[str, object]] = []
    saved_watermarks: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        gold.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: domain_artifact_calls.append(dict(kwargs)) or {"artifactPath": "earnings/_metadata/domain.json"},
    )
    monkeypatch.setattr(gold.domain_artifacts, "write_bucket_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(gold, "save_watermarks", lambda key, items: saved_watermarks.append((key, dict(items))))
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_earnings_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["AAPL"],
                "record_type": ["actual"],
            }
        )
        if path == DataPaths.get_silver_earnings_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda _df: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-01")],
                "symbol": ["AAPL"],
                "reported_eps": [1.23],
                "eps_estimate": [1.11],
                "calendar_time_of_day": [pd.NA],
                "calendar_currency": [pd.NA],
                "next_earnings_time_of_day": ["post-market"],
                "has_upcoming_earnings": [1],
                "is_scheduled_earnings_day": [0],
            }
        ),
    )
    monkeypatch.setattr(delta_core, "store_delta", lambda *_args, **_kwargs: None)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_earnings_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 0
    assert watermarks_dirty is True
    assert alpha26_symbols == 1
    assert index_path == "index"
    assert len(saved_watermarks) == 1
    assert saved_watermarks[0][0] == "gold_earnings_features"
    assert saved_watermarks[0][1]["bucket::A"]["silver_last_commit"] == 1
    assert len(domain_artifact_calls) == 1
    assert domain_artifact_calls[0]["symbol_index_path"] == "index"
    assert domain_artifact_calls[0]["symbol_count_override"] == 1
    assert any(
        "gold_checkpoint_aggregate_publication layer=gold domain=earnings bucket=A status=published" in message
        for message in messages
    )
    assert any("artifact_status=skipped" in message for message in messages)
    assert any(
        "artifact_publication_status layer=gold domain=earnings status=published reason=none "
        "failure_mode=none buckets_ok=1 failed=0 failed_symbols=0 failed_buckets=0 "
        "failed_finalization=0 processed=1 skipped_unchanged=0 skipped_missing_source=0" in message
        for message in messages
    )


def test_main_runs_earnings_reconciliation_and_persists_watermarks(monkeypatch):
    reconciliation_calls: list[dict[str, str]] = []
    saved_watermarks: list[tuple[tuple, dict]] = []

    monkeypatch.setattr(core_module, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(gold.layer_bucketing, "gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (None, None))
    monkeypatch.setattr(gold, "load_watermarks", lambda _name: {"bucket::A": {"silver_last_commit": 1}})
    monkeypatch.setattr(
        gold,
        "_build_job_config",
        lambda: gold.FeatureJobConfig(
            silver_container="silver",
            gold_container="gold",
        ),
    )
    monkeypatch.setattr(
        gold,
        "_run_alpha26_earnings_gold",
        lambda **_kwargs: (1, 0, 0, 0, True, 1, "system/gold-index/earnings/latest.parquet"),
    )

    def _run_reconciliation(*, silver_container: str, gold_container: str):
        reconciliation_calls.append(
            {
                "silver_container": silver_container,
                "gold_container": gold_container,
            }
        )
        return 2, 3

    monkeypatch.setattr(gold, "_run_earnings_reconciliation", _run_reconciliation)
    monkeypatch.setattr(
        gold,
        "save_watermarks",
        lambda *args, **kwargs: saved_watermarks.append((args, kwargs)),
    )
    monkeypatch.setattr(core_module, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_error", lambda *_args, **_kwargs: None)

    assert gold.main() == 0
    assert reconciliation_calls == [{"silver_container": "silver", "gold_container": "gold"}]
    assert len(saved_watermarks) == 1


def test_main_fails_closed_when_earnings_reconciliation_fails(monkeypatch):
    save_watermarks_calls = {"count": 0}

    monkeypatch.setattr(core_module, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(gold.layer_bucketing, "gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (None, None))
    monkeypatch.setattr(gold, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(
        gold,
        "_build_job_config",
        lambda: gold.FeatureJobConfig(
            silver_container="silver",
            gold_container="gold",
        ),
    )
    monkeypatch.setattr(
        gold,
        "_run_alpha26_earnings_gold",
        lambda **_kwargs: (1, 0, 0, 0, True, 1, "system/gold-index/earnings/latest.parquet"),
    )
    monkeypatch.setattr(
        gold,
        "_run_earnings_reconciliation",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(
        gold,
        "save_watermarks",
        lambda *_args, **_kwargs: save_watermarks_calls.__setitem__("count", save_watermarks_calls["count"] + 1),
    )
    monkeypatch.setattr(core_module, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_error", lambda *_args, **_kwargs: None)

    assert gold.main() == 1
    assert save_watermarks_calls["count"] == 0
