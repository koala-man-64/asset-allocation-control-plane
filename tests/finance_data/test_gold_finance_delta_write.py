import pandas as pd
import pytest

from asset_allocation_runtime_common.market_data import core as core_module
from asset_allocation_runtime_common.market_data import delta_core
from asset_allocation_runtime_common.foundation.postgres import PostgresError
from asset_allocation_runtime_common.market_data.pipeline import DataPaths
from tasks.finance_data import gold_finance_data

EXPECTED_GOLD_FINANCE_COLUMNS = list(gold_finance_data._GOLD_FINANCE_PIOTROSKI_COLUMNS)
EXPECTED_GOLD_FINANCE_FLOAT_COLUMNS = list(gold_finance_data._GOLD_FINANCE_FLOAT_COLUMNS)
EXPECTED_GOLD_FINANCE_INT_COLUMNS = list(gold_finance_data._GOLD_FINANCE_PIOTROSKI_INTEGER_COLUMNS)


def test_build_job_config_reads_required_containers(monkeypatch):
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")
    cfg = gold_finance_data._build_job_config()
    assert cfg.silver_container == "silver"
    assert cfg.gold_container == "gold"


def test_build_job_config_requires_containers(monkeypatch):
    monkeypatch.delenv("AZURE_CONTAINER_SILVER", raising=False)
    monkeypatch.delenv("AZURE_CONTAINER_GOLD", raising=False)
    with pytest.raises(ValueError, match="AZURE_CONTAINER_SILVER"):
        gold_finance_data._build_job_config()


def test_run_alpha26_finance_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_finance_alpha26_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
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
    ) = gold_finance_data._run_alpha26_finance_gold(
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


def test_run_alpha26_finance_gold_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_gold_finance_alpha26_bucket_path("A")
    existing_cols = EXPECTED_GOLD_FINANCE_COLUMNS
    captured: dict[str, object] = {"store_calls": 0, "paths": [], "frames": []}

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == target_path else None,
    )
    monkeypatch.setattr(
        delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {column: pd.Series(dtype="float64") for column in EXPECTED_GOLD_FINANCE_FLOAT_COLUMNS}
            | {column: pd.Series(dtype="Int64") for column in EXPECTED_GOLD_FINANCE_INT_COLUMNS}
            | {
                "date": pd.Series(dtype="datetime64[ns]"),
                "symbol": pd.Series(dtype="string"),
            }
        )
        if path == target_path
        else pd.DataFrame(),
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["paths"] = [*list(captured["paths"]), path]
        captured["frames"] = [*list(captured["frames"]), df.copy()]
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
    ) = gold_finance_data._run_alpha26_finance_gold(
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
    assert captured["paths"] == [target_path]
    assert captured["mode"] == "overwrite"
    for df_written in captured["frames"]:
        assert isinstance(df_written, pd.DataFrame)
        assert df_written.empty
        assert list(df_written.columns) == EXPECTED_GOLD_FINANCE_COLUMNS


def test_project_gold_finance_piotroski_frame_limits_output_schema() -> None:
    projected = gold_finance_data._project_gold_finance_piotroski_frame(
        pd.DataFrame(
            [
                {
                    "date": "2024-01-01",
                    "symbol": "AAPL",
                    "market_cap": 1_000_000.0,
                    "pe_ratio": 20.0,
                    "price_to_book": 5.0,
                    "current_ratio": 1.5,
                    "piotroski_roa_pos": 1,
                    "piotroski_cfo_pos": 1,
                    "piotroski_delta_roa_pos": 1,
                    "piotroski_accruals_pos": 1,
                    "piotroski_leverage_decrease": 1,
                    "piotroski_liquidity_increase": 1,
                    "piotroski_no_new_shares": 1,
                    "piotroski_gross_margin_increase": 1,
                    "piotroski_asset_turnover_increase": 0,
                    "piotroski_f_score": 8,
                    "shares_outstanding": 100,
                }
            ]
        )
    )

    assert list(projected.columns) == EXPECTED_GOLD_FINANCE_COLUMNS
    assert projected.loc[0, "market_cap"] == 1_000_000.0
    assert projected.loc[0, "pe_ratio"] == 20.0
    assert projected.loc[0, "price_to_book"] == 5.0
    assert projected.loc[0, "current_ratio"] == 1.5
    assert projected.loc[0, "piotroski_f_score"] == 8
    assert "shares_outstanding" not in projected.columns


def test_run_alpha26_finance_gold_projects_optional_valuation_metrics(monkeypatch):
    target_path = DataPaths.get_gold_finance_alpha26_bucket_path("A")
    captured: dict[str, object] = {}

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(gold_finance_data, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if "finance-data/" in path else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)

    date_value = pd.Timestamp("2024-01-01")
    ticker = "AAPL"
    income_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "total_revenue": [100.0],
            "gross_profit": [40.0],
            "net_income": [10.0],
            "shares_outstanding": [100.0],
        }
    )
    balance_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "long_term_debt": [250.0],
            "total_assets": [1_000.0],
            "current_assets": [500.0],
            "current_liabilities": [250.0],
        }
    )
    cashflow_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "operating_cash_flow": [25.0],
        }
    )
    valuation_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "market_cap": [1_000_000.0],
            "pe_ratio": [20.0],
            "price_to_book": [5.0],
            "current_ratio": [1.4],
        }
    )

    def _fake_load_delta(_container: str, path: str):
        if "income_statement" in path:
            return income_df
        if "balance_sheet" in path:
            return balance_df
        if "cash_flow" in path:
            return cashflow_df
        if "valuation" in path:
            return valuation_df
        return pd.DataFrame()

    monkeypatch.setattr(delta_core, "load_delta", _fake_load_delta)

    def _fake_compute_features(merged: pd.DataFrame) -> pd.DataFrame:
        captured["merged"] = merged.copy()
        return pd.DataFrame(
            [
                {
                    "date": date_value,
                    "symbol": ticker,
                    "market_cap": 1_000_000.0,
                    "pe_ratio": 20.0,
                    "price_to_book": 5.0,
                    "current_ratio": 1.4,
                    "piotroski_roa_pos": 1,
                    "piotroski_cfo_pos": 1,
                    "piotroski_delta_roa_pos": 1,
                    "piotroski_accruals_pos": 1,
                    "piotroski_leverage_decrease": 1,
                    "piotroski_liquidity_increase": 1,
                    "piotroski_no_new_shares": 1,
                    "piotroski_gross_margin_increase": 1,
                    "piotroski_asset_turnover_increase": 1,
                    "piotroski_f_score": 9,
                }
            ]
        )

    monkeypatch.setattr(gold_finance_data, "compute_features", _fake_compute_features)

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["path"] = path
        captured["mode"] = mode
        captured["df"] = df.copy()

    monkeypatch.setattr(delta_core, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold_finance_data._run_alpha26_finance_gold(
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
    assert "market_cap" in list(captured["merged"].columns)
    assert "pe_ratio" in list(captured["merged"].columns)
    assert "price_to_book" in list(captured["merged"].columns)
    assert "current_ratio" in list(captured["merged"].columns)
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    assert list(captured["df"].columns) == EXPECTED_GOLD_FINANCE_COLUMNS
    assert captured["df"].loc[0, "market_cap"] == 1_000_000.0
    assert captured["df"].loc[0, "pe_ratio"] == 20.0
    assert captured["df"].loc[0, "price_to_book"] == 5.0
    assert captured["df"].loc[0, "current_ratio"] == 1.4


def test_run_alpha26_finance_gold_checkpoint_persists_index_and_defers_root_artifact(monkeypatch):
    messages: list[str] = []
    domain_artifact_calls: list[dict[str, object]] = []
    saved_watermarks: list[tuple[str, dict[str, object]]] = []
    index_calls = {"count": 0}
    date_value = pd.Timestamp("2024-01-01")
    ticker = "AAPL"

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())

    def _fake_write_layer_symbol_index(**_kwargs):
        index_calls["count"] += 1
        return "system/gold-index/finance/latest.parquet"

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "write_layer_symbol_index", _fake_write_layer_symbol_index)
    monkeypatch.setattr(gold_finance_data, "resolve_postgres_dsn", lambda: None)
    monkeypatch.setattr(
        gold_finance_data.domain_artifacts,
        "load_domain_artifact",
        lambda **_kwargs: {"totalBytes": 1024, "fileCount": 9},
    )
    monkeypatch.setattr(
        gold_finance_data.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: domain_artifact_calls.append(dict(kwargs)) or {"artifactPath": "finance/_metadata/domain.json"},
    )
    monkeypatch.setattr(gold_finance_data.domain_artifacts, "write_bucket_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(
        gold_finance_data,
        "save_watermarks",
        lambda key, items: saved_watermarks.append((key, dict(items))),
    )
    monkeypatch.setattr(core_module, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_warning", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_error", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if "finance-data/" in path else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)

    income_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "total_revenue": [100.0],
            "gross_profit": [40.0],
            "net_income": [10.0],
            "shares_outstanding": [100.0],
        }
    )
    balance_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "long_term_debt": [250.0],
            "total_assets": [1_000.0],
            "current_assets": [500.0],
            "current_liabilities": [250.0],
        }
    )
    cashflow_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "operating_cash_flow": [25.0],
        }
    )
    valuation_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "market_cap": [1_000_000.0],
            "pe_ratio": [20.0],
            "price_to_book": [5.0],
            "current_ratio": [1.4],
        }
    )

    def _fake_load_delta(_container: str, path: str):
        if "income_statement" in path:
            return income_df
        if "balance_sheet" in path:
            return balance_df
        if "cash_flow" in path:
            return cashflow_df
        if "valuation" in path:
            return valuation_df
        return pd.DataFrame()

    monkeypatch.setattr(delta_core, "load_delta", _fake_load_delta)
    monkeypatch.setattr(
        gold_finance_data,
        "compute_features",
        lambda _merged: pd.DataFrame(
            [
                {
                    "date": date_value,
                    "symbol": ticker,
                    "market_cap": 1_000_000.0,
                    "pe_ratio": 20.0,
                    "price_to_book": 5.0,
                    "current_ratio": 1.4,
                    "piotroski_roa_pos": 1,
                    "piotroski_cfo_pos": 1,
                    "piotroski_delta_roa_pos": 1,
                    "piotroski_accruals_pos": 1,
                    "piotroski_leverage_decrease": 1,
                    "piotroski_liquidity_increase": 1,
                    "piotroski_no_new_shares": 1,
                    "piotroski_gross_margin_increase": 1,
                    "piotroski_asset_turnover_increase": 1,
                    "piotroski_f_score": 9,
                }
            ]
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
    ) = gold_finance_data._run_alpha26_finance_gold(
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
    assert index_path == "system/gold-index/finance/latest.parquet"
    assert index_calls["count"] == 1
    assert len(saved_watermarks) == 1
    assert saved_watermarks[0][0] == "gold_finance_features"
    assert saved_watermarks[0][1]["bucket::A"]["silver_last_commit"] == 1
    assert len(domain_artifact_calls) == 1
    assert domain_artifact_calls[0]["symbol_count_override"] == 1
    assert domain_artifact_calls[0]["symbol_index_path"] == "system/gold-index/finance/latest.parquet"
    assert "total_bytes_override" not in domain_artifact_calls[0]
    assert "file_count_override" not in domain_artifact_calls[0]
    assert any(
        "gold_checkpoint_aggregate_publication layer=gold domain=finance bucket=A status=published" in message
        for message in messages
    )
    assert any("artifact_status=skipped" in message for message in messages)
    assert any(
        "artifact_publication_status layer=gold domain=finance status=published reason=none "
        "failure_mode=none buckets_ok=1 failed=0 failed_symbols=0 failed_buckets=0 "
        "failed_finalization=0 processed=1 skipped_unchanged=0 skipped_missing_source=0" in message
        for message in messages
    )


def test_run_alpha26_finance_gold_preflight_blocks_missing_required_inputs(monkeypatch):
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    index_calls = {"count": 0}
    monkeypatch.setattr(
        gold_finance_data.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: index_calls.__setitem__("count", int(index_calls["count"]) + 1) or "index",
    )
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if "finance-data/" in path else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(delta_core, "store_delta", lambda *_args, **_kwargs: None)

    date_value = pd.Timestamp("2024-01-01")
    ticker = "AAPL"

    income_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Total Revenue": [100.0],
            "Gross Profit": [40.0],
            "Net Income": [10.0],
        }
    )
    balance_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Long Term Debt": [250.0],
            "Total Assets": [1_000.0],
            "Current Assets": [500.0],
            "Current Liabilities": [250.0],
            # Intentionally omit Shares Outstanding so preflight fails on missing inputs.
        }
    )
    cashflow_df = pd.DataFrame(
        {
            "date": [date_value],
            "symbol": [ticker],
            "Operating Cash Flow": [25.0],
        }
    )

    def _fake_load_delta(_container: str, path: str):
        if "income_statement" in path:
            return income_df
        if "balance_sheet" in path:
            return balance_df
        if "cash_flow" in path:
            return cashflow_df
        return pd.DataFrame(
            {
                "date": pd.Series(dtype="datetime64[ns]"),
                "symbol": pd.Series(dtype="string"),
            }
        )

    monkeypatch.setattr(delta_core, "load_delta", _fake_load_delta)
    compute_calls = {"count": 0}

    def _unexpected_compute(_merged: pd.DataFrame) -> pd.DataFrame:
        compute_calls["count"] += 1
        return pd.DataFrame({"date": [], "symbol": []})

    monkeypatch.setattr(gold_finance_data, "compute_features", _unexpected_compute)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold_finance_data._run_alpha26_finance_gold(
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
    assert compute_calls["count"] == 0
    assert index_calls["count"] == 0


def test_run_alpha26_finance_gold_fails_closed_on_postgres_schema_drift(monkeypatch):
    index_calls = {"count": 0}
    store_calls = {"count": 0}
    messages: list[str] = []

    monkeypatch.setattr(gold_finance_data.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold_finance_data.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: index_calls.__setitem__("count", int(index_calls["count"]) + 1) or "index",
    )
    monkeypatch.setattr(gold_finance_data, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(
        gold_finance_data,
        "validate_sync_target_schema",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PostgresError("schema drift")),
    )
    monkeypatch.setattr(core_module, "write_error", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(
        delta_core,
        "store_delta",
        lambda *_args, **_kwargs: store_calls.__setitem__("count", int(store_calls["count"]) + 1),
    )

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold_finance_data._run_alpha26_finance_gold(
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
    assert store_calls["count"] == 0
    assert index_calls["count"] == 0
    assert any(
        "artifact_publication_status layer=gold domain=finance status=blocked "
        "reason=postgres_schema_drift failure_mode=finalization failed=1 failed_symbols=0 "
        "failed_buckets=0 failed_finalization=1 processed=0 skipped_unchanged=0 "
        "skipped_missing_source=0" in message
        for message in messages
    )


def test_main_runs_finance_reconciliation_and_persists_watermarks(monkeypatch: pytest.MonkeyPatch) -> None:
    reconciliation_calls: list[dict[str, str]] = []
    saved_watermarks: list[tuple[tuple, dict]] = []

    monkeypatch.setattr(core_module, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(gold_finance_data, "get_backfill_range", lambda: (None, None))
    monkeypatch.setattr(gold_finance_data, "load_watermarks", lambda _name: {"bucket::A": {"silver_last_commit": 1}})
    monkeypatch.setattr(
        gold_finance_data,
        "_build_job_config",
        lambda: gold_finance_data.FeatureJobConfig(
            silver_container="silver",
            gold_container="gold",
        ),
    )
    monkeypatch.setattr(
        gold_finance_data,
        "_run_alpha26_finance_gold",
        lambda **_kwargs: (1, 0, 0, 0, True, 1, "system/gold-index/finance/latest.parquet"),
    )

    def _run_reconciliation(*, silver_container: str, gold_container: str):
        reconciliation_calls.append(
            {
                "silver_container": silver_container,
                "gold_container": gold_container,
            }
        )
        return 4, 5

    monkeypatch.setattr(gold_finance_data, "_run_finance_reconciliation", _run_reconciliation)
    monkeypatch.setattr(
        gold_finance_data,
        "save_watermarks",
        lambda *args, **kwargs: saved_watermarks.append((args, kwargs)),
    )
    monkeypatch.setattr(core_module, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_error", lambda *_args, **_kwargs: None)

    assert gold_finance_data.main() == 0
    assert reconciliation_calls == [{"silver_container": "silver", "gold_container": "gold"}]
    assert len(saved_watermarks) == 1


def test_main_fails_closed_when_finance_reconciliation_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    save_watermarks_calls = {"count": 0}

    monkeypatch.setattr(core_module, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(gold_finance_data.layer_bucketing, "gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(gold_finance_data, "get_backfill_range", lambda: (None, None))
    monkeypatch.setattr(gold_finance_data, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(
        gold_finance_data,
        "_build_job_config",
        lambda: gold_finance_data.FeatureJobConfig(
            silver_container="silver",
            gold_container="gold",
        ),
    )
    monkeypatch.setattr(
        gold_finance_data,
        "_run_alpha26_finance_gold",
        lambda **_kwargs: (1, 0, 0, 0, True, 1, "system/gold-index/finance/latest.parquet"),
    )
    monkeypatch.setattr(
        gold_finance_data,
        "_run_finance_reconciliation",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(
        gold_finance_data,
        "save_watermarks",
        lambda *_args, **_kwargs: save_watermarks_calls.__setitem__("count", save_watermarks_calls["count"] + 1),
    )
    monkeypatch.setattr(core_module, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_error", lambda *_args, **_kwargs: None)

    assert gold_finance_data.main() == 1
    assert save_watermarks_calls["count"] == 0
