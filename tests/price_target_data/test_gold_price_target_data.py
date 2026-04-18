import pandas as pd
import pytest

from asset_allocation_runtime_common.market_data import core as core_module
from asset_allocation_runtime_common.market_data import delta_core
from asset_allocation_runtime_common.market_data.pipeline import DataPaths
from tasks.price_target_data import gold_price_target_data as gold
from tasks.common.gold_output_contracts import GOLD_PRICE_TARGET_OUTPUT_COLUMNS


def _capture_log_messages(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []
    monkeypatch.setattr(core_module, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_warning", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_error", lambda msg: messages.append(str(msg)))
    return messages


def test_build_job_config_reads_required_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")

    cfg = gold._build_job_config()

    assert cfg.silver_container == "silver"
    assert cfg.gold_container == "gold"


def test_build_job_config_requires_silver_container(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AZURE_CONTAINER_SILVER", raising=False)
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold")

    with pytest.raises(ValueError, match="AZURE_CONTAINER_SILVER"):
        gold._build_job_config()


def test_build_job_config_requires_gold_container(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver")
    monkeypatch.delenv("AZURE_CONTAINER_GOLD", raising=False)

    with pytest.raises(ValueError, match="AZURE_CONTAINER_GOLD"):
        gold._build_job_config()


def test_run_alpha26_price_target_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_price_targets_bucket_path("A")
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
    ) = gold._run_alpha26_price_target_gold(
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


def test_run_alpha26_price_target_gold_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_gold_price_targets_bucket_path("A")
    existing_cols = ["obs_date", "symbol", "tp_mean_est"]
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
    ) = gold._run_alpha26_price_target_gold(
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


def test_run_alpha26_price_target_gold_projects_contract_before_write(monkeypatch):
    target_path = DataPaths.get_gold_price_targets_bucket_path("A")
    captured: dict[str, object] = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_price_target_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["NVDA"],
            }
        )
        if path == DataPaths.get_silver_price_target_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda _df: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["nvda"],
                "tp_mean_est": [220.5],
                "tp_cnt_est": [17],
                "extra_metric": [99],
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
    ) = gold._run_alpha26_price_target_gold(
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
    assert list(captured["df"].columns) == list(GOLD_PRICE_TARGET_OUTPUT_COLUMNS)
    assert "extra_metric" not in captured["df"].columns
    assert captured["df"].loc[0, "symbol"] == "NVDA"


def test_run_alpha26_price_target_gold_blocks_publication_when_bucket_fails(monkeypatch):
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
        lambda _container, path: 1 if path == DataPaths.get_silver_price_target_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["NVDA"],
            }
        )
        if path == DataPaths.get_silver_price_target_bucket_path("A")
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
    ) = gold._run_alpha26_price_target_gold(
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


def test_run_alpha26_price_target_gold_uses_checkpoint_helper_and_final_log_contract(monkeypatch):
    messages = _capture_log_messages(monkeypatch)
    domain_artifact_calls: list[dict[str, object]] = []
    saved_watermarks: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        gold.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: domain_artifact_calls.append(dict(kwargs)) or {"artifactPath": "price-target/_metadata/domain.json"},
    )
    monkeypatch.setattr(gold.domain_artifacts, "write_bucket_artifact", lambda **_kwargs: None)
    monkeypatch.setattr(gold, "save_watermarks", lambda key, items: saved_watermarks.append((key, dict(items))))
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: None)
    monkeypatch.setattr(
        delta_core,
        "get_delta_last_commit",
        lambda _container, path: 1 if path == DataPaths.get_silver_price_target_bucket_path("A") else None,
    )
    monkeypatch.setattr(delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core,
        "load_delta",
        lambda _container, path: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["NVDA"],
            }
        )
        if path == DataPaths.get_silver_price_target_bucket_path("A")
        else pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda _df: pd.DataFrame(
            {
                "obs_date": [pd.Timestamp("2026-02-14")],
                "symbol": ["NVDA"],
                "tp_mean_est": [220.5],
                "tp_cnt_est": [17],
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
    ) = gold._run_alpha26_price_target_gold(
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
    assert saved_watermarks[0][0] == "gold_price_target_features"
    assert saved_watermarks[0][1]["bucket::A"]["silver_last_commit"] == 1
    assert len(domain_artifact_calls) == 1
    assert domain_artifact_calls[0]["symbol_index_path"] == "index"
    assert domain_artifact_calls[0]["symbol_count_override"] == 1
    assert any(
        "gold_checkpoint_aggregate_publication layer=gold domain=price-target bucket=A status=published" in message
        for message in messages
    )
    assert any("artifact_status=skipped" in message for message in messages)
    assert any(
        "artifact_publication_status layer=gold domain=price-target status=published reason=none "
        "failure_mode=none buckets_ok=1 failed=0 failed_symbols=0 failed_buckets=0 "
        "failed_finalization=0 processed=1 skipped_unchanged=0 skipped_missing_source=0" in message
        for message in messages
    )


def test_main_runs_price_target_reconciliation_and_persists_watermarks(monkeypatch):
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
        "_run_alpha26_price_target_gold",
        lambda **_kwargs: (1, 0, 0, 0, True, 1, "system/gold-index/price-target/latest.parquet"),
    )

    def _run_reconciliation(*, silver_container: str, gold_container: str):
        reconciliation_calls.append(
            {
                "silver_container": silver_container,
                "gold_container": gold_container,
            }
        )
        return 3, 4

    monkeypatch.setattr(gold, "_run_price_target_reconciliation", _run_reconciliation)
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


def test_main_fails_closed_when_price_target_reconciliation_fails(monkeypatch):
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
        "_run_alpha26_price_target_gold",
        lambda **_kwargs: (1, 0, 0, 0, True, 1, "system/gold-index/price-target/latest.parquet"),
    )
    monkeypatch.setattr(
        gold,
        "_run_price_target_reconciliation",
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
