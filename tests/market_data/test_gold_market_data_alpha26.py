from __future__ import annotations

import pandas as pd
import pytest

from core import core as core_module
from core import delta_core as delta_core_module
from core.pipeline import DataPaths
from core.postgres import PostgresError
from tasks.market_data import gold_market_data as gold
from core.gold_sync_contracts import GoldSyncResult


def _silver_bucket_df(symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02")],
            "symbol": [symbol],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000.0],
        }
    )


def _bucket_df(*symbols: str) -> pd.DataFrame:
    return pd.concat([_silver_bucket_df(symbol) for symbol in symbols], ignore_index=True)


def _gold_feature_df(symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02")],
            "symbol": [symbol],
            "close": [100.5],
        }
    )


class _FakeCursor:
    def __init__(self, *, fetchall_rows=None) -> None:
        self.fetchall_rows = list(fetchall_rows or [])
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))

    def fetchall(self):
        return list(self.fetchall_rows)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def _capture_log_messages(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    messages: list[str] = []
    monkeypatch.setattr(core_module, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_warning", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(core_module, "write_error", lambda msg: messages.append(str(msg)))
    return messages


@pytest.fixture(autouse=True)
def _install_fake_gold_market_staging(monkeypatch: pytest.MonkeyPatch) -> None:
    staged_chunks: dict[str, pd.DataFrame] = {}

    def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        if len(frames) == 1:
            return frames[0].reset_index(drop=True).copy()
        return pd.concat(frames, ignore_index=True)

    def _frames_for_prefix(prefix: str) -> list[pd.DataFrame]:
        clean_prefix = str(prefix or "").strip().strip("/")
        return [staged_chunks[path].copy() for path in sorted(staged_chunks) if path.startswith(clean_prefix)]

    def _fake_write_staged_market_chunk(**kwargs):
        frame = _concat_frames(list(kwargs["chunk_frames"]))
        chunk_blob_path = str(kwargs["chunk_blob_path"])
        staged_chunks[chunk_blob_path] = frame.copy()
        return gold.BucketChunkWriteResult(
            chunk_number=int(kwargs["chunk_number"]),
            rows=int(len(frame)),
            symbols=int(frame["symbol"].astype("string").nunique()) if "symbol" in frame.columns else 0,
            columns=int(len(frame.columns)),
            memory_mb=gold._frame_memory_mb(frame),
            summary=gold.domain_artifacts.summarize_frame(frame, domain="market", date_column="date"),
        )

    def _fake_iter_staged_market_chunk_frames(*, gold_container: str, chunk_prefix: str):
        del gold_container
        for frame in _frames_for_prefix(chunk_prefix):
            yield frame

    def _fake_promote_staged_market_bucket(*, gold_container: str, staging_delta_path: str, gold_path: str) -> int:
        chunk_prefix = str(staging_delta_path).rstrip("/")
        if chunk_prefix.endswith("/delta"):
            chunk_prefix = f"{chunk_prefix[:-len('/delta')]}/chunks"
        frames = _frames_for_prefix(chunk_prefix)
        promoted = _concat_frames(frames)
        delta_core_module.store_delta(promoted, gold_container, gold_path, mode="overwrite")
        return len(frames)

    def _fake_cleanup_staged_market_bucket(*, gold_container: str, staging_root: str) -> int:
        del gold_container
        clean_root = str(staging_root or "").strip().strip("/")
        to_delete = [path for path in staged_chunks if path.startswith(clean_root)]
        for path in to_delete:
            staged_chunks.pop(path, None)
        return len(to_delete)

    def _fake_sync_gold_bucket_chunks(**kwargs):
        frames_arg = kwargs.pop("frames")
        frames = list(frames_arg() if callable(frames_arg) else frames_arg)
        frame = _concat_frames([df.copy() for df in frames])
        return gold.sync_gold_bucket(frame=frame, **kwargs)

    monkeypatch.setattr(gold, "_write_staged_market_chunk", _fake_write_staged_market_chunk)
    monkeypatch.setattr(gold, "_iter_staged_market_chunk_frames", _fake_iter_staged_market_chunk_frames)
    monkeypatch.setattr(gold, "_promote_staged_market_bucket", _fake_promote_staged_market_bucket)
    monkeypatch.setattr(gold, "_cleanup_staged_market_bucket", _fake_cleanup_staged_market_bucket)
    monkeypatch.setattr(gold, "sync_gold_bucket_chunks", _fake_sync_gold_bucket_chunks)
    monkeypatch.setattr(gold, "_write_gold_market_bucket_artifact_from_summaries", lambda **_kwargs: None)
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)


def test_run_alpha26_market_gold_hard_fails_on_critical_symbol_compute_failure(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}
    messages = _capture_log_messages(monkeypatch)

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["S"])
    monkeypatch.setattr(
        gold.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["OLD"], "bucket": ["S"]}),
    )
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: 123.0)
    monkeypatch.setattr(
        delta_core_module,
        "load_delta",
        lambda *_args, **_kwargs: _bucket_df("SLV", "SPY"),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("store_delta should not be called")),
    )
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: (
            (_ for _ in ()).throw(ValueError("boom"))
            if str(df["symbol"].iloc[0]).strip().upper() == "SPY"
            else _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper())
        ),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert watermarks == {}
    assert index_path is None
    assert captured_index == {}
    assert any(
        "status=failed bucket=S reason=compute_failure symbols_in=2 symbols_out=0 failures=1 "
        "critical_symbol=true symbol=SPY" in message
        for message in messages
    )
    assert any("bucket_statuses={'failed_compute': 1} failed=1" in message for message in messages)


def test_run_alpha26_market_gold_logs_bucket_progress_and_loads_required_columns(monkeypatch):
    messages = _capture_log_messages(monkeypatch)
    load_calls: list[dict[str, object]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")

    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    def _fake_load_delta(_container: str, path: str, **kwargs):
        load_calls.append({"path": path, "columns": tuple(kwargs.get("columns") or ())})
        return _silver_bucket_df("AAPL")

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", _fake_load_delta)
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(delta_core_module, "store_delta", lambda *_args, **_kwargs: None)

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        _watermarks_dirty,
        _alpha26_symbols,
        _index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 1
    assert failed == 0
    assert load_calls == [
        {
            "path": DataPaths.get_silver_market_bucket_path("A"),
            "columns": gold._GOLD_MARKET_SILVER_SOURCE_COLUMNS,
        }
    ]
    assert any("gold_market_bucket_progress bucket=A stage=bucket_start" in message for message in messages)
    assert any("gold_market_bucket_progress bucket=A stage=source_loaded" in message for message in messages)
    assert any("gold_market_bucket_progress bucket=A stage=compute_complete" in message for message in messages)
    assert any("gold_market_bucket_progress bucket=A stage=write_completed" in message for message in messages)
    assert any("status=ok bucket=A symbols_in=1 symbols_out=1 failures=0" in message for message in messages)


def test_run_alpha26_market_gold_chunked_publish_preserves_rows_symbols_and_contract_columns(monkeypatch):
    watermarks: dict = {}
    written: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(gold, "_MARKET_CHUNK_SYMBOL_LIMIT", 1)
    monkeypatch.setattr(gold, "_MARKET_CHUNK_ROW_LIMIT", 1)

    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _bucket_df("AAPL", "AMZN"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda df, _container, path, **_kwargs: written.update({str(path): df.copy()}),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        _index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    expected_columns = list(projected.columns) if (projected := gold.project_gold_output_frame(pd.DataFrame(columns=["date", "symbol"]), domain="market")) is not None else []
    assert processed == 1
    assert failed == 0
    assert watermarks_dirty is True
    assert written["market/buckets/A"]["symbol"].tolist() == ["AAPL", "AMZN"]
    assert list(written["market/buckets/A"].columns) == expected_columns


def test_run_alpha26_market_gold_checkpoint_defers_root_domain_artifact_until_finalization(monkeypatch):
    watermarks: dict = {}
    messages = _capture_log_messages(monkeypatch)
    saved_watermarks: list[tuple[str, dict[str, object]]] = []
    domain_artifact_calls: list[dict[str, object]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(
        gold.domain_artifacts,
        "load_domain_artifact",
        lambda **_kwargs: {"totalBytes": 2048, "fileCount": 7},
    )
    monkeypatch.setattr(
        gold.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: domain_artifact_calls.append(dict(kwargs)) or {"artifactPath": "market/_metadata/domain.json"},
    )
    monkeypatch.setattr(gold, "save_watermarks", lambda key, items: saved_watermarks.append((key, dict(items))))

    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(delta_core_module, "store_delta", lambda *_args, **_kwargs: None)

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 1
    assert failed == 0
    assert watermarks_dirty is True
    assert index_path == "system/gold-index/market/latest.parquet"
    assert watermarks["bucket::A"]["silver_last_commit"] == 100.0
    assert len(saved_watermarks) == 1
    assert saved_watermarks[0][0] == "gold_market_features"
    assert len(domain_artifact_calls) == 1
    assert domain_artifact_calls[0]["symbol_index_path"] == "system/gold-index/market/latest.parquet"
    assert domain_artifact_calls[0]["symbol_count_override"] == 1
    assert "total_bytes_override" not in domain_artifact_calls[0]
    assert "file_count_override" not in domain_artifact_calls[0]
    assert any(
        "gold_checkpoint_aggregate_publication layer=gold domain=market bucket=A status=published" in message
        for message in messages
    )
    assert any("artifact_status=skipped" in message for message in messages)
    assert any(
        "artifact_publication_status layer=gold domain=market status=published reason=none "
        "failure_mode=none buckets_ok=1 failed=0 failed_symbols=0 failed_buckets=0 "
        "failed_finalization=0 processed=1 skipped_unchanged=0 skipped_missing_source=0" in message
        for message in messages
    )


def test_run_alpha26_market_gold_writes_healthy_symbols_and_blocks_publication_on_ordinary_failures(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}
    messages = _capture_log_messages(monkeypatch)
    written: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(
        gold.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )

    def _fake_last_commit(_container: str, path: str):
        if path.endswith("/A"):
            return 100.0
        return None

    def _fake_load_delta(_container: str, path: str, **_kwargs):
        if path.endswith("/A"):
            return _bucket_df("AAPL", "AMZN")
        return pd.DataFrame()

    def _fake_compute_features(df: pd.DataFrame) -> pd.DataFrame:
        symbol = str(df["symbol"].iloc[0]).strip().upper()
        if symbol == "AMZN":
            raise ValueError("compute failure")
        return _gold_feature_df(symbol)

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(delta_core_module, "load_delta", _fake_load_delta)
    monkeypatch.setattr(gold, "compute_features", _fake_compute_features)
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda df, _container, path, **_kwargs: written.update({str(path): df.copy()}),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 1
    assert failed == 1
    assert watermarks_dirty is False
    assert index_path is None
    assert set(written) == {"market/buckets/A"}
    assert written["market/buckets/A"]["symbol"].tolist() == ["AAPL"]
    assert watermarks == {}
    assert captured_index == {}
    assert any(
        "status=ok_with_failures bucket=A symbols_in=2 symbols_out=1 failures=1" in message
        for message in messages
    )
    assert any(
        "artifact_publication_status layer=gold domain=market status=blocked reason=failed_symbols "
        "failure_mode=symbol failed=1 failed_symbols=1 failed_buckets=0 failed_finalization=0 "
        "processed=1 skipped_unchanged=0 skipped_missing_source=0" in message
        for message in messages
    )
    assert any(
        "bucket_statuses={'ok_with_failures': 1} failed=1 failed_symbols=1 failed_buckets=0 "
        "failed_finalization=0" in message
        for message in messages
    )


def test_run_alpha26_market_gold_rerun_skips_checkpointed_bucket_and_recomputes_failed_bucket(monkeypatch):
    watermarks: dict = {}
    symbol_index_map: dict[str, str] = {}
    gold_commits: dict[str, float] = {}
    run_state = {"attempt": 1}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A", "B"])

    def _fake_load_layer_symbol_index(**_kwargs):
        if not symbol_index_map:
            return pd.DataFrame()
        return pd.DataFrame(
            {
                "symbol": list(symbol_index_map.keys()),
                "bucket": list(symbol_index_map.values()),
            }
        )

    def _fake_write_layer_symbol_index(**kwargs):
        symbol_index_map.clear()
        symbol_index_map.update(kwargs["symbol_to_bucket"])
        return "system/gold-index/market/latest.parquet"

    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        if path == DataPaths.get_silver_market_bucket_path("B"):
            return 100.0
        return gold_commits.get(path)

    def _fake_load_delta(_container: str, path: str, **_kwargs):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return _bucket_df("AAPL")
        if path == DataPaths.get_silver_market_bucket_path("B"):
            return _bucket_df("BABA")
        return pd.DataFrame()

    def _fake_compute_features(df: pd.DataFrame) -> pd.DataFrame:
        symbol = str(df["symbol"].iloc[0]).strip().upper()
        if symbol == "BABA" and run_state["attempt"] == 1:
            raise ValueError("transient failure")
        return _gold_feature_df(symbol)

    def _fake_store_delta(df: pd.DataFrame, _container: str, path: str, **_kwargs):
        if str(path).startswith("market/buckets/"):
            gold_commits[str(path)] = 100.0 if not df.empty else 0.0

    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", _fake_load_layer_symbol_index)
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", _fake_write_layer_symbol_index)
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", _fake_load_delta)
    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store_delta)
    monkeypatch.setattr(gold, "compute_features", _fake_compute_features)

    first = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )
    watermarks_after_first = dict(watermarks)
    run_state["attempt"] = 2
    second = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert first[:4] == (1, 0, 0, 1)
    assert watermarks_after_first["bucket::A"]["silver_last_commit"] == 100.0
    assert "bucket::B" not in watermarks_after_first
    assert second[:4] == (1, 1, 0, 0)
    assert watermarks["bucket::B"]["silver_last_commit"] == 100.0
    assert symbol_index_map == {"AAPL": "A", "BABA": "B"}


def test_run_alpha26_market_gold_contract_failure_logs_real_symbol_counts(monkeypatch):
    messages = _capture_log_messages(monkeypatch)
    captured_index: dict = {}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: 123.0)
    monkeypatch.setattr(
        delta_core_module,
        "load_delta",
        lambda *_args, **_kwargs: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-02")],
                "symbol": ["AAPL"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
            }
        ),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("store_delta should not be called")),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks={},
    )

    assert processed == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert index_path is None
    assert captured_index == {}
    assert any(
        "status=failed bucket=A reason=contract_validation symbols_in=1 symbols_out=0 failures=1" in message
        for message in messages
    )
    assert any("bucket_statuses={'failed_contract': 1} failed=1" in message for message in messages)


def test_run_alpha26_market_gold_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_market_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: None)

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(*_args, **_kwargs):
        captured["store_calls"] = int(captured["store_calls"]) + 1

    monkeypatch.setattr(delta_core_module, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
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


def test_run_alpha26_market_gold_processes_bucket_when_postgres_bootstrap_missing(monkeypatch):
    watermarks = {"bucket::A": {"silver_last_commit": 100.0}}
    captured_index: dict = {}
    written_paths: list[str] = []
    sync_calls: list[dict[str, object]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(gold, "_verify_postgres_critical_market_symbols", lambda **_kwargs: None)
    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )

    def _fake_sync_gold_bucket(**kwargs):
        sync_calls.append(kwargs)
        return GoldSyncResult(
            status="ok",
            domain="market",
            bucket="A",
            row_count=1,
            symbol_count=1,
            scope_symbol_count=1,
            source_commit=100.0,
            min_key=pd.Timestamp("2026-01-02").date(),
            max_key=pd.Timestamp("2026-01-02").date(),
        )

    monkeypatch.setattr(gold, "sync_gold_bucket", _fake_sync_gold_bucket)

    (
        processed,
        skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        _index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 1
    assert skipped_unchanged == 0
    assert failed == 0
    assert watermarks_dirty is True
    assert written_paths == ["market/buckets/A"]
    assert len(sync_calls) == 1
    assert sync_calls[0]["bucket"] == "A"
    assert sync_calls[0]["scope_symbols"] == ["AAPL"]
    assert watermarks["bucket::A"]["silver_last_commit"] == 100.0
    assert captured_index["symbol_to_bucket"] == {"AAPL": "A"}


def test_run_alpha26_market_gold_blocks_watermark_when_postgres_sync_fails(monkeypatch):
    watermarks = {"bucket::A": {"silver_last_commit": 90.0}}
    written_paths: list[str] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )
    monkeypatch.setattr(
        gold,
        "sync_gold_bucket",
        lambda **_kwargs: (_ for _ in ()).throw(PostgresError("sync failed")),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert index_path is None
    assert watermarks["bucket::A"]["silver_last_commit"] == 90.0
    assert written_paths == ["market/buckets/A"]


def test_run_alpha26_market_gold_blocks_watermark_on_checkpoint_failure(monkeypatch):
    watermarks: dict = {}
    written_paths: list[str] = []
    messages = _capture_log_messages(monkeypatch)

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("index write boom")),
    )
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: 100.0)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert failed == 1
    assert watermarks_dirty is False
    assert index_path is None
    assert watermarks == {}
    assert written_paths == ["market/buckets/A"]
    assert any("stage=checkpoint_failed" in message for message in messages)


def test_run_alpha26_market_gold_aligns_empty_bucket_to_existing_schema(monkeypatch):
    target_path = DataPaths.get_gold_market_bucket_path("A")
    existing_cols = ["date", "symbol", "close", "return_1d"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        delta_core_module,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs):
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
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
    assert captured["df"].empty
    assert list(captured["df"].columns) == existing_cols


def test_run_alpha26_market_gold_does_not_advance_watermark_for_empty_bucket_without_schema(monkeypatch):
    target_path = DataPaths.get_gold_market_bucket_path("A")
    watermarks: dict = {}
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(gold.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", lambda *_args, **_kwargs: 123.0)
    monkeypatch.setattr(delta_core_module, "load_delta", lambda *_args, **_kwargs: _silver_bucket_df("AAPL"))
    monkeypatch.setattr(gold, "compute_features", lambda *_args, **_kwargs: pd.DataFrame(columns=["date", "symbol"]))

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(*_args, **_kwargs):
        captured["store_calls"] = int(captured["store_calls"]) + 1

    monkeypatch.setattr(delta_core_module, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(delta_core_module, "store_delta", _fake_store)

    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert skipped_unchanged == 0
    assert skipped_missing_source == 0
    assert failed == 0
    assert watermarks_dirty is False
    assert watermarks == {}
    assert alpha26_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_run_alpha26_market_gold_rebuilds_domain_artifact_when_all_buckets_are_skipped(monkeypatch):
    watermarks = {"bucket::A": {"silver_last_commit": 100.0}}
    domain_artifact_calls: list[dict[str, object]] = []

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["A"])
    monkeypatch.setattr(
        gold.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["AAPL"], "bucket": ["A"]}),
    )
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: "system/gold-index/market/latest.parquet",
    )

    def _fake_last_commit(_container: str, path: str):
        if path == DataPaths.get_silver_market_bucket_path("A"):
            return 100.0
        if path == DataPaths.get_gold_market_bucket_path("A"):
            return 99.0
        return None

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(
        delta_core_module,
        "load_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("load_delta should not be called")),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("store_delta should not be called")),
    )
    monkeypatch.setattr(
        gold.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: domain_artifact_calls.append(kwargs) or {"artifactPath": "system/domain.json"},
    )

    (
        processed,
        skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 0
    assert skipped_unchanged == 1
    assert failed == 0
    assert watermarks_dirty is False
    assert index_path == "system/gold-index/market/latest.parquet"
    assert len(domain_artifact_calls) == 1
    assert domain_artifact_calls[0]["symbol_index_path"] == "system/gold-index/market/latest.parquet"
    assert domain_artifact_calls[0]["symbol_count_override"] == 1


def test_run_alpha26_market_gold_blocks_publication_when_critical_symbol_verification_fails(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}
    messages = _capture_log_messages(monkeypatch)
    written_paths: list[str] = []
    cursor = _FakeCursor(fetchall_rows=[("SPY", 10), ("^VIX", 10)])

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["S", "V"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(gold, "connect", lambda _dsn: _FakeConnection(cursor))

    def _fake_last_commit(_container: str, path: str):
        if path in {
            DataPaths.get_silver_market_bucket_path("S"),
            DataPaths.get_silver_market_bucket_path("V"),
        }:
            return 100.0
        return None

    def _fake_load_delta(_container: str, path: str, **_kwargs):
        if path == DataPaths.get_silver_market_bucket_path("S"):
            return _bucket_df("SPY")
        if path == DataPaths.get_silver_market_bucket_path("V"):
            return _bucket_df("^VIX", "^VIX3M")
        return pd.DataFrame()

    def _fake_sync_gold_bucket(**kwargs):
        frame = kwargs["frame"]
        return GoldSyncResult(
            status="ok",
            domain="market",
            bucket=str(kwargs["bucket"]).upper(),
            row_count=int(len(frame)),
            symbol_count=int(frame["symbol"].nunique()),
            scope_symbol_count=int(len(kwargs["scope_symbols"])),
            source_commit=float(kwargs["source_commit"]),
            min_key=pd.Timestamp("2026-01-02").date(),
            max_key=pd.Timestamp("2026-01-02").date(),
        )

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", _fake_load_delta)
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(
        delta_core_module,
        "store_delta",
        lambda _df, _container, path, **_kwargs: written_paths.append(str(path)),
    )
    monkeypatch.setattr(gold, "sync_gold_bucket", _fake_sync_gold_bucket)

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 2
    assert failed == 1
    assert watermarks_dirty is True
    assert index_path == "system/gold-index/market/latest.parquet"
    assert watermarks["bucket::S"]["silver_last_commit"] == 100.0
    assert watermarks["bucket::V"]["silver_last_commit"] == 100.0
    assert captured_index["symbol_to_bucket"] == {"SPY": "S", "^VIX": "V", "^VIX3M": "V"}
    assert set(written_paths) == {"market/buckets/S", "market/buckets/V"}
    assert any("postgres_gold_critical_symbol_status domain=market status=failed" in message for message in messages)
    assert any(
        "artifact_publication_status layer=gold domain=market status=blocked "
        "reason=critical_symbol_verification_failed failure_mode=finalization failed=1 "
        "failed_symbols=0 failed_buckets=0 failed_finalization=1 processed=2 "
        "skipped_unchanged=0 skipped_missing_source=0" in message
        for message in messages
    )


def test_run_alpha26_market_gold_completes_when_critical_symbol_verification_passes(monkeypatch):
    watermarks: dict = {}
    captured_index: dict = {}
    messages = _capture_log_messages(monkeypatch)
    cursor = _FakeCursor(fetchall_rows=[("SPY", 10), ("^VIX", 10), ("^VIX3M", 10)])

    monkeypatch.setattr(gold.layer_bucketing, "ALPHABET_BUCKETS", ["S", "V"])
    monkeypatch.setattr(gold.layer_bucketing, "load_layer_symbol_index", lambda **_kwargs: pd.DataFrame())
    monkeypatch.setattr(
        gold.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(gold, "resolve_postgres_dsn", lambda: "postgresql://test")
    monkeypatch.setattr(gold, "load_domain_sync_state", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(gold, "connect", lambda _dsn: _FakeConnection(cursor))

    def _fake_last_commit(_container: str, path: str):
        if path in {
            DataPaths.get_silver_market_bucket_path("S"),
            DataPaths.get_silver_market_bucket_path("V"),
        }:
            return 100.0
        return None

    def _fake_load_delta(_container: str, path: str, **_kwargs):
        if path == DataPaths.get_silver_market_bucket_path("S"):
            return _bucket_df("SPY")
        if path == DataPaths.get_silver_market_bucket_path("V"):
            return _bucket_df("^VIX", "^VIX3M")
        return pd.DataFrame()

    def _fake_sync_gold_bucket(**kwargs):
        frame = kwargs["frame"]
        return GoldSyncResult(
            status="ok",
            domain="market",
            bucket=str(kwargs["bucket"]).upper(),
            row_count=int(len(frame)),
            symbol_count=int(frame["symbol"].nunique()),
            scope_symbol_count=int(len(kwargs["scope_symbols"])),
            source_commit=float(kwargs["source_commit"]),
            min_key=pd.Timestamp("2026-01-02").date(),
            max_key=pd.Timestamp("2026-01-02").date(),
        )

    monkeypatch.setattr(delta_core_module, "get_delta_last_commit", _fake_last_commit)
    monkeypatch.setattr(delta_core_module, "load_delta", _fake_load_delta)
    monkeypatch.setattr(
        gold,
        "compute_features",
        lambda df: _gold_feature_df(str(df["symbol"].iloc[0]).strip().upper()),
    )
    monkeypatch.setattr(delta_core_module, "store_delta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(gold, "sync_gold_bucket", _fake_sync_gold_bucket)

    (
        processed,
        _skipped_unchanged,
        _skipped_missing_source,
        failed,
        watermarks_dirty,
        _alpha26_symbols,
        index_path,
    ) = gold._run_alpha26_market_gold(
        silver_container="silver",
        gold_container="gold",
        backfill_start_iso=None,
        watermarks=watermarks,
    )

    assert processed == 2
    assert failed == 0
    assert watermarks_dirty is True
    assert index_path == "system/gold-index/market/latest.parquet"
    assert watermarks["bucket::S"]["silver_last_commit"] == 100.0
    assert watermarks["bucket::V"]["silver_last_commit"] == 100.0
    assert captured_index["symbol_to_bucket"] == {"SPY": "S", "^VIX": "V", "^VIX3M": "V"}
    assert any("postgres_gold_critical_symbol_status domain=market status=ok" in message for message in messages)


def test_main_fails_closed_when_gold_reconciliation_fails(monkeypatch):
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
        "_run_alpha26_market_gold",
        lambda **_kwargs: (1, 0, 0, 0, False, 1, "system/gold-index/market/latest.parquet"),
    )
    monkeypatch.setattr(
        gold,
        "_run_market_reconciliation",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(gold, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(core_module, "write_error", lambda *_args, **_kwargs: None)

    assert gold.main() == 1


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
