from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tasks.common import gold_checkpoint_publication


def test_publish_gold_checkpoint_aggregate_merges_symbols_and_persists_watermarks(monkeypatch) -> None:
    captured: dict[str, object] = {}
    messages: list[str] = []
    saved: list[tuple[str, dict[str, object]]] = []
    checkpoint_time = datetime(2026, 3, 25, 14, 45, tzinfo=timezone.utc)
    watermarks = {"bucket::M": {"silver_last_commit": 90.0, "updated_at": "2026-03-25T14:00:00+00:00"}}

    def _fake_write_layer_symbol_index(**kwargs):
        captured["index_kwargs"] = dict(kwargs)
        return "system/gold-index/market/latest.parquet"

    def _fake_write_domain_artifact(**kwargs):
        captured["artifact_kwargs"] = dict(kwargs)
        return {"artifactPath": "market/_metadata/domain.json"}

    def _fake_save_watermarks(key: str, items: dict[str, object]) -> None:
        saved.append((key, dict(items)))

    monkeypatch.setattr(
        gold_checkpoint_publication.layer_bucketing,
        "write_layer_symbol_index",
        _fake_write_layer_symbol_index,
    )
    monkeypatch.setattr(
        gold_checkpoint_publication.domain_artifacts,
        "load_domain_artifact",
        lambda **_kwargs: {"totalBytes": 2048, "fileCount": 11},
    )
    monkeypatch.setattr(
        gold_checkpoint_publication.domain_artifacts,
        "write_domain_artifact",
        _fake_write_domain_artifact,
    )
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_warning", lambda msg: messages.append(str(msg)))

    result = gold_checkpoint_publication.publish_gold_checkpoint_aggregate(
        domain="market",
        bucket="A",
        symbol_to_bucket={"OLD": "A", "MSFT": "M"},
        touched_symbol_to_bucket={"AAPL": "A", "AMZN": "A"},
        watermarks=watermarks,
        watermarks_key="gold_market_features",
        watermark_key="bucket::A",
        source_commit=123.0,
        date_column="date",
        job_name="gold-market-job",
        save_watermarks_fn=_fake_save_watermarks,
        job_run_id="run-123",
        run_id="run-123",
        updated_at=checkpoint_time,
    )

    assert result.symbol_to_bucket == {"MSFT": "M", "AAPL": "A", "AMZN": "A"}
    assert result.index_path == "system/gold-index/market/latest.parquet"
    assert result.domain_artifact_path == "market/_metadata/domain.json"
    assert captured["index_kwargs"] == {
        "layer": "gold",
        "domain": "market",
        "symbol_to_bucket": {"MSFT": "M", "AAPL": "A", "AMZN": "A"},
        "updated_at": checkpoint_time,
    }
    assert captured["artifact_kwargs"] == {
        "layer": "gold",
        "domain": "market",
        "date_column": "date",
        "symbol_count_override": 3,
        "symbol_index_path": "system/gold-index/market/latest.parquet",
        "job_name": "gold-market-job",
        "job_run_id": "run-123",
        "run_id": "run-123",
        "total_bytes_override": 2048,
        "file_count_override": 11,
    }
    assert saved == [
        (
            "gold_market_features",
            {
                "bucket::M": {"silver_last_commit": 90.0, "updated_at": "2026-03-25T14:00:00+00:00"},
                "bucket::A": {"silver_last_commit": 123.0, "updated_at": checkpoint_time.isoformat()},
            },
        )
    ]
    assert watermarks == saved[0][1]
    assert any(
        "gold_checkpoint_aggregate_publication layer=gold domain=market bucket=A status=published" in message
        for message in messages
    )


def test_publish_gold_checkpoint_aggregate_skips_root_domain_artifact_when_disabled(monkeypatch) -> None:
    messages: list[str] = []
    saved: list[tuple[str, dict[str, object]]] = []
    watermarks: dict[str, object] = {}
    artifact_call_count = {"count": 0}

    monkeypatch.setattr(
        gold_checkpoint_publication.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: "system/gold-index/earnings/latest.parquet",
    )
    monkeypatch.setattr(
        gold_checkpoint_publication.domain_artifacts,
        "write_domain_artifact",
        lambda **_kwargs: artifact_call_count.__setitem__("count", artifact_call_count["count"] + 1),
    )
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_warning", lambda msg: messages.append(str(msg)))

    result = gold_checkpoint_publication.publish_gold_checkpoint_aggregate(
        domain="earnings",
        bucket="A",
        symbol_to_bucket={},
        touched_symbol_to_bucket={"AAPL": "A"},
        watermarks=watermarks,
        watermarks_key="gold_earnings_features",
        watermark_key="bucket::A",
        source_commit=456.0,
        date_column="date",
        job_name="gold-earnings-job",
        save_watermarks_fn=lambda key, items: saved.append((key, dict(items))),
        publish_domain_artifact=False,
    )

    assert result.symbol_to_bucket == {"AAPL": "A"}
    assert result.index_path == "system/gold-index/earnings/latest.parquet"
    assert result.domain_artifact_path is None
    assert saved == [
        (
            "gold_earnings_features",
            {
                "bucket::A": {
                    "silver_last_commit": 456.0,
                    "updated_at": watermarks["bucket::A"]["updated_at"],
                }
            },
        )
    ]
    assert artifact_call_count["count"] == 0
    assert any("artifact_status=skipped" in message for message in messages)


def test_finalize_gold_publication_publishes_root_artifact_when_no_failures(monkeypatch) -> None:
    messages: list[str] = []
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        gold_checkpoint_publication.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured.update({"index_kwargs": dict(kwargs)}) or "system/gold-index/market/latest.parquet",
    )
    monkeypatch.setattr(
        gold_checkpoint_publication.domain_artifacts,
        "write_domain_artifact",
        lambda **kwargs: captured.update({"artifact_kwargs": dict(kwargs)}) or {"artifactPath": "market/_metadata/domain.json"},
    )
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_warning", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_error", lambda msg: messages.append(str(msg)))

    result = gold_checkpoint_publication.finalize_gold_publication(
        domain="market",
        symbol_to_bucket={"AAPL": "A"},
        date_column="date",
        job_name="gold-market-job",
        processed=1,
        skipped_unchanged=2,
        skipped_missing_source=3,
        failed_symbols=0,
        failed_buckets=0,
        failed_finalization=0,
        job_run_id="run-1",
        run_id="run-1",
    )

    assert result.failed == 0
    assert result.failure_mode == "none"
    assert result.publication_reason == "none"
    assert result.index_path == "system/gold-index/market/latest.parquet"
    assert result.domain_artifact_path == "market/_metadata/domain.json"
    assert captured["artifact_kwargs"] == {
        "layer": "gold",
        "domain": "market",
        "date_column": "date",
        "symbol_count_override": 1,
        "symbol_index_path": "system/gold-index/market/latest.parquet",
        "job_name": "gold-market-job",
        "job_run_id": "run-1",
        "run_id": "run-1",
    }
    assert any(
        "artifact_publication_status layer=gold domain=market status=published reason=none "
        "failure_mode=none buckets_ok=1 failed=0 failed_symbols=0 failed_buckets=0 "
        "failed_finalization=0 processed=1 skipped_unchanged=2 skipped_missing_source=3" in message
        for message in messages
    )


@pytest.mark.parametrize(
    ("failed_symbols", "failed_buckets", "failed_finalization", "publication_reason", "expected_reason", "expected_mode"),
    [
        (2, 0, 0, None, "failed_symbols", "symbol"),
        (0, 1, 0, None, "failed_buckets", "bucket"),
        (0, 0, 1, "critical_symbol_verification_failed", "critical_symbol_verification_failed", "finalization"),
        (2, 1, 1, None, "mixed_failures", "mixed"),
    ],
)
def test_finalize_gold_publication_resolves_blocked_failure_modes(
    monkeypatch,
    failed_symbols: int,
    failed_buckets: int,
    failed_finalization: int,
    publication_reason: str | None,
    expected_reason: str,
    expected_mode: str,
) -> None:
    messages: list[str] = []
    index_calls = {"count": 0}
    artifact_calls = {"count": 0}

    monkeypatch.setattr(
        gold_checkpoint_publication.layer_bucketing,
        "write_layer_symbol_index",
        lambda **_kwargs: index_calls.__setitem__("count", index_calls["count"] + 1) or "index",
    )
    monkeypatch.setattr(
        gold_checkpoint_publication.domain_artifacts,
        "write_domain_artifact",
        lambda **_kwargs: artifact_calls.__setitem__("count", artifact_calls["count"] + 1) or {"artifactPath": "artifact"},
    )
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_line", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_warning", lambda msg: messages.append(str(msg)))
    monkeypatch.setattr(gold_checkpoint_publication.mdc, "write_error", lambda msg: messages.append(str(msg)))

    result = gold_checkpoint_publication.finalize_gold_publication(
        domain="market",
        symbol_to_bucket={"AAPL": "A"},
        date_column="date",
        job_name="gold-market-job",
        processed=3,
        skipped_unchanged=1,
        skipped_missing_source=0,
        failed_symbols=failed_symbols,
        failed_buckets=failed_buckets,
        failed_finalization=failed_finalization,
        publication_reason=publication_reason,
    )

    assert result.failed == failed_symbols + failed_buckets + failed_finalization
    assert result.failure_mode == expected_mode
    assert result.publication_reason == expected_reason
    assert index_calls["count"] == 0
    assert artifact_calls["count"] == 0
    assert any(
        "artifact_publication_status layer=gold domain=market status=blocked "
        f"reason={expected_reason} failure_mode={expected_mode} "
        f"failed={result.failed} failed_symbols={failed_symbols} failed_buckets={failed_buckets} "
        f"failed_finalization={failed_finalization} processed=3 skipped_unchanged=1 skipped_missing_source=0"
        in message
        for message in messages
    )
