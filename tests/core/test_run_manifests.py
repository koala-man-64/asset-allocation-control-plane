from __future__ import annotations

import json
from datetime import datetime, timezone

from core import run_manifests


def test_create_bronze_alpha26_manifest_writes_manifest_and_latest(monkeypatch):
    saved: dict[str, dict] = {}

    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())
    monkeypatch.setattr(
        run_manifests.mdc,
        "save_common_json_content",
        lambda payload, path: saved.setdefault(path, payload),
    )

    out = run_manifests.create_bronze_alpha26_manifest(
        domain="finance",
        producer_job_name="bronze-finance-job",
        data_prefix="finance-data/runs/run-123",
        bucket_paths=[
            {
                "name": "finance-data/runs/run-123/buckets/A.parquet",
                "bucket": "A",
                "etag": "etag-a",
                "last_modified": datetime(2026, 2, 26, 16, 0, tzinfo=timezone.utc),
                "size": 42,
            }
        ],
        index_path="system/bronze-index/finance/latest.parquet",
        metadata={"symbolCount": 1},
        run_id="run-123",
    )

    assert out is not None
    manifest_path = "system/run-manifests/bronze_finance/run-123.json"
    latest_path = "system/run-manifests/bronze_finance/latest.json"
    assert manifest_path in saved
    assert latest_path in saved
    assert saved[manifest_path]["bucketCount"] == 1
    assert saved[manifest_path]["bucketPaths"][0]["name"] == "finance-data/runs/run-123/buckets/A.parquet"
    assert saved[manifest_path]["metadata"]["symbolCount"] == 1
    assert saved[latest_path]["runId"] == "run-123"
    assert saved[latest_path]["dataPrefix"] == "finance-data/runs/run-123"


def test_create_bronze_finance_manifest_normalizes_string_last_modified(monkeypatch):
    saved: dict[str, dict] = {}

    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())
    monkeypatch.setattr(
        run_manifests.mdc,
        "save_common_json_content",
        lambda payload, path: saved.setdefault(path, payload),
    )

    out = run_manifests.create_bronze_finance_manifest(
        producer_job_name="bronze-finance-job",
        listed_blobs=[
            {
                "name": "finance-data/Valuation/AAPL_quarterly_valuation_measures.json",
                "etag": "etag-a",
                "last_modified": "2026-02-26T16:00:00Z",
                "size": 42,
            }
        ],
    )

    assert out is not None
    manifest_path = str(out["manifestPath"])
    assert saved[manifest_path]["blobs"] == [
        {
            "name": "finance-data/Valuation/AAPL_quarterly_valuation_measures.json",
            "etag": "etag-a",
            "last_modified": "2026-02-26T16:00:00+00:00",
            "size": 42,
        }
    ]


def test_load_latest_bronze_finance_manifest_resolves_pointer(monkeypatch):
    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())

    def _fake_read(path: str, client=None, *, missing_ok=False, missing_message=None):
        del client, missing_ok, missing_message
        if path.endswith("/latest.json"):
            return json.dumps(
                {
                    "runId": "bronze-finance-20260226T000000000000Z-abcd1234",
                    "manifestPath": (
                        "system/run-manifests/bronze_finance/"
                        "bronze-finance-20260226T000000000000Z-abcd1234.json"
                    ),
                }
            ).encode("utf-8")
        if path.endswith("abcd1234.json"):
            return json.dumps(
                {
                    "runId": "bronze-finance-20260226T000000000000Z-abcd1234",
                    "dataPrefix": "finance-data/runs/bronze-finance-20260226T000000000000Z-abcd1234",
                    "bucketPaths": [{"name": "finance-data/runs/bronze-finance-20260226T000000000000Z-abcd1234/buckets/A.parquet"}],
                }
            ).encode("utf-8")
        return b""

    monkeypatch.setattr(run_manifests.mdc, "read_raw_bytes", _fake_read)
    manifest = run_manifests.load_latest_bronze_finance_manifest()
    assert manifest is not None
    assert manifest["runId"].endswith("abcd1234")
    assert manifest["manifestPath"].endswith("abcd1234.json")
    assert manifest["dataPrefix"].endswith("abcd1234")


def test_resolve_active_bronze_alpha26_prefix_returns_manifest_data_prefix(monkeypatch):
    monkeypatch.setattr(
        run_manifests,
        "load_latest_bronze_alpha26_manifest",
        lambda domain: {
            "domain": domain,
            "dataPrefix": "market-data/runs/run-456",
        },
    )

    assert run_manifests.resolve_active_bronze_alpha26_prefix("market") == "market-data/runs/run-456"


def test_load_latest_bronze_finance_manifest_backfills_legacy_blob_fields(monkeypatch):
    monkeypatch.setattr(
        run_manifests,
        "load_latest_bronze_alpha26_manifest",
        lambda _domain: {
            "runId": "run-123",
            "manifestPath": "system/run-manifests/bronze_finance/run-123.json",
            "producedAt": "2026-02-26T00:00:00+00:00",
            "dataPrefix": "finance-data/runs/run-123",
            "bucketPaths": [
                {"name": "finance-data/runs/run-123/buckets/A.parquet"},
                {"name": "finance-data/runs/run-123/buckets/B.parquet"},
            ],
        },
    )

    manifest = run_manifests.load_latest_bronze_finance_manifest()

    assert manifest is not None
    assert manifest["blobPrefix"] == "finance-data/"
    assert manifest["blobCount"] == 2
    assert [item["name"] for item in manifest["blobs"]] == [
        "finance-data/runs/run-123/buckets/A.parquet",
        "finance-data/runs/run-123/buckets/B.parquet",
    ]
    assert all(item["last_modified"] == "2026-02-26T00:00:00+00:00" for item in manifest["blobs"])


def test_write_and_read_silver_manifest_ack(monkeypatch):
    saved: dict[str, dict] = {}

    monkeypatch.setattr(run_manifests.mdc, "common_storage_client", object())
    monkeypatch.setattr(
        run_manifests.mdc,
        "save_common_json_content",
        lambda payload, path: saved.setdefault(path, payload),
    )
    monkeypatch.setattr(
        run_manifests.mdc,
        "get_common_json_content",
        lambda path: saved.get(path),
    )

    ack_path = run_manifests.write_silver_finance_ack(
        run_id="bronze-finance-20260226T000000000000Z-abcd1234",
        manifest_path="system/run-manifests/bronze_finance/bronze-finance-20260226T000000000000Z-abcd1234.json",
        status="succeeded",
        metadata={"processed": 10},
    )
    assert ack_path is not None
    assert run_manifests.silver_finance_ack_exists("bronze-finance-20260226T000000000000Z-abcd1234") is True


def test_manifest_blobs_normalizes_sorts_and_inherits_produced_at():
    manifest = {
        "producedAt": "2026-02-26T00:00:00+00:00",
        "bucketPaths": [
            {"name": "finance-data/runs/run-1/buckets/B.parquet"},
            {"name": "finance-data/runs/run-1/buckets/A.parquet"},
        ],
    }
    out = run_manifests.manifest_blobs(manifest)
    assert [item["name"] for item in out] == [
        "finance-data/runs/run-1/buckets/A.parquet",
        "finance-data/runs/run-1/buckets/B.parquet",
    ]
    assert all(item["last_modified"] == "2026-02-26T00:00:00+00:00" for item in out)
