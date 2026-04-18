from __future__ import annotations

from asset_allocation_runtime_common.foundation import run_manifests as owner_run_manifests
from tasks.common import run_manifests as legacy_run_manifests


def test_legacy_run_manifests_wrapper_exposes_core_behavior() -> None:
    manifest = {
        "producedAt": "2026-02-26T00:00:00+00:00",
        "bucketPaths": [{"name": "finance-data/runs/run-1/buckets/B.parquet"}],
    }
    assert legacy_run_manifests._SILVER_FINANCE_PREFIX == owner_run_manifests._SILVER_FINANCE_PREFIX
    assert legacy_run_manifests.manifest_blobs(manifest) == owner_run_manifests.manifest_blobs(manifest)
