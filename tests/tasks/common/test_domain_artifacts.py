from __future__ import annotations

from core import domain_artifacts as owner_domain_artifacts
from core import domain_metadata_snapshots as owner_domain_metadata_snapshots
from tasks.common import domain_artifacts as legacy_domain_artifacts
from tasks.common import domain_metadata_snapshots as legacy_domain_metadata_snapshots


def test_legacy_domain_artifacts_wrapper_exposes_core_behavior() -> None:
    assert legacy_domain_artifacts.FINANCE_SUBDOMAINS == owner_domain_artifacts.FINANCE_SUBDOMAINS
    assert legacy_domain_artifacts.normalize_sub_domain("balance-sheet") == owner_domain_artifacts.normalize_sub_domain(
        "balance-sheet"
    )
    assert legacy_domain_artifacts.domain_artifact_path(layer="gold", domain="regime") == owner_domain_artifacts.domain_artifact_path(
        layer="gold",
        domain="regime",
    )


def test_legacy_domain_metadata_snapshots_wrapper_exposes_core_behavior() -> None:
    assert legacy_domain_metadata_snapshots.DOMAIN_METADATA_CACHE_PATH_DEFAULT == (
        owner_domain_metadata_snapshots.DOMAIN_METADATA_CACHE_PATH_DEFAULT
    )
    legacy_payload = legacy_domain_metadata_snapshots.build_snapshot_miss_payload(layer="gold", domain="market")
    owner_payload = owner_domain_metadata_snapshots.build_snapshot_miss_payload(layer="gold", domain="market")
    assert legacy_payload["layer"] == owner_payload["layer"]
    assert legacy_payload["domain"] == owner_payload["domain"]
    assert legacy_payload["warnings"] == owner_payload["warnings"]
