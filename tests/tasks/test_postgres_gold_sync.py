from __future__ import annotations

from asset_allocation_runtime_common.market_data import gold_sync_contracts as owner_gold_sync_contracts
from tasks.common import postgres_gold_sync as legacy_gold_sync_contracts


def test_legacy_gold_sync_wrapper_exposes_core_behavior() -> None:
    assert legacy_gold_sync_contracts.GoldSyncConfig is owner_gold_sync_contracts.GoldSyncConfig
    assert legacy_gold_sync_contracts.get_sync_config("market") == owner_gold_sync_contracts.get_sync_config("market")
