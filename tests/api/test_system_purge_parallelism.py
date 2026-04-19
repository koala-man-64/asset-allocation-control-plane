import time

import pandas as pd
import pytest

from api.endpoints import system


@pytest.mark.parametrize(
    ("env_name", "resolver", "max_workers_const", "invalid_value", "requested_count"),
    [
        (
            "PURGE_PREVIEW_LOAD_MAX_WORKERS",
            system._resolve_purge_preview_load_workers,
            system._MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS,
            "not-a-number",
            4,
        ),
        (
            "PURGE_SCOPE_MAX_WORKERS",
            system._resolve_purge_scope_workers,
            system._MAX_PURGE_SCOPE_MAX_WORKERS,
            "invalid",
            5,
        ),
        (
            "PURGE_SYMBOL_TARGET_MAX_WORKERS",
            system._resolve_purge_symbol_target_workers,
            system._MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS,
            "bad",
            4,
        ),
        (
            "PURGE_SYMBOL_LAYER_MAX_WORKERS",
            system._resolve_purge_symbol_layer_workers,
            system._MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS,
            "oops",
            3,
        ),
    ],
)
def test_resolve_worker_limits(
    env_name: str,
    resolver,
    max_workers_const: int,
    invalid_value: str,
    requested_count: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(env_name, raising=False)
    assert resolver(0) == 1
    assert resolver(3) == 3

    monkeypatch.setenv(env_name, "2")
    assert resolver(10) == 2

    monkeypatch.setenv(env_name, "999")
    assert resolver(100) == max_workers_const

    monkeypatch.setenv(env_name, invalid_value)
    assert resolver(requested_count) == requested_count


def test_load_rule_frame_parallel_preserves_table_order(monkeypatch: pytest.MonkeyPatch) -> None:
    table_paths = ["market-data/a", "market-data/b", "market-data/c"]
    delay_by_path = {
        "market-data/a": 0.03,
        "market-data/b": 0.01,
        "market-data/c": 0.0,
    }

    monkeypatch.setenv("PURGE_PREVIEW_LOAD_MAX_WORKERS", "3")
    monkeypatch.setattr(system, "_resolve_purge_rule_table", lambda layer, domain: ("silver-container", "market-data/"))
    monkeypatch.setattr(system, "_discover_delta_tables_for_prefix", lambda **kwargs: table_paths)

    def _fake_load_delta(*, container: str, path: str):
        assert container == "silver-container"
        time.sleep(delay_by_path[path])
        return pd.DataFrame([{"source": path, "symbol": "AAA", "value": 1.0}])

    monkeypatch.setattr(system, "load_delta", _fake_load_delta)

    frame = system._load_rule_frame("silver", "market")

    assert list(frame["source"]) == table_paths


def test_run_purge_operation_parallel_preserves_target_order(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = [
        {"layer": "silver", "domain": "market", "container": "c", "prefix": "p1"},
        {"layer": "silver", "domain": "finance", "container": "c", "prefix": "p2"},
        {"layer": "silver", "domain": "earnings", "container": "c", "prefix": "p3"},
    ]

    checkpoint_prefixes = [
        "system/watermarks/bronze_market_data.json",
        "system/watermarks/runs/silver_market_data.json",
        "system/watermarks/bronze_finance_data.json",
        "system/watermarks/runs/silver_finance_data.json",
        "system/watermarks/bronze_earnings_data.json",
        "system/watermarks/runs/silver_earnings_data.json",
    ]
    ordered_prefixes = ["p1", "p2", "p3", *checkpoint_prefixes]
    delay_by_prefix = {prefix: 0.0 for prefix in ordered_prefixes}
    delay_by_prefix.update({"p1": 0.03, "p2": 0.01})
    deleted_by_prefix = {
        "p1": 1,
        "p2": 2,
        "p3": 3,
        "system/watermarks/bronze_market_data.json": 1,
        "system/watermarks/runs/silver_market_data.json": 1,
        "system/watermarks/bronze_finance_data.json": 1,
        "system/watermarks/runs/silver_finance_data.json": 1,
        "system/watermarks/bronze_earnings_data.json": 1,
        "system/watermarks/runs/silver_earnings_data.json": 1,
    }

    monkeypatch.setenv("PURGE_SCOPE_MAX_WORKERS", "3")
    monkeypatch.setattr(system, "_resolve_purge_targets", lambda scope, layer, domain: [dict(t) for t in targets])
    monkeypatch.setattr(system, "_mark_purged_domain_metadata_snapshots", lambda targets: None)

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

        def has_blobs(self, prefix):
            return True

        def delete_prefix(self, prefix):
            time.sleep(delay_by_prefix[prefix])
            return deleted_by_prefix[prefix]

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)

    payload = system.PurgeRequest(scope="layer", layer="silver", confirm=True)
    result = system._run_purge_operation(payload)

    assert [entry.get("prefix") for entry in result["targets"]] == ordered_prefixes
    assert [entry.get("deleted") for entry in result["targets"]] == [deleted_by_prefix[prefix] for prefix in ordered_prefixes]
    assert result["totalDeleted"] == sum(deleted_by_prefix.values())


def test_build_silver_checkpoint_reset_targets_for_layer_scope_includes_all_silver_jobs() -> None:
    targets = [{"layer": "silver", "domain": None, "container": "silver", "prefix": None}]

    reset_targets = system._build_silver_checkpoint_reset_targets(targets)

    assert [entry.get("prefix") for entry in reset_targets] == [
        "system/watermarks/bronze_market_data.json",
        "system/watermarks/runs/silver_market_data.json",
        "system/watermarks/bronze_finance_data.json",
        "system/watermarks/runs/silver_finance_data.json",
        "system/watermarks/bronze_earnings_data.json",
        "system/watermarks/runs/silver_earnings_data.json",
        "system/watermarks/bronze_price_target_data.json",
        "system/watermarks/runs/silver_price_target_data.json",
        "system/watermarks/bronze_government_signal_data.json",
        "system/watermarks/runs/silver_government_signal_data.json",
    ]


def test_build_gold_checkpoint_reset_targets_for_layer_scope_includes_all_gold_jobs() -> None:
    targets = [{"layer": "gold", "domain": None, "container": "gold", "prefix": None}]

    reset_targets = system._build_gold_checkpoint_reset_targets(targets)

    assert [entry.get("prefix") for entry in reset_targets] == [
        "system/watermarks/gold_market_features.json",
        "system/watermarks/gold_finance_features.json",
        "system/watermarks/gold_earnings_features.json",
        "system/watermarks/gold_price_target_features.json",
        "system/watermarks/gold_government_signals.json",
    ]


def test_run_purge_operation_appends_gold_checkpoint_resets(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = [{"layer": "gold", "domain": "market", "container": "g", "prefix": "market/"}]
    deleted_by_prefix = {
        "market/": 3,
        "system/watermarks/gold_market_features.json": 1,
    }

    monkeypatch.setattr(system, "_resolve_purge_targets", lambda scope, layer, domain: [dict(t) for t in targets])
    monkeypatch.setattr(system, "_mark_purged_domain_metadata_snapshots", lambda targets: None)

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

        def has_blobs(self, prefix):
            return True

        def delete_prefix(self, prefix):
            return deleted_by_prefix[prefix]

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)

    payload = system.PurgeRequest(scope="layer-domain", layer="gold", domain="market", confirm=True)
    result = system._run_purge_operation(payload)

    assert [entry.get("prefix") for entry in result["targets"]] == [
        "market/",
        "system/watermarks/gold_market_features.json",
    ]
    assert result["totalDeleted"] == 4


def test_run_purge_operation_marks_layer_domain_metadata_purged(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = [{"layer": "gold", "domain": "market", "container": "gold-container", "prefix": "market/"}]
    metadata_targets: list[dict[str, str]] = []

    monkeypatch.setattr(system, "_resolve_purge_targets", lambda scope, layer, domain: [dict(t) for t in targets])
    monkeypatch.setattr(system, "_build_silver_checkpoint_reset_targets", lambda targets: [])
    monkeypatch.setattr(system, "_build_gold_checkpoint_reset_targets", lambda targets: [])
    monkeypatch.setattr(
        system,
        "_mark_purged_domain_metadata_snapshots",
        lambda targets: metadata_targets.extend(dict(item) for item in targets),
    )

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

        def has_blobs(self, prefix):
            return True

        def delete_prefix(self, prefix):
            return 1

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)

    payload = system.PurgeRequest(scope="layer-domain", layer="gold", domain="market", confirm=True)
    system._run_purge_operation(payload)

    assert metadata_targets == [{"layer": "gold", "domain": "market", "container": "gold-container"}]


def test_run_purge_operation_marks_all_layer_metadata_purged(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = [{"layer": "silver", "domain": None, "container": "silver-container", "prefix": None}]
    metadata_targets: list[dict[str, str]] = []

    monkeypatch.setattr(system, "_resolve_purge_targets", lambda scope, layer, domain: [dict(t) for t in targets])
    monkeypatch.setattr(system, "_build_silver_checkpoint_reset_targets", lambda targets: [])
    monkeypatch.setattr(system, "_build_gold_checkpoint_reset_targets", lambda targets: [])
    monkeypatch.setattr(
        system,
        "_mark_purged_domain_metadata_snapshots",
        lambda targets: metadata_targets.extend(dict(item) for item in targets),
    )

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

        def has_blobs(self, prefix):
            return True

        def delete_prefix(self, prefix):
            return 4

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)

    payload = system.PurgeRequest(scope="layer", layer="silver", confirm=True)
    system._run_purge_operation(payload)

    assert metadata_targets == [
        {"layer": "silver", "domain": "market", "container": "silver-container"},
        {"layer": "silver", "domain": "finance", "container": "silver-container"},
        {"layer": "silver", "domain": "earnings", "container": "silver-container"},
        {"layer": "silver", "domain": "price-target", "container": "silver-container"},
        {"layer": "silver", "domain": "government-signals", "container": "silver-container"},
    ]


def test_run_purge_operation_marks_all_domain_metadata_purged(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = [
        {"layer": "bronze", "domain": "market", "container": "bronze-container", "prefix": "market-data/"},
        {"layer": "silver", "domain": "market", "container": "silver-container", "prefix": "market-data/"},
        {"layer": "gold", "domain": "market", "container": "gold-container", "prefix": "market/"},
    ]
    metadata_targets: list[dict[str, str]] = []

    monkeypatch.setattr(system, "_resolve_purge_targets", lambda scope, layer, domain: [dict(t) for t in targets])
    monkeypatch.setattr(system, "_build_silver_checkpoint_reset_targets", lambda targets: [])
    monkeypatch.setattr(system, "_build_gold_checkpoint_reset_targets", lambda targets: [])
    monkeypatch.setattr(
        system,
        "_mark_purged_domain_metadata_snapshots",
        lambda targets: metadata_targets.extend(dict(item) for item in targets),
    )

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

        def has_blobs(self, prefix):
            return True

        def delete_prefix(self, prefix):
            return 2

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)

    payload = system.PurgeRequest(scope="domain", domain="market", confirm=True)
    system._run_purge_operation(payload)

    assert metadata_targets == [
        {"layer": "bronze", "domain": "market", "container": "bronze-container"},
        {"layer": "silver", "domain": "market", "container": "silver-container"},
        {"layer": "gold", "domain": "market", "container": "gold-container"},
    ]


def test_run_purge_symbol_operation_parallel_preserves_layer_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PURGE_SYMBOL_LAYER_MAX_WORKERS", "3")
    monkeypatch.setenv("PURGE_SYMBOL_TARGET_MAX_WORKERS", "1")
    monkeypatch.setattr(system, "_resolve_container", lambda layer: f"{layer}-container")

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(system, "_append_symbol_to_bronze_blacklists", lambda client, symbol: {"updated": 0, "paths": []})

    delays = {"bronze": 0.04, "silver": 0.02, "gold": 0.0}

    def _fake_bronze(client, symbol):
        time.sleep(delays["bronze"])
        return [{"layer": "bronze", "domain": "market", "container": client.container_name, "path": "b", "deleted": 1}]

    def _fake_layer(client, container, symbol, layer):
        time.sleep(delays[layer])
        return [{"layer": layer, "domain": "market", "container": container, "path": layer, "deleted": 1}]

    monkeypatch.setattr(system, "_remove_symbol_from_bronze_storage", _fake_bronze)
    monkeypatch.setattr(system, "_remove_symbol_from_layer_storage", _fake_layer)

    payload = system.PurgeSymbolRequest(symbol="AAPL", confirm=True)
    result = system._run_purge_symbol_operation(payload)

    target_layers = [entry.get("layer") for entry in result["targets"] if entry.get("operation") != "blacklist"]
    assert target_layers == ["bronze", "silver", "gold"]
