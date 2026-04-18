from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.market_data import delta_core
from monitoring.domain_metadata import (
    _count_finance_symbols_from_listing,
    collect_delta_table_metadata,
    collect_domain_metadata,
)
from deltalake.exceptions import TableNotFoundError


def test_collect_delta_table_metadata_reports_rows_and_date_range(tmp_path) -> None:
    # Use the test storage redirection fixture (see tests/conftest.py) which patches delta URIs to local paths.
    container = "test-container"
    table_path = "market-data/AAPL"

    df = pd.DataFrame(
        {
            "symbol": ["A", "B", "A", "C"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]),
            "value": [1, 2, 3, 4],
        }
    )

    delta_core.store_delta(df, container=container, path=table_path, mode="overwrite")

    meta = collect_delta_table_metadata(container, table_path)
    assert meta["totalRows"] == 4
    assert meta["fileCount"] >= 1
    assert meta["totalBytes"] > 0
    assert meta["deltaVersion"] >= 0

    date_range = meta["dateRange"]
    assert date_range is not None
    assert date_range["source"] == "stats"
    assert date_range["column"] in {"date", "Date"}

    min_dt = datetime.fromisoformat(date_range["min"]).astimezone(timezone.utc)
    max_dt = datetime.fromisoformat(date_range["max"]).astimezone(timezone.utc)
    assert min_dt.date().isoformat() == "2024-01-01"
    assert max_dt.date().isoformat() == "2024-01-04"


def test_collect_delta_table_metadata_parses_string_date_stats(monkeypatch) -> None:
    class _FakeStructArray:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_pylist(self) -> list[dict[str, object]]:
            return self._rows

    class _FakeActions:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_struct_array(self) -> _FakeStructArray:
            return _FakeStructArray(self._rows)

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 7

        def get_add_actions(self, *args, **kwargs) -> _FakeActions:
            return _FakeActions(
                [
                    {
                        "num_records": 2,
                        "size_bytes": 120,
                        "min.date": "2024-01-01",
                        "max.date": "2024-01-03",
                    },
                    {
                        "num_records": 3,
                        "size_bytes": 220,
                        "min.date": "2024-01-04",
                        "max.date": "2024-01-10",
                    },
                ]
            )

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "stats"
    assert meta["dateRange"]["column"] == "date"
    assert meta["totalRows"] == 5
    assert meta["fileCount"] == 2
    assert meta["totalBytes"] == 340
    assert meta["deltaVersion"] == 7

    min_dt = datetime.fromisoformat(meta["dateRange"]["min"]).astimezone(timezone.utc)
    max_dt = datetime.fromisoformat(meta["dateRange"]["max"]).astimezone(timezone.utc)
    assert min_dt.date().isoformat() == "2024-01-01"
    assert max_dt.date().isoformat() == "2024-01-10"


def _fake_add_action_factory() -> type:
    """Build a fake add-actions object for collect_delta_table_metadata tests."""

    class _FakeStructArray:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_pylist(self) -> list[dict[str, object]]:
            return self._rows

    class _FakeActions:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_struct_array(self) -> _FakeStructArray:
            return _FakeStructArray(self._rows)

    return _FakeActions


def test_collect_delta_table_metadata_uses_partition_date_when_available(monkeypatch) -> None:
    fake_actions = _fake_add_action_factory()

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 3

        def get_add_actions(self, *args, **kwargs):
            rows = [
                {
                    "num_records": 1,
                    "size_bytes": 100,
                    "partition.Date": "2024-01-03T00:00:00",
                    "path": "part-1",
                },
                {
                    "num_records": 2,
                    "size_bytes": 120,
                    "partition.date": "2024-01-01T00:00:00",
                    "path": "part-2",
                },
            ]
            return fake_actions(rows)

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "partition"
    assert meta["dateRange"]["column"] == "Date"
    assert datetime.fromisoformat(meta["dateRange"]["min"]).date().isoformat() == "2024-01-03"
    assert datetime.fromisoformat(meta["dateRange"]["max"]).date().isoformat() == "2024-01-03"


def test_collect_delta_table_metadata_prefers_partition_over_stats(monkeypatch) -> None:
    fake_actions = _fake_add_action_factory()

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 9

        def get_add_actions(self, flatten: bool = False, *args, **kwargs):
            if flatten is False:
                return fake_actions(
                    [
                        {
                            "num_records": 1,
                            "size_bytes": 120,
                            "partition": {"Date": "2024-01-05"},
                            "path": "part-3",
                            "min": {"date": "2024-02-01"},
                            "max": {"date": "2024-02-10"},
                        }
                    ]
                )

            return fake_actions(
                [
                    {
                        "num_records": 1,
                        "size_bytes": 120,
                        "partition.Date": "2024-01-01",
                        "min.date": "2024-02-01",
                        "max.date": "2024-02-10",
                    }
                ]
            )

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "partition"
    assert meta["dateRange"]["column"] == "Date"
    assert datetime.fromisoformat(meta["dateRange"]["min"]).date().isoformat() == "2024-01-01"
    assert datetime.fromisoformat(meta["dateRange"]["max"]).date().isoformat() == "2024-01-01"


def test_collect_delta_table_metadata_uses_partition_values(monkeypatch) -> None:
    fake_actions = _fake_add_action_factory()

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 11

        def get_add_actions(self, *args, **kwargs):
            return fake_actions(
                [
                    {
                        "num_records": 2,
                        "size_bytes": 160,
                        "partition_values": {"date": "2024-05-01"},
                    },
                    {
                        "num_records": 1,
                        "size_bytes": 80,
                        "partition_values": {"Date": "2024-05-03"},
                    },
                ]
            )

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "partition"
    assert meta["dateRange"]["column"] == "Date"
    assert datetime.fromisoformat(meta["dateRange"]["min"]).date().isoformat() == "2024-05-03"
    assert datetime.fromisoformat(meta["dateRange"]["max"]).date().isoformat() == "2024-05-03"


def test_collect_delta_table_metadata_handles_no_files_in_log_segment(monkeypatch) -> None:
    def _raise(*_args, **_kwargs) -> None:
        raise TableNotFoundError("Generic delta kernel error: No files in log segment")

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _raise)

    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert meta["deltaVersion"] is None
    assert meta["fileCount"] == 0
    assert meta["totalBytes"] == 0
    assert meta["totalRows"] == 0
    assert meta["dateRange"] is None
    assert warnings == ["Delta table not readable at market-data/AAPL; no commit files found in _delta_log yet."]


def test_count_finance_symbols_from_listing_dedupes_and_tracks_subfolders() -> None:
    class _Blob:
        def __init__(self, name: str, size: int = 1) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "finance-data/"
            return [
                _Blob("finance-data/balance_sheet/AAPL_quarterly_balance-sheet/_delta_log/00000000000000000000.json"),
                _Blob("finance-data/income_statement/AAPL_quarterly_financials/_delta_log/00000000000000000000.json"),
                _Blob("finance-data/cash_flow/MSFT_quarterly_cash-flow/_delta_log/00000000000000000000.json"),
                _Blob("finance-data/valuation/MSFT_quarterly_valuation_measures/_delta_log/00000000000000000000.json"),
                _Blob("finance-data/Balance Sheet/NVDA_quarterly_balance-sheet.json"),
                _Blob("finance-data/balance_sheet/not-a-table/_delta_log/00000000000000000000.json"),
            ]

    class _Client:
        container_name = "test-container"
        container_client = _ContainerClient()

    symbol_count, subfolder_counts, truncated = _count_finance_symbols_from_listing(
        _Client(),
        prefix="finance-data/",
        max_scanned_blobs=200_000,
    )

    assert symbol_count == 3
    assert subfolder_counts == {
        "balance_sheet": 2,
        "income_statement": 1,
        "cash_flow": 1,
        "valuation": 1,
    }
    assert truncated is False


def test_collect_domain_metadata_counts_symbols_for_silver_finance(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "finance-data/"
            return [
                _Blob(
                    "finance-data/balance_sheet/AAPL_quarterly_balance-sheet/_delta_log/00000000000000000000.json", 10
                ),
                _Blob(
                    "finance-data/income_statement/AAPL_quarterly_financials/_delta_log/00000000000000000000.json", 11
                ),
                _Blob("finance-data/cash_flow/MSFT_quarterly_cash-flow/_delta_log/00000000000000000000.json", 12),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            raise AssertionError("download_data should not be called for silver symbol counting.")

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT"},
    )

    payload = collect_domain_metadata(layer="silver", domain="finance")

    assert payload["layer"] == "silver"
    assert payload["domain"] == "finance"
    assert payload["type"] == "blob"
    assert payload["symbolCount"] == 2
    assert payload["financeSubfolderSymbolCounts"] == {
        "balance_sheet": 1,
        "income_statement": 1,
        "cash_flow": 1,
        "valuation": 0,
    }


def test_collect_domain_metadata_force_refresh_skips_process_cache(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setattr(
        "monitoring.domain_metadata._read_cached_domain_metadata",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("process cache should be bypassed")),
    )
    monkeypatch.setattr(
        "monitoring.domain_metadata._artifact_domain_metadata_payload",
        lambda **_kwargs: {
            "layer": "silver",
            "domain": "market",
            "container": "silver-container",
            "type": "blob",
            "computedAt": "2026-03-16T00:00:00+00:00",
            "symbolCount": 4,
            "columnCount": 3,
            "columns": ["date", "symbol", "close"],
            "warnings": [],
        },
    )
    monkeypatch.setattr("monitoring.domain_metadata._blob_prefix", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("monitoring.domain_metadata._cache_domain_metadata", lambda *_args, **_kwargs: None)

    payload = collect_domain_metadata(layer="silver", domain="market", force_refresh=True)

    assert payload["layer"] == "silver"
    assert payload["domain"] == "market"
    assert payload["symbolCount"] == 4
    assert payload["columnCount"] == 3


def test_collect_domain_metadata_uses_file_count_for_bronze_market_symbols(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return [
                _Blob("market-data/buckets/A.parquet", 10),
                _Blob("market-data/buckets/M.parquet", 11),
                _Blob("market-data/whitelist.csv", 2),
                _Blob("market-data/blacklist.csv", 2),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, path: str):
            if path.endswith("whitelist.csv"):
                return b"Symbol\n"
            if path.endswith("blacklist.csv"):
                return b"Symbol\nZZZZ\n"
            return None

    monkeypatch.setenv("AZURE_CONTAINER_BRONZE", "bronze-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.bronze_bucketing.load_symbol_set",
        lambda domain: {"AAPL", "MSFT"} if domain == "market" else set(),
    )

    payload = collect_domain_metadata(layer="bronze", domain="market")

    assert payload["layer"] == "bronze"
    assert payload["domain"] == "market"
    assert payload["type"] == "blob"
    assert payload["fileCount"] == 4
    assert payload["symbolCount"] == 2
    assert payload["blacklistedSymbolCount"] == 1


def test_collect_domain_metadata_reports_folder_last_modified(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int, last_modified: datetime) -> None:
            self.name = name
            self.size = size
            self.last_modified = last_modified

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return [
                _Blob(
                    "market-data/buckets/A.parquet",
                    10,
                    datetime(2026, 2, 25, 12, 0, tzinfo=timezone.utc),
                ),
                _Blob(
                    "market-data/buckets/M.parquet",
                    11,
                    datetime(2026, 2, 26, 8, 16, tzinfo=timezone.utc),
                ),
                _Blob(
                    "market-data/whitelist.csv",
                    2,
                    datetime(2026, 2, 24, 9, 0, tzinfo=timezone.utc),
                ),
                _Blob(
                    "market-data/blacklist.csv",
                    2,
                    datetime(2026, 2, 24, 10, 0, tzinfo=timezone.utc),
                ),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, path: str):
            if path.endswith("whitelist.csv"):
                return b"Symbol\n"
            if path.endswith("blacklist.csv"):
                return b"Symbol\nZZZZ\n"
            return None

    monkeypatch.setenv("AZURE_CONTAINER_BRONZE", "bronze-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)

    payload = collect_domain_metadata(layer="bronze", domain="market")

    assert payload["folderLastModified"] == "2026-02-26T08:16:00+00:00"


def test_collect_domain_metadata_uses_file_count_for_bronze_price_target_symbols(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "price-target-data/"
            return [
                _Blob("price-target-data/AAPL.parquet", 10),
                _Blob("price-target-data/MSFT.parquet", 11),
                _Blob("price-target-data/whitelist.csv", 2),
                _Blob("price-target-data/blacklist.csv", 2),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, path: str):
            if path.endswith("whitelist.csv"):
                return b"Symbol\n"
            if path.endswith("blacklist.csv"):
                return b"Symbol\nZZZZ\n"
            return None

    monkeypatch.setenv("AZURE_CONTAINER_BRONZE", "bronze-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.bronze_bucketing.load_symbol_set",
        lambda domain: {"AAPL", "MSFT"} if domain == "price-target" else set(),
    )

    payload = collect_domain_metadata(layer="bronze", domain="price-target")

    assert payload["layer"] == "bronze"
    assert payload["domain"] == "price-target"
    assert payload["type"] == "blob"
    assert payload["fileCount"] == 4
    assert payload["symbolCount"] == 2
    assert payload["blacklistedSymbolCount"] == 1


def test_collect_domain_metadata_uses_file_count_for_bronze_earnings_symbols(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "earnings-data/"
            return [
                _Blob("earnings-data/AAPL.json", 10),
                _Blob("earnings-data/MSFT.json", 11),
                _Blob("earnings-data/whitelist.csv", 2),
                _Blob("earnings-data/blacklist.csv", 2),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, path: str):
            if path.endswith("whitelist.csv"):
                # Ensure whitelist size does not override file-based symbol count.
                return b"Symbol\nAAPL\nMSFT\nNVDA\n"
            if path.endswith("blacklist.csv"):
                return b"Symbol\nZZZZ\n"
            return None

    monkeypatch.setenv("AZURE_CONTAINER_BRONZE", "bronze-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.bronze_bucketing.load_symbol_set",
        lambda domain: {"AAPL", "MSFT"} if domain == "earnings" else set(),
    )

    payload = collect_domain_metadata(layer="bronze", domain="earnings")

    assert payload["layer"] == "bronze"
    assert payload["domain"] == "earnings"
    assert payload["type"] == "blob"
    assert payload["fileCount"] == 4
    assert payload["symbolCount"] == 2
    assert payload["blacklistedSymbolCount"] == 1


def test_collect_domain_metadata_uses_listing_count_for_bronze_finance_symbols(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "finance-data/"
            return [
                _Blob("finance-data/balance_sheet/AAPL_quarterly_balance-sheet.json", 10),
                _Blob("finance-data/income_statement/AAPL_quarterly_financials.json", 11),
                _Blob("finance-data/cash_flow/MSFT_quarterly_cash-flow.json", 12),
                _Blob("finance-data/valuation/MSFT_quarterly_valuation_measures.json", 13),
                _Blob("finance-data/whitelist.csv", 2),
                _Blob("finance-data/blacklist.csv", 2),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, path: str):
            if path.endswith("whitelist.csv"):
                return b"Symbol\n"
            if path.endswith("blacklist.csv"):
                return b"Symbol\nZZZZ\n"
            return None

    monkeypatch.setenv("AZURE_CONTAINER_BRONZE", "bronze-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.bronze_bucketing.load_symbol_set",
        lambda domain: {"AAPL", "MSFT"} if domain == "finance" else set(),
    )

    payload = collect_domain_metadata(layer="bronze", domain="finance")

    assert payload["layer"] == "bronze"
    assert payload["domain"] == "finance"
    assert payload["type"] == "blob"
    assert payload["fileCount"] == 6
    assert payload["symbolCount"] == 2
    assert payload["financeSubfolderSymbolCounts"] == {
        "balance_sheet": 1,
        "income_statement": 1,
        "cash_flow": 1,
        "valuation": 1,
    }
    assert payload["blacklistedSymbolCount"] == 1

def test_collect_domain_metadata_uses_alpha26_index_for_silver_market(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return [
                _Blob("market-data/buckets/A/_delta_log/00000000000000000000.json", 10),
                _Blob("market-data/buckets/M/_delta_log/00000000000000000000.json", 11),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr("monitoring.domain_metadata.layer_bucketing.is_silver_alpha26_mode", lambda: True)
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT", "NVDA"},
    )

    payload = collect_domain_metadata(layer="silver", domain="market")

    assert payload["layer"] == "silver"
    assert payload["domain"] == "market"
    assert payload["symbolCount"] == 3


def test_collect_domain_metadata_falls_back_to_bucket_artifacts_for_gold_finance(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "finance/"
            return [
                _Blob("finance/buckets/A/_delta_log/00000000000000000000.json", 10),
                _Blob("finance/buckets/B/_delta_log/00000000000000000000.json", 11),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr("monitoring.domain_metadata.layer_bucketing.gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_domain_artifact",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: set(),
    )
    monkeypatch.setattr("monitoring.domain_metadata.bronze_bucketing.ALPHABET_BUCKETS", ("A", "B"))
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_bucket_artifact",
        lambda *, layer, domain, bucket, client=None, sub_domain=None: (
            {"symbolCount": 2}
            if bucket == "A"
            else {"symbolCount": 3}
            if bucket == "B"
            else None
        ),
    )

    payload = collect_domain_metadata(layer="gold", domain="finance")

    assert payload["fileCount"] == 2
    assert payload["symbolCount"] == 5
    assert payload["financeSubfolderSymbolCounts"] is None
    assert any("symbol count derived from bucket artifacts" in warning for warning in payload["warnings"])


def test_collect_domain_metadata_falls_back_to_bucket_artifacts_for_gold_market(monkeypatch) -> None:
    class _Blob:
        def __init__(self, name: str, size: int) -> None:
            self.name = name
            self.size = size

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market/"
            return [
                _Blob("market/buckets/A/_delta_log/00000000000000000000.json", 10),
                _Blob("market/buckets/B/_delta_log/00000000000000000000.json", 11),
            ]

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr("monitoring.domain_metadata.layer_bucketing.gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_domain_artifact",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: set(),
    )
    monkeypatch.setattr("monitoring.domain_metadata.bronze_bucketing.ALPHABET_BUCKETS", ("A", "B"))
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_bucket_artifact",
        lambda *, layer, domain, bucket, client=None, sub_domain=None: (
            {"symbolCount": 4}
            if bucket == "A"
            else {"symbolCount": 6}
            if bucket == "B"
            else None
        ),
    )

    payload = collect_domain_metadata(layer="gold", domain="market")

    assert payload["fileCount"] == 2
    assert payload["symbolCount"] == 10
    assert any("symbol count derived from bucket artifacts" in warning for warning in payload["warnings"])


def test_collect_domain_metadata_prefers_writer_artifact(monkeypatch) -> None:
    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_domain_artifact",
        lambda *, layer, domain, client=None, sub_domain=None: {
            "symbolCount": 4,
            "columns": ["date", "symbol", "close"],
            "columnCount": 3,
            "totalBytes": 2048,
            "dateRange": {
                "min": "2026-01-01T00:00:00+00:00",
                "max": "2026-03-01T00:00:00+00:00",
                "column": "date",
                "source": "artifact",
            },
            "artifactPath": "market-data/_metadata/domain.json",
        },
    )

    payload = collect_domain_metadata(layer="silver", domain="market")

    assert payload["symbolCount"] == 4
    assert payload["columns"] == ["date", "symbol", "close"]
    assert payload["columnCount"] == 3
    assert payload["dateRange"]["source"] == "artifact"
    assert payload["totalBytes"] == 2048
    assert payload["metadataPath"] == "market-data/_metadata/domain.json"
    assert payload["metadataSource"] == "artifact"
    assert payload["fileCount"] is None


def test_collect_domain_metadata_reports_zero_symbols_when_target_prefix_is_empty(monkeypatch) -> None:
    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            return []

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT", "NVDA"},
    )

    payload = collect_domain_metadata(layer="silver", domain="market")

    assert payload["fileCount"] == 0
    assert payload["symbolCount"] == 0


def test_collect_domain_metadata_does_not_use_bucket_artifacts_when_gold_prefix_is_empty(monkeypatch) -> None:
    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "finance/"
            return []

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr("monitoring.domain_metadata.layer_bucketing.gold_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_domain_artifact",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: set(),
    )
    monkeypatch.setattr(
        "monitoring.domain_metadata.domain_artifacts.load_bucket_artifact",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("bucket artifacts should not be loaded")),
    )

    payload = collect_domain_metadata(layer="gold", domain="finance")

    assert payload["fileCount"] == 0
    assert payload["symbolCount"] == 0


def test_collect_domain_metadata_reports_zero_symbols_when_listing_prefix_not_found(monkeypatch) -> None:
    class _NotFoundError(Exception):
        status_code = 404

    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            raise _NotFoundError("Container not found")

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT", "NVDA"},
    )

    payload = collect_domain_metadata(layer="silver", domain="market")

    assert payload["fileCount"] == 0
    assert payload["symbolCount"] == 0


def test_collect_domain_metadata_keeps_symbol_count_unknown_when_listing_fails(monkeypatch) -> None:
    class _ContainerClient:
        def list_blobs(self, *, name_starts_with: str):
            assert name_starts_with == "market-data/"
            raise RuntimeError("Listing failed")

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists
            self.container_client = _ContainerClient()

        def download_data(self, _path: str):
            return None

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setenv("DOMAIN_METADATA_CACHE_TTL_SECONDS", "0")
    monkeypatch.setattr("monitoring.domain_metadata.BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(
        "monitoring.domain_metadata.layer_bucketing.load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT", "NVDA"},
    )

    payload = collect_domain_metadata(layer="silver", domain="market")

    assert payload["fileCount"] is None
    assert payload["symbolCount"] is None
    assert any("symbol count set to unknown" in warning for warning in payload["warnings"])
