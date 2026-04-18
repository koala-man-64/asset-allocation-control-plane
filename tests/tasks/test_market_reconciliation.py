from __future__ import annotations

from asset_allocation_runtime_common.market_data import layer_bucketing
from tasks.common.market_reconciliation import (
    collect_bronze_earnings_symbols_from_blob_infos,
    collect_bronze_finance_symbols_from_blob_infos,
    collect_delta_silver_finance_symbols,
    collect_bronze_price_target_symbols_from_blob_infos,
    collect_bronze_market_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)
import tasks.common.market_reconciliation as reconciliation
import pandas as pd


def test_collect_bronze_market_symbols_ignores_non_symbol_files(monkeypatch) -> None:
    monkeypatch.setattr(
        reconciliation,
        "_load_index_symbols",
        lambda domain: {"AAPL", "MSFT"} if domain == "market" else set(),
    )
    blob_infos = [
        {"name": "market-data/buckets/A.parquet"},
        {"name": "market-data/buckets/M.parquet"},
        {"name": "market-data/whitelist.csv"},
        {"name": "market-data/blacklist.csv"},
        {"name": "market-data/README.txt"},
        {"name": "system/bronze-index/market/latest.parquet"},
    ]

    symbols = collect_bronze_market_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT"}


def test_collect_delta_market_symbols_reads_from_layer_index(monkeypatch) -> None:
    monkeypatch.setattr(
        layer_bucketing,
        "load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT"}
        if (layer, domain, sub_domain) == ("silver", "market", None)
        else set(),
    )

    symbols = collect_delta_market_symbols(client=object(), root_prefix="market-data")

    assert symbols == {"AAPL", "MSFT"}


def test_purge_orphan_rows_from_bucket_tables_rewrites_and_deletes() -> None:
    saved: dict[str, pd.DataFrame] = {}
    deleted_paths: list[str] = []

    def _delete_prefix(path: str) -> int:
        deleted_paths.append(path)
        return 2

    def _load_table(path: str) -> pd.DataFrame | None:
        if path.endswith("/M"):
            return pd.DataFrame({"symbol": ["META", "MSFT"], "date": [pd.Timestamp("2024-01-10")] * 2})
        if path.endswith("/N"):
            return pd.DataFrame({"symbol": ["NVDA"], "date": [pd.Timestamp("2024-01-10")]})
        return None

    def _store_table(df: pd.DataFrame, path: str) -> None:
        saved[path] = df.copy()

    orphan_symbols, stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols={"AAPL", "META"},
        downstream_symbols={"AAPL", "META", "MSFT", "NVDA"},
        table_paths_for_symbol=lambda symbol: [f"market-data/buckets/{symbol[0]}"],
        load_table=_load_table,
        store_table=_store_table,
        delete_prefix=_delete_prefix,
        symbol_column_candidates=("symbol",),
    )

    assert orphan_symbols == ["MSFT", "NVDA"]
    assert list(saved.keys()) == ["market-data/buckets/M"]
    assert saved["market-data/buckets/M"]["symbol"].tolist() == ["META"]
    assert deleted_paths == ["market-data/buckets/N"]
    assert stats.tables_scanned == 2
    assert stats.tables_rewritten == 1
    assert stats.deleted_blobs == 2
    assert stats.rows_deleted == 2
    assert stats.errors == 0


def test_collect_delta_silver_finance_symbols_reads_from_layer_index(monkeypatch) -> None:
    monkeypatch.setattr(
        layer_bucketing,
        "load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: {"AAPL", "MSFT", "NVDA", "TSLA"}
        if (layer, domain, sub_domain) == ("silver", "finance", None)
        else set(),
    )

    symbols = collect_delta_silver_finance_symbols(client=object())

    assert symbols == {"AAPL", "MSFT", "NVDA", "TSLA"}


def test_collect_bronze_earnings_symbols_extracts_json_symbols(monkeypatch) -> None:
    monkeypatch.setattr(
        reconciliation,
        "_load_index_symbols",
        lambda domain: {"AAPL", "MSFT"} if domain == "earnings" else set(),
    )
    blob_infos = [
        {"name": "earnings-data/AAPL.json"},
        {"name": "earnings-data/MSFT.json"},
        {"name": "earnings-data/whitelist.csv"},
        {"name": "earnings-data/not_json.parquet"},
    ]

    symbols = collect_bronze_earnings_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT"}


def test_collect_bronze_price_target_symbols_extracts_parquet_symbols(monkeypatch) -> None:
    monkeypatch.setattr(
        reconciliation,
        "_load_index_symbols",
        lambda domain: {"AAPL", "MSFT"} if domain == "price-target" else set(),
    )
    blob_infos = [
        {"name": "price-target-data/AAPL.parquet"},
        {"name": "price-target-data/MSFT.parquet"},
        {"name": "price-target-data/not_parquet.json"},
    ]

    symbols = collect_bronze_price_target_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT"}


def test_collect_bronze_finance_symbols_extracts_known_suffixes(monkeypatch) -> None:
    monkeypatch.setattr(
        reconciliation,
        "_load_index_symbols",
        lambda domain: {"AAPL", "MSFT", "NVDA"} if domain == "finance" else set(),
    )
    blob_infos = [
        {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"},
        {"name": "finance-data/Income Statement/AAPL_quarterly_financials.json"},
        {"name": "finance-data/Cash Flow/MSFT_quarterly_cash-flow.json"},
        {"name": "finance-data/Valuation/NVDA_quarterly_valuation_measures.json"},
        {"name": "finance-data/Valuation/NVDA_other_suffix.json"},
        {"name": "finance-data/blacklist.csv"},
    ]

    symbols = collect_bronze_finance_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT", "NVDA"}


def test_enforce_backfill_cutoff_on_bucket_tables_rewrites_and_deletes() -> None:
    saved: dict[str, pd.DataFrame] = {}
    deleted_paths: list[str] = []
    vacuumed_paths: list[str] = []

    def _load_table(path: str) -> pd.DataFrame | None:
        if path.endswith("/A"):
            return pd.DataFrame(
                {
                    "Date": [pd.Timestamp("2015-12-31"), pd.Timestamp("2016-01-02")],
                    "value": [1, 2],
                }
            )
        if path.endswith("/M"):
            return pd.DataFrame({"Date": [pd.Timestamp("2015-12-30")], "value": [7]})
        return None

    def _store_table(df: pd.DataFrame, path: str) -> None:
        saved[path] = df.copy()

    def _delete_prefix(path: str) -> int:
        deleted_paths.append(path)
        return 3

    def _vacuum(path: str) -> None:
        vacuumed_paths.append(path)

    stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=["market-data/buckets/A", "market-data/buckets/M"],
        load_table=_load_table,
        store_table=_store_table,
        delete_prefix=_delete_prefix,
        date_column_candidates=("date", "obs_date"),
        backfill_start=pd.Timestamp("2016-01-01"),
        context="test cutoff",
        vacuum_table=_vacuum,
    )

    assert stats.tables_scanned == 2
    assert stats.tables_rewritten == 1
    assert stats.deleted_blobs == 3
    assert stats.rows_dropped == 2
    assert stats.errors == 0

    assert list(saved.keys()) == ["market-data/buckets/A"]
    assert pd.to_datetime(saved["market-data/buckets/A"]["Date"]).min() == pd.Timestamp("2016-01-02")
    assert deleted_paths == ["market-data/buckets/M"]
    assert vacuumed_paths == ["market-data/buckets/A"]


def test_enforce_backfill_cutoff_on_bucket_tables_handles_missing_date_column() -> None:
    stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=["market-data/buckets/A"],
        load_table=lambda _path: pd.DataFrame({"close": [1.0, 2.0]}),
        store_table=lambda _df, _path: None,
        delete_prefix=lambda _path: 0,
        date_column_candidates=("date", "obs_date"),
        backfill_start=pd.Timestamp("2016-01-01"),
        context="test cutoff",
        vacuum_table=None,
    )

    assert stats.tables_scanned == 1
    assert stats.tables_rewritten == 0
    assert stats.deleted_blobs == 0
    assert stats.rows_dropped == 0
    assert stats.errors == 0
