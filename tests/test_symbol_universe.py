import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone

import asset_allocation_runtime_common.market_data.core as core_module
from asset_allocation_runtime_common.market_data.core import (
    _parse_alpha_vantage_listing_status_csv,
    _symbols_refresh_due,
    merge_symbol_sources,
    strip_source_availability_columns,
    upsert_symbols_to_db,
)


def test_parse_alpha_vantage_listing_status_filters_active_stock():
    csv_text = """symbol,name,exchange,assetType,ipoDate,delistingDate,status
AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active
ETF1,Example ETF,NYSE,ETF,2000-01-01,null,Active
OLD,Old Co,NYSE,Stock,1990-01-01,2020-01-01,Delisted
"""
    df = _parse_alpha_vantage_listing_status_csv(csv_text)
    assert set(df["Symbol"].tolist()) == {"AAPL"}
    assert "Exchange" in df.columns
    assert "AssetType" in df.columns
    assert "Status" in df.columns


def test_merge_symbol_sources_prefers_nasdaq_name_and_keeps_massive_metadata():
    df_nasdaq = pd.DataFrame([{"Symbol": "AAPL", "Name": "Apple Inc", "Sector": "Tech"}])
    df_massive = pd.DataFrame(
        [{"Symbol": "AAPL", "Name": "APPLE", "Exchange": "NASDAQ", "AssetType": "CS"}]
    )
    df_av = pd.DataFrame(
        [{"Symbol": "AAPL", "Name": "Apple Alpha", "Exchange": "NASDAQ", "AssetType": "Stock", "Status": "Active"}]
    )
    merged = merge_symbol_sources(df_nasdaq, df_massive, df_alpha_vantage=df_av)
    row = merged[merged["Symbol"] == "AAPL"].iloc[0]

    assert row["Name"] == "Apple Inc"
    assert row["Exchange"] == "NASDAQ"
    assert row["source_nasdaq"] == True
    assert row["source_massive"] == True
    assert row["source_alpha_vantage"] == True
    assert "source_alphavantage" not in merged.columns
    assert "source" not in merged.columns


def test_merge_symbol_sources_includes_alpha_only_symbols():
    df_nasdaq = pd.DataFrame(columns=["Symbol", "Name"])
    df_massive = pd.DataFrame(columns=["Symbol", "Name"])
    df_av = pd.DataFrame(
        [{"Symbol": "NEW", "Name": "New Co", "Exchange": "NYSE", "AssetType": "Stock", "Status": "Active"}]
    )

    merged = merge_symbol_sources(df_nasdaq, df_massive, df_alpha_vantage=df_av)
    assert set(merged["Symbol"].tolist()) == {"NEW"}
    row = merged.iloc[0]
    assert row["source_nasdaq"] == False
    assert row["source_massive"] == False
    assert row["source_alpha_vantage"] == True
    assert "source_alphavantage" not in merged.columns


class _FakeCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.fetchone_result: tuple[object, ...] | None = None

    def executemany(self, sql: str, rows) -> None:
        self.executemany_calls.append((sql, list(rows)))

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        return self.fetchone_result


def test_upsert_symbols_coerces_source_flags_to_bool():
    cur = _FakeCursor()
    df_symbols = pd.DataFrame(
        [
            {
                "Symbol": "AAPL",
                "source_nasdaq": True,
                "source_massive": True,
                "source_alpha_vantage": True,
            },
            {
                "Symbol": "SPY",
                "source_nasdaq": float("nan"),
                "source_massive": float("nan"),
                "source_alpha_vantage": float("nan"),
            },
            {
                "Symbol": "QQQ",
                "source_nasdaq": 1.0,
                "source_massive": 1.0,
                "source_alpha_vantage": 0.0,
            },
        ]
    )

    upsert_symbols_to_db(df_symbols, cur=cur)

    assert len(cur.executemany_calls) == 1
    sql, rows = cur.executemany_calls[0]
    assert "INSERT INTO core.symbols AS s" in sql
    assert "source_nasdaq" in sql
    assert "source_massive" in sql
    assert "source_alpha_vantage" in sql

    # row tuple shape: (symbol, source_nasdaq, source_massive, source_alpha_vantage)
    assert rows[0][1] is True
    assert rows[0][2] is True
    assert rows[0][3] is True
    assert rows[1][1] is False
    assert rows[1][2] is False
    assert rows[1][3] is False
    assert rows[2][1] is True
    assert rows[2][2] is True
    assert rows[2][3] is False


def test_symbols_refresh_due_when_never_refreshed():
    cur = _FakeCursor()
    cur.fetchone_result = (None,)
    assert _symbols_refresh_due(cur, interval_hours=24.0) is True


def test_symbols_refresh_due_false_when_recent():
    cur = _FakeCursor()
    cur.fetchone_result = (datetime.now(timezone.utc) - timedelta(hours=1),)
    assert _symbols_refresh_due(cur, interval_hours=24.0) is False


def test_symbols_refresh_due_true_when_stale():
    cur = _FakeCursor()
    cur.fetchone_result = (datetime.now(timezone.utc) - timedelta(hours=36),)
    assert _symbols_refresh_due(cur, interval_hours=24.0) is True


def test_strip_source_availability_columns_removes_provider_flags():
    df = pd.DataFrame(
        [
            {
                "Symbol": "AAPL",
                "Name": "Apple",
                "source_nasdaq": True,
                "source_massive": True,
                "source_alpha_vantage": True,
            }
        ]
    )

    stripped = strip_source_availability_columns(df)

    assert list(stripped.columns) == ["Symbol", "Name"]


def test_get_symbols_no_longer_calls_sync_symbols_to_db(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(core_module, "refresh_symbols_to_db_if_due", lambda: None)
    monkeypatch.setattr(core_module, "get_symbols_from_db", lambda: pd.DataFrame({"Symbol": ["AAPL"]}))
    monkeypatch.setattr(core_module.cfg, "TICKERS_TO_ADD", [])
    monkeypatch.setattr(
        core_module,
        "sync_symbols_to_db",
        lambda _df: (_ for _ in ()).throw(AssertionError("sync_symbols_to_db should not be called")),
    )

    df = core_module.get_symbols()

    assert df["Symbol"].tolist() == ["AAPL"]
