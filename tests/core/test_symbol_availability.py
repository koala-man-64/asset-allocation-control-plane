from __future__ import annotations

import pandas as pd
import pytest

from core import symbol_availability


class _FakeCursor:
    def __init__(self, *, existing_count: int = 0, disabled_count: int = 0) -> None:
        self.existing_count = existing_count
        self.disabled_count = disabled_count
        self.execute_calls: list[tuple[str, object]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.rowcount = 0
        self._fetchone_result: tuple[object, ...] | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))
        if "SELECT COUNT(*)" in sql:
            self._fetchone_result = (self.existing_count,)
            self.rowcount = 1
            return
        if "UPDATE core.symbols AS s" in sql:
            self.rowcount = self.disabled_count
            return
        self.rowcount = 0

    def executemany(self, sql: str, rows) -> None:
        self.executemany_calls.append((sql, list(rows)))

    def fetchone(self):
        return self._fetchone_result


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_get_symbol_availability_mask_uses_canonical_alpha_vantage_column() -> None:
    df = pd.DataFrame(
        [
            {"Symbol": "AAPL", "source_alpha_vantage": True},
            {"Symbol": "MSFT", "source_alpha_vantage": False},
            {"Symbol": "TSLA", "source_alpha_vantage": True},
        ]
    )

    mask = symbol_availability.get_symbol_availability_mask(df, "alpha_vantage")

    assert mask.tolist() == [True, False, True]


def test_get_domain_symbols_filters_by_canonical_alpha_vantage_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        symbol_availability.mdc,
        "get_symbols_from_db",
        lambda: pd.DataFrame(
            [
                {"Symbol": "AAPL", "source_alpha_vantage": True},
                {"Symbol": "MSFT", "source_alpha_vantage": False},
            ]
        ),
    )

    out = symbol_availability.get_domain_symbols("earnings")

    assert out["Symbol"].tolist() == ["AAPL"]
    assert out["source_alpha_vantage"].tolist() == [True]


def test_apply_availability_sync_inserts_new_symbols_and_disables_removed(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor(existing_count=1, disabled_count=2)
    captured: dict[str, pd.DataFrame] = {}

    monkeypatch.setattr(symbol_availability.mdc, "_ensure_symbols_tables", lambda _cur: None)
    monkeypatch.setattr(
        symbol_availability.mdc,
        "upsert_symbols_to_db",
        lambda df, cur=None: captured.setdefault("df", df.copy()),
    )

    inserted_count, disabled_count = symbol_availability._apply_availability_sync(
        cur,
        df_symbols=pd.DataFrame({"Symbol": ["AAPL", "^VIX"], "source_massive": [True, True]}),
        source_column="source_massive",
    )

    assert inserted_count == 1
    assert disabled_count == 2
    assert captured["df"]["Symbol"].tolist() == ["AAPL", "^VIX"]
    assert cur.executemany_calls[0][1] == [("AAPL",), ("^VIX",)]


def test_sync_domain_availability_uses_canonical_alpha_vantage_column(monkeypatch: pytest.MonkeyPatch) -> None:
    cur = _FakeCursor()
    applied: dict[str, pd.DataFrame] = {}

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://example")
    monkeypatch.setattr(
        symbol_availability,
        "_fetch_provider_symbols_df",
        lambda provider: pd.DataFrame([{"Symbol": "AAPL", "source_alpha_vantage": True}]),
    )
    monkeypatch.setattr(symbol_availability, "connect", lambda _dsn: _FakeConnection(cur))

    def _capture_apply(target_cur, *, df_symbols: pd.DataFrame, source_column: str) -> tuple[int, int]:
        assert target_cur is cur
        assert source_column == "source_alpha_vantage"
        applied["df"] = df_symbols.copy()
        return (1, 0)

    monkeypatch.setattr(symbol_availability, "_apply_availability_sync", _capture_apply)

    result = symbol_availability.sync_domain_availability("earnings")

    assert result.provider == "alpha_vantage"
    assert result.source_column == "source_alpha_vantage"
    assert applied["df"]["source_alpha_vantage"].tolist() == [True]
    assert any("INSERT INTO core.symbol_sync_state" in sql for sql, _ in cur.execute_calls)


def test_get_domain_symbols_market_filters_to_supported_asset_types(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        symbol_availability.mdc,
        "get_symbols_from_db",
        lambda: pd.DataFrame(
            [
                {"Symbol": "AAPL", "AssetType": "Stock", "source_massive": True},
                {"Symbol": "SPY", "AssetType": "ETF", "source_massive": True},
                {"Symbol": "ABRPD", "AssetType": "PFD", "source_massive": True},
                {"Symbol": "I:VIX", "AssetType": "INDEX", "source_massive": True},
                {"Symbol": "^VIX", "AssetType": "INDEX", "source_massive": True},
            ]
        ),
    )
    logged_messages: list[str] = []
    monkeypatch.setattr(symbol_availability.mdc, "write_line", lambda message: logged_messages.append(str(message)))

    out = symbol_availability.get_domain_symbols("market")

    assert out["Symbol"].tolist() == ["AAPL", "SPY", "^VIX"]
    assert out["AssetType"].tolist() == ["Stock", "ETF", "INDEX"]
    assert any("asset_type: PFD=1" in message for message in logged_messages)


def test_get_domain_symbols_finance_does_not_apply_market_asset_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        symbol_availability.mdc,
        "get_symbols_from_db",
        lambda: pd.DataFrame(
            [
                {"Symbol": "AAPL", "AssetType": "Stock", "source_massive": True},
                {"Symbol": "ABRPD", "AssetType": "PFD", "source_massive": True},
            ]
        ),
    )

    out = symbol_availability.get_domain_symbols("finance")

    assert out["Symbol"].tolist() == ["AAPL", "ABRPD"]
