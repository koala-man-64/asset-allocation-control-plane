import pandas as pd

import api.data_service as data_service_module
from api.data_service import DataService
from core.pipeline import DataPaths


def test_delta_nan_values_are_json_safe(monkeypatch):
    monkeypatch.setattr(
        data_service_module.delta_core,
        "load_delta",
        lambda _container, _path: pd.DataFrame([{"symbol": "AAPL", "eps": 1.23}, {"symbol": "AAPL", "eps": float("nan")}]),
    )

    rows = DataService.get_data("silver", "earnings", ticker="AAPL", limit=2)

    assert len(rows) == 2
    assert rows[1]["eps"] is None


def test_delta_inf_values_are_json_safe(monkeypatch):
    monkeypatch.setattr(
        data_service_module.delta_core,
        "load_delta",
        lambda _container, _path: pd.DataFrame(
            [{"symbol": "AAPL", "eps": float("inf")}, {"symbol": "AAPL", "eps": float("-inf")}]
        ),
    )

    rows = DataService.get_data("silver", "earnings", ticker="AAPL", limit=2)

    assert len(rows) == 2
    assert rows[0]["eps"] is None
    assert rows[1]["eps"] is None


def test_finance_regular_folders_are_supported_for_silver(monkeypatch):
    calls = []

    monkeypatch.setattr(
        DataService,
        "_discover_delta_table_paths",
        lambda _container, _prefix: [
            "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
            "finance-data/income_statement/MSFT_quarterly_financials",
        ],
    )

    def fake_load_delta(_container, path):
        calls.append(path)
        symbol = "AAPL" if "AAPL_" in path else "MSFT"
        return pd.DataFrame([{"symbol": symbol, "metric": 123}])

    monkeypatch.setattr(data_service_module.delta_core, "load_delta", fake_load_delta)

    rows = DataService.get_data("silver", "finance", limit=10)

    assert len(rows) == 2
    assert rows[0]["symbol"] == "AAPL"
    assert calls == [
        "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
        "finance-data/income_statement/MSFT_quarterly_financials",
    ]


def test_market_silver_alpha26_reads_bucket_and_filters_symbol(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: True)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: False)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [
            {"symbol": "AAPL", "date": "2026-02-01", "close": 100.0},
            {"symbol": "AMZN", "date": "2026-02-01", "close": 200.0},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_data("silver", "market", ticker="AAPL", limit=5)

    assert rows == [{"symbol": "AAPL", "date": "2026-02-01", "close": 100.0}]
    assert calls == [DataPaths.get_silver_market_bucket_path("A")]


def test_finance_subdomain_reads_silver_alpha26_bucket(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: True)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: False)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [
            {"symbol": "AAPL", "sub_domain": "balance_sheet", "metric": 1},
            {"symbol": "AMZN", "sub_domain": "balance_sheet", "metric": 2},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_finance_data("silver", "balance_sheet", ticker="AAPL")

    assert rows == [{"symbol": "AAPL", "sub_domain": "balance_sheet", "metric": 1}]
    assert calls == [DataPaths.get_silver_finance_bucket_path("balance_sheet", "A")]


def test_finance_valuation_subdomain_reads_silver_alpha26_bucket(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: True)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: False)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [
            {"symbol": "AAPL", "sub_domain": "valuation", "market_cap": 1_000_000.0},
            {"symbol": "AMZN", "sub_domain": "valuation", "market_cap": 2_000_000.0},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_finance_data("silver", "valuation", ticker="AAPL")

    assert rows == [{"symbol": "AAPL", "sub_domain": "valuation", "market_cap": 1_000_000.0}]
    assert calls == [DataPaths.get_silver_finance_bucket_path("valuation", "A")]


def test_finance_subdomain_reads_gold_alpha26_bucket(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: False)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: True)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [
            {"symbol": "AAPL", "date": "2026-02-01", "value": 1.2},
            {"symbol": "AMZN", "date": "2026-02-01", "value": 3.4},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_finance_data("gold", "balance_sheet", ticker="AAPL")

    assert rows == [{"symbol": "AAPL", "date": "2026-02-01", "value": 1.2, "sub_domain": "balance_sheet"}]
    assert calls == [DataPaths.get_gold_finance_alpha26_bucket_path("A")]


def test_finance_subdomain_injects_requested_gold_subdomain_from_unified_bucket(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: False)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: True)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [{"symbol": "AAPL", "date": "2026-02-01", "value": 1.2}]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_finance_data("gold", "cash_flow", ticker="AAPL")

    assert rows == [{"symbol": "AAPL", "date": "2026-02-01", "value": 1.2, "sub_domain": "cash_flow"}]
    assert calls == [DataPaths.get_gold_finance_alpha26_bucket_path("A")]


def test_finance_valuation_subdomain_reads_gold_alpha26_bucket(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: False)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: True)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [
            {"symbol": "AAPL", "date": "2026-02-01", "market_cap": 1_000_000.0, "pe_ratio": 20.0},
            {"symbol": "AMZN", "date": "2026-02-01", "market_cap": 2_000_000.0, "pe_ratio": 30.0},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_finance_data("gold", "valuation", ticker="AAPL")

    assert rows == [
        {
            "symbol": "AAPL",
            "date": "2026-02-01",
            "market_cap": 1_000_000.0,
            "pe_ratio": 20.0,
            "sub_domain": "valuation",
        }
    ]
    assert calls == [DataPaths.get_gold_finance_alpha26_bucket_path("A")]


def test_gold_finance_regular_reads_unified_alpha26_bucket(monkeypatch):
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_silver_alpha26_mode", lambda: False)
    monkeypatch.setattr(data_service_module.layer_bucketing, "is_gold_alpha26_mode", lambda: True)

    calls: list[str] = []

    def fake_read_delta(_container: str, path: str, limit=None):
        calls.append(path)
        return [
            {"symbol": "AAPL", "date": "2026-02-01", "piotroski_f_score": 8},
            {"symbol": "AMZN", "date": "2026-02-01", "piotroski_f_score": 5},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_data("gold", "finance", ticker="AAPL")

    assert rows == [{"symbol": "AAPL", "date": "2026-02-01", "piotroski_f_score": 8}]
    assert calls == [DataPaths.get_gold_finance_alpha26_bucket_path("A")]


def test_market_sorts_by_date_desc_before_limit(monkeypatch):
    def fake_read_delta(_container: str, _path: str, limit=None):
        return [
            {"symbol": "AAPL", "date": "2026-02-01", "close": 101.0},
            {"symbol": "AAPL", "date": "2026-02-03", "close": 103.0},
            {"symbol": "AAPL", "date": "invalid-date", "close": 999.0},
            {"symbol": "AAPL", "date": "2026-02-02", "close": 102.0},
        ]

    monkeypatch.setattr(DataService, "_read_delta", staticmethod(fake_read_delta))

    rows = DataService.get_data(
        "silver",
        "market",
        ticker="AAPL",
        limit=2,
        sort_by_date="desc",
    )

    assert [row["date"] for row in rows] == ["2026-02-03", "2026-02-02"]
