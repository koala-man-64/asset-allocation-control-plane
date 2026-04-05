import pandas as pd

import api.data_service as data_service_module
from api.data_service import DataService


def test_bronze_market_reads_alpha26_bucket_parquet(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    payload = pd.DataFrame(
        [{"symbol": "AAPL", "date": "2025-01-01", "open": 1.0, "close": 2.0}],
    ).to_parquet(index=False)
    monkeypatch.setattr(
        data_service_module.mdc,
        "read_raw_bytes",
        lambda _path, client=None: payload,
    )

    rows = DataService.get_data("bronze", "market", ticker="AAPL", limit=1)

    assert len(rows) == 1
    assert rows[0]["date"] == "2025-01-01"


def test_bronze_earnings_reads_alpha26_bucket_parquet(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    payload = pd.DataFrame([{"symbol": "AAPL", "eps": 1.23}, {"symbol": "AAPL", "eps": 2.34}]).to_parquet(index=False)
    monkeypatch.setattr(
        data_service_module.mdc,
        "read_raw_bytes",
        lambda _path, client=None: payload,
    )

    rows = DataService.get_data("bronze", "earnings", ticker="AAPL", limit=1)

    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


def test_bronze_earnings_missing_values_are_json_safe(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    payload = pd.DataFrame([{"symbol": "AAPL", "eps": 1.23}, {"symbol": "AAPL"}]).to_parquet(index=False)
    monkeypatch.setattr(
        data_service_module.mdc,
        "read_raw_bytes",
        lambda _path, client=None: payload,
    )

    rows = DataService.get_data("bronze", "earnings", ticker="AAPL", limit=2)

    assert len(rows) == 2
    assert rows[1]["eps"] is None


def test_bronze_market_defaults_to_first_blob_when_ticker_missing(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        data_service_module.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda domain, client: [
            {"name": "market-data/runs/run-123/buckets/M.parquet"},
            {"name": "market-data/runs/run-123/buckets/A.parquet"},
        ],
    )

    payload = pd.DataFrame([{"symbol": "AAPL", "date": "2025-01-01", "open": 1.0, "close": 2.0}]).to_parquet(index=False)

    def fake_read_raw_bytes(path, client=None):
        del client
        assert path == "market-data/runs/run-123/buckets/A.parquet"
        return payload

    monkeypatch.setattr(data_service_module.mdc, "read_raw_bytes", fake_read_raw_bytes)

    rows = DataService.get_data("bronze", "market", limit=1)

    assert len(rows) == 1
    assert rows[0]["date"] == "2025-01-01"


def test_bronze_finance_defaults_to_first_blob_when_ticker_missing(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        data_service_module.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda domain, client: [
            {"name": "finance-data/runs/run-123/buckets/M.parquet"},
            {"name": "finance-data/runs/run-123/buckets/A.parquet"},
        ],
    )

    payload = pd.DataFrame(
        [{"symbol": "AAPL", "report_type": "valuation", "metric": 123}],
    ).to_parquet(index=False)

    def fake_read_raw_bytes(path, client=None):
        del client
        assert path == "finance-data/runs/run-123/buckets/A.parquet"
        return payload

    monkeypatch.setattr(data_service_module.mdc, "read_raw_bytes", fake_read_raw_bytes)

    rows = DataService.get_finance_data("bronze", "valuation", ticker=None, limit=1)

    assert len(rows) == 1
    assert rows[0]["metric"] == 123


def test_bronze_generic_finance_defaults_to_first_blob_when_ticker_missing(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        data_service_module.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda domain, client: [
            {"name": "finance-data/runs/run-123/buckets/M.parquet"},
            {"name": "finance-data/runs/run-123/buckets/A.parquet"},
        ],
    )

    payload = pd.DataFrame([{"symbol": "AAPL", "metric": 123}]).to_parquet(index=False)

    def fake_read_raw_bytes(path, client=None):
        del client
        assert path == "finance-data/runs/run-123/buckets/A.parquet"
        return payload

    monkeypatch.setattr(data_service_module.mdc, "read_raw_bytes", fake_read_raw_bytes)

    rows = DataService.get_data("bronze", "finance", ticker=None, limit=1)

    assert len(rows) == 1
    assert rows[0]["metric"] == 123
