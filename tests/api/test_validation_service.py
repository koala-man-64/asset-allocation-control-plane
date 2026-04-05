import pytest

from api.service.validation_service import ValidationService


def test_validation_report_uses_ticker_and_computes_null_percentage(monkeypatch):
    calls = []

    def fake_get_data(
        layer: str,
        domain: str,
        ticker: str | None = None,
        limit: int | None = None,
    ):
        calls.append((layer, domain, ticker, limit))
        return [
            {"symbol": "AAPL", "value": 1},
            {"symbol": "MSFT", "value": None},
            {"symbol": "", "value": 2},
        ]

    monkeypatch.setattr("api.service.validation_service.DataService.get_data", fake_get_data)

    report = ValidationService.get_validation_report("silver", "market", "aapl")

    assert calls == [("silver", "market", "AAPL", 1000)]
    assert report["status"] == "healthy"
    assert report["rowCount"] == 3
    assert report["columns"]

    by_name = {col["name"]: col for col in report["columns"]}
    assert "unique" not in by_name["symbol"]
    assert by_name["symbol"]["notNull"] == 2
    assert by_name["symbol"]["nullPct"] == pytest.approx(33.33, abs=0.01)
    assert by_name["value"]["nullPct"] == pytest.approx(33.33, abs=0.01)
