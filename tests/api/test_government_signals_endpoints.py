from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from api.service.app import create_app
from core import government_signals_repository as repo_module
from tests.api._client import get_test_client


def _congress_response():
    return {
        "events": [
            {
                "event_id": "congress-1",
                "source_name": "quiver",
                "source_event_key": "q-1",
                "member_name": "Jane Doe",
                "chamber": "house",
                "committee_names": ["Financial Services"],
                "traded_at": datetime(2026, 4, 18, 13, 0, tzinfo=timezone.utc),
                "transaction_type": "purchase",
                "asset_name": "Example Corp",
                "issuer_ticker": "AAPL",
                "mapping_status": "mapped",
            }
        ],
        "total": 1,
        "limit": 50,
        "offset": 0,
    }


def _contract_response():
    return {
        "events": [
            {
                "event_id": "contract-1",
                "source_name": "usaspending",
                "source_event_key": "usa-1",
                "event_type": "award",
                "event_at": datetime(2026, 4, 18, 15, 0, tzinfo=timezone.utc),
                "recipient_name": "Example Corp",
                "recipient_ticker": "AAPL",
                "awarding_agency": "NASA",
                "title": "Satellite services",
                "mapping_status": "mapped",
            }
        ],
        "total": 1,
        "limit": 50,
        "offset": 0,
    }


@pytest.mark.asyncio
async def test_government_signals_congress_endpoint_calls_repository(monkeypatch):
    calls: list[dict[str, object]] = []

    def _fake(self, **kwargs):
        calls.append(kwargs)
        return repo_module.CongressTradeEventListResponse.model_validate(_congress_response())

    monkeypatch.setattr(repo_module.GovernmentSignalsRepository, "list_congress_events", _fake)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/government-signals/events/congress",
            params={"symbol": "aapl", "limit": 50},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["events"][0]["issuer_ticker"] == "AAPL"
    assert calls == [{"symbol": "aapl", "member_id": None, "chamber": None, "from_date": None, "to_date": None, "limit": 50, "offset": 0}]


@pytest.mark.asyncio
async def test_government_signals_contract_endpoint_calls_repository(monkeypatch):
    calls: list[dict[str, object]] = []

    def _fake(self, **kwargs):
        calls.append(kwargs)
        return repo_module.GovernmentContractEventListResponse.model_validate(_contract_response())

    monkeypatch.setattr(repo_module.GovernmentSignalsRepository, "list_contract_events", _fake)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/government-signals/events/contracts",
            params={"symbol": "AAPL", "awarding_agency": "NASA"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["events"][0]["awarding_agency"] == "NASA"
    assert calls == [
        {
            "symbol": "AAPL",
            "awarding_agency": "NASA",
            "event_type": None,
            "from_date": None,
            "to_date": None,
            "limit": 100,
            "offset": 0,
        }
    ]


@pytest.mark.asyncio
async def test_government_signals_issuer_summary_endpoint_returns_contract_payload(monkeypatch):
    issuer_daily = repo_module.IssuerGovernmentSignalDaily.model_validate(
        {
            "as_of_date": date(2026, 4, 18),
            "symbol": "AAPL",
            "issuer_name": "Example Corp",
            "mapping_status": "mapped",
        }
    )

    def _fake(self, **kwargs):
        return repo_module.GovernmentSignalIssuerSummaryResponse.model_validate(
            {
                "symbol": "AAPL",
                "issuer_name": "Example Corp",
                "as_of_date": "2026-04-18",
                "issuer_daily": issuer_daily.model_dump(mode="json"),
                "recent_congress_trades": _congress_response()["events"],
                "recent_contract_events": _contract_response()["events"],
                "active_alerts": [],
            }
        )

    monkeypatch.setattr(repo_module.GovernmentSignalsRepository, "get_issuer_summary", _fake)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/government-signals/issuers/AAPL/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "AAPL"
    assert payload["issuer_daily"]["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_government_signals_mapping_override_endpoint_maps_errors(monkeypatch):
    def _fake(self, *, mapping_id, request, actor=None):
        assert mapping_id == "map-1"
        assert request.action == "map"
        return repo_module.GovernmentSignalMappingOverrideResponse.model_validate(
            {
                "mapping_id": "map-1",
                "status": "mapped",
                "symbol": "AAPL",
                "updated_at": "2026-04-19T00:00:00+00:00",
            }
        )

    monkeypatch.setattr(repo_module.GovernmentSignalsRepository, "apply_mapping_override", _fake)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/government-signals/entity-mappings/map-1/override",
            json={"action": "map", "symbol": "AAPL", "reason": "manual match"},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "mapped"
    assert payload["symbol"] == "AAPL"


@pytest.mark.asyncio
async def test_government_signals_portfolio_exposure_endpoint_forwards_request(monkeypatch):
    def _fake(self, payload):
        return repo_module.GovernmentSignalPortfolioExposureResponse.model_validate(
            {
                "as_of_date": "2026-04-18",
                "holdings_analyzed": 2,
                "matched_holdings": 1,
                "unmatched_symbols": ["MSFT"],
                "total_market_value": 1500.0,
                "total_portfolio_weight": 0.3,
                "exposures": [
                    {
                        "symbol": "AAPL",
                        "issuer_name": "Example Corp",
                        "matched": True,
                        "market_value": 1000.0,
                        "portfolio_weight": 0.2,
                        "issuer_daily": {
                            "as_of_date": "2026-04-18",
                            "symbol": "AAPL",
                            "issuer_name": "Example Corp",
                            "mapping_status": "mapped",
                        },
                        "alerts": [],
                    },
                    {
                        "symbol": "MSFT",
                        "issuer_name": None,
                        "matched": False,
                        "market_value": 500.0,
                        "portfolio_weight": 0.1,
                        "issuer_daily": None,
                        "alerts": [],
                    },
                ],
            }
        )

    monkeypatch.setattr(repo_module.GovernmentSignalsRepository, "build_portfolio_exposure", _fake)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.post(
            "/api/government-signals/portfolio/exposure",
            json={
                "holdings": [
                    {"symbol": "AAPL", "market_value": 1000.0, "portfolio_weight": 0.2},
                    {"symbol": "MSFT", "market_value": 500.0, "portfolio_weight": 0.1},
                ]
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["matched_holdings"] == 1
    assert payload["unmatched_symbols"] == ["MSFT"]
