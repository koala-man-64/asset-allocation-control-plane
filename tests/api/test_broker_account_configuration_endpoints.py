from __future__ import annotations

import pytest

from api.service.app import create_app
from api.service.broker_account_configuration_service import BrokerAccountConfigurationService
from tests.api._client import get_test_client

pytestmark = pytest.mark.asyncio


def _configuration_payload() -> dict[str, object]:
    return {
        "accountId": "acct-paper",
        "accountName": "Core Paper",
        "baseCurrency": "USD",
        "configurationVersion": 4,
        "requestedPolicy": {
            "maxOpenPositions": 12,
            "maxSinglePositionExposure": {
                "mode": "pct_of_allocatable_capital",
                "value": 8.0,
            },
            "allowedSides": ["long"],
            "allowedAssetClasses": ["equity", "option"],
            "requireOrderConfirmation": True,
        },
        "effectivePolicy": {
            "maxOpenPositions": 10,
            "maxSinglePositionExposure": {
                "mode": "pct_of_allocatable_capital",
                "value": 8.0,
            },
            "allowedSides": ["long"],
            "allowedAssetClasses": ["equity"],
            "requireOrderConfirmation": True,
        },
        "capabilities": {
            "canReadBalances": True,
            "canReadPositions": True,
            "canReadOrders": True,
            "canTrade": True,
            "canReadTradingPolicy": True,
            "canWriteTradingPolicy": True,
            "canReadAllocation": True,
            "canWriteAllocation": True,
            "canReleaseTradeConfirmation": True,
        },
        "allocation": {
            "portfolioName": "core-balanced",
            "portfolioVersion": 2,
            "allocationMode": "percent",
            "allocatableCapital": 250000.0,
            "allocatedPercent": 100.0,
            "remainingPercent": 0.0,
            "items": [
                {
                    "sleeveId": "quality-core",
                    "strategy": {"strategyName": "quality-trend", "strategyVersion": 4},
                    "allocationMode": "percent",
                    "targetWeightPct": 60.0,
                },
                {
                    "sleeveId": "defensive",
                    "strategy": {"strategyName": "defensive-value", "strategyVersion": 2},
                    "allocationMode": "percent",
                    "targetWeightPct": 40.0,
                },
            ],
        },
        "warnings": [],
        "audit": [],
    }


async def test_broker_account_configuration_endpoints_round_trip_contract_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    payload = _configuration_payload()

    monkeypatch.setattr(BrokerAccountConfigurationService, "get_configuration", lambda self, account_id: payload)
    monkeypatch.setattr(
        BrokerAccountConfigurationService,
        "save_trading_policy",
        lambda self, account_id, payload, *, actor, request_id, granted_roles: {
            **_configuration_payload(),
            "warnings": ["Current open positions exceed the tighter policy."],
        },
    )
    monkeypatch.setattr(
        BrokerAccountConfigurationService,
        "save_allocation",
        lambda self, account_id, payload, *, actor, request_id, granted_roles: {
            **_configuration_payload(),
            "allocation": {
                **_configuration_payload()["allocation"],
                "allocationMode": "notional_base_ccy",
                "allocatableCapital": 250000.0,
                "allocatedPercent": 100.0,
                "allocatedNotionalBaseCcy": 250000.0,
                "remainingNotionalBaseCcy": 0.0,
                "items": [
                    {
                        "sleeveId": "quality-core",
                        "strategy": {"strategyName": "quality-trend", "strategyVersion": 4},
                        "allocationMode": "notional_base_ccy",
                        "targetNotionalBaseCcy": 150000.0,
                        "derivedWeightPct": 60.0,
                    },
                    {
                        "sleeveId": "defensive",
                        "strategy": {"strategyName": "defensive-value", "strategyVersion": 2},
                        "allocationMode": "notional_base_ccy",
                        "targetNotionalBaseCcy": 100000.0,
                        "derivedWeightPct": 40.0,
                    },
                ],
            },
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        get_response = await client.get("/api/broker-accounts/acct-paper/configuration")
        policy_response = await client.put(
            "/api/broker-accounts/acct-paper/trading-policy",
            json={
                "expectedConfigurationVersion": 4,
                "requestedPolicy": _configuration_payload()["requestedPolicy"],
            },
        )
        allocation_response = await client.put(
            "/api/broker-accounts/acct-paper/allocation",
            json={
                "expectedConfigurationVersion": 4,
                "allocationMode": "notional_base_ccy",
                "allocatableCapital": 250000.0,
                "items": [
                    {
                        "sleeveId": "quality-core",
                        "strategy": {"strategyName": "quality-trend", "strategyVersion": 4},
                        "allocationMode": "notional_base_ccy",
                        "targetNotionalBaseCcy": 150000.0,
                    },
                    {
                        "sleeveId": "defensive",
                        "strategy": {"strategyName": "defensive-value", "strategyVersion": 2},
                        "allocationMode": "notional_base_ccy",
                        "targetNotionalBaseCcy": 100000.0,
                    },
                ],
            },
        )

    assert get_response.status_code == 200
    assert get_response.json()["requestedPolicy"]["requireOrderConfirmation"] is True
    assert policy_response.status_code == 200
    assert policy_response.json()["warnings"][0] == "Current open positions exceed the tighter policy."
    assert allocation_response.status_code == 200
    assert allocation_response.json()["allocation"]["allocationMode"] == "notional_base_ccy"
    assert allocation_response.json()["allocation"]["items"][0]["targetNotionalBaseCcy"] == 150000.0
