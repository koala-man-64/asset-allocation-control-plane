from __future__ import annotations

import logging

import pytest

from api.service.kalshi_gateway import KalshiGateway
from api.service.settings import KalshiSettings


def _settings_with_demo_only() -> KalshiSettings:
    return KalshiSettings(
        enabled=True,
        demo_api_key_id="demo-key",
        demo_private_key_pem="demo-private-key",
    )


def test_gateway_resolves_environment_specific_credentials() -> None:
    settings = KalshiSettings(
        enabled=True,
        demo_api_key_id="demo-key",
        demo_private_key_pem="demo-private-key",
        live_api_key_id="live-key",
        live_private_key_pem="live-private-key",
    )
    gateway = KalshiGateway(settings)

    demo = gateway._provider_config_for("demo")
    live = gateway._provider_config_for("live")

    assert demo.api_key_id == "demo-key"
    assert demo.private_key_pem == "demo-private-key"
    assert live.api_key_id == "live-key"
    assert live.private_key_pem == "live-private-key"


def test_gateway_keeps_unconfigured_environment_explicit() -> None:
    gateway = KalshiGateway(_settings_with_demo_only())

    live = gateway._provider_config_for("live")

    assert live.api_key_id is None
    assert live.private_key_pem is None


def test_gateway_logs_trade_audit_entries(caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = KalshiGateway(_settings_with_demo_only())

    class _FakeClient:
        last_request_id = "req-123"

        def create_order(self, order):
            return {"order": {"order_id": "order-1", "ticker": order["ticker"]}}

    monkeypatch.setattr(gateway, "_client_for", lambda environment: _FakeClient())

    with caplog.at_level(logging.INFO, logger="asset-allocation.api.kalshi"):
        payload = gateway.create_order(
            environment="demo",
            order={"ticker": "KXTEST-24JAN01-T1", "yes_price_dollars": "0.45", "count": 1},
            subject="user-123",
        )

    assert payload["order"]["order_id"] == "order-1"
    assert any("Kalshi trade audit" in record.message for record in caplog.records)
