from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest

from alpaca.errors import AlpacaNotConfiguredError
from api.service.alpaca_gateway import AlpacaGateway
from api.service.settings import AlpacaSettings


def _settings_with_paper_only() -> AlpacaSettings:
    return AlpacaSettings(
        paper_api_key_id="paper-key",
        paper_secret_key="paper-secret",
    )


def test_gateway_resolves_environment_specific_credentials() -> None:
    settings = AlpacaSettings(
        paper_api_key_id="paper-key",
        paper_secret_key="paper-secret",
        live_api_key_id="live-key",
        live_secret_key="live-secret",
    )
    gateway = AlpacaGateway(settings)

    paper = gateway._provider_config_for("paper")
    live = gateway._provider_config_for("live")

    assert paper.api_key == "paper-key"
    assert paper.api_secret == "paper-secret"
    assert live.api_key == "live-key"
    assert live.api_secret == "live-secret"


def test_gateway_rejects_unconfigured_live_environment() -> None:
    gateway = AlpacaGateway(_settings_with_paper_only())

    with pytest.raises(AlpacaNotConfiguredError, match="live credentials are not configured"):
        gateway._provider_config_for("live")


def test_gateway_logs_trade_audit_entries(caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    gateway = AlpacaGateway(_settings_with_paper_only())

    class _FakeClient:
        last_request_id = "req-123"

        def submit_order(self, **kwargs):
            return {
                "id": "order-1",
                "client_order_id": kwargs["client_order_id"],
                "symbol": kwargs["symbol"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "submitted_at": datetime.now(timezone.utc).isoformat(),
                "asset_id": "asset-1",
                "asset_class": "us_equity",
                "qty": kwargs["qty"],
                "filled_qty": 0,
                "type": kwargs["type"],
                "side": kwargs["side"],
                "time_in_force": kwargs["time_in_force"],
                "limit_price": None,
                "stop_price": None,
                "status": "new",
            }

    monkeypatch.setattr(gateway, "_client_for", lambda environment: _FakeClient())

    with caplog.at_level(logging.INFO, logger="asset-allocation.api.alpaca"):
        payload = gateway.submit_order(
            environment="paper",
            order={
                "symbol": "AAPL",
                "qty": 1.0,
                "side": "buy",
                "type": "market",
                "time_in_force": "day",
                "client_order_id": "client-1",
            },
            subject="user-123",
        )

    assert payload["id"] == "order-1"
    assert any("Alpaca trade audit" in record.message for record in caplog.records)
