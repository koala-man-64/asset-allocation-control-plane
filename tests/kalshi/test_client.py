from __future__ import annotations

from unittest.mock import patch

import pytest

from kalshi.client import KalshiTradingClient
from kalshi.config import HttpConfig, KalshiEnvironmentConfig
from kalshi.errors import KalshiInvalidResponseError


@pytest.fixture
def mock_config() -> KalshiEnvironmentConfig:
    return KalshiEnvironmentConfig(
        environment="demo",
        api_key_id="demo-key",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
        http=HttpConfig(timeout_s=5.0, read_retry_attempts=0, read_retry_base_s=0.0),
    )


@pytest.fixture
def client(mock_config: KalshiEnvironmentConfig):
    with patch("kalshi.client.KalshiHttpTransport") as mock_transport_cls:
        mock_transport = mock_transport_cls.return_value
        client = KalshiTradingClient(mock_config)
        client._transport = mock_transport
        yield client


def test_get_account_limits_parses_nested_live_bucket_shape(client: KalshiTradingClient) -> None:
    client._transport.get.return_value = {
        "usage_tier": "basic",
        "read": {"bucket_capacity": 200, "refill_rate": 200},
        "write": {"bucket_capacity": 100, "refill_rate": 100},
    }

    limits = client.get_account_limits()

    assert limits.usage_tier == "basic"
    assert limits.read_limit == 200
    assert limits.write_limit == 100
    client._transport.get.assert_called_with("/account/limits", authenticated=True)


def test_get_account_limits_parses_flat_limit_fields(client: KalshiTradingClient) -> None:
    client._transport.get.return_value = {
        "usage_tier": "basic",
        "read_limit": 200,
        "write_limit": 100,
    }

    limits = client.get_account_limits()

    assert limits.usage_tier == "basic"
    assert limits.read_limit == 200
    assert limits.write_limit == 100
    client._transport.get.assert_called_with("/account/limits", authenticated=True)


def test_get_account_limits_parses_rate_limit_alias_fields(client: KalshiTradingClient) -> None:
    client._transport.get.return_value = {
        "usage_tier": "basic",
        "read_rate_limit": "200",
        "write_rate_limit": "100",
    }

    limits = client.get_account_limits()

    assert limits.usage_tier == "basic"
    assert limits.read_limit == 200
    assert limits.write_limit == 100
    client._transport.get.assert_called_with("/account/limits", authenticated=True)


@pytest.mark.parametrize(
    "payload",
    [
        {"usage_tier": "basic"},
        {"usage_tier": "", "read_limit": 200, "write_limit": 100},
        {"usage_tier": None, "read_limit": 200, "write_limit": 100},
        {"usage_tier": 123, "read_limit": 200, "write_limit": 100},
        {"usage_tier": "basic", "read": {"refill_rate": 200}, "write_limit": 100},
        {"usage_tier": "basic", "read": [], "write_limit": 100},
        {"usage_tier": "basic", "read_limit": True, "write_limit": 100},
        {"usage_tier": "basic", "read_limit": 1.5, "write_limit": 100},
        {"usage_tier": "basic", "read_limit": "1.5", "write_limit": 100},
        {"usage_tier": "basic", "read_limit": -1, "write_limit": 100},
        {"usage_tier": "basic", "read_limit": "", "write_limit": 100},
        {"usage_tier": "basic", "read_limit": None, "write_limit": 100},
        {
            "usage_tier": "basic",
            "read": {"bucket_capacity": 200},
            "read_limit": 100,
            "write": {"bucket_capacity": 100},
        },
    ],
)
def test_get_account_limits_rejects_invalid_payloads(
    client: KalshiTradingClient,
    payload: dict[str, object],
) -> None:
    client._transport.get.return_value = payload

    with pytest.raises(KalshiInvalidResponseError):
        client.get_account_limits()

    client._transport.get.assert_called_with("/account/limits", authenticated=True)


def test_get_market_parses_fixed_point_fields(client: KalshiTradingClient) -> None:
    client._transport.get.return_value = {
        "market": {
            "ticker": "KXTEST-1",
            "event_ticker": "KXTEST",
            "status": "open",
            "yes_ask_dollars": "0.5600",
            "yes_ask_size_fp": "10.00",
            "fractional_trading_enabled": True,
        }
    }

    market = client.get_market("KXTEST-1")

    assert market.ticker == "KXTEST-1"
    assert format(market.yes_ask_dollars or 0, "f") == "0.5600"
    assert format(market.yes_ask_size_fp or 0, "f") == "10.00"
    assert market.fractional_trading_enabled is True
    client._transport.get.assert_called_with("/markets/KXTEST-1", authenticated=False)


def test_cancel_order_parses_reduced_quantity(client: KalshiTradingClient) -> None:
    client._transport.delete.return_value = {
        "order": {
            "order_id": "order-1",
            "user_id": "user-1",
            "client_order_id": "client-1",
            "ticker": "KXTEST-1",
            "side": "yes",
            "action": "buy",
            "status": "canceled",
            "fill_count_fp": "0.00",
            "remaining_count_fp": "0.00",
            "initial_count_fp": "1.00",
        },
        "reduced_by_fp": "1.00",
    }

    result = client.cancel_order("order-1", subaccount=0)

    assert result.order.order_id == "order-1"
    assert format(result.reduced_by_fp, "f") == "1.00"
    client._transport.delete.assert_called_with(
        "/portfolio/orders/order-1",
        params={"subaccount": 0},
        authenticated=True,
    )
