import pytest

from schwab.config import SCHWAB_AUTH_BASE_URL, SCHWAB_TIMEOUT_SECONDS, SCHWAB_TRADER_BASE_URL, SchwabConfig


def test_from_env_reads_schwab_values(monkeypatch):
    monkeypatch.setenv("SCHWAB_CLIENT_ID", "client-id")
    monkeypatch.setenv("SCHWAB_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SCHWAB_APP_CALLBACK_URL", "https://127.0.0.1/callback")
    monkeypatch.setenv("SCHWAB_ACCESS_TOKEN", "access-token")
    monkeypatch.setenv("SCHWAB_REFRESH_TOKEN", "refresh-token")
    monkeypatch.setenv("SCHWAB_TIMEOUT_SECONDS", "45")

    cfg = SchwabConfig.from_env()

    assert cfg.client_id == "client-id"
    assert cfg.client_secret == "client-secret"
    assert cfg.app_callback_url == "https://127.0.0.1/callback"
    assert cfg.auth_base_url == SCHWAB_AUTH_BASE_URL
    assert cfg.trader_base_url == SCHWAB_TRADER_BASE_URL
    assert cfg.timeout_seconds == 45.0
    assert cfg.access_token == "access-token"
    assert cfg.refresh_token == "refresh-token"


def test_from_env_requires_client_id(monkeypatch):
    monkeypatch.delenv("SCHWAB_CLIENT_ID", raising=False)
    monkeypatch.setenv("SCHWAB_CLIENT_SECRET", "client-secret")

    with pytest.raises(ValueError, match="SCHWAB_CLIENT_ID"):
        SchwabConfig.from_env()


def test_from_env_requires_client_secret(monkeypatch):
    monkeypatch.setenv("SCHWAB_CLIENT_ID", "client-id")
    monkeypatch.delenv("SCHWAB_CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="SCHWAB_CLIENT_SECRET"):
        SchwabConfig.from_env()


def test_from_mapping_reads_schwab_values():
    cfg = SchwabConfig.from_mapping(
        {
            "SCHWAB_CLIENT_ID": "client-id",
            "SCHWAB_CLIENT_SECRET": "client-secret",
            "SCHWAB_APP_CALLBACK_URL": "https://127.0.0.1",
            "SCHWAB_ACCESS_TOKEN": "access-token",
            "SCHWAB_REFRESH_TOKEN": "refresh-token",
            "SCHWAB_TIMEOUT_SECONDS": "15",
        }
    )

    assert cfg.client_id == "client-id"
    assert cfg.client_secret == "client-secret"
    assert cfg.app_callback_url == "https://127.0.0.1"
    assert cfg.auth_base_url == SCHWAB_AUTH_BASE_URL
    assert cfg.trader_base_url == SCHWAB_TRADER_BASE_URL
    assert cfg.timeout_seconds == 15.0
    assert cfg.access_token == "access-token"
    assert cfg.refresh_token == "refresh-token"


def test_from_mapping_defaults_timeout():
    cfg = SchwabConfig.from_mapping(
        {
            "SCHWAB_CLIENT_ID": "client-id",
            "SCHWAB_CLIENT_SECRET": "client-secret",
        }
    )

    assert cfg.timeout_seconds == SCHWAB_TIMEOUT_SECONDS
