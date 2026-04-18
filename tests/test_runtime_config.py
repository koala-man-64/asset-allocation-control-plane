import logging

from asset_allocation_runtime_common.shared_core import runtime_config
from asset_allocation_runtime_common.shared_core.runtime_config import normalize_env_override


def test_normalize_env_override_passthrough_for_unmanaged_keys():
    assert normalize_env_override("LEGACY_FLAG", "true") == "true"
    assert normalize_env_override("LEGACY_FLAG", "1") == "1"
    assert normalize_env_override("LEGACY_FLAG", "off") == "off"


def test_normalize_env_override_debug_symbols_to_uppercase_csv():
    assert normalize_env_override("DEBUG_SYMBOLS", '["aapl", " msft ", "NvDa"]') == "AAPL,MSFT,NVDA"


def test_normalize_env_override_alpha_vantage_rate_wait_timeout_float():
    assert normalize_env_override("ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS", "120.5") == "120.5"


def test_normalize_env_override_alpha_vantage_throttle_cooldown_float():
    assert normalize_env_override("ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS", "60.5") == "60.5"


def test_normalize_env_override_alpha_vantage_gateway_retry_attempts_int():
    assert normalize_env_override("ALPHA_VANTAGE_GATEWAY_RETRY_ATTEMPTS", "3") == "3"


def test_normalize_env_override_alpha_vantage_gateway_retry_backoff_float():
    assert normalize_env_override("ALPHA_VANTAGE_GATEWAY_RETRY_BASE_SECONDS", "120.5") == "120.5"
    assert normalize_env_override("ALPHA_VANTAGE_GATEWAY_RETRY_MAX_SECONDS", "300.5") == "300.5"


def test_normalize_env_override_required_nonempty_rejects_blank():
    try:
        normalize_env_override("SYSTEM_HEALTH_TTL_SECONDS", "")
    except ValueError as exc:
        assert "cannot be empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty SYSTEM_HEALTH_TTL_SECONDS")


def test_normalize_env_override_debug_symbols_rejects_blank():
    try:
        normalize_env_override("DEBUG_SYMBOLS", "")
    except ValueError as exc:
        assert "cannot be empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty DEBUG_SYMBOLS")


def test_normalize_env_override_unknown_key_passthrough():
    assert normalize_env_override("UNMANAGED_KEY", "  any-value  ") == "any-value"


def test_apply_runtime_config_logs_info_for_local_db_connectivity_error(monkeypatch, caplog):
    def _raise_connectivity_error(*args, **kwargs):
        raise RuntimeError(
            "connection failed: could not send SSL negotiation packet: Socket is not connected"
        )

    for key in (
        "CONTAINER_APP_ENV_DNS_SUFFIX",
        "CONTAINER_APP_JOB_EXECUTION_NAME",
        "CONTAINER_APP_REPLICA_NAME",
        "KUBERNETES_SERVICE_HOST",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(runtime_config, "get_effective_runtime_config", _raise_connectivity_error)

    with caplog.at_level(logging.INFO, logger=runtime_config.logger.name):
        applied = runtime_config.apply_runtime_config_to_env()

    assert applied == {}
    matching = [
        record
        for record in caplog.records
        if "Runtime config load skipped (db unavailable?)" in record.getMessage()
    ]
    assert matching
    assert all(record.levelno == logging.INFO for record in matching)


def test_apply_runtime_config_logs_warning_for_cloud_runtime_db_connectivity_error(
    monkeypatch, caplog
):
    def _raise_connectivity_error(*args, **kwargs):
        raise RuntimeError("connection failed: timeout expired")

    monkeypatch.setenv("CONTAINER_APP_ENV_DNS_SUFFIX", "azurecontainerapps.io")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setattr(runtime_config, "get_effective_runtime_config", _raise_connectivity_error)

    with caplog.at_level(logging.INFO, logger=runtime_config.logger.name):
        applied = runtime_config.apply_runtime_config_to_env()

    assert applied == {}
    matching = [
        record
        for record in caplog.records
        if "Runtime config load skipped (db unavailable?)" in record.getMessage()
    ]
    assert matching
    assert any(record.levelno == logging.WARNING for record in matching)


