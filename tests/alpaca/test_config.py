import pytest
from alpaca.config import LiveConfig, validate_live_config_dict_strict

def test_config_strict_validation_success():
    data = {
        "alpaca": {
            "env": "paper",
            "api_key_env": "KEY",
            "api_secret_env": "SECRET",
        },
        "execution": {
            "allow_fractional_shares": True,
        }
    }
    validate_live_config_dict_strict(data)

def test_config_strict_validation_failure_top_level():
    data = {
        "alpaca": {},
        "extra": {}
    }
    with pytest.raises(ValueError, match="Unknown top-level config field"):
        validate_live_config_dict_strict(data)

def test_config_strict_validation_failure_section():
    data = {
        "alpaca": {
            "env": "paper",
            "wrong_key": "val"
        }
    }
    with pytest.raises(ValueError, match="Unknown alpaca field"):
        validate_live_config_dict_strict(data)

def test_live_config_from_dict():
    data = {
        "alpaca": {
            "env": "live", 
            "api_key_env": "K", 
            "api_secret_env": "S",
            "marketdata_feed": "v2/sip"
        },
        "execution": {
            "lot_size": 100,
            "rounding_mode": "floor"
        }
    }
    cfg = LiveConfig.from_dict(data)
    assert cfg.alpaca.env == "live"
    assert cfg.alpaca.marketdata_feed == "v2/sip"
    assert cfg.execution.lot_size == 100
    assert cfg.execution.rounding_mode == "floor"

def test_execution_config_defaults():
    data = {}
    from alpaca.config import ExecutionConfig
    cfg = ExecutionConfig.from_dict(data)
    assert cfg.allow_fractional_shares is True
    assert cfg.default_order_type == "market"
