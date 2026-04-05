from core.config import AppSettings


def test_debug_symbols_normalized_to_uppercase(monkeypatch):
    monkeypatch.setenv("DEBUG_SYMBOLS", "aapl, msft, f , bac")
    settings = AppSettings()
    assert settings.DEBUG_SYMBOLS == ["AAPL", "MSFT", "F", "BAC"]


def test_debug_symbols_json_array_normalized_to_uppercase(monkeypatch):
    monkeypatch.setenv("DEBUG_SYMBOLS", '["aapl", "MsFt"]')
    settings = AppSettings()
    assert settings.DEBUG_SYMBOLS == ["AAPL", "MSFT"]

