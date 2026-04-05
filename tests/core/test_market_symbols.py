from __future__ import annotations

from core import market_symbols


def test_regime_required_market_symbols_contract() -> None:
    assert market_symbols.REGIME_REQUIRED_MARKET_SYMBOLS == ("SPY", "^VIX", "^VIX3M")
