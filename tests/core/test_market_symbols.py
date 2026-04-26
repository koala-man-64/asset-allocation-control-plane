from __future__ import annotations

from asset_allocation_runtime_common.market_data import market_symbols


def test_regime_required_market_symbols_contract() -> None:
    assert market_symbols.REGIME_REQUIRED_MARKET_SYMBOLS == ("SPY", "QQQ", "IWM", "ACWI", "^VIX", "^VIX3M")
