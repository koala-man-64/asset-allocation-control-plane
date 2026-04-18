from __future__ import annotations

from asset_allocation_contracts import finance as owner_finance_contracts
from asset_allocation_runtime_common.market_data import market_symbols as owner_market_symbols
from tasks.common import finance_contracts as legacy_finance_contracts
from tasks.common import market_symbols as legacy_market_symbols


def test_legacy_finance_contracts_wrapper_exposes_core_contracts() -> None:
    assert legacy_finance_contracts.SILVER_FINANCE_SUBDOMAINS == owner_finance_contracts.SILVER_FINANCE_SUBDOMAINS
    assert legacy_finance_contracts.SILVER_FINANCE_REPORT_TYPE_TO_LAYOUT == (
        owner_finance_contracts.SILVER_FINANCE_REPORT_TYPE_TO_LAYOUT
    )


def test_legacy_market_symbols_wrapper_exposes_core_contracts() -> None:
    assert legacy_market_symbols.REGIME_REQUIRED_MARKET_SYMBOLS == owner_market_symbols.REGIME_REQUIRED_MARKET_SYMBOLS
