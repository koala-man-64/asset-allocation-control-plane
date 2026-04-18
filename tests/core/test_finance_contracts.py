from __future__ import annotations

from asset_allocation_contracts import finance as finance_contracts
def test_silver_finance_subdomains_include_valuation() -> None:
    assert finance_contracts.SILVER_FINANCE_SUBDOMAINS == (
        "balance_sheet",
        "income_statement",
        "cash_flow",
        "valuation",
    )


def test_finance_layouts_and_columns_stay_aligned() -> None:
    assert set(finance_contracts.SILVER_FINANCE_REPORT_TYPE_TO_LAYOUT) == set(
        finance_contracts.SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN
    )
    assert finance_contracts.SILVER_FINANCE_COLUMNS_BY_SUBDOMAIN["valuation"][2:] == finance_contracts.VALUATION_FINANCE_COLUMNS
