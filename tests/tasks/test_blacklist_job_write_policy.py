from unittest.mock import patch

from core.pipeline import ListManager
from tasks.earnings_data import bronze_earnings_data as bronze_earnings
from tasks.finance_data import bronze_finance_data as bronze_finance
from tasks.market_data import bronze_market_data as bronze_market
from tasks.price_target_data import bronze_price_target_data as bronze_price_target


def test_list_manager_disables_blacklist_mutations_when_configured() -> None:
    manager = ListManager(
        client=object(),
        folder="demo-domain",
        auto_flush=True,
        allow_blacklist_updates=False,
    )
    manager.blacklist = {"EXISTING"}
    manager._loaded = True

    with patch("core.pipeline.mdc.update_csv_set") as mock_update_csv_set:
        manager.add_to_blacklist("NEW")

    assert manager.blacklist == {"EXISTING"}
    assert manager._dirty_blacklist is False
    mock_update_csv_set.assert_not_called()


def test_bronze_domain_jobs_disable_blacklist_mutations() -> None:
    assert bronze_market.list_manager.allow_blacklist_updates is False
    assert bronze_price_target.list_manager.allow_blacklist_updates is False
    assert bronze_finance.list_manager.allow_blacklist_updates is False
    assert bronze_earnings.list_manager.allow_blacklist_updates is False
