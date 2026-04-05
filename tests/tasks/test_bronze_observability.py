from unittest.mock import patch

from tasks.common import bronze_observability


def test_should_log_bronze_success_samples_initial_entries_and_interval():
    assert bronze_observability.should_log_bronze_success(1) is True
    assert bronze_observability.should_log_bronze_success(20) is True
    assert bronze_observability.should_log_bronze_success(21) is False
    assert bronze_observability.should_log_bronze_success(250) is True
    assert bronze_observability.should_log_bronze_success(251) is False


def test_log_bronze_success_formats_context_fields():
    with patch("tasks.common.bronze_observability.mdc.write_line") as mock_write_line:
        bronze_observability.log_bronze_success(
            domain="finance",
            operation="symbol_processed",
            symbol="AAPL",
            success_count=3,
            coverage_unavailable=False,
            note=None,
        )

    mock_write_line.assert_called_once_with(
        "Bronze finance success: operation=symbol_processed symbol=AAPL success_count=3 "
        "coverage_unavailable=false note=n/a"
    )
