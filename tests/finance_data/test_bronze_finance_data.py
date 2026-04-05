import asyncio
import json
import uuid
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.finance_data import bronze_finance_data as bronze


@pytest.fixture
def unique_ticker():
    return f"TEST_FIN_{uuid.uuid4().hex[:8].upper()}"


def _sync_result() -> bronze.symbol_availability.SyncResult:
    return bronze.symbol_availability.SyncResult(
        provider="massive",
        source_column="source_massive",
        listed_count=1,
        inserted_count=0,
        disabled_count=0,
        duration_ms=1,
        lock_wait_ms=0,
    )


def _statement_payload(*rows: dict) -> dict:
    return {
        "status": "OK",
        "request_id": "req-1",
        "results": list(rows),
    }


def test_fetch_and_save_raw_writes_raw_balance_sheet_payload(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    raw_payload = _statement_payload(
        {
            "period_end": "2024-12-31",
            "timeframe": "annual",
            "total_assets": 1200.0,
            "total_current_assets": 600.0,
            "total_current_liabilities": 300.0,
            "long_term_debt_and_capital_lease_obligations": 240.0,
        },
        {
            "period_end": "2024-03-31",
            "timeframe": "quarterly",
            "total_assets": 1000.0,
            "total_current_assets": 500.0,
            "total_current_liabilities": 250.0,
            "long_term_debt_and_capital_lease_obligations": 200.0,
        },
    )
    mock_massive.get_finance_report.return_value = raw_payload
    row_store: dict[tuple[str, str], dict[str, object]] = {}

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            alpha26_mode=True,
            alpha26_rows=row_store,
        )

    assert wrote is True
    stored = row_store[(symbol, "balance_sheet")]
    payload = json.loads(str(stored["payload_json"]))
    assert payload == raw_payload
    assert stored["source_min_date"] == "2024-03-31"
    assert stored["source_max_date"] == "2024-12-31"
    mock_massive.get_finance_report.assert_called_once_with(
        symbol=symbol,
        report="balance_sheet",
        pagination=True,
    )


def test_fetch_and_save_raw_marks_empty_valuation_payload_as_coverage_unavailable(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_ratios.return_value = {"results": []}

    report = {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "valuation",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.BronzeCoverageUnavailableError) as exc_info:
            bronze.fetch_and_save_raw(symbol, report, mock_massive, alpha26_mode=True, alpha26_rows={})

        assert exc_info.value.reason_code == "empty_finance_payload"
        assert exc_info.value.payload == {"symbol": symbol, "report": "valuation"}
        mock_list_manager.add_to_blacklist.assert_not_called()
        mock_massive.get_ratios.assert_called_once_with(symbol=symbol, pagination=True)


def test_fetch_and_save_raw_writes_full_history_valuation_payload(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    raw_payload = {
        "status": "OK",
        "request_id": "req-1",
        "results": [
            {"date": "2024-03-31", "market_cap": 900.0, "price_to_earnings": 18.0},
            {"date": "2024-06-30", "market_cap": 1000.0, "price_to_earnings": 20.0},
        ],
    }
    mock_massive.get_ratios.return_value = raw_payload
    row_store: dict[tuple[str, str], dict[str, object]] = {}
    coverage_summary = bronze._empty_coverage_summary()

    report = {
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "report": "valuation",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            coverage_summary=coverage_summary,
            alpha26_mode=True,
            alpha26_rows=row_store,
        )

    assert wrote is True
    stored = row_store[(symbol, "valuation")]
    payload = json.loads(str(stored["payload_json"]))
    assert payload == raw_payload
    assert stored["source_min_date"] == "2024-03-31"
    assert stored["source_max_date"] == "2024-06-30"
    assert coverage_summary["provider_valuation_requests"] == 1
    assert coverage_summary["provider_valuation_nonempty_raw_payloads"] == 1
    assert coverage_summary["provider_valuation_canonical_rows"] == 2
    mock_massive.get_ratios.assert_called_once_with(symbol=symbol, pagination=True)


def test_fetch_and_save_raw_tracks_empty_statement_payload_diagnostics(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_finance_report.return_value = {"status": "OK", "request_id": "empty", "results": []}
    coverage_summary = bronze._empty_coverage_summary()

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.BronzeCoverageUnavailableError):
            bronze.fetch_and_save_raw(
                symbol,
                report,
                mock_massive,
                coverage_summary=coverage_summary,
                alpha26_mode=True,
                alpha26_rows={},
            )

    assert coverage_summary["provider_statement_requests"] == 1
    assert coverage_summary["provider_statement_empty_raw_payloads"] == 1
    assert coverage_summary["provider_statement_nonempty_raw_payloads"] == 0
    assert coverage_summary["provider_statement_canonical_rows"] == 0
    assert coverage_summary["provider_statement_canonical_empty_payloads"] == 1


def test_fetch_and_save_raw_preserves_raw_payload_when_backfill_cutoff_is_set(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    raw_payload = _statement_payload(
        {
            "period_end": "2023-12-31",
            "timeframe": "annual",
            "total_assets": 900.0,
        },
        {
            "period_end": "2024-03-31",
            "timeframe": "quarterly",
            "total_assets": 1000.0,
        },
    )
    mock_massive.get_finance_report.return_value = raw_payload
    row_store: dict[tuple[str, str], dict[str, object]] = {}

    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            backfill_start=date(2024, 1, 1),
            alpha26_mode=True,
            alpha26_rows=row_store,
        )

    assert wrote is True
    stored = row_store[(symbol, "balance_sheet")]
    payload = json.loads(str(stored["payload_json"]))
    assert payload == raw_payload
    assert stored["source_min_date"] == "2023-12-31"
    assert stored["source_max_date"] == "2024-03-31"


def test_fetch_and_save_raw_coverage_gap_overrides_fresh_current_payload(unique_ticker):
    symbol = unique_ticker
    existing_payload = {
        "status": "OK",
        "request_id": "old",
        "results": [
            {
                "period_end": "2025-01-01",
                "timeframe": "quarterly",
                "total_assets": 100.0,
            }
        ],
    }
    existing_row = bronze._build_finance_bucket_row(
        symbol=symbol,
        report_type="balance_sheet",
        payload=existing_payload,
        source_min_date=date(2025, 1, 1),
        source_max_date=date(2025, 1, 1),
    )
    existing_row["ingested_at"] = datetime.now(timezone.utc).isoformat()

    mock_massive = MagicMock()
    mock_massive.get_finance_report.return_value = _statement_payload(
        {
            "period_end": "2023-12-31",
            "timeframe": "annual",
            "total_assets": 90.0,
        },
        {
            "period_end": "2025-03-31",
            "timeframe": "quarterly",
            "total_assets": 110.0,
        },
    )
    row_store = {(symbol, "balance_sheet"): dict(existing_row)}
    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }
    coverage_summary = bronze._empty_coverage_summary()

    with (
        patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager,
        patch(
            "tasks.finance_data.bronze_finance_data.load_coverage_marker",
            return_value=None,
        ),
        patch("tasks.finance_data.bronze_finance_data._mark_coverage") as mock_mark_coverage,
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            backfill_start=date(2024, 1, 1),
            coverage_summary=coverage_summary,
            alpha26_mode=True,
            alpha26_existing_row=dict(existing_row),
            alpha26_rows=row_store,
        )

    assert wrote is True
    assert coverage_summary["coverage_checked"] == 1
    assert coverage_summary["coverage_forced_refetch"] == 1
    assert mock_massive.get_finance_report.call_count == 1
    mock_mark_coverage.assert_called_once()


def test_fetch_and_save_raw_rewrites_fresh_legacy_canonical_payload_to_raw(unique_ticker):
    symbol = unique_ticker
    existing_payload = {
        "schema_version": 2,
        "provider": "massive",
        "report_type": "balance_sheet",
        "rows": [
            {
                "date": "2025-03-31",
                "timeframe": "quarterly",
                "total_assets": 100.0,
            }
        ],
    }
    existing_row = bronze._build_finance_bucket_row(
        symbol=symbol,
        report_type="balance_sheet",
        payload=existing_payload,
        source_min_date=date(2025, 3, 31),
        source_max_date=date(2025, 3, 31),
    )
    existing_row["ingested_at"] = datetime.now(timezone.utc).isoformat()

    raw_payload = _statement_payload(
        {
            "period_end": "2025-03-31",
            "timeframe": "quarterly",
            "total_assets": 100.0,
        }
    )
    mock_massive = MagicMock()
    mock_massive.get_finance_report.return_value = raw_payload
    row_store = {(symbol, "balance_sheet"): dict(existing_row)}
    report = {
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "report": "balance_sheet",
    }

    with patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            report,
            mock_massive,
            alpha26_mode=True,
            alpha26_existing_row=dict(existing_row),
            alpha26_rows=row_store,
        )

    assert wrote is True
    stored = row_store[(symbol, "balance_sheet")]
    assert json.loads(str(stored["payload_json"])) == raw_payload


def test_process_symbol_with_recovery_retries_transient_report(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    attempts: dict[str, int] = {"balance_sheet": 0}

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        if report_name == "balance_sheet":
            attempts["balance_sheet"] += 1
            if attempts["balance_sheet"] == 1:
                raise bronze.MassiveGatewayRateLimitError("throttled", status_code=429)
            return True
        return False

    with (
        patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch),
        patch("tasks.finance_data.bronze_finance_data.time.sleep") as mock_sleep,
    ):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.5,
        )

    assert result.wrote == 1
    assert result.invalid_candidate is False
    assert result.valid_symbol is True
    assert result.failures == []
    assert attempts["balance_sheet"] == 2
    assert result.coverage_summary["coverage_checked"] == 0
    manager.reset_current.assert_called_once()
    mock_sleep.assert_called_once_with(0.5)


def test_process_symbol_with_recovery_does_not_retry_provider_400(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    attempts = {"balance_sheet": 0}

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        if report_name == "balance_sheet":
            attempts["balance_sheet"] += 1
            raise bronze.MassiveGatewayError("invalid query parameter", status_code=400, detail="bad request")
        return False

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert attempts["balance_sheet"] == 1
    assert any(name == "balance_sheet" for name, _ in result.failures)
    manager.reset_current.assert_not_called()


def test_process_symbol_with_recovery_continues_after_single_invalid_core_report(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    seen_reports: list[str] = []

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        seen_reports.append(report_name)
        if report_name == "balance_sheet":
            raise bronze.MassiveGatewayNotFoundError("invalid", status_code=404)
        return True

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert result.wrote == len(bronze.REPORTS) - 1
    assert result.invalid_candidate is False
    assert result.valid_symbol is True
    assert result.coverage_unavailable is True
    assert result.failures == []
    assert [name for name, _ in result.invalid_evidence] == ["balance_sheet"]
    assert result.coverage_summary["coverage_checked"] == 0
    manager.reset_current.assert_not_called()
    assert seen_reports == [report["report"] for report in bronze.REPORTS]


def test_process_symbol_with_recovery_emits_invalid_candidate_only_after_all_core_invalid(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    seen_reports: list[str] = []

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        seen_reports.append(report_name)
        if bronze._is_core_finance_report(report_name):
            raise bronze.MassiveGatewayNotFoundError("invalid", status_code=404)
        return False

    with patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert result.wrote == 0
    assert result.invalid_candidate is True
    assert result.valid_symbol is False
    assert result.coverage_unavailable is False
    assert result.failures == []
    assert {name for name, _ in result.invalid_evidence} == set(bronze._CORE_FINANCE_REPORTS)
    assert seen_reports == [report["report"] for report in bronze.REPORTS]


def test_process_symbol_with_recovery_downgrades_valuation_transient_failure_when_core_reports_succeed(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    valuation_attempts = 0

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        nonlocal valuation_attempts
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        if report_name == "valuation":
            valuation_attempts += 1
            raise bronze.MassiveGatewayError(
                "gateway unavailable",
                status_code=503,
                detail="upstream unavailable",
                payload={"path": "/api/providers/massive/fundamentals/ratios"},
            )
        return True

    with (
        patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch),
        patch("tasks.finance_data.bronze_finance_data.time.sleep") as mock_sleep,
    ):
        result = bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=3,
            sleep_seconds=0.0,
        )

    assert result.wrote == len(bronze.REPORTS) - 1
    assert result.invalid_candidate is False
    assert result.valid_symbol is True
    assert result.coverage_unavailable is True
    assert result.failures == []
    assert [name for name, _ in result.invalid_evidence] == ["valuation"]
    assert valuation_attempts == 3
    assert result.coverage_summary["coverage_checked"] == 0
    assert manager.reset_current.call_count == 2
    mock_sleep.assert_not_called()


def test_process_symbol_with_recovery_transient_warning_includes_failure_details(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    manager = MagicMock()
    manager.get_client.return_value = mock_massive
    attempts: dict[str, int] = {"balance_sheet": 0}

    def _fake_fetch(symbol_arg, report, massive_client, *, backfill_start=None, coverage_summary=None):
        assert symbol_arg == symbol
        assert massive_client is mock_massive
        del backfill_start, coverage_summary
        report_name = report["report"]
        if report_name == "balance_sheet":
            attempts["balance_sheet"] += 1
            if attempts["balance_sheet"] == 1:
                raise bronze.MassiveGatewayError(
                    "gateway unavailable",
                    status_code=503,
                    detail="upstream unavailable",
                    payload={"path": "/api/providers/massive/financials/balance_sheet"},
                )
        return False

    with (
        patch("tasks.finance_data.bronze_finance_data.fetch_and_save_raw", side_effect=_fake_fetch),
        patch("tasks.finance_data.bronze_finance_data.mdc.write_warning") as mock_write_warning,
    ):
        bronze._process_symbol_with_recovery(
            symbol,
            manager,
            max_attempts=2,
            sleep_seconds=0.0,
        )

    transient_messages = [
        str(call.args[0])
        for call in mock_write_warning.call_args_list
        if call.args and "Transient Massive error for" in str(call.args[0])
    ]
    assert transient_messages
    assert "details=report=balance_sheet" in transient_messages[0]
    assert "status=503" in transient_messages[0]
    assert "path=/api/providers/massive/financials/balance_sheet" in transient_messages[0]


def test_main_async_returns_success_when_symbol_is_only_invalid_candidate(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()
    coverage_summary = bronze._empty_coverage_summary()
    invalid_error = bronze.MassiveGatewayNotFoundError("invalid", status_code=404)

    async def run_test():
        with (
            patch("tasks.finance_data.bronze_finance_data._validate_environment"),
            patch("tasks.finance_data.bronze_finance_data.mdc.log_environment_diagnostics"),
            patch(
                "tasks.finance_data.bronze_finance_data.symbol_availability.sync_domain_availability",
                return_value=_sync_result(),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.symbol_availability.get_domain_symbols",
                return_value=pd.DataFrame({"Symbol": [symbol]}),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.bronze_bucketing.is_alpha26_mode",
                return_value=True,
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._load_alpha26_finance_row_map",
                return_value={},
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.resolve_backfill_start_date",
                return_value=None,
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._ThreadLocalMassiveClientManager",
                return_value=client_manager,
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._process_symbol_with_recovery",
                return_value=bronze._FinanceSymbolOutcome(
                    wrote=0,
                    valid_symbol=False,
                    invalid_candidate=True,
                    coverage_unavailable=False,
                    invalid_evidence=[
                        ("balance_sheet", invalid_error),
                        ("cash_flow", invalid_error),
                        ("income_statement", invalid_error),
                    ],
                    failures=[],
                    coverage_summary=coverage_summary,
                ),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.record_invalid_symbol_candidate",
                return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
            ) as mock_record_invalid,
            patch("tasks.finance_data.bronze_finance_data.clear_invalid_candidate_marker"),
            patch(
                "tasks.finance_data.bronze_finance_data._write_alpha26_finance_buckets",
                return_value=(0, "index", len(bronze._BUCKET_COLUMNS)),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._delete_flat_finance_symbol_blobs",
                return_value=0,
            ),
            patch("tasks.finance_data.bronze_finance_data.bronze_client") as mock_bronze_client,
            patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager,
            patch("tasks.finance_data.bronze_finance_data.mdc.write_line"),
            patch("tasks.finance_data.bronze_finance_data.mdc.write_warning"),
        ):
            mock_bronze_client.list_blob_infos.return_value = []
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_record_invalid.assert_called_once()
        mock_list_manager.add_to_blacklist.assert_not_called()
        mock_list_manager.flush.assert_called_once()
        client_manager.close_all.assert_called_once()

    asyncio.run(run_test())


def test_main_async_logs_symbol_success(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()
    coverage_summary = bronze._empty_coverage_summary()

    async def run_test():
        with (
            patch("tasks.finance_data.bronze_finance_data._validate_environment"),
            patch("tasks.finance_data.bronze_finance_data.mdc.log_environment_diagnostics"),
            patch(
                "tasks.finance_data.bronze_finance_data.symbol_availability.sync_domain_availability",
                return_value=_sync_result(),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.symbol_availability.get_domain_symbols",
                return_value=pd.DataFrame({"Symbol": [symbol]}),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.bronze_bucketing.is_alpha26_mode",
                return_value=True,
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._load_alpha26_finance_row_map",
                return_value={},
            ),
            patch(
                "tasks.finance_data.bronze_finance_data.resolve_backfill_start_date",
                return_value=None,
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._ThreadLocalMassiveClientManager",
                return_value=client_manager,
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._process_symbol_with_recovery",
                return_value=bronze._FinanceSymbolOutcome(
                    wrote=2,
                    valid_symbol=True,
                    invalid_candidate=False,
                    coverage_unavailable=False,
                    invalid_evidence=[],
                    failures=[],
                    coverage_summary=coverage_summary,
                ),
            ),
            patch("tasks.finance_data.bronze_finance_data.clear_invalid_candidate_marker"),
            patch(
                "tasks.finance_data.bronze_finance_data._write_alpha26_finance_buckets",
                return_value=(1, "index", len(bronze._BUCKET_COLUMNS)),
            ),
            patch(
                "tasks.finance_data.bronze_finance_data._delete_flat_finance_symbol_blobs",
                return_value=0,
            ),
            patch("tasks.finance_data.bronze_finance_data.bronze_client") as mock_bronze_client,
            patch("tasks.finance_data.bronze_finance_data.list_manager") as mock_list_manager,
            patch("tasks.finance_data.bronze_finance_data.mdc.write_line") as mock_write_line,
            patch("tasks.finance_data.bronze_finance_data.mdc.write_warning"),
        ):
            mock_bronze_client.list_blob_infos.return_value = []
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        messages = [str(call.args[0]) for call in mock_write_line.call_args_list if call.args]
        assert any(
            f"Bronze finance success: operation=symbol_processed symbol={symbol}" in message
            for message in messages
        )
        assert any("Bronze finance success: operation=list_flush" in message for message in messages)

    asyncio.run(run_test())


def test_run_bronze_finance_job_entrypoint_skips_downstream_after_nonzero_exit() -> None:
    call_order: list[str] = []
    captured_on_success: list[object] = []

    class _FakeLock:
        def __init__(self, name: str, **_kwargs):
            self.name = name

        def __enter__(self):
            call_order.append(f"enter:{self.name}")
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            call_order.append(f"exit:{self.name}")
            return False

    def _fake_run_logged_job(*, job_name, run, on_success):
        assert job_name == "bronze-finance-job"
        assert run is bronze.main
        captured_on_success.extend(on_success)
        call_order.append("run")
        return 1

    result = bronze.run_bronze_finance_job_entrypoint(
        run_logged_job_fn=_fake_run_logged_job,
        ensure_api_awake_fn=lambda *, required: call_order.append(f"awake:{required}"),
        trigger_next_job_fn=lambda: call_order.append("trigger"),
        write_system_health_marker_fn=lambda **kwargs: call_order.append(f"marker:{kwargs['job_name']}"),
        job_lock_factory=_FakeLock,
        shared_lock_name="finance-pipeline-shared",
        shared_wait_timeout=0.0,
    )

    assert result == 1
    assert len(captured_on_success) == 1
    assert call_order == [
        "awake:True",
        "enter:finance-pipeline-shared",
        "enter:bronze-finance-job",
        "run",
        "exit:bronze-finance-job",
        "exit:finance-pipeline-shared",
    ]


def test_run_bronze_finance_job_entrypoint_skips_downstream_after_exception() -> None:
    call_order: list[str] = []

    class _FakeLock:
        def __init__(self, name: str, **_kwargs):
            self.name = name

        def __enter__(self):
            call_order.append(f"enter:{self.name}")
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            call_order.append(f"exit:{self.name}")
            return False

    def _fake_run_logged_job(*, job_name, run, on_success):
        assert job_name == "bronze-finance-job"
        assert run is bronze.main
        assert len(on_success) == 1
        call_order.append("run")
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        bronze.run_bronze_finance_job_entrypoint(
            run_logged_job_fn=_fake_run_logged_job,
            ensure_api_awake_fn=lambda *, required: call_order.append(f"awake:{required}"),
            trigger_next_job_fn=lambda: call_order.append("trigger"),
            write_system_health_marker_fn=lambda **kwargs: call_order.append(f"marker:{kwargs['job_name']}"),
            job_lock_factory=_FakeLock,
            shared_lock_name="finance-pipeline-shared",
            shared_wait_timeout=0.0,
        )

    assert call_order == [
        "awake:True",
        "enter:finance-pipeline-shared",
        "enter:bronze-finance-job",
        "run",
        "exit:bronze-finance-job",
        "exit:finance-pipeline-shared",
    ]


def test_run_bronze_finance_job_entrypoint_triggers_downstream_once_on_success() -> None:
    call_order: list[str] = []

    class _FakeLock:
        def __init__(self, name: str, **_kwargs):
            self.name = name

        def __enter__(self):
            call_order.append(f"enter:{self.name}")
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            call_order.append(f"exit:{self.name}")
            return False

    def _fake_run_logged_job(*, job_name, run, on_success):
        assert job_name == "bronze-finance-job"
        assert run is bronze.main
        call_order.append("run")
        for callback in on_success:
            callback()
        return 0

    result = bronze.run_bronze_finance_job_entrypoint(
        run_logged_job_fn=_fake_run_logged_job,
        ensure_api_awake_fn=lambda *, required: call_order.append(f"awake:{required}"),
        trigger_next_job_fn=lambda: call_order.append("trigger"),
        write_system_health_marker_fn=lambda **kwargs: call_order.append(f"marker:{kwargs['job_name']}"),
        job_lock_factory=_FakeLock,
        shared_lock_name="finance-pipeline-shared",
        shared_wait_timeout=0.0,
    )

    assert result == 0
    assert call_order == [
        "awake:True",
        "enter:finance-pipeline-shared",
        "enter:bronze-finance-job",
        "run",
        "marker:bronze-finance-job",
        "exit:bronze-finance-job",
        "exit:finance-pipeline-shared",
        "trigger",
    ]


def test_run_bronze_finance_job_entrypoint_triggers_downstream_after_locked_section() -> None:
    call_order: list[str] = []
    active_locks: list[str] = []

    class _FakeLock:
        def __init__(self, name: str, **_kwargs):
            self.name = name

        def __enter__(self):
            active_locks.append(self.name)
            call_order.append(f"enter:{self.name}")
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            call_order.append(f"exit:{self.name}")
            assert active_locks[-1] == self.name
            active_locks.pop()
            return False

    def _fake_run_logged_job(*, on_success, **_kwargs):
        call_order.append(f"run_with_active_locks:{','.join(active_locks)}")
        for callback in on_success:
            callback()
        return 0

    def _fake_trigger() -> None:
        call_order.append(f"trigger_with_active_locks:{','.join(active_locks)}")
        assert active_locks == []

    result = bronze.run_bronze_finance_job_entrypoint(
        run_logged_job_fn=_fake_run_logged_job,
        ensure_api_awake_fn=lambda *, required: call_order.append(f"awake:{required}"),
        trigger_next_job_fn=_fake_trigger,
        write_system_health_marker_fn=lambda **kwargs: call_order.append(f"marker:{kwargs['job_name']}"),
        job_lock_factory=_FakeLock,
        shared_lock_name="finance-pipeline-shared",
        shared_wait_timeout=0.0,
    )

    assert result == 0
    assert call_order[-1] == "trigger_with_active_locks:"


def test_run_bronze_finance_job_entrypoint_raises_when_successful_run_cannot_trigger_downstream() -> None:
    with pytest.raises(RuntimeError, match="downstream unavailable"):
        bronze.run_bronze_finance_job_entrypoint(
            run_logged_job_fn=lambda **kwargs: 0,
            ensure_api_awake_fn=lambda *, required: None,
            trigger_next_job_fn=lambda: (_ for _ in ()).throw(RuntimeError("downstream unavailable")),
            write_system_health_marker_fn=lambda **kwargs: None,
            job_lock_factory=lambda *args, **kwargs: type(
                "_NoopLock",
                (),
                {
                    "__enter__": lambda self: self,
                    "__exit__": lambda self, exc_type, exc, tb: False,
                },
            )(),
            shared_lock_name="finance-pipeline-shared",
            shared_wait_timeout=0.0,
        )
