import asyncio
import json
import threading
import uuid
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.earnings_data import bronze_earnings_data as bronze

@pytest.fixture
def unique_ticker():
    return f"TEST_EARN_{uuid.uuid4().hex[:8].upper()}"


def _sync_result() -> bronze.symbol_availability.SyncResult:
    return bronze.symbol_availability.SyncResult(
        provider="alpha_vantage",
        source_column="source_alpha_vantage",
        listed_count=1,
        inserted_count=0,
        disabled_count=0,
        duration_ms=1,
        lock_wait_ms=0,
    )

def _staged_frame(collected_symbol_frames: dict[str, pd.DataFrame], symbol: str) -> pd.DataFrame:
    assert symbol in collected_symbol_frames
    return collected_symbol_frames[symbol].sort_values(["date", "record_type"]).reset_index(drop=True)


def test_fetch_and_save_raw(unique_ticker):
    """
    Verifies fetch_and_save_raw:
    1. Checks blacklist (mocked).
    2. Calls API gateway client (mocked).
    3. Stages canonical alpha26 rows for bucket publication.
    """
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}

    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-01-01",
                "reportedEPS": "1.6",
                "estimatedEPS": "1.5",
                "surprisePercentage": "10.0",
            }
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )
        assert wrote is True

        mock_list_manager.add_to_whitelist.assert_called_with(symbol)
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 1
        row = staged.iloc[0]
        assert row["record_type"] == "actual"
        assert pd.Timestamp(row["date"]).date().isoformat() == "2024-01-01"
        assert row["reported_eps"] == pytest.approx(1.6)
        assert row["eps_estimate"] == pytest.approx(1.5)
        assert row["surprise"] == pytest.approx(0.1)


def test_fetch_and_save_raw_applies_backfill_start_cutoff(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
            {
                "fiscalDateEnding": "2024-03-31",
                "reportedEPS": "1.8",
                "estimatedEPS": "1.7",
                "surprisePercentage": "3.0",
            },
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=pd.Timestamp("2024-01-01"),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )
        assert wrote is True

        staged = _staged_frame(collected_symbol_frames, symbol)
        parsed_dates = [pd.Timestamp(value).date().isoformat() for value in staged["date"].tolist()]
        assert parsed_dates == ["2024-03-31"]


def test_fetch_and_save_raw_returns_false_when_cutoff_removes_all_rows(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2023-12-31",
                "reportedEPS": "1.4",
                "estimatedEPS": "1.2",
                "surprisePercentage": "5.0",
            },
        ],
    }

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=pd.Timestamp("2024-01-01"),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )
        assert wrote is False

        mock_list_manager.add_to_whitelist.assert_called_with(symbol)
        assert symbol not in collected_symbol_frames


def test_fetch_and_save_raw_requires_collected_symbol_frames(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {"symbol": symbol, "quarterlyEarnings": []}

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(ValueError, match="collected_symbol_frames"):
            bronze.fetch_and_save_raw(symbol, mock_av)


def test_fetch_and_save_raw_marks_missing_earnings_history_as_coverage_unavailable(unique_ticker):
    symbol = unique_ticker
    payload = {
        "symbol": symbol,
        "quarterlyEarnings": [],
        "note": "missing earnings history",
    }
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.BronzeCoverageUnavailableError) as exc_info:
            bronze.fetch_and_save_raw(symbol, mock_av, collected_symbol_frames={})

    assert exc_info.value.reason_code == "no_earnings_records"
    assert exc_info.value.payload == payload


def test_fetch_and_save_raw_merges_scheduled_calendar_rows(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2024-12-31",
                "reportedDate": "2025-02-10",
                "reportedEPS": "1.6",
                "estimatedEPS": "1.5",
                "surprisePercentage": "6.0",
            }
        ],
    }
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-07"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.7,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            calendar_rows=calendar_rows,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

        assert wrote is True
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 2
        scheduled = staged.loc[staged["record_type"] == "scheduled"].iloc[0]
        assert pd.Timestamp(scheduled["date"]).date().isoformat() == "2026-05-07"
        assert pd.Timestamp(scheduled["report_date"]).date().isoformat() == "2026-05-07"
        assert pd.isna(scheduled["reported_eps"])
        assert scheduled["calendar_time_of_day"] == "post-market"
        assert scheduled["calendar_currency"] == "USD"


def test_fetch_and_save_raw_replaces_stale_scheduled_row_when_calendar_date_moves(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {"symbol": symbol, "quarterlyEarnings": []}
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-08"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.8,
                "currency": "USD",
                "time_of_the_day": "post-market",
            },
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-01"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.7,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data._utc_today",
        return_value=pd.Timestamp("2026-05-02"),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            calendar_rows=calendar_rows,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

        assert wrote is True
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 1
        row = staged.iloc[0]
        assert row["record_type"] == "scheduled"
        assert pd.Timestamp(row["report_date"]).date().isoformat() == "2026-05-08"
        assert pd.Timestamp(row["fiscal_date_ending"]).date().isoformat() == "2026-03-31"
        assert row["eps_estimate"] == pytest.approx(1.8)


def test_fetch_and_save_raw_actual_replaces_scheduled_row_for_same_fiscal_period(unique_ticker):
    symbol = unique_ticker
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = {
        "symbol": symbol,
        "quarterlyEarnings": [
            {
                "fiscalDateEnding": "2026-03-31",
                "reportedDate": "2026-05-09",
                "reportedEPS": "1.9",
                "estimatedEPS": "1.8",
                "surprisePercentage": "5.5",
            }
        ],
    }
    calendar_rows = pd.DataFrame(
        [
            {
                "symbol": symbol,
                "name": "Test Co",
                "report_date": pd.Timestamp("2026-05-08"),
                "fiscal_date_ending": pd.Timestamp("2026-03-31"),
                "estimate": 1.8,
                "currency": "USD",
                "time_of_the_day": "post-market",
            }
        ]
    )

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data._utc_today",
        return_value=pd.Timestamp("2026-05-10"),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            calendar_rows=calendar_rows,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

        assert wrote is True
        staged = _staged_frame(collected_symbol_frames, symbol)
        assert len(staged) == 1
        row = staged.iloc[0]
        assert row["record_type"] == "actual"
        assert pd.Timestamp(row["fiscal_date_ending"]).date().isoformat() == "2026-03-31"
        assert pd.Timestamp(row["report_date"]).date().isoformat() == "2026-05-09"
        assert row["reported_eps"] == pytest.approx(1.9)


@pytest.mark.parametrize(
    ("payload", "expected_status", "expected_earliest"),
    [
        (
            {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2023-12-31",
                        "reportedEPS": "1.4",
                        "estimatedEPS": "1.2",
                        "surprisePercentage": "5.0",
                    },
                    {
                        "fiscalDateEnding": "2025-03-31",
                        "reportedEPS": "1.9",
                        "estimatedEPS": "1.8",
                        "surprisePercentage": "2.0",
                    },
                ]
            },
            "covered",
            date(2023, 12, 31),
        ),
        (
            {
                "quarterlyEarnings": [
                    {
                        "fiscalDateEnding": "2025-03-31",
                        "reportedEPS": "1.9",
                        "estimatedEPS": "1.8",
                        "surprisePercentage": "2.0",
                    },
                ]
            },
            "limited",
            date(2025, 3, 31),
        ),
    ],
)
def test_fetch_and_save_raw_marks_coverage_status_from_source_payload(
    unique_ticker,
    payload,
    expected_status,
    expected_earliest,
):
    symbol = unique_ticker
    payload = {"symbol": symbol, **payload}
    collected_symbol_frames: dict[str, pd.DataFrame] = {}
    mock_av = MagicMock()
    mock_av.get_earnings.return_value = payload

    coverage_summary = bronze._empty_coverage_summary()

    with patch("tasks.earnings_data.bronze_earnings_data.list_manager") as mock_list_manager, patch(
        "tasks.earnings_data.bronze_earnings_data._mark_coverage"
    ) as mock_mark_coverage:
        mock_list_manager.is_blacklisted.return_value = False

        wrote = bronze.fetch_and_save_raw(
            symbol,
            mock_av,
            backfill_start=date(2024, 1, 1),
            coverage_summary=coverage_summary,
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=threading.Lock(),
        )

    assert wrote is True
    assert symbol in collected_symbol_frames
    _, kwargs = mock_mark_coverage.call_args
    assert kwargs["status"] == expected_status
    assert kwargs["earliest_available"] == expected_earliest


def test_main_async_logs_invalid_payload_preview(unique_ticker):
    symbol = unique_ticker
    payload = {"detail": "X" * 700}
    expected_preview = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)[:500] + "..."
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError("invalid", payload=payload),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        assert mock_record_invalid.call_count == 1
        assert mock_record_invalid.call_args.kwargs["symbol"] == symbol
        mock_list_manager.add_to_blacklist.assert_not_called()
        warning_messages = [call.args[0] for call in mock_write_warning.call_args_list if call.args]
        assert any(
            message
            == (
                f"Bronze earnings invalid symbol candidate for {symbol}. payload_preview={expected_preview}"
            )
            for message in warning_messages
        )

    asyncio.run(run_test())


def test_main_async_logs_symbol_success(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )
    client_manager = MagicMock()
    client_manager.get_client.return_value = mock_av

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._ThreadLocalAlphaVantageClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            return_value=True,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(1, "earnings-data/buckets/index.parquet"),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ) as mock_write_line:
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        messages = [str(call.args[0]) for call in mock_write_line.call_args_list if call.args]
        assert any(
            f"Bronze earnings success: operation=symbol_processed symbol={symbol}" in message
            for message in messages
        )

    asyncio.run(run_test())


def test_main_async_writes_alpha26_buckets_and_cleans_flat_blobs(unique_ticker):
    symbol = unique_ticker
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )
    client_manager = MagicMock()
    client_manager.get_client.return_value = mock_av

    def fake_fetch_and_stage(symbol_arg, _av, **kwargs):
        kwargs["collected_symbol_frames"][symbol_arg] = pd.DataFrame({"symbol": [symbol_arg]})
        return True

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._ThreadLocalAlphaVantageClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=fake_fetch_and_stage,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(1, "earnings-data/buckets/index.parquet"),
        ) as mock_write_buckets, patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=3,
        ) as mock_delete_flat_blobs, patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_write_buckets.assert_called_once()
        collected_frames = mock_write_buckets.call_args.args[0]
        assert symbol in collected_frames
        assert collected_frames[symbol]["symbol"].tolist() == [symbol]
        assert mock_write_buckets.call_args.kwargs["run_id"]
        mock_delete_flat_blobs.assert_called_once_with()
        mock_list_manager.flush.assert_called_once_with()

    asyncio.run(run_test())


def test_main_async_logs_invalid_payload_detail_preview_when_payload_missing(unique_ticker):
    symbol = unique_ticker
    detail = "X" * 700
    expected_payload = {"status_code": 404, "detail": detail, "message": "invalid"}
    expected_preview = json.dumps(expected_payload, separators=(",", ":"), ensure_ascii=False)[:500] + "..."
    mock_av = MagicMock()
    mock_av.get_earnings_calendar_csv.return_value = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    )

    async def run_test():
        with patch(
            "tasks.earnings_data.bronze_earnings_data._validate_environment"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.AlphaVantageGatewayClient.from_env",
            return_value=mock_av,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.fetch_and_save_raw",
            side_effect=bronze.AlphaVantageGatewayInvalidSymbolError(
                "invalid",
                status_code=404,
                detail=detail,
            ),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.earnings_data.bronze_earnings_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._write_alpha26_earnings_buckets",
            return_value=(0, None),
        ), patch(
            "tasks.earnings_data.bronze_earnings_data._delete_flat_symbol_blobs",
            return_value=0,
        ), patch(
            "tasks.earnings_data.bronze_earnings_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_warning"
        ) as mock_write_warning, patch(
            "tasks.earnings_data.bronze_earnings_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_record_invalid.assert_called_once()
        mock_list_manager.add_to_blacklist.assert_not_called()
        warning_messages = [call.args[0] for call in mock_write_warning.call_args_list if call.args]
        assert any(
            message
            == (
                f"Bronze earnings invalid symbol candidate for {symbol}. payload_preview={expected_preview}"
            )
            for message in warning_messages
        )

    asyncio.run(run_test())


def test_failure_bucket_key_includes_status_and_path():
    exc = bronze.AlphaVantageGatewayError(
        "gateway unavailable",
        status_code=504,
        detail="timeout",
        payload={"path": "/api/providers/alpha-vantage/earnings"},
    )
    key = bronze._failure_bucket_key(exc)

    assert "type=AlphaVantageGatewayError" in key
    assert "status=504" in key
    assert "path=/api/providers/alpha-vantage/earnings" in key


def test_coerce_datetime_column_skips_numeric_parse_for_iso_dates(monkeypatch):
    original = bronze.pd.to_datetime

    def guarded_to_datetime(*args, **kwargs):
        if kwargs.get("unit") == "ms":
            raise AssertionError("unexpected numeric datetime parse for ISO date strings")
        return original(*args, **kwargs)

    monkeypatch.setattr(bronze.pd, "to_datetime", guarded_to_datetime)

    series = pd.Series(["2025-12-31", "2025-09-30", None], dtype="object")
    parsed = bronze._coerce_datetime_column(series)

    assert parsed.tolist()[:2] == [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")]
    assert pd.isna(parsed.iloc[2])


def test_coerce_datetime_column_parses_epoch_milliseconds_strings():
    first_ms = str(int(pd.Timestamp("2025-12-31", tz="UTC").timestamp() * 1000))
    second_ms = str(int(pd.Timestamp("2025-09-30", tz="UTC").timestamp() * 1000))

    series = pd.Series([first_ms, second_ms, None], dtype="object")
    parsed = bronze._coerce_datetime_column(series)

    assert parsed.tolist()[:2] == [pd.Timestamp("2025-12-31"), pd.Timestamp("2025-09-30")]
    assert pd.isna(parsed.iloc[2])


def test_thread_local_alpha_vantage_client_manager_reuses_per_thread_and_closes_all():
    created: list[MagicMock] = []

    def make_client() -> MagicMock:
        client = MagicMock()
        created.append(client)
        return client

    manager = bronze._ThreadLocalAlphaVantageClientManager(factory=make_client)
    first = manager.get_client()
    second = manager.get_client()
    assert first is second

    observed_from_thread: list[MagicMock] = []

    def worker() -> None:
        observed_from_thread.append(manager.get_client())

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()

    assert len(created) == 2
    assert observed_from_thread[0] is not first

    manager.close_all()

    for client in created:
        client.close.assert_called_once_with()
