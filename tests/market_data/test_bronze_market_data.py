import asyncio
import uuid
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from tasks.market_data import bronze_market_data as bronze


@pytest.fixture
def unique_ticker():
    return f"TEST_MKT_{uuid.uuid4().hex[:8].upper()}"


def _market_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    return pd.DataFrame(rows)


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


def _empty_existing_bucket_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=bronze._EXISTING_MARKET_BUCKET_COLUMNS)


def _fake_empty_bucket_load(*, bucket: str) -> pd.DataFrame:
    del bucket
    return _empty_existing_bucket_frame()


def _fake_publish_result(
    *,
    written_symbols: int = 0,
    index_path: str = "system/bronze-index/market/latest.parquet",
    manifest_path: str = "system/manifests/bronze/market/latest.json",
    file_count: int = 26,
) -> MagicMock:
    return MagicMock(
        written_symbols=written_symbols,
        index_path=index_path,
        manifest_path=manifest_path,
        file_count=file_count,
    )


def test_download_and_stage_market_data_fetches_full_history_for_new_symbol(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {
        "symbol": symbol,
        "status": "ok",
        "rows": [
            {
                "date": "2024-01-02",
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "short_interest": 1000,
                "short_volume": None,
            },
            {
                "date": "2024-01-03",
                "open": 10.5,
                "high": 12,
                "low": 10,
                "close": 11,
                "volume": 150,
                "short_interest": 1200,
                "short_volume": 500,
            },
        ],
    }
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(
            symbol,
            mock_massive,
            collected_symbol_frames=collected_frames,
        )

    _, fetch_kwargs = mock_massive.get_market_history.call_args
    assert fetch_kwargs["from_date"] == "2016-01-01"
    assert fetch_kwargs["to_date"] is not None
    assert symbol in collected_frames
    staged = collected_frames[symbol]
    assert staged["Date"].tolist() == ["2024-01-02", "2024-01-03"]
    assert float(staged["ShortInterest"].iloc[-1]) == pytest.approx(1200.0)
    assert pd.isna(staged["ShortVolume"].iloc[0])
    assert float(staged["ShortVolume"].iloc[-1]) == pytest.approx(500.0)


def test_no_history_payload_marks_symbol_as_coverage_unavailable(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {"symbol": symbol, "status": "no_history", "rows": []}
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False

        with pytest.raises(bronze.BronzeCoverageUnavailableError) as exc_info:
            bronze.download_and_save_raw(
                symbol,
                mock_massive,
                collected_symbol_frames=collected_frames,
            )

        assert exc_info.value.reason_code == "provider_no_market_history"
        mock_list_manager.add_to_blacklist.assert_not_called()
        assert symbol not in collected_frames


def test_no_history_with_existing_data_stages_existing_frame_and_does_not_blacklist(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {"symbol": symbol, "status": "no_history", "rows": []}
    existing_df = _market_frame(
        [
            {
                "Date": "2024-01-03",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 100.0,
                "ShortInterest": 1000.0,
                "ShortVolume": 500.0,
            }
        ]
    )
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False
        bronze.download_and_save_raw(
            symbol,
            mock_massive,
            collected_symbol_frames=collected_frames,
            existing_symbol_df=existing_df,
        )

        mock_list_manager.add_to_blacklist.assert_not_called()
        mock_list_manager.add_to_whitelist.assert_called_once_with(symbol)

    pd.testing.assert_frame_equal(collected_frames[symbol], bronze._canonical_market_df(existing_df))


def test_download_allows_regime_required_symbol_even_when_blacklisted():
    symbol = "^VIX"
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {
        "symbol": symbol,
        "status": "ok",
        "rows": [
            {
                "date": "2024-01-02",
                "open": 20,
                "high": 21,
                "low": 19,
                "close": 20.5,
                "volume": 100,
                "short_interest": None,
                "short_volume": None,
            }
        ],
    }
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 2),
    ):
        mock_list_manager.is_blacklisted.return_value = True

        bronze.download_and_save_raw(
            symbol,
            mock_massive,
            collected_symbol_frames=collected_frames,
        )

        mock_massive.get_market_history.assert_called_once()
        mock_list_manager.add_to_whitelist.assert_called_once_with(symbol)
        assert symbol in collected_frames


def test_download_uses_existing_data_window_and_merges(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {
        "symbol": symbol,
        "status": "ok",
        "rows": [
            {
                "date": "2024-01-03",
                "open": 20,
                "high": 21,
                "low": 19,
                "close": 20.5,
                "volume": 200,
                "short_interest": 1000,
                "short_volume": 500,
            },
            {
                "date": "2024-01-04",
                "open": 21,
                "high": 22,
                "low": 20,
                "close": 21.5,
                "volume": 250,
                "short_interest": 1500,
                "short_volume": 700,
            },
        ],
    }
    existing_df = _market_frame(
        [
            {
                "Date": "2024-01-02",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 100.0,
                "ShortInterest": 1000.0,
                "ShortVolume": 500.0,
            },
            {
                "Date": "2024-01-03",
                "Open": 11.0,
                "High": 12.0,
                "Low": 10.0,
                "Close": 11.5,
                "Volume": 120.0,
                "ShortInterest": 1000.0,
                "ShortVolume": 500.0,
            },
        ]
    )
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 4),
    ):
        mock_list_manager.is_blacklisted.return_value = False
        bronze.download_and_save_raw(
            symbol,
            mock_massive,
            collected_symbol_frames=collected_frames,
            existing_symbol_df=existing_df,
        )

    _, fetch_kwargs = mock_massive.get_market_history.call_args
    assert fetch_kwargs["from_date"] == "2024-01-03"
    assert fetch_kwargs["to_date"] == "2024-01-04"
    staged = collected_frames[symbol]
    assert staged["Date"].tolist() == ["2024-01-02", "2024-01-03", "2024-01-04"]
    assert float(staged.loc[staged["Date"] == "2024-01-03", "Close"].iloc[0]) == pytest.approx(20.5)
    assert float(staged.loc[staged["Date"] == "2024-01-04", "ShortInterest"].iloc[0]) == pytest.approx(1500.0)


def test_download_uses_2016_floor_when_existing_history_predates_floor(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {
        "symbol": symbol,
        "status": "ok",
        "rows": [
            {
                "date": "2016-01-04",
                "open": 11.0,
                "high": 12.0,
                "low": 10.0,
                "close": 11.5,
                "volume": 150.0,
                "short_interest": None,
                "short_volume": None,
            }
        ],
    }
    existing_df = _market_frame(
        [
            {
                "Date": "2015-12-31",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 100.0,
                "ShortInterest": 1000.0,
                "ShortVolume": 500.0,
            }
        ]
    )
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2016, 1, 4),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(
            symbol,
            mock_massive,
            collected_symbol_frames=collected_frames,
            existing_symbol_df=existing_df,
        )

    _, fetch_kwargs = mock_massive.get_market_history.call_args
    assert fetch_kwargs["from_date"] == "2016-01-01"
    staged = collected_frames[symbol]
    assert staged["Date"].tolist() == ["2015-12-31", "2016-01-04"]


def test_download_skips_when_market_history_matches_existing_frame(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {
        "symbol": symbol,
        "status": "ok",
        "rows": [
            {
                "date": "2024-01-03",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100.0,
                "short_interest": 1000.0,
                "short_volume": 500.0,
            }
        ],
    }
    existing_df = _market_frame(
        [
            {
                "Date": "2024-01-03",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 100.0,
                "ShortInterest": 1000.0,
                "ShortVolume": 500.0,
            }
        ]
    )
    collected_frames: dict[str, pd.DataFrame] = {}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager, patch(
        "tasks.market_data.bronze_market_data._utc_today",
        return_value=date(2024, 1, 3),
    ):
        mock_list_manager.is_blacklisted.return_value = False

        bronze.download_and_save_raw(
            symbol,
            mock_massive,
            collected_symbol_frames=collected_frames,
            existing_symbol_df=existing_df,
        )

    mock_massive.get_market_history.assert_called_once()
    pd.testing.assert_frame_equal(collected_frames[symbol], bronze._canonical_market_df(existing_df))


def test_download_normalizes_market_history_payload_errors(unique_ticker):
    symbol = unique_ticker
    mock_massive = MagicMock()
    mock_massive.get_market_history.return_value = {"symbol": symbol, "status": "ok", "rows": {"bad": "shape"}}

    with patch("tasks.market_data.bronze_market_data.list_manager") as mock_list_manager:
        mock_list_manager.is_blacklisted.return_value = False
        with pytest.raises(bronze.MassiveGatewayError) as exc_info:
            bronze.download_and_save_raw(
                symbol,
                mock_massive,
                collected_symbol_frames={},
            )

    assert "/api/providers/massive/market-history" in str(exc_info.value.payload["path"])


class _FakeClientManager:
    def __init__(self) -> None:
        self.reset_current_calls = 0

    def get_client(self):
        return object()

    def reset_current(self) -> None:
        self.reset_current_calls += 1


def test_download_with_recovery_retries_three_attempts(monkeypatch):
    symbol = "RETRYME"
    manager = _FakeClientManager()
    call_count = {"count": 0}
    sleep_calls: list[float] = []

    def _fake_download(sym, _client, *, snapshot_row=None, collected_symbol_frames, collected_lock=None, existing_symbol_df=None):
        del snapshot_row, collected_symbol_frames, collected_lock, existing_symbol_df
        assert sym == symbol
        call_count["count"] += 1
        if call_count["count"] < 3:
            raise bronze.MassiveGatewayError("API gateway call failed: ConnectError: boom")

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    bronze._download_and_save_raw_with_recovery(symbol, manager, collected_symbol_frames={}, max_attempts=3, sleep_seconds=0.25)

    assert call_count["count"] == 3
    assert manager.reset_current_calls == 2
    assert sleep_calls == [0.25, 0.25]


def test_download_with_recovery_logs_transient_failure_details(monkeypatch):
    symbol = "TRACE"
    manager = _FakeClientManager()
    call_count = {"count": 0}
    warning_messages: list[str] = []

    def _fake_download(sym, _client, *, snapshot_row=None, collected_symbol_frames, collected_lock=None, existing_symbol_df=None):
        del snapshot_row, collected_symbol_frames, collected_lock, existing_symbol_df
        assert sym == symbol
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise bronze.MassiveGatewayError(
                "gateway unavailable",
                status_code=503,
                detail="upstream unavailable",
                payload={"path": "/api/providers/massive/time-series/daily"},
            )

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(bronze.mdc, "write_warning", lambda message: warning_messages.append(str(message)))

    bronze._download_and_save_raw_with_recovery(symbol, manager, collected_symbol_frames={}, max_attempts=2, sleep_seconds=0.0)

    assert call_count["count"] == 2
    assert any("Transient Massive error for TRACE" in message for message in warning_messages)
    assert any("details=type=MassiveGatewayError status=503" in message for message in warning_messages)
    assert any("path=/api/providers/massive/time-series/daily" in message for message in warning_messages)


def test_failure_bucket_key_includes_status_and_path():
    exc = bronze.MassiveGatewayError(
        "gateway unavailable",
        status_code=503,
        detail="upstream unavailable",
        payload={"path": "/api/providers/massive/snapshot"},
    )
    key = bronze._failure_bucket_key(exc)

    assert "type=MassiveGatewayError" in key
    assert "status=503" in key
    assert "path=/api/providers/massive/snapshot" in key


def test_download_with_recovery_does_not_retry_not_found(monkeypatch):
    symbol = "MISSING"
    manager = _FakeClientManager()
    sleep_calls: list[float] = []

    def _fake_download(_sym, _client, *, snapshot_row=None, collected_symbol_frames, collected_lock=None, existing_symbol_df=None):
        del snapshot_row, collected_symbol_frames, collected_lock, existing_symbol_df
        raise bronze.MassiveGatewayNotFoundError("No data")

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(bronze.MassiveGatewayNotFoundError):
        bronze._download_and_save_raw_with_recovery(symbol, manager, collected_symbol_frames={}, max_attempts=3, sleep_seconds=0.25)

    assert manager.reset_current_calls == 0
    assert sleep_calls == []


def test_download_with_recovery_does_not_retry_non_recoverable_gateway_error(monkeypatch):
    symbol = "AAPL"
    manager = _FakeClientManager()
    sleep_calls: list[float] = []
    call_count = {"count": 0}

    def _fake_download(_sym, _client, *, snapshot_row=None, collected_symbol_frames, collected_lock=None, existing_symbol_df=None):
        del snapshot_row, collected_symbol_frames, collected_lock, existing_symbol_df
        call_count["count"] += 1
        raise bronze.MassiveGatewayError(
            "API gateway error (status=400).",
            status_code=400,
        )

    monkeypatch.setattr(bronze, "download_and_save_raw", _fake_download)
    monkeypatch.setattr(bronze.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    with pytest.raises(bronze.MassiveGatewayError):
        bronze._download_and_save_raw_with_recovery(symbol, manager, collected_symbol_frames={}, max_attempts=3, sleep_seconds=0.25)

    assert call_count["count"] == 1
    assert manager.reset_current_calls == 0
    assert sleep_calls == []


def test_fetch_snapshot_daily_rows_chunks_requests(monkeypatch):
    symbols = [f"SYM{i:03d}" for i in range(300)]
    requested_chunks: list[list[str]] = []

    class _FakeClient:
        def get_unified_snapshot(self, *, symbols, asset_type="stocks"):
            requested_chunks.append(list(symbols))
            return {
                "results": [
                    {
                        "ticker": symbol,
                        "session": {
                            "date": "2024-01-03",
                            "open": 10.0,
                            "high": 11.0,
                            "low": 9.0,
                            "close": 10.5,
                            "volume": 1000,
                        },
                    }
                    for symbol in symbols
                ]
            }

        def close(self):
            return None

    monkeypatch.setattr(bronze.MassiveGatewayClient, "from_env", staticmethod(lambda: _FakeClient()))
    rows = bronze._fetch_snapshot_daily_rows(symbols)

    assert len(requested_chunks) == 2
    assert len(requested_chunks[0]) == 250
    assert len(requested_chunks[1]) == 50
    assert rows["SYM000"]["Date"] == "2024-01-03"
    assert rows["SYM299"]["Close"] == pytest.approx(10.5)


def test_main_async_returns_success_when_symbol_is_only_invalid_candidate(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_empty_bucket_load,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
            side_effect=bronze.MassiveGatewayNotFoundError("invalid", status_code=404),
        ), patch(
            "tasks.market_data.bronze_market_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_invalid, patch(
            "tasks.market_data.bronze_market_data.clear_invalid_candidate_marker"
        ), patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            return_value={"size": 1},
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ), patch(
            "tasks.market_data.bronze_market_data.mdc.write_warning"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_record_invalid.assert_called_once()
        mock_list_manager.add_to_blacklist.assert_not_called()
        mock_list_manager.flush.assert_called_once()
        client_manager.close_all.assert_called_once()

    asyncio.run(run_test())


def test_main_async_records_no_history_candidate_for_market_history_gap(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_empty_bucket_load,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
            side_effect=bronze.BronzeCoverageUnavailableError(
                "provider_no_market_history",
                detail=f"Massive returned no market history for {symbol}.",
            ),
        ), patch(
            "tasks.market_data.bronze_market_data.record_invalid_symbol_candidate",
            return_value={"promoted": False, "observedRunCount": 1, "blacklistPath": None},
        ) as mock_record_candidate, patch(
            "tasks.market_data.bronze_market_data.clear_invalid_candidate_marker"
        ) as mock_clear, patch(
            "tasks.market_data.bronze_market_data.resolve_job_run_status",
            return_value=("succeededWithWarnings", 0),
        ) as mock_resolve_status, patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            return_value={"size": 1},
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ), patch(
            "tasks.market_data.bronze_market_data.mdc.write_warning"
        ):
            mock_list_manager.is_blacklisted.return_value = False
            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_record_candidate.assert_called_once()
        assert mock_record_candidate.call_args.kwargs["reason_code"] == "provider_no_market_history"
        mock_clear.assert_not_called()
        mock_resolve_status.assert_called_once_with(failed_count=0, warning_count=1)

    asyncio.run(run_test())


def test_main_async_does_not_promote_non_header_coverage_unavailable(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_empty_bucket_load,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
            side_effect=bronze.BronzeCoverageUnavailableError(
                "coverage_unavailable",
                detail=f"Massive returned no usable bars for {symbol}.",
            ),
        ), patch(
            "tasks.market_data.bronze_market_data.record_invalid_symbol_candidate"
        ) as mock_record_candidate, patch(
            "tasks.market_data.bronze_market_data.resolve_job_run_status",
            return_value=("succeeded", 0),
        ) as mock_resolve_status, patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            return_value={"size": 1},
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ), patch(
            "tasks.market_data.bronze_market_data.mdc.write_warning"
        ):
            mock_list_manager.is_blacklisted.return_value = False
            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_record_candidate.assert_not_called()
        mock_resolve_status.assert_called_once_with(failed_count=0, warning_count=0)

    asyncio.run(run_test())


def test_main_async_schedules_regime_required_symbol_even_when_blacklisted():
    symbol = "^VIX"
    client_manager = MagicMock()

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_empty_bucket_load,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
        ) as mock_download, patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            return_value={"size": 1},
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ), patch(
            "tasks.market_data.bronze_market_data.mdc.write_warning"
        ):
            mock_list_manager.is_blacklisted.return_value = True

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_download.assert_called_once()
        assert mock_download.call_args.args[0] == symbol
        client_manager.close_all.assert_called_once()

    asyncio.run(run_test())


def test_main_async_fails_closed_when_alpha26_preload_errors(unique_ticker):
    symbol = unique_ticker
    client_manager = MagicMock()

    def _boom(*, bucket: str) -> pd.DataFrame:
        raise RuntimeError(f"Bronze market alpha26 preload failed bucket={bucket}: boom")

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_boom,
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=client_manager,
        ), patch(
            "tasks.market_data.bronze_market_data.resolve_job_run_status",
            return_value=("failed", 1),
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
        ) as mock_download, patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
        ) as mock_write, patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
        ) as mock_finalize, patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager:
            mock_list_manager.is_blacklisted.return_value = False
            exit_code = await bronze.main_async()

        assert exit_code == 1
        mock_download.assert_not_called()
        mock_write.assert_not_called()
        mock_finalize.assert_not_called()
        client_manager.close_all.assert_called_once()

    asyncio.run(run_test())


def test_main_async_debug_mode_preserves_seeded_frames_during_bucket_rewrite():
    captured_bucket_frames: dict[str, pd.DataFrame] = {}

    def _fake_load_bucket(*, bucket: str) -> pd.DataFrame:
        if bucket != "A":
            return _empty_existing_bucket_frame()
        return pd.DataFrame(
            [
                {
                    "Symbol": "AAPL",
                    "Date": "2024-01-02",
                    "Open": 10.0,
                    "High": 11.0,
                    "Low": 9.0,
                    "Close": 10.5,
                    "Volume": 100.0,
                    "ShortInterest": 1000.0,
                    "ShortVolume": 500.0,
                },
                {
                    "Symbol": "MSFT",
                    "Date": "2024-01-02",
                    "Open": 20.0,
                    "High": 21.0,
                    "Low": 19.0,
                    "Close": 20.5,
                    "Volume": 200.0,
                    "ShortInterest": 2000.0,
                    "ShortVolume": 800.0,
                },
            ]
        )

    def _fake_download(
        symbol,
        _client_manager,
        *,
        snapshot_row=None,
        collected_symbol_frames,
        collected_lock=None,
        existing_symbol_df=None,
        max_attempts=0,
        sleep_seconds=0.0,
    ):
        del snapshot_row, max_attempts, sleep_seconds
        assert symbol == "AAPL"
        assert existing_symbol_df is not None
        bronze._set_collected_market_frame(
            symbol=symbol,
            frame=_market_frame(
                [
                    {
                        "Date": "2024-01-03",
                        "Open": 11.0,
                        "High": 12.0,
                        "Low": 10.0,
                        "Close": 11.5,
                        "Volume": 150.0,
                        "ShortInterest": 1100.0,
                        "ShortVolume": 550.0,
                    }
                ]
            ),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=collected_lock,
        )

    def _fake_bucket_write(_session, *, bucket, frame, symbol_to_bucket=None):
        del symbol_to_bucket
        captured_bucket_frames[str(bucket)] = frame.copy()
        return {"size": len(frame)}

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": ["AAPL", "MSFT"]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch.object(
            bronze.cfg,
            "DEBUG_SYMBOLS",
            ["AAPL"],
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_load_bucket,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=MagicMock(),
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
            side_effect=_fake_download,
        ), patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            side_effect=_fake_bucket_write,
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(written_symbols=2),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False
            exit_code = await bronze.main_async()

        assert exit_code == 0

    asyncio.run(run_test())

    assert "A" in captured_bucket_frames
    bucket_a = captured_bucket_frames["A"].sort_values(["symbol", "date"]).reset_index(drop=True)
    assert bucket_a["symbol"].tolist() == ["AAPL", "MSFT"]
    assert bucket_a.loc[bucket_a["symbol"] == "AAPL", "date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-03"]
    assert bucket_a.loc[bucket_a["symbol"] == "MSFT", "date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02"]


def test_main_async_normal_run_drops_unscheduled_seeded_rows_during_bucket_rewrite():
    captured_bucket_frames: dict[str, pd.DataFrame] = {}

    def _fake_load_bucket(*, bucket: str) -> pd.DataFrame:
        if bucket != "A":
            return _empty_existing_bucket_frame()
        return pd.DataFrame(
            [
                {
                    "Symbol": "AAPL",
                    "Date": "2024-01-02",
                    "Open": 10.0,
                    "High": 11.0,
                    "Low": 9.0,
                    "Close": 10.5,
                    "Volume": 100.0,
                    "ShortInterest": 1000.0,
                    "ShortVolume": 500.0,
                },
                {
                    "Symbol": "AMZN",
                    "Date": "2024-01-02",
                    "Open": 20.0,
                    "High": 21.0,
                    "Low": 19.0,
                    "Close": 20.5,
                    "Volume": 200.0,
                    "ShortInterest": 2000.0,
                    "ShortVolume": 800.0,
                },
            ]
        )

    def _fake_download(
        symbol,
        _client_manager,
        *,
        snapshot_row=None,
        collected_symbol_frames,
        collected_lock=None,
        existing_symbol_df=None,
        max_attempts=0,
        sleep_seconds=0.0,
    ):
        del snapshot_row, max_attempts, sleep_seconds
        assert symbol == "AAPL"
        assert existing_symbol_df is not None
        bronze._set_collected_market_frame(
            symbol=symbol,
            frame=_market_frame(
                [
                    {
                        "Date": "2024-01-03",
                        "Open": 11.0,
                        "High": 12.0,
                        "Low": 10.0,
                        "Close": 11.5,
                        "Volume": 150.0,
                        "ShortInterest": 1100.0,
                        "ShortVolume": 550.0,
                    }
                ]
            ),
            collected_symbol_frames=collected_symbol_frames,
            collected_lock=collected_lock,
        )

    def _fake_bucket_write(_session, *, bucket, frame, symbol_to_bucket=None):
        del symbol_to_bucket
        captured_bucket_frames[str(bucket)] = frame.copy()
        return {"size": len(frame)}

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": ["AAPL"]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_load_bucket,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=MagicMock(),
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
            side_effect=_fake_download,
        ), patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            side_effect=_fake_bucket_write,
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(written_symbols=1),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False
            exit_code = await bronze.main_async()

        assert exit_code == 0

    asyncio.run(run_test())

    bucket_a = captured_bucket_frames["A"].sort_values(["symbol", "date"]).reset_index(drop=True)
    assert bucket_a["symbol"].tolist() == ["AAPL"]
    assert bucket_a["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-03"]


def test_main_async_failed_scheduled_symbol_retains_seeded_rows_in_bucket_rewrite():
    captured_bucket_frames: dict[str, pd.DataFrame] = {}

    def _fake_load_bucket(*, bucket: str) -> pd.DataFrame:
        if bucket != "A":
            return _empty_existing_bucket_frame()
        return pd.DataFrame(
            [
                {
                    "Symbol": "AAPL",
                    "Date": "2024-01-02",
                    "Open": 10.0,
                    "High": 11.0,
                    "Low": 9.0,
                    "Close": 10.5,
                    "Volume": 100.0,
                    "ShortInterest": 1000.0,
                    "ShortVolume": 500.0,
                },
                {
                    "Symbol": "AMZN",
                    "Date": "2024-01-02",
                    "Open": 20.0,
                    "High": 21.0,
                    "Low": 19.0,
                    "Close": 20.5,
                    "Volume": 200.0,
                    "ShortInterest": 2000.0,
                    "ShortVolume": 800.0,
                },
            ]
        )

    def _fake_download(
        symbol,
        _client_manager,
        *,
        snapshot_row=None,
        collected_symbol_frames,
        collected_lock=None,
        existing_symbol_df=None,
        max_attempts=0,
        sleep_seconds=0.0,
    ):
        del snapshot_row, max_attempts, sleep_seconds
        assert existing_symbol_df is not None
        if symbol == "AAPL":
            bronze._set_collected_market_frame(
                symbol=symbol,
                frame=_market_frame(
                    [
                        {
                            "Date": "2024-01-03",
                            "Open": 11.0,
                            "High": 12.0,
                            "Low": 10.0,
                            "Close": 11.5,
                            "Volume": 150.0,
                            "ShortInterest": 1100.0,
                            "ShortVolume": 550.0,
                        }
                    ]
                ),
                collected_symbol_frames=collected_symbol_frames,
                collected_lock=collected_lock,
            )
            return
        raise bronze.MassiveGatewayError(f"boom for {symbol}")

    def _fake_bucket_write(_session, *, bucket, frame, symbol_to_bucket=None):
        del symbol_to_bucket
        captured_bucket_frames[str(bucket)] = frame.copy()
        return {"size": len(frame)}

    async def run_test():
        with patch("tasks.market_data.bronze_market_data._validate_environment"), patch(
            "tasks.market_data.bronze_market_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.market_data.bronze_market_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": ["AAPL", "AMZN"]}),
        ), patch(
            "tasks.market_data.bronze_market_data.bronze_bucketing.bronze_layout_mode",
            return_value="alpha26",
        ), patch(
            "tasks.market_data.bronze_market_data._load_alpha26_existing_market_bucket",
            side_effect=_fake_load_bucket,
        ), patch(
            "tasks.market_data.bronze_market_data._fetch_snapshot_daily_rows",
            return_value={},
        ), patch(
            "tasks.market_data.bronze_market_data._ThreadLocalMassiveClientManager",
            return_value=MagicMock(),
        ), patch(
            "tasks.market_data.bronze_market_data._get_max_workers",
            return_value=1,
        ), patch(
            "tasks.market_data.bronze_market_data._download_and_save_raw_with_recovery",
            side_effect=_fake_download,
        ), patch(
            "tasks.market_data.bronze_market_data.start_alpha26_bronze_publish",
            return_value=object(),
        ), patch(
            "tasks.market_data.bronze_market_data.write_alpha26_bronze_bucket",
            side_effect=_fake_bucket_write,
        ), patch(
            "tasks.market_data.bronze_market_data.finalize_alpha26_bronze_publish",
            return_value=_fake_publish_result(written_symbols=2),
        ), patch(
            "tasks.market_data.bronze_market_data.resolve_job_run_status",
            return_value=("failed", 1),
        ), patch(
            "tasks.market_data.bronze_market_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.market_data.bronze_market_data.mdc.write_line"
        ), patch(
            "tasks.market_data.bronze_market_data.mdc.write_warning"
        ):
            mock_list_manager.is_blacklisted.return_value = False
            exit_code = await bronze.main_async()

        assert exit_code == 1

    asyncio.run(run_test())

    bucket_a = captured_bucket_frames["A"].sort_values(["symbol", "date"]).reset_index(drop=True)
    assert bucket_a["symbol"].tolist() == ["AAPL", "AMZN"]
    assert bucket_a.loc[bucket_a["symbol"] == "AAPL", "date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-03"]
    assert bucket_a.loc[bucket_a["symbol"] == "AMZN", "date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02"]


def test_bronze_source_contains_no_symbol_csv_contract():
    source = Path(bronze.__file__).read_text(encoding="utf-8")
    assert "market-data/{symbol}.csv" not in source
    assert "_write_alpha26_market_buckets" not in source
