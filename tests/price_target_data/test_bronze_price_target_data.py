import pytest
import pandas as pd
import asyncio
import uuid
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

from tasks.price_target_data import bronze_price_target_data as bronze
from asset_allocation_runtime_common.foundation import config as cfg
from asset_allocation_runtime_common.market_data import core as mdc
# --- Helpers ---

@pytest.fixture
def unique_ticker():
    return f"TEST_INT_{uuid.uuid4().hex[:8].upper()}"

@pytest.fixture
def storage_cleanup(unique_ticker):
    container = cfg.AZURE_CONTAINER_BRONZE
    mdc.get_storage_client(container) 
    yield unique_ticker


def _sync_result() -> bronze.symbol_availability.SyncResult:
    return bronze.symbol_availability.SyncResult(
        provider="nasdaq",
        source_column="source_nasdaq",
        listed_count=1,
        inserted_count=0,
        disabled_count=0,
        duration_ms=1,
        lock_wait_ms=0,
    )

# --- Integration Tests ---


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze(mock_list_manager, mock_client, mock_nasdaq, unique_ticker, storage_cleanup):
    symbol = unique_ticker
    
    # 1. Mock Blob checks (return False -> Stale -> Fetch)
    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    
    # 2. Mock API return
    mock_api_df = pd.DataFrame({
        'ticker': [symbol],
        'obs_date': [pd.Timestamp('2023-01-01')],
        'tp_mean_est': [50.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df
    
    # 3. Execute
    semaphore = asyncio.Semaphore(1)
    
    async def run_test():
        # We patch store_raw_bytes to verify write
        with patch('core.core.store_raw_bytes') as mock_store:
            await bronze.process_batch_bronze([symbol], semaphore)
            
            # 4. Verify
            # Check API called
            mock_nasdaq.get_table.assert_called()
            
            # Check Store Raw
            mock_store.assert_called_once()
            args, kwargs = mock_store.call_args
            # args[1] should be path
            assert args[1] == f"price-target-data/{symbol}.parquet"
            
            # Check Whitelist updated
            mock_list_manager.add_to_whitelist.assert_called_with(symbol)

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_logs_symbol_processed_success(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    mock_nasdaq.get_table.return_value = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-01")],
            "tp_mean_est": [55.0],
        }
    )

    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.store_raw_bytes"), patch(
            "tasks.price_target_data.bronze_price_target_data.log_bronze_success"
        ) as mock_log_success:
            summary = await bronze.process_batch_bronze([symbol], semaphore)

        assert summary["saved"] == 1
        mock_log_success.assert_any_call(
            domain="price-target",
            operation="symbol_processed",
            symbol=symbol,
            disposition="written",
            success_count=1,
            coverage_status=None,
            row_count=1,
        )

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_handles_filtered_missing_symbol(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol_with_data = "AAA"
    symbol_missing = "BBB"

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client

    mock_api_df = pd.DataFrame({
        'ticker': [symbol_with_data],
        'obs_date': [pd.Timestamp('2024-03-01')],
        'tp_mean_est': [55.0]
    })
    mock_nasdaq.get_table.return_value = mock_api_df

    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch('core.core.store_raw_bytes') as mock_store:
            summary = await bronze.process_batch_bronze(
                [symbol_with_data, symbol_missing],
                semaphore,
                backfill_start=pd.Timestamp('2024-01-01').date(),
            )
            assert summary["filtered_missing"] == 1
            assert summary["deleted"] == 1
            assert mock_store.call_count >= 1
            stored_paths = [call.args[1] for call in mock_store.call_args_list if len(call.args) >= 2]
            assert f"price-target-data/{symbol_with_data}.parquet" in stored_paths
            mock_client.delete_file.assert_called_once_with(f"price-target-data/{symbol_missing}.parquet")

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_deletes_stale_when_cutoff_and_empty_response(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbols = ["AAA", "BBB"]

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    mock_nasdaq.get_table.return_value = pd.DataFrame()

    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch('core.core.store_raw_bytes') as mock_store:
            summary = await bronze.process_batch_bronze(
                symbols,
                semaphore,
                backfill_start=pd.Timestamp('2024-01-01').date(),
            )
            assert summary["filtered_missing"] == 2
            assert summary["deleted"] == 2
            assert summary["save_failed"] == 0
            mock_store.assert_not_called()
            assert mock_client.delete_file.call_count == 2
            mock_client.delete_file.assert_any_call("price-target-data/AAA.parquet")
            mock_client.delete_file.assert_any_call("price-target-data/BBB.parquet")

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_uses_watermark_and_appends_existing(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    existing_df = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-01")],
            "tp_mean_est": [50.0],
        }
    )
    existing_parquet = existing_df.to_parquet(index=False)

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=2)
    )
    mock_client.get_blob_client.return_value = mock_blob_client

    mock_nasdaq.get_table.return_value = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-02")],
            "tp_mean_est": [55.0],
        }
    )
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.read_raw_bytes", return_value=existing_parquet), patch(
            "core.core.store_raw_bytes"
        ) as mock_store:
            summary = await bronze.process_batch_bronze([symbol], semaphore)
            assert summary["saved"] == 1

            _, call_kwargs = mock_nasdaq.get_table.call_args
            assert call_kwargs["obs_date"]["gte"] == "2024-03-02"

            args, _ = mock_store.call_args
            written_df = pd.read_parquet(BytesIO(args[0]))
            assert set(pd.to_datetime(written_df["obs_date"]).dt.date.astype(str).tolist()) == {
                "2024-03-01",
                "2024-03-02",
            }

    asyncio.run(run_test())


@patch('tasks.price_target_data.bronze_price_target_data.nasdaqdatalink')
@patch('tasks.price_target_data.bronze_price_target_data.bronze_client')
@patch('tasks.price_target_data.bronze_price_target_data.list_manager')
def test_process_batch_bronze_missing_after_watermark_keeps_existing(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    existing_df = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-01")],
            "tp_mean_est": [50.0],
        }
    )
    existing_parquet = existing_df.to_parquet(index=False)

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(
        last_modified=datetime.now(timezone.utc) - timedelta(days=2)
    )
    mock_client.get_blob_client.return_value = mock_blob_client
    mock_nasdaq.get_table.return_value = pd.DataFrame()
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.read_raw_bytes", return_value=existing_parquet), patch(
            "core.core.store_raw_bytes"
        ) as mock_store:
            summary = await bronze.process_batch_bronze([symbol], semaphore)
            assert summary["saved"] == 0
            assert summary["filtered_missing"] == 1
            mock_store.assert_not_called()
            mock_client.delete_file.assert_not_called()
            mock_list_manager.add_to_whitelist.assert_called_with(symbol)

    asyncio.run(run_test())


@patch("tasks.price_target_data.bronze_price_target_data.nasdaqdatalink")
@patch("tasks.price_target_data.bronze_price_target_data.bronze_client")
@patch("tasks.price_target_data.bronze_price_target_data.list_manager")
def test_process_batch_bronze_forces_backfill_when_coverage_gap_exists(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    existing_df = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2025-01-01")],
            "tp_mean_est": [50.0],
        }
    )
    existing_parquet = existing_df.to_parquet(index=False)

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(last_modified=datetime.now(timezone.utc))
    mock_client.get_blob_client.return_value = mock_blob_client

    mock_nasdaq.get_table.return_value = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-02-01")],
            "tp_mean_est": [55.0],
        }
    )
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.read_raw_bytes", return_value=existing_parquet), patch(
            "tasks.price_target_data.bronze_price_target_data.load_coverage_marker",
            return_value=None,
        ), patch(
            "tasks.price_target_data.bronze_price_target_data._mark_coverage"
        ) as mock_mark_coverage, patch(
            "core.core.store_raw_bytes"
        ) as mock_store:
            summary = await bronze.process_batch_bronze(
                [symbol],
                semaphore,
                backfill_start=date(2024, 1, 1),
            )
            assert summary["coverage_checked"] == 1
            assert summary["coverage_forced_refetch"] == 1
            assert summary["stale"] == 1
            _, kwargs = mock_nasdaq.get_table.call_args
            assert kwargs["obs_date"]["gte"] == "2024-01-01"
            mock_store.assert_called_once()
            mock_mark_coverage.assert_called_once()

    asyncio.run(run_test())


@patch("tasks.price_target_data.bronze_price_target_data.nasdaqdatalink")
@patch("tasks.price_target_data.bronze_price_target_data.bronze_client")
@patch("tasks.price_target_data.bronze_price_target_data.list_manager")
def test_process_batch_bronze_skips_force_when_limited_marker_present(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    existing_df = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2025-01-01")],
            "tp_mean_est": [50.0],
        }
    )
    existing_parquet = existing_df.to_parquet(index=False)

    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = True
    mock_blob_client.get_blob_properties.return_value = MagicMock(last_modified=datetime.now(timezone.utc))
    mock_client.get_blob_client.return_value = mock_blob_client
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.read_raw_bytes", return_value=existing_parquet), patch(
            "tasks.price_target_data.bronze_price_target_data.load_coverage_marker",
            return_value={
                "coverageStatus": "limited",
                "backfillStart": "2024-01-01",
            },
        ):
            summary = await bronze.process_batch_bronze(
                [symbol],
                semaphore,
                backfill_start=date(2024, 1, 1),
            )
            assert summary["coverage_checked"] == 1
            assert summary["coverage_forced_refetch"] == 0
            assert summary["coverage_skipped_limited_marker"] == 1
            assert summary["stale"] == 0
            mock_nasdaq.get_table.assert_not_called()

    asyncio.run(run_test())


def test_failure_bucket_key_includes_type_and_path():
    exc = RuntimeError("boom")
    setattr(exc, "payload", {"path": "/api/providers/nasdaq/zacks-tp"})
    key = bronze._failure_bucket_key(exc)

    assert "type=RuntimeError" in key
    assert "path=/api/providers/nasdaq/zacks-tp" in key


@patch("tasks.price_target_data.bronze_price_target_data.nasdaqdatalink")
@patch("tasks.price_target_data.bronze_price_target_data.bronze_client")
@patch("tasks.price_target_data.bronze_price_target_data.list_manager")
def test_process_batch_bronze_logs_structured_save_failure(
    mock_list_manager,
    mock_client,
    mock_nasdaq,
):
    symbol = "AAA"
    mock_blob_client = MagicMock()
    mock_blob_client.exists.return_value = False
    mock_client.get_blob_client.return_value = mock_blob_client
    mock_nasdaq.get_table.return_value = pd.DataFrame(
        {
            "ticker": [symbol],
            "obs_date": [pd.Timestamp("2024-03-01")],
            "tp_mean_est": [55.0],
        }
    )
    semaphore = asyncio.Semaphore(1)

    async def run_test():
        with patch("core.core.store_raw_bytes", side_effect=RuntimeError("disk full")), patch(
            "tasks.price_target_data.bronze_price_target_data.mdc.write_warning"
        ) as mock_warning, patch(
            "tasks.price_target_data.bronze_price_target_data.mdc.write_error"
        ) as mock_error:
            summary = await bronze.process_batch_bronze([symbol], semaphore)
            assert summary["save_failed"] == 1

        warning_messages = [str(call.args[0]) for call in mock_warning.call_args_list if call.args]
        error_messages = [str(call.args[0]) for call in mock_error.call_args_list if call.args]
        assert any("Bronze price target batch failure summary:" in msg for msg in warning_messages)
        assert any("scope=symbol=AAA type=RuntimeError" in msg for msg in warning_messages)
        assert any("Failed to save AAA: type=RuntimeError message=disk full" in msg for msg in error_messages)

    asyncio.run(run_test())


def test_main_async_returns_success_when_only_filtered_missing_symbols_are_detected():
    symbol = "AAA"

    async def run_test():
        with patch(
            "tasks.price_target_data.bronze_price_target_data._validate_environment"
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.mdc.log_environment_diagnostics"
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.resolve_backfill_start_date",
            return_value=None,
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.symbol_availability.sync_domain_availability",
            return_value=_sync_result(),
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.symbol_availability.get_domain_symbols",
            return_value=pd.DataFrame({"Symbol": [symbol]}),
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.bronze_bucketing.is_alpha26_mode",
            return_value=False,
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.process_batch_bronze",
            new=AsyncMock(return_value={"filtered_missing": 1}),
        ), patch(
            "tasks.price_target_data.bronze_price_target_data.list_manager"
        ) as mock_list_manager, patch(
            "tasks.price_target_data.bronze_price_target_data.mdc.write_line"
        ):
            mock_list_manager.is_blacklisted.return_value = False

            exit_code = await bronze.main_async()

        assert exit_code == 0
        mock_list_manager.flush.assert_called_once()

    asyncio.run(run_test())
