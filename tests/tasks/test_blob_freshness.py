import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Add project root to path so we can import modules
sys.path.append(os.path.join(os.path.dirname(__file__), "..", ".."))

from monitoring import system_health


def test_blob_recursive_check():
    mock_store = MagicMock()
    mock_store.get_blob_last_modified.return_value = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    mock_spec = system_health.LayerProbeSpec(
        name="Silver",
        description="Silver data",
        container_env="TEST_CONTAINER",
        max_age_seconds=3600,
        marker_blobs=(system_health.DomainSpec("market-data/whitelist.csv"),),
    )

    with patch.dict(os.environ, {
        "TEST_CONTAINER": "test-container",
        "AZURE_CONTAINER_COMMON": "common",
        "SYSTEM_HEALTH_RUN_IN_TEST": "true",
    }):
        with patch('monitoring.system_health.AzureBlobStore', return_value=mock_store):
            with patch('monitoring.system_health._default_layer_specs', return_value=[mock_spec]):
                with patch('monitoring.system_health.collect_container_apps', return_value=[]), \
                     patch('monitoring.system_health.collect_jobs_and_executions', return_value=[]), \
                     patch('monitoring.system_health.collect_resource_health_signals', return_value=[]), \
                     patch('monitoring.system_health.collect_monitor_metrics', return_value=[]), \
                     patch('monitoring.system_health.collect_log_analytics_signals', return_value=[]):

                    now = datetime(2024, 1, 1, 12, 30, 0, tzinfo=timezone.utc)
                    result = system_health.collect_system_health_snapshot(now=now)

                    mock_store.get_blob_last_modified.assert_called_with(
                        container="common",
                        blob_name="system/health_markers/silver/market.json",
                    )

                    domain = result['dataLayers'][0]['domains'][0]
                    assert domain['name'] == 'market'
                    assert domain['status'] == 'healthy'
                    assert domain['lastUpdated'] == '2024-01-01T12:00:00+00:00'
                    assert domain['freshnessSource'] == 'marker'
