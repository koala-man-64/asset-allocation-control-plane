import pytest
import os
from unittest.mock import patch, MagicMock

# Force hermetic test env so local shell/.env values cannot leak into unit tests.
os.environ["DISABLE_DOTENV"] = "true"
os.environ.setdefault("LOG_FORMAT", "JSON")
os.environ.setdefault("LOG_LEVEL", "INFO")

# Mock Environment Variables for Testing (Set fallbacks if missing)
# Note: NASDAQ_API_KEY should be in .env for actual data fetching.
os.environ.setdefault("ALPHA_VANTAGE_API_KEY", "test_alpha_vantage_key")
os.environ.setdefault("AZURE_STORAGE_ACCOUNT_NAME", "test_account")
os.environ["AZURE_STORAGE_CONNECTION_STRING"] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"
os.environ["TEST_MODE"] = "true"
# Keep tests hermetic even when local shell has production-like env values.
os.environ.pop("POSTGRES_DSN", None)
os.environ.pop("BACKFILL_START_DATE", None)
os.environ.pop("CONTAINER_APP_ENV_DNS_SUFFIX", None)
os.environ.pop("CONTAINER_APP_JOB_EXECUTION_NAME", None)
os.environ.pop("CONTAINER_APP_REPLICA_NAME", None)
os.environ.pop("KUBERNETES_SERVICE_HOST", None)
os.environ.setdefault("SYSTEM_HEALTH_TTL_SECONDS", "10")
os.environ.setdefault("SYSTEM_HEALTH_MAX_AGE_SECONDS", "129600")
os.environ.setdefault("SYSTEM_HEALTH_ARM_API_VERSION", "2023-05-01")
os.environ.setdefault("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", "5.0")
os.environ.setdefault("SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION", "2022-10-01")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION", "2018-01-01")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES", "15")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL", "PT1M")
os.environ.setdefault("SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION", "Average")
os.environ.setdefault("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS", "5.0")
os.environ.setdefault("SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES", "15")
os.environ.setdefault("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", "3")

# Container Mocks
containers = [
    "AZURE_FOLDER_MARKET", "AZURE_FOLDER_FINANCE", 
    "AZURE_FOLDER_EARNINGS", "AZURE_FOLDER_TARGETS", 
    "AZURE_CONTAINER_COMMON",
    "AZURE_CONTAINER_BRONZE",
    "AZURE_CONTAINER_SILVER",
    "AZURE_CONTAINER_GOLD",
    "AZURE_CONTAINER_PLATINUM",
]
for container in containers:
    os.environ.setdefault(container, "test-container")
from core.blob_storage import BlobStorageClient

@pytest.fixture(scope="session", autouse=True)
def redirect_storage(tmp_path_factory):
    """
    Global autouse fixture to redirect storage calls to a local temp directory.
    This prevents tests from attempting to connect to Azure.
    """
    temp_storage_root = tmp_path_factory.mktemp("local_test_storage")
    
    # Patch delta_core to use local file URIs
    def mock_get_uri(container, path, account_name=None):
        full_path = temp_storage_root / container / path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        return str(full_path)

    with patch("core.delta_core.get_delta_table_uri", side_effect=mock_get_uri), \
         patch("core.delta_core.get_delta_storage_options", return_value={}), \
         patch("core.delta_core._ensure_container_exists", return_value=None):
        yield temp_storage_root

@pytest.fixture(scope="session")
def azure_client():
    """
    Provides a Mocked BlobStorageClient for tests if actual Azure config is missing.
    In actual integration tests, this would use a real client.
    """
    mock_client = MagicMock(spec=BlobStorageClient)
    return mock_client

