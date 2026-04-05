
import os
import logging
from unittest.mock import patch
from core import core as mdc

def test_log_environment_diagnostics(caplog, capsys):
    """
    Verifies that log_environment_diagnostics:
    1. Logs a safe allowlist of env vars (not the full env).
    2. Does not emit secrets/PII to logs.
    """
    # Setup test env
    test_env = {
        "NORMAL_VAR": "visible",
        "AZURE_STORAGE_ACCOUNT_NAME": "test_account",
        "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net",
        "ALPHA_VANTAGE_API_KEY": "SuperSecretApiKey",
        "CONTAINER_APP_JOB_NAME": "my-job",
        "CONTAINER_APP_JOB_EXECUTION_NAME": "my-job-123",
    }
    
    with patch.dict(os.environ, test_env, clear=True):
         with caplog.at_level(logging.INFO):
             mdc.log_environment_diagnostics()
             
    # Analyze logs
    logs = caplog.text
    captured = capsys.readouterr()
    stdout_content = captured.out
    
    # 1. Check Header (Printed to stdout via write_section)
    assert "ENVIRONMENT DIAGNOSTICS" in stdout_content
    
    # 2. Check allowlisted vars are logged
    assert "CONTAINER_APP_JOB_NAME = my-job" in logs
    assert "CONTAINER_APP_JOB_EXECUTION_NAME = my-job-123" in logs
    assert "AZURE_STORAGE_ACCOUNT_NAME = test_account" in logs
    
    # 3. Check non-allowlisted vars are NOT logged (even if present)
    assert "NORMAL_VAR = visible" not in logs

    # 4. Ensure secrets/PII are NOT present in logs
    assert "AZURE_STORAGE_CONNECTION_STRING" not in logs
    assert "ALPHA_VANTAGE_API_KEY" not in logs
    assert "SuperSecretApiKey" not in logs
