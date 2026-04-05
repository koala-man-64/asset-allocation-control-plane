import os
from unittest.mock import patch

from core import core as mdc


def test_get_storage_client_initializes_when_test_mode_false():
    sentinel = object()
    env = {
        "TEST_MODE": "false",
        "AZURE_STORAGE_CONNECTION_STRING": (
            "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"
        ),
    }

    with patch.dict(os.environ, env, clear=True), patch("core.core.BlobStorageClient", return_value=sentinel) as ctor:
        client = mdc.get_storage_client("bronze")

    assert client is sentinel
    assert ctor.call_args.kwargs.get("container_name") == "bronze"


def test_get_storage_client_skips_when_test_mode_true():
    env = {
        "TEST_MODE": "true",
        "AZURE_STORAGE_CONNECTION_STRING": (
            "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key;EndpointSuffix=core.windows.net"
        ),
    }

    with patch.dict(os.environ, env, clear=True), patch("core.core.BlobStorageClient") as ctor:
        client = mdc.get_storage_client("bronze")

    assert client is None
    ctor.assert_not_called()

