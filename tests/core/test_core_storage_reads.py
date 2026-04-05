from unittest.mock import MagicMock, patch

from core import core


def test_read_raw_bytes_warns_on_missing_blob_by_default() -> None:
    client = MagicMock()
    client.download_data.return_value = None

    with patch("core.core.logger.warning") as mock_warning, patch("core.core.logger.info") as mock_info:
        assert core.read_raw_bytes("system/missing.json", client=client) == b""

    client.download_data.assert_called_once()
    mock_warning.assert_called_once_with(
        "Failed to load bytes from system/missing.json (client=True)."
    )
    mock_info.assert_not_called()


def test_read_raw_bytes_supports_expected_missing_blob_reads() -> None:
    client = MagicMock()
    client.download_data.return_value = None

    with patch("core.core.logger.warning") as mock_warning, patch("core.core.logger.info") as mock_info:
        assert (
            core.read_raw_bytes(
                "system/invalid_symbol_candidates/bronze/market/AAPL.json",
                client=client,
                missing_ok=True,
                missing_message="Expected missing bronze invalid-candidate marker; continuing.",
            )
            == b""
        )

    client.download_data.assert_called_once()
    mock_warning.assert_not_called()
    mock_info.assert_called_once_with(
        "Expected missing bronze invalid-candidate marker; continuing."
    )
