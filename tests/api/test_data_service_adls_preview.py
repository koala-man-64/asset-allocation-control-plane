from types import SimpleNamespace

import pandas as pd

import api.data_service as data_service_module
from api.data_service import DataService


class _Download:
    def __init__(self, payload: bytes):
        self._payload = payload

    def readall(self) -> bytes:
        return self._payload


class _BlobClient:
    def __init__(self, payload: bytes, *, content_type: str = "application/octet-stream", exists: bool = True):
        self._payload = payload
        self._content_type = content_type
        self._exists = exists

    def exists(self) -> bool:
        return self._exists

    def download_blob(self, offset: int = 0, length: int | None = None) -> _Download:
        payload = self._payload[offset:]
        if length is not None:
            payload = payload[:length]
        return _Download(payload)

    def get_blob_properties(self) -> SimpleNamespace:
        return SimpleNamespace(content_settings=SimpleNamespace(content_type=self._content_type))


class _ContainerClient:
    def __init__(self, blobs: dict[str, _BlobClient]):
        self._blobs = blobs

    def get_blob_client(self, name: str) -> _BlobClient:
        return self._blobs[name]

    def list_blobs(self, name_starts_with: str | None = None):
        for name in sorted(self._blobs):
            if name_starts_with is None or name.startswith(name_starts_with):
                yield SimpleNamespace(name=name)


class _StorageClient:
    def __init__(self, blobs: dict[str, _BlobClient]):
        self.container_client = _ContainerClient(blobs)


def test_adls_file_preview_returns_delta_table_snapshot_for_parquet_files(monkeypatch):
    selected_path = "market/buckets/A/part-00000.snappy.parquet"
    blobs = {
        selected_path: _BlobClient(b"PAR1", content_type="application/octet-stream"),
        "market/buckets/A/_delta_log/00000000000000000000.json": _BlobClient(b"{}"),
        "market/buckets/A/_delta_log/00000000000000000001.json": _BlobClient(b"{}"),
        "market/buckets/A/_delta_log/00000000000000000002.json": _BlobClient(b"{}"),
    }

    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: _StorageClient(blobs))

    load_calls = []

    def fake_load_delta(_container: str, path: str, version: int = None, columns=None, filters=None, log_buffer_size=None):
        load_calls.append((path, version, log_buffer_size))
        return pd.DataFrame(
            [
                {"symbol": "AAPL", "close": 101.25},
                {"symbol": "MSFT", "close": 402.10},
            ]
        )

    monkeypatch.setattr(data_service_module.delta_core, "load_delta", fake_load_delta)

    preview = DataService.get_adls_file_preview(
        layer="gold",
        path=selected_path,
        max_bytes=4096,
        max_delta_files=2,
    )

    assert load_calls == [("market/buckets/A", 1, None)]
    assert preview["previewMode"] == "delta-table"
    assert preview["isPlainText"] is False
    assert preview["resolvedTablePath"] == "market/buckets/A"
    assert preview["deltaLogPath"] == "market/buckets/A/_delta_log/"
    assert preview["tableVersion"] == 1
    assert preview["processedDeltaFiles"] == 2
    assert preview["tableColumns"] == ["symbol", "close"]
    assert preview["tableRowCount"] == 2
    assert preview["tableRows"][0]["symbol"] == "AAPL"


def test_adls_file_preview_anchors_delta_table_snapshot_to_selected_commit(monkeypatch):
    selected_path = "market/buckets/A/_delta_log/00000000000000000001.json"
    blobs = {
        selected_path: _BlobClient(b'{"commitInfo":{}}', content_type="application/json"),
        "market/buckets/A/_delta_log/00000000000000000000.json": _BlobClient(b"{}"),
        "market/buckets/A/_delta_log/00000000000000000002.json": _BlobClient(b"{}"),
    }

    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: _StorageClient(blobs))

    load_calls = []

    def fake_load_delta(_container: str, path: str, version: int = None, columns=None, filters=None, log_buffer_size=None):
        load_calls.append((path, version, log_buffer_size))
        return pd.DataFrame([{"symbol": "AAPL", "close": 101.25}])

    monkeypatch.setattr(data_service_module.delta_core, "load_delta", fake_load_delta)

    preview = DataService.get_adls_file_preview(
        layer="gold",
        path=selected_path,
        max_bytes=4096,
        max_delta_files=4,
    )

    assert load_calls == [("market/buckets/A", 1, None)]
    assert preview["previewMode"] == "delta-table"
    assert preview["tableVersion"] == 1
    assert preview["processedDeltaFiles"] == 2
    assert preview["resolvedTablePath"] == "market/buckets/A"


def test_adls_file_preview_returns_raw_parquet_table_when_not_delta_backed(monkeypatch):
    selected_path = "bronze/raw/sample.parquet"
    payload = pd.DataFrame([{"symbol": "AAPL", "value": 1}, {"symbol": "MSFT", "value": 2}]).to_parquet(index=False)
    blobs = {
        selected_path: _BlobClient(payload, content_type="application/octet-stream"),
    }

    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: _StorageClient(blobs))

    preview = DataService.get_adls_file_preview(
        layer="bronze",
        path=selected_path,
        max_bytes=4096,
        max_delta_files=0,
    )

    assert preview["previewMode"] == "parquet-table"
    assert preview["resolvedTablePath"] is None
    assert preview["tableRowCount"] == 2
    assert preview["tableRows"][1]["symbol"] == "MSFT"
