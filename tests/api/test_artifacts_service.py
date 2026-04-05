from __future__ import annotations

from api.service import artifacts


def test_list_remote_artifacts_accepts_string_last_modified(monkeypatch):
    class _FakeBlobStorageClient:
        def __init__(self, *, container_name: str, ensure_container_exists: bool):
            assert container_name == "silver"
            assert ensure_container_exists is False

        def list_blob_infos(self, *, name_starts_with: str):
            assert name_starts_with == "runs/demo/"
            return [
                {
                    "name": "runs/demo/report.json",
                    "size": 64,
                    "last_modified": "2026-03-04T01:00:00Z",
                }
            ]

    monkeypatch.setattr(artifacts, "BlobStorageClient", _FakeBlobStorageClient)

    out = artifacts.list_remote_artifacts(container="silver", prefix="runs/demo")

    assert len(out) == 1
    assert out[0].name == "report.json"
    assert out[0].size_bytes == 64
    assert out[0].last_modified == "2026-03-04T01:00:00+00:00"
