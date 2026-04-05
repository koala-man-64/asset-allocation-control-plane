from __future__ import annotations

from core import backtest_artifacts


def test_list_artifacts_accepts_string_last_modified(monkeypatch):
    class _FakeCommonStorageClient:
        def list_blob_infos(self, *, name_starts_with: str):
            assert name_starts_with == "backtests/run-123"
            return [
                {
                    "name": "backtests/run-123/report.json",
                    "size": 128,
                    "last_modified": "2026-03-04T01:00:00Z",
                }
            ]

    monkeypatch.setattr(backtest_artifacts.mdc, "common_storage_client", _FakeCommonStorageClient())

    out = backtest_artifacts.list_artifacts("run-123")

    assert out == [
        {
            "name": "report.json",
            "path": "backtests/run-123/report.json",
            "size": 128,
            "updatedAt": "2026-03-04T01:00:00+00:00",
            "contentType": "application/json",
        }
    ]
