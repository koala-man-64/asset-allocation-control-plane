from __future__ import annotations

import pytest

from asset_allocation_runtime_common.market_data import core as mdc
class _FakeContainerClient:
    def get_blob_client(self, _name: str):
        return object()


class _FakeCommonStorageClient:
    def __init__(self) -> None:
        self.container_client = _FakeContainerClient()

    def file_exists(self, _name: str) -> bool:
        return True

    def upload_data(self, _name: str, _payload: bytes, overwrite: bool = False) -> None:
        del overwrite


def _conflict_error():
    exc = mdc.ResourceExistsError("lock already held")
    exc.status_code = 409
    return exc


def test_job_lock_skip_success_exits_zero_on_conflict(monkeypatch):
    monkeypatch.setattr(mdc, "common_storage_client", _FakeCommonStorageClient())

    class _LeaseClient:
        def __init__(self, _blob_client):
            self.id = "lease-1"

        def acquire(self, lease_duration: int) -> None:
            del lease_duration
            raise _conflict_error()

    monkeypatch.setattr(mdc, "BlobLeaseClient", _LeaseClient)

    with pytest.raises(SystemExit) as exc:
        with mdc.JobLock("bronze-market-job", conflict_policy="skip_success"):
            pass

    assert exc.value.code == 0


def test_job_lock_fail_exits_one_on_conflict(monkeypatch):
    monkeypatch.setattr(mdc, "common_storage_client", _FakeCommonStorageClient())

    class _LeaseClient:
        def __init__(self, _blob_client):
            self.id = "lease-2"

        def acquire(self, lease_duration: int) -> None:
            del lease_duration
            raise _conflict_error()

    monkeypatch.setattr(mdc, "BlobLeaseClient", _LeaseClient)

    with pytest.raises(SystemExit) as exc:
        with mdc.JobLock("bronze-market-job", conflict_policy="fail"):
            pass

    assert exc.value.code == 1


def test_job_lock_wait_then_fail_times_out_with_non_zero_exit(monkeypatch):
    monkeypatch.setattr(mdc, "common_storage_client", _FakeCommonStorageClient())

    class _LeaseClient:
        def __init__(self, _blob_client):
            self.id = "lease-3"

        def acquire(self, lease_duration: int) -> None:
            del lease_duration
            raise _conflict_error()

    monotonic_values = iter([0.0, 1.0])
    monkeypatch.setattr(mdc, "BlobLeaseClient", _LeaseClient)
    monkeypatch.setattr(mdc.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(mdc.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(mdc.random, "uniform", lambda _a, _b: 0.0)

    with pytest.raises(SystemExit) as exc:
        with mdc.JobLock(
            "bronze-market-job",
            conflict_policy="wait_then_fail",
            wait_timeout_seconds=0.5,
            poll_interval_seconds=0.1,
        ):
            pass

    assert exc.value.code == 1
