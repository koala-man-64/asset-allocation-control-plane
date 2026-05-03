from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client

EMPTY_JOB_LOG_ROWS_ERROR = (
    "No Log Analytics console rows were returned for this execution. Verify API runtime "
    "identity Log Analytics Reader access and Container Apps log ingestion."
)


class _FakeJobArmClient:
    def __init__(self, _cfg) -> None:
        return None

    def __enter__(self) -> "_FakeJobArmClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"/subscriptions/sub/resourceGroups/rg/providers/{provider}/{resource_type}/{name}"

    def get_json(self, url: str):
        if url.endswith("/jobs/bronze-market-job/executions"):
            return {
                "value": [
                    {
                        "name": "bronze-market-job-exec-001",
                        "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job/executions/bronze-market-job-exec-001",
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2026-02-10T00:00:00Z",
                            "endTime": "2026-02-10T00:01:00Z",
                        },
                    }
                ]
            }
        raise ValueError(f"Unexpected ARM URL: {url}")


class _FakeJobLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_FakeJobLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "executionName", "type": "string"},
                        {"name": "stream_s", "type": "string"},
                        {"name": "msg", "type": "string"},
                    ],
                    "rows": [
                        [
                            "2026-02-10T00:00:01Z",
                            "bronze-market-job-exec-001",
                            "stdout",
                            "job booted",
                        ],
                        [
                            "2026-02-10T00:00:05Z",
                            "bronze-market-job-exec-001",
                            "stderr",
                            "transient warning",
                        ],
                    ],
                }
            ]
        }


class _EmptyJobLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_EmptyJobLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "executionName", "type": "string"},
                        {"name": "stream_s", "type": "string"},
                        {"name": "msg", "type": "string"},
                    ],
                    "rows": [],
                }
            ]
        }


class _FailingJobLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds

    def __enter__(self) -> "_FailingJobLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        raise RuntimeError("Log analytics denied")


class _UnsuffixedJobLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_UnsuffixedJobLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "ContainerGroupName", "type": "string"},
                        {"name": "Stream", "type": "string"},
                        {"name": "Log", "type": "string"},
                    ],
                    "rows": [
                        [
                            "2026-02-10T00:00:02Z",
                            "bronze-market-job-exec-001",
                            "stdout",
                            "current schema line",
                        ],
                    ],
                }
            ]
        }


class _AnchoredJobArmClient:
    def __init__(self, _cfg) -> None:
        return None

    def __enter__(self) -> "_AnchoredJobArmClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"/subscriptions/sub/resourceGroups/rg/providers/{provider}/{resource_type}/{name}"

    def get_json(self, url: str):
        if url.endswith("/jobs/bronze-market-job/executions"):
            return {
                "value": [
                    {
                        "name": "bronze-market-job-exec-003",
                        "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job/executions/bronze-market-job-exec-003",
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2026-02-12T00:00:00Z",
                            "endTime": "2026-02-12T00:01:00Z",
                        },
                    },
                    {
                        "name": "bronze-market-job-exec-002",
                        "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job/executions/bronze-market-job-exec-002",
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2026-02-11T00:00:00Z",
                            "endTime": "2026-02-11T00:01:00Z",
                        },
                    },
                    {
                        "name": "bronze-market-job-exec-001",
                        "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job/executions/bronze-market-job-exec-001",
                        "properties": {
                            "status": "Running",
                            "startTime": "2026-02-10T00:00:00Z",
                        },
                    },
                ]
            }
        raise ValueError(f"Unexpected ARM URL: {url}")


class _AnchoredJobLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_AnchoredJobLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        execution_name = (
            "bronze-market-job-exec-001"
            if "bronze-market-job-exec-001" in query
            else "bronze-market-job-exec-003"
        )
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "executionName", "type": "string"},
                        {"name": "stream_s", "type": "string"},
                        {"name": "msg", "type": "string"},
                    ],
                    "rows": [
                        [
                            "2026-02-10T00:00:01Z",
                            execution_name,
                            "stdout",
                            f"logs for {execution_name}",
                        ]
                    ],
                }
            ]
        }


class _FreshRunningJobArmClient:
    def __init__(self, _cfg) -> None:
        return None

    def __enter__(self) -> "_FreshRunningJobArmClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"/subscriptions/sub/resourceGroups/rg/providers/{provider}/{resource_type}/{name}"

    def get_json(self, url: str):
        if url.endswith("/jobs/bronze-market-job/executions"):
            return {
                "value": [
                    {
                        "name": "bronze-market-job-exec-001",
                        "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job/executions/bronze-market-job-exec-001",
                        "properties": {
                            "status": "Running",
                            "startTime": "2026-02-10T00:00:00Z",
                        },
                    }
                ]
            }
        raise ValueError(f"Unexpected ARM URL: {url}")


class _FreshRunningDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 2, 10, 0, 4, 0, tzinfo=timezone.utc)
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)


class _StaleRunningDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 2, 10, 0, 15, 0, tzinfo=timezone.utc)
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)


def _set_job_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "bronze-market-job")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace-id")


@pytest.mark.asyncio
async def test_get_job_logs_returns_console_log_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_job_env(monkeypatch)

    fake_logs = _FakeJobLogAnalyticsClient()
    with patch("api.endpoints.system.AzureArmClient", _FakeJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["jobName"] == "bronze-market-job"
    assert payload["runsRequested"] == 1
    assert payload["runsReturned"] == 1
    assert payload["tailLines"] == 10

    run = payload["runs"][0]
    assert run["executionName"] == "bronze-market-job-exec-001"
    assert run["status"] == "Succeeded"
    assert run["tail"] == ["job booted", "transient warning"]
    assert run["error"] is None
    assert run["consoleLogs"] == [
        {
            "timestamp": "2026-02-10T00:00:01Z",
            "stream_s": "stdout",
            "executionName": "bronze-market-job-exec-001",
            "message": "job booted",
        },
        {
            "timestamp": "2026-02-10T00:00:05Z",
            "stream_s": "stderr",
            "executionName": "bronze-market-job-exec-001",
            "message": "transient warning",
        },
    ]

    assert len(fake_logs.queries) == 1
    workspace_id, query, timespan = fake_logs.queries[0]
    assert workspace_id == "workspace-id"
    assert "stream_s" in query
    assert timespan is not None


@pytest.mark.asyncio
async def test_get_job_logs_supports_current_unsuffixed_console_log_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_job_env(monkeypatch)

    fake_logs = _UnsuffixedJobLogAnalyticsClient()
    with patch("api.endpoints.system.AzureArmClient", _FakeJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    payload = resp.json()
    run = payload["runs"][0]
    assert run["tail"] == ["current schema line"]
    assert run["consoleLogs"] == [
        {
            "timestamp": "2026-02-10T00:00:02Z",
            "stream_s": "stdout",
            "executionName": "bronze-market-job-exec-001",
            "message": "current schema line",
        }
    ]

    assert len(fake_logs.queries) == 1
    _workspace_id, query, _timespan = fake_logs.queries[0]
    assert "ContainerAppName" in query
    assert "ContainerAppName_s" in query
    assert "let nonempty = " in query
    assert "nonempty(column_ifexists('ContainerAppName', ''))" in query
    assert "Log" in query
    assert "Stream" in query
    assert "| where msg != ''" in query


@pytest.mark.asyncio
async def test_get_job_logs_anchors_to_an_older_active_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_job_env(monkeypatch)

    fake_logs = _AnchoredJobLogAnalyticsClient()
    with patch("api.endpoints.system.AzureArmClient", _AnchoredJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["runsReturned"] == 1
    assert payload["runs"][0]["executionName"] == "bronze-market-job-exec-001"
    assert payload["runs"][0]["status"] == "Running"
    assert payload["runs"][0]["tail"] == ["logs for bronze-market-job-exec-001"]
    assert len(fake_logs.queries) == 1
    assert "bronze-market-job-exec-001" in fake_logs.queries[0][1]


@pytest.mark.asyncio
async def test_get_job_logs_sets_error_when_completed_run_returns_no_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_job_env(monkeypatch)

    fake_logs = _EmptyJobLogAnalyticsClient()
    with patch("api.endpoints.system.AzureArmClient", _FakeJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    run = resp.json()["runs"][0]
    assert run["status"] == "Succeeded"
    assert run["tail"] == []
    assert run["consoleLogs"] == []
    assert run["error"] == EMPTY_JOB_LOG_ROWS_ERROR


@pytest.mark.asyncio
async def test_get_job_logs_keeps_fresh_running_empty_rows_as_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_job_env(monkeypatch)

    fake_logs = _EmptyJobLogAnalyticsClient()
    with patch("api.endpoints.system.AzureArmClient", _FreshRunningJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs
    ), patch("api.endpoints.system.datetime", _FreshRunningDatetime):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    run = resp.json()["runs"][0]
    assert run["status"] == "Running"
    assert run["tail"] == []
    assert run["consoleLogs"] == []
    assert run["error"] is None


@pytest.mark.asyncio
async def test_get_job_logs_sets_error_when_stale_running_run_returns_no_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_job_env(monkeypatch)

    fake_logs = _EmptyJobLogAnalyticsClient()
    with patch("api.endpoints.system.AzureArmClient", _FreshRunningJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient", return_value=fake_logs
    ), patch("api.endpoints.system.datetime", _StaleRunningDatetime):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    run = resp.json()["runs"][0]
    assert run["status"] == "Running"
    assert run["tail"] == []
    assert run["consoleLogs"] == []
    assert run["error"] == EMPTY_JOB_LOG_ROWS_ERROR


@pytest.mark.asyncio
async def test_get_job_logs_sets_error_from_log_analytics_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_job_env(monkeypatch)

    with patch("api.endpoints.system.AzureArmClient", _FakeJobArmClient), patch(
        "api.endpoints.system.AzureLogAnalyticsClient",
        return_value=_FailingJobLogAnalyticsClient(),
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/jobs/bronze-market-job/logs?runs=1")

    assert resp.status_code == 200
    run = resp.json()["runs"][0]
    assert run["tail"] == []
    assert run["consoleLogs"] == []
    assert run["error"] == "Log analytics denied"
