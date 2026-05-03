from __future__ import annotations

from typing import Any, Dict, Optional

from monitoring.control_plane import collect_jobs_and_executions, discover_container_app_job_names


class FakeArmClient:
    def __init__(self, *, responses: Dict[str, Dict[str, Any]]) -> None:
        self._responses = responses

    def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
        return f"https://example.test/{provider}/{resource_type}/{name}"

    def resource_collection_url(self, *, provider: str, resource_type: str) -> str:
        return f"https://example.test/{provider}/{resource_type}"

    def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        return self._responses[url]


def test_collect_jobs_and_executions_maps_status_sorts_and_limits() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs/my-backtest-job": {
                "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/my-backtest-job",
                "systemData": {"lastModifiedAt": "2024-01-05T00:00:00Z"},
                "properties": {"provisioningState": "Succeeded"},
            },
            "https://example.test/Microsoft.App/jobs/my-backtest-job/executions": {
                "value": [
                    {
                        "properties": {
                            "status": "Failed",
                            "startTime": "2024-01-01T00:00:00Z",
                            "endTime": "2024-01-01T00:01:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2024-01-02T00:00:00Z",
                            "endTime": "2024-01-02T00:02:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Running",
                            "startTime": "2024-01-03T00:00:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Unknown",
                            "startTime": "2024-01-04T00:00:00Z",
                        }
                    },
                ]
            },
        }
    )

    resources, runs = collect_jobs_and_executions(
        arm,
        job_names=["my-backtest-job"],
        last_checked_iso="2024-01-10T00:00:00+00:00",
        include_ids=False,
        max_executions_per_job=3,
    )

    assert len(resources) == 1
    assert resources[0].name == "my-backtest-job"
    assert resources[0].status == "healthy"
    assert resources[0].last_modified_at == "2024-01-05T00:00:00Z"
    assert resources[0].to_dict(include_ids=False)["lastModifiedAt"] == "2024-01-05T00:00:00Z"

    # Limit applies after sorting (most recent executions).
    assert len(runs) == 3
    assert [r["status"] for r in runs] == ["pending", "running", "success"]
    assert [r["startTime"] for r in runs] == [
        "2024-01-04T00:00:00+00:00",
        "2024-01-03T00:00:00+00:00",
        "2024-01-02T00:00:00+00:00",
    ]
    assert [r["duration"] for r in runs] == [None, None, 120]
    assert all(r["jobType"] == "backtest" for r in runs)
    assert all(r["triggeredBy"] == "azure" for r in runs)
    assert runs[0]["statusCode"] == "Unknown"
    assert runs[1]["statusCode"] == "Running"
    assert runs[2]["statusCode"] == "Succeeded"


def test_discover_container_app_job_names_lists_all_jobs_and_dedupes() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs": {
                "value": [
                    {"name": "silver-market-job"},
                    {"name": "bronze-market-job"},
                    {"name": "bronze-market-job"},
                    {"name": "../not-a-job"},
                ],
                "nextLink": "https://example.test/Microsoft.App/jobs?page=2&api-version=2023-05-01",
            },
            "https://example.test/Microsoft.App/jobs?page=2&api-version=2023-05-01": {
                "value": [{"name": "gold-market-job"}]
            },
        }
    )

    assert discover_container_app_job_names(arm) == [
        "bronze-market-job",
        "gold-market-job",
        "silver-market-job",
    ]


def test_collect_jobs_and_executions_preserves_warning_status_codes() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs/bronze-market-job": {
                "properties": {"provisioningState": "Succeeded"}
            },
            "https://example.test/Microsoft.App/jobs/bronze-market-job/executions": {
                "value": [
                    {
                        "properties": {
                            "status": "SucceededWithWarnings",
                            "startTime": "2024-01-05T00:00:00Z",
                            "endTime": "2024-01-05T00:01:00Z",
                        }
                    }
                ]
            },
        }
    )

    _, runs = collect_jobs_and_executions(
        arm,
        job_names=["bronze-market-job"],
        last_checked_iso="2024-01-10T00:00:00+00:00",
        include_ids=False,
        max_executions_per_job=1,
    )

    assert len(runs) == 1
    assert runs[0]["status"] == "warning"
    assert runs[0]["statusCode"] == "SucceededWithWarnings"


def test_collect_jobs_and_executions_treats_running_with_end_time_as_completed() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs/bronze-earnings-job": {
                "properties": {"provisioningState": "Succeeded"}
            },
            "https://example.test/Microsoft.App/jobs/bronze-earnings-job/executions": {
                "value": [
                    {
                        "properties": {
                            "status": "Running",
                            "startTime": "2024-01-05T00:00:00Z",
                            "endTime": "2024-01-05T00:01:00Z",
                        }
                    }
                ]
            },
        }
    )

    _, runs = collect_jobs_and_executions(
        arm,
        job_names=["bronze-earnings-job"],
        last_checked_iso="2024-01-10T00:00:00+00:00",
        include_ids=False,
        max_executions_per_job=1,
    )

    assert len(runs) == 1
    assert runs[0]["status"] == "success"
    assert runs[0]["statusCode"] == "Running"


def test_collect_jobs_and_executions_prioritizes_active_runs_without_start_time() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs/silver-earnings-job": {
                "properties": {"provisioningState": "Succeeded"}
            },
            "https://example.test/Microsoft.App/jobs/silver-earnings-job/executions": {
                "value": [
                    {
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2024-01-05T00:00:00Z",
                            "endTime": "2024-01-05T00:01:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "In Progress",
                        }
                    },
                ]
            },
        }
    )

    _, runs = collect_jobs_and_executions(
        arm,
        job_names=["silver-earnings-job"],
        last_checked_iso="2024-01-10T00:00:00+00:00",
        include_ids=False,
        max_executions_per_job=1,
    )

    assert len(runs) == 1
    assert runs[0]["status"] == "running"
    assert runs[0]["statusCode"] == "In Progress"
    assert runs[0]["startTime"] == "2024-01-10T00:00:00+00:00"


def test_collect_jobs_and_executions_includes_older_active_run_outside_latest_sample() -> None:
    arm = FakeArmClient(
        responses={
            "https://example.test/Microsoft.App/jobs/bronze-market-job": {
                "properties": {"provisioningState": "Succeeded"}
            },
            "https://example.test/Microsoft.App/jobs/bronze-market-job/executions": {
                "value": [
                    {
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2024-01-05T00:00:00Z",
                            "endTime": "2024-01-05T00:01:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Succeeded",
                            "startTime": "2024-01-04T00:00:00Z",
                            "endTime": "2024-01-04T00:01:00Z",
                        }
                    },
                    {
                        "properties": {
                            "status": "Running",
                            "startTime": "2024-01-01T00:00:00Z",
                        }
                    },
                ]
            },
        }
    )

    _, runs = collect_jobs_and_executions(
        arm,
        job_names=["bronze-market-job"],
        last_checked_iso="2024-01-10T00:00:00+00:00",
        include_ids=False,
        max_executions_per_job=2,
    )

    assert len(runs) == 2
    assert [r["status"] for r in runs] == ["success", "running"]
    assert [r["startTime"] for r in runs] == [
        "2024-01-05T00:00:00+00:00",
        "2024-01-01T00:00:00+00:00",
    ]

