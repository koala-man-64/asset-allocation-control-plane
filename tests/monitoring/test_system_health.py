from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest

from api.service.app import create_app
from api.service.auth import AuthContext
from api.endpoints import system as system_endpoint
from monitoring.delta_log import find_latest_delta_version
from monitoring import system_health
from monitoring.ttl_cache import CacheGetResult
from monitoring.ttl_cache import TtlCache
from tests.api._auth import install_auth_stub
from tests.api._client import get_test_client


def test_find_latest_delta_version_finds_highest_contiguous() -> None:
    def exists(version: int) -> bool:
        return 0 <= version <= 9

    assert find_latest_delta_version(exists, start_version=0) == 9
    assert find_latest_delta_version(exists, start_version=6) == 9


def test_find_latest_delta_version_returns_none_when_missing() -> None:
    def exists(_version: int) -> bool:
        return False

    assert find_latest_delta_version(exists, start_version=0) is None


def test_ttl_cache_returns_stale_value_on_refresh_error() -> None:
    now = 0.0

    def time_fn() -> float:
        return now

    cache: TtlCache[str] = TtlCache(ttl_seconds=10.0, time_fn=time_fn)

    calls = {"count": 0}

    def refresh_ok() -> str:
        calls["count"] += 1
        return "value-1"

    first = cache.get(refresh_ok)
    assert first.value == "value-1"
    assert first.cache_hit is False
    assert first.refresh_error is None
    assert calls["count"] == 1

    now = 5.0
    second = cache.get(lambda: "value-2")
    assert second.value == "value-1"
    assert second.cache_hit is True
    assert second.refresh_error is None
    assert calls["count"] == 1

    now = 15.0

    def refresh_fail() -> str:
        raise RuntimeError("boom")

    third = cache.get(refresh_fail)
    assert third.value == "value-1"
    assert third.cache_hit is True
    assert third.refresh_error is not None


def test_ttl_cache_coalesces_refresh_after_wait_timeout() -> None:
    cache: TtlCache[str] = TtlCache(ttl_seconds=10.0, refresh_wait_seconds=0.01)

    started = threading.Event()
    release = threading.Event()
    calls_lock = threading.Lock()
    calls = {"count": 0}

    def refresh() -> str:
        with calls_lock:
            calls["count"] += 1
            call_number = calls["count"]
        if call_number == 1:
            started.set()
            assert release.wait(timeout=1.0)
        return f"value-{call_number}"

    results: list[Any] = []
    errors: list[Exception] = []

    def invoke() -> None:
        try:
            results.append(cache.get(refresh))
        except Exception as exc:  # pragma: no cover - defensive guard
            errors.append(exc)

    worker1 = threading.Thread(target=invoke)
    worker2 = threading.Thread(target=invoke)

    worker1.start()
    assert started.wait(timeout=0.2)

    worker2.start()
    time.sleep(0.08)
    with calls_lock:
        assert calls["count"] == 1

    release.set()
    worker1.join(timeout=1.0)
    worker2.join(timeout=1.0)

    assert not errors
    assert len(results) == 2
    with calls_lock:
        assert calls["count"] == 1
    assert sorted(result.value for result in results) == ["value-1", "value-1"]
    assert sorted(result.cache_hit for result in results) == [False, True]


def test_make_job_portal_url_uses_resource_anchor() -> None:
    url = system_health._make_job_portal_url("sub", "rg", "myjob")
    assert url == (
        "https://portal.azure.com/#resource/subscriptions/sub"
        "/resourceGroups/rg/providers/Microsoft.App/jobs/myjob/overview"
    )


def test_system_health_normalizes_government_signals_storage_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AZURE_FOLDER_GOVERNMENT_SIGNALS", "gov-signals")

    assert system_health._domain_name_from_marker_path("gov-signals/runs/") == "government-signals"
    assert system_health._domain_name_from_marker_path("gov-signals/") == "government-signals"
    assert system_health._domain_name_from_delta_path("gov-signals/") == "government-signals"

    job_names = system_health._collect_job_names_for_layers(system_health._default_layer_specs())
    assert "bronze-government-signals-job" in job_names
    assert "silver-government-signals-job" in job_names
    assert "gold-government-signals-job" in job_names
    assert all("/" not in job_name for job_name in job_names)


def test_system_health_omits_trigger_metadata_for_missing_arm_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker_time = datetime(2026, 3, 3, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_MARKERS_CONTAINER", "common")
    monkeypatch.setenv("AZURE_CONTAINER_BRONZE", "bronze")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "storage")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)

    class _FakeStore:
        def get_blob_last_modified(self, *, container: str, blob_name: str) -> datetime:
            assert container == "common"
            assert blob_name.startswith("system/health_markers/bronze/")
            return marker_time

    class _FakeBlobConfig:
        @classmethod
        def from_env(cls) -> "_FakeBlobConfig":
            return cls()

    class _FakeNotFound(Exception):
        status_code = 404

    class _FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "_FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            return (
                f"https://management.azure.com/subscriptions/{self._cfg.subscription_id}"
                f"/resourceGroups/{self._cfg.resource_group}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def resource_collection_url(self, *, provider: str, resource_type: str) -> str:
            return (
                f"https://management.azure.com/subscriptions/{self._cfg.subscription_id}"
                f"/resourceGroups/{self._cfg.resource_group}"
                f"/providers/{provider}/{resource_type}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            del params
            if url.endswith("/jobs/bronze-government-signals-job"):
                raise _FakeNotFound("Resource not found")
            if url.endswith("/jobs/bronze-market-job"):
                return {
                    "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job",
                    "properties": {
                        "configuration": {
                            "triggerType": "Schedule",
                            "scheduleTriggerConfig": {"cronExpression": "0 22 * * 1-5"},
                        }
                    },
                }
            raise AssertionError(f"Unexpected ARM URL: {url}")

    spec = system_health.LayerProbeSpec(
        name="Bronze",
        description="Raw ingestion",
        container_env="AZURE_CONTAINER_BRONZE",
        max_age_seconds=129600,
        marker_blobs=(
            system_health.DomainSpec("market-data/whitelist.csv", cron="0 22 * * 1-5"),
            system_health.DomainSpec("government-signals/runs/", cron="*/15 * * * *"),
        ),
    )

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [spec])
    monkeypatch.setattr(system_health, "AzureBlobStoreConfig", _FakeBlobConfig)
    monkeypatch.setattr(system_health, "AzureBlobStore", lambda _cfg: _FakeStore())
    monkeypatch.setattr(system_health, "AzureArmClient", _FakeAzureArmClient)
    monkeypatch.setattr(
        system_health,
        "resolve_container_app_job_names",
        lambda _arm, *, configured_job_names: [],
    )

    payload = system_health.collect_system_health_snapshot(
        now=datetime(2026, 3, 3, 12, 30, tzinfo=timezone.utc),
        include_resource_ids=False,
    )

    domains = {
        str(item["name"]): item
        for item in payload["dataLayers"][0]["domains"]
    }
    assert domains["market"]["jobName"] == "bronze-market-job"
    assert domains["market"]["jobUrl"].endswith("/jobs/bronze-market-job/overview")
    assert domains["government-signals"]["jobName"] is None
    assert domains["government-signals"]["jobUrl"] is None
    assert domains["government-signals"]["cron"] == "*/15 * * * *"


@pytest.mark.asyncio
async def test_system_health_public_when_no_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("API_OIDC_AUDIENCE", raising=False)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/health")
    assert resp.status_code == 200
    payload = resp.json()
    assert set(payload.keys()) >= {"overall", "dataLayers", "recentJobs", "alerts"}


@pytest.mark.asyncio
async def test_system_health_requires_oidc_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OIDC_ISSUER", "https://issuer.example.com")
    monkeypatch.setenv("API_OIDC_AUDIENCE", "asset-allocation-api")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/health")
        assert resp.status_code == 401

        install_auth_stub(
            monkeypatch,
            app.state.auth,
            auth_context=AuthContext(mode="oidc", subject="user-123", claims={"sub": "user-123"}),
        )
        resp2 = await client.get("/api/system/health", headers={"Authorization": "Bearer token"})
        assert resp2.status_code == 200


@pytest.mark.asyncio
async def test_system_health_sanitizes_non_finite_signal_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:

    class FakeCache:
        def get(self, refresh_fn: Any, *, force_refresh: bool = False) -> CacheGetResult[Dict[str, Any]]:
            return CacheGetResult(
                value={
                    "overall": "degraded",
                    "dataLayers": [],
                    "recentJobs": [],
                    "alerts": [],
                    "resources": [
                        {
                            "name": "asset-allocation-api",
                            "resourceType": "Microsoft.App/containerApps",
                            "status": "warning",
                            "lastChecked": "2026-03-12T00:00:00Z",
                            "details": "metric anomaly",
                            "signals": [
                                {
                                    "name": "CpuUsage",
                                    "value": float("nan"),
                                    "unit": "Percent",
                                    "timestamp": "2026-03-12T00:00:00Z",
                                    "status": "unknown",
                                    "source": "metrics",
                                },
                                {
                                    "name": "MemoryUsage",
                                    "value": float("inf"),
                                    "unit": "Bytes",
                                    "timestamp": "2026-03-12T00:00:00Z",
                                    "status": "unknown",
                                    "source": "metrics",
                                },
                                {
                                    "name": "Requests",
                                    "value": 42.0,
                                    "unit": "count",
                                    "timestamp": "2026-03-12T00:00:00Z",
                                    "status": "healthy",
                                    "source": "metrics",
                                },
                            ],
                        }
                    ],
                },
                cache_hit=True,
                refresh_error=None,
            )

    monkeypatch.setattr(system_endpoint, "get_system_health_cache", lambda request: FakeCache())

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/health")

    assert resp.status_code == 200
    payload = resp.json()
    signals = payload["resources"][0]["signals"]
    assert signals[0]["value"] is None
    assert signals[1]["value"] is None
    assert signals[2]["value"] == 42.0


def test_system_health_control_plane_redacts_resource_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])
    monkeypatch.setattr(system_health, "collect_backtest_operational_summary", lambda _dsn: None)

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"

    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        },
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "systemData": {"lastModifiedAt": "2024-01-01T00:00:10Z"},
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:00:05Z",
                    }
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert len(payload["resources"]) == 2
    assert all("azureId" not in item for item in payload["resources"])
    job_resource = next((item for item in payload["resources"] if item.get("resourceType") == "Microsoft.App/jobs"), {})
    assert job_resource.get("lastModifiedAt") == "2024-01-01T00:00:10Z"
    assert len(payload["recentJobs"]) == 1
    assert payload["recentJobs"][0]["status"] == "success"
    assert payload["recentJobs"][0]["triggeredBy"] == "azure"
    assert payload["alerts"] == []

    verbose = system_health.collect_system_health_snapshot(now=now, include_resource_ids=True)
    assert all("azureId" in item for item in verbose["resources"])


def test_system_health_discovers_arm_jobs_when_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])
    monkeypatch.setattr(system_health, "collect_backtest_operational_summary", lambda _dsn: None)

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job"
    job_url = f"https://management.azure.com{job_id}"
    jobs_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs"
    responses: Dict[str, Dict[str, Any]] = {
        jobs_url: {"value": [{"name": "bronze-market-job"}]},
        job_url: {
            "id": job_id,
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:01:00Z",
                    }
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_collection_url(self, *, provider: str, resource_type: str) -> str:
            return (
                f"https://management.azure.com/subscriptions/{self._cfg.subscription_id}"
                f"/resourceGroups/{self._cfg.resource_group}"
                f"/providers/{provider}/{resource_type}"
            )

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            return (
                f"https://management.azure.com/subscriptions/{self._cfg.subscription_id}"
                f"/resourceGroups/{self._cfg.resource_group}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)

    assert payload["overall"] == "healthy"
    assert payload["resources"][0]["name"] == "bronze-market-job"
    assert payload["recentJobs"][0]["jobName"] == "bronze-market-job"


def test_system_health_includes_intraday_monitor_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    class _FakeBlobConfig:
        @classmethod
        def from_env(cls) -> "_FakeBlobConfig":
            return cls()

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])
    monkeypatch.setattr(system_health, "AzureBlobStoreConfig", _FakeBlobConfig)
    monkeypatch.setattr(system_health, "AzureBlobStore", lambda _cfg: object())
    monkeypatch.setattr(
        system_health,
        "get_intraday_health_summary",
        lambda _dsn: {"dueRunBacklogCount": 2, "failedRunCount": 1},
    )

    payload = system_health.collect_system_health_snapshot(
        now=datetime(2024, 1, 1, tzinfo=timezone.utc),
        include_resource_ids=False,
    )

    assert payload["overall"] == "healthy"
    assert payload["intradayMonitor"] == {"dueRunBacklogCount": 2, "failedRunCount": 1}


def test_system_health_warns_when_intraday_monitor_summary_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")

    class _FakeBlobConfig:
        @classmethod
        def from_env(cls) -> "_FakeBlobConfig":
            return cls()

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])
    monkeypatch.setattr(system_health, "AzureBlobStoreConfig", _FakeBlobConfig)
    monkeypatch.setattr(system_health, "AzureBlobStore", lambda _cfg: object())

    def _raise_intraday(_dsn: str) -> dict[str, object]:
        raise RuntimeError("intraday db unavailable")

    monkeypatch.setattr(system_health, "get_intraday_health_summary", _raise_intraday)

    payload = system_health.collect_system_health_snapshot(
        now=datetime(2024, 1, 1, tzinfo=timezone.utc),
        include_resource_ids=False,
    )

    assert "intradayMonitor" not in payload
    assert any(
        alert["title"] == "Intraday monitoring unavailable" and alert["severity"] == "warning"
        for alert in payload["alerts"]
    )


def test_system_health_derives_job_cpu_and_memory_percent_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    resource_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    job_url = f"https://management.azure.com{resource_id}"
    metrics_url = f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"

    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": resource_id,
            "systemData": {"lastModifiedAt": "2024-01-01T00:00:10Z"},
            "properties": {
                "provisioningState": "Succeeded",
                "runningState": "Running",
                "template": {
                    "containers": [
                        {
                            "resources": {
                                "cpu": 2.0,
                                "memory": "4Gi",
                            }
                        }
                    ]
                },
            },
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Running",
                        "startTime": "2024-01-01T00:00:00Z",
                    }
                }
            ]
        },
        metrics_url: {
            "value": [
                {
                    "name": {"value": "UsageNanoCores"},
                    "unit": "NanoCores",
                    "timeseries": [{"data": [{"timeStamp": "2024-01-01T00:00:00Z", "average": 1_000_000_000.0}]}],
                },
                {
                    "name": {"value": "UsageBytes"},
                    "unit": "Bytes",
                    "timeseries": [{"data": [{"timeStamp": "2024-01-01T00:00:00Z", "average": 1_073_741_824.0}]}],
                },
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)
    monkeypatch.setattr(system_health, "collect_backtest_operational_summary", lambda _dsn: None)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)

    assert payload["overall"] == "healthy"
    assert len(payload["recentJobs"]) == 1
    assert payload["recentJobs"][0]["status"] == "running"

    job_resource = next(
        item for item in payload["resources"] if item.get("resourceType") == "Microsoft.App/jobs"
    )
    signals_by_name = {
        str(signal.get("name")): signal
        for signal in job_resource.get("signals", [])
        if isinstance(signal, dict)
    }

    assert signals_by_name["UsageNanoCores"]["value"] == 1_000_000_000.0
    assert signals_by_name["UsageBytes"]["value"] == 1_073_741_824.0
    assert signals_by_name["CpuPercent"]["unit"] == "Percent"
    assert signals_by_name["MemoryPercent"]["unit"] == "Percent"
    assert signals_by_name["CpuPercent"]["value"] == pytest.approx(50.0)
    assert signals_by_name["MemoryPercent"]["value"] == pytest.approx(25.0)


def test_system_health_defaults_arm_api_version_when_not_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_API_VERSION", raising=False)

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    captured: dict[str, str] = {}
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        }
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg
            captured["api_version"] = cfg.api_version

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert payload["resources"][0]["name"] == "myapp"
    assert captured.get("api_version") == system_health.DEFAULT_ARM_API_VERSION


def test_system_health_defaults_job_monitor_metric_names_when_env_not_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS", raising=False)

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    resource_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    metrics_url = f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"
    captured_metric_names: list[str] = []

    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": resource_id,
            "properties": {"provisioningState": "Succeeded", "runningState": "Running"},
        },
        f"{job_url}/executions": {"value": []},
        metrics_url: {
            "value": [
                {
                    "name": {"value": "UsageNanoCores"},
                    "unit": "NanoCores",
                    "timeseries": [{"data": [{"timeStamp": "2024-01-01T00:00:00Z", "average": 750000000}]}],
                },
                {
                    "name": {"value": "UsageBytes"},
                    "unit": "Bytes",
                    "timeseries": [{"data": [{"timeStamp": "2024-01-01T00:00:00Z", "average": 2147483648}]}],
                },
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            if url == metrics_url and params is not None:
                captured_metric_names.extend(str(params.get("metricnames") or "").split(","))
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)

    assert captured_metric_names == ["UsageNanoCores", "UsageBytes"]
    job_resource = next(
        (item for item in payload["resources"] if item.get("resourceType") == "Microsoft.App/jobs"),
        {},
    )
    assert job_resource.get("signals") == [
        {
            "name": "UsageNanoCores",
            "value": 750000000.0,
            "unit": "NanoCores",
            "timestamp": "2024-01-01T00:00:00Z",
            "status": "unknown",
            "source": "metrics",
        },
        {
            "name": "UsageBytes",
            "value": 2147483648.0,
            "unit": "Bytes",
            "timestamp": "2024-01-01T00:00:00Z",
            "status": "unknown",
            "source": "metrics",
        },
    ]


def test_system_health_degraded_on_warning_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": ""},
        }
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "degraded"
    assert payload["resources"][0]["status"] == "warning"
    assert any(alert["title"] == "Azure resource health" and alert["severity"] == "warning" for alert in payload["alerts"])


def test_system_health_critical_on_failed_job_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Failed",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:00:05Z",
                    }
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert any(alert["title"] == "Job execution failed" and alert["severity"] == "error" for alert in payload["alerts"])


def test_system_health_healthy_when_latest_job_execution_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")
    monkeypatch.setenv("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", "1")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "systemData": {"lastModifiedAt": "2024-01-01T00:00:10Z"},
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Failed",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:00:05Z",
                    }
                },
                {
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-02T00:00:00Z",
                        "endTime": "2024-01-02T00:00:05Z",
                    }
                },
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert len(payload["recentJobs"]) == 1
    assert payload["recentJobs"][0]["status"] == "success"
    assert not any(alert["title"] == "Job execution failed" for alert in payload["alerts"])


def test_system_health_defaults_blank_arm_timeout_and_job_execution_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", "")
    monkeypatch.setenv("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", "")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    captured_cfg: Dict[str, Any] = {}
    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "systemData": {"lastModifiedAt": "2024-01-05T00:00:10Z"},
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "name": "myjob-exec-1",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-05T00:00:00Z",
                        "endTime": "2024-01-05T00:00:05Z",
                    },
                },
                {
                    "name": "myjob-exec-2",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-04T00:00:00Z",
                        "endTime": "2024-01-04T00:00:05Z",
                    },
                },
                {
                    "name": "myjob-exec-3",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-03T00:00:00Z",
                        "endTime": "2024-01-03T00:00:05Z",
                    },
                },
                {
                    "name": "myjob-exec-4",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-02T00:00:00Z",
                        "endTime": "2024-01-02T00:00:05Z",
                    },
                },
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            captured_cfg["timeout_seconds"] = cfg.timeout_seconds
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert captured_cfg["timeout_seconds"] == pytest.approx(
        system_health.DEFAULT_SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS
    )
    assert payload["overall"] == "healthy"
    assert len(payload["recentJobs"]) == system_health.DEFAULT_SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB
    assert payload["recentJobs"][0]["status"] == "success"
    assert not any(alert["title"] == "Azure monitoring unavailable" for alert in payload["alerts"])


def test_system_health_defaults_missing_monitor_metrics_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION", raising=False)

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 5, tzinfo=timezone.utc)
    resource_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    metrics_url = f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"
    captured_metrics_params: Dict[str, str] = {}
    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": resource_id,
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        },
        metrics_url: {
            "value": [
                {
                    "name": {"value": "UsageNanoCores"},
                    "unit": "Count",
                    "timeseries": [{"data": [{"timeStamp": "2024-01-05T00:00:00Z", "average": 1.0}]}],
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            if url == metrics_url and params is not None:
                captured_metrics_params.update(params)
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert payload["resources"][0]["status"] == "healthy"
    assert captured_metrics_params == {
        "api-version": system_health.DEFAULT_MONITOR_METRICS_API_VERSION,
        "metricnames": "UsageNanoCores,WorkingSetBytes",
        "timespan": "2024-01-04T23:45:00+00:00/2024-01-05T00:00:00+00:00",
        "interval": system_health.DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL,
        "aggregation": system_health.DEFAULT_SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION,
    }
    assert not any(alert["title"] == "Azure monitoring unavailable" for alert in payload["alerts"])


def test_system_health_critical_on_job_failure_reason_alerts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "name": "myjob-exec-1",
                    "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob/executions/myjob-exec-1",
                    "properties": {
                        "status": "Failed",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:10:00Z",
                    },
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    class FakeAzureLogAnalyticsClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
            assert workspace_id == "workspace"
            assert "ContainerAppSystemLogs" in query
            return {
                "tables": [
                    {
                        "columns": [{"name": "msg"}],
                        "rows": [
                            ["Replica terminated with exit code 137"],
                            ["BackoffLimitExceeded"],
                        ],
                    }
                ]
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)
    monkeypatch.setattr(system_health, "AzureLogAnalyticsClient", FakeAzureLogAnalyticsClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert any(alert["title"] == "Job terminated with exit 137" for alert in payload["alerts"])
    assert any(alert["title"] == "Job hit BackoffLimitExceeded" for alert in payload["alerts"])
    assert not any(alert["title"] == "Job execution failed" for alert in payload["alerts"])


def test_system_health_enriches_backtest_job_with_operational_signals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "backtests-job")
    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])
    monkeypatch.setattr(
        system_health,
        "collect_backtest_operational_summary",
        lambda _dsn=None: {
            "jobName": "backtests-job",
            "queuedCount": 3,
            "oldestQueuedAgeSeconds": 420.0,
            "dispatchFailureCount": 1,
            "runningCount": 2,
            "staleHeartbeatCount": 1,
            "durationP95Seconds": 95.0,
            "queueDispatchGraceSeconds": 120,
        },
    )

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/backtests-job"
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/backtests-job",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {"value": []},
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)

    job_resource = next(item for item in payload["resources"] if item["name"] == "backtests-job")
    signals_by_name = {signal["name"]: signal for signal in job_resource.get("signals", [])}
    assert signals_by_name["BacktestQueuedCount"]["value"] == 3.0
    assert signals_by_name["BacktestDispatchFailureCount"]["status"] == "warning"
    assert signals_by_name["BacktestStaleHeartbeatCount"]["status"] == "error"
    assert any(alert["title"] == "Backtest stale heartbeat" for alert in payload["alerts"])


def test_system_health_degraded_on_bronze_symbol_jump(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "bronze-finance-job")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace")
    monkeypatch.setenv("SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_LOOKBACK_HOURS", "168")
    monkeypatch.setenv(
        "SYSTEM_HEALTH_BRONZE_SYMBOL_JUMP_THRESHOLDS_JSON",
        '{"*":{"warnFactor":3.0,"errorFactor":100.0,"minPreviousSymbols":100,"minCurrentSymbols":1000}}',
    )

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-finance-job"
    )
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-finance-job",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "name": "bronze-finance-job-exec-1",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:10:00Z",
                    },
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    class FakeAzureLogAnalyticsClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
            assert workspace_id == "workspace"
            if "Retry-on-next-run candidates (not promoted):" in query:
                return {"tables": [{"columns": [], "rows": []}]}
            if "alpha26 buckets written" in query:
                return {
                    "tables": [
                        {
                            "columns": [
                                {"name": "TimeGenerated"},
                                {"name": "symbol_count"},
                                {"name": "msg"},
                            ],
                            "rows": [
                                ["2024-01-01T00:05:00Z", 18246, "Bronze finance alpha26 buckets written: symbols=18246"],
                                ["2023-12-31T00:05:00Z", 200, "Bronze finance alpha26 buckets written: symbols=200"],
                            ],
                        }
                    ]
                }
            if "Bronze Massive finance ingest complete:" in query:
                return {"tables": [{"columns": [], "rows": []}]}
            raise AssertionError(f"Unexpected query: {query}")

        def close(self) -> None:
            return None

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)
    monkeypatch.setattr(system_health, "AzureLogAnalyticsClient", FakeAzureLogAnalyticsClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "degraded"
    assert any(alert["title"] == "Bronze symbol count jump" and alert["severity"] == "warning" for alert in payload["alerts"])


def test_system_health_critical_on_bronze_finance_zero_write(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "bronze-finance-job")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace")
    monkeypatch.setenv("SYSTEM_HEALTH_BRONZE_FINANCE_ZERO_WRITE_LOOKBACK_HOURS", "168")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-finance-job"
    )
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-finance-job",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "name": "bronze-finance-job-exec-1",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:10:00Z",
                    },
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    class FakeAzureLogAnalyticsClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
            assert workspace_id == "workspace"
            if "Retry-on-next-run candidates (not promoted):" in query:
                return {"tables": [{"columns": [], "rows": []}]}
            assert "Bronze Massive finance ingest complete:" in query
            return {
                "tables": [
                    {
                        "columns": [
                            {"name": "TimeGenerated"},
                            {"name": "processed"},
                            {"name": "written"},
                            {"name": "msg"},
                        ],
                        "rows": [
                            [
                                "2024-01-01T00:10:00Z",
                                12269,
                                0,
                                "Bronze Massive finance ingest complete: processed=12269 written=0 failed=12269",
                            ],
                        ],
                    }
                ]
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)
    monkeypatch.setattr(system_health, "AzureLogAnalyticsClient", FakeAzureLogAnalyticsClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert any(
        alert["title"] == "Bronze finance wrote zero rows" and alert["severity"] == "error"
        for alert in payload["alerts"]
    )


def test_system_health_adds_retry_symbol_metadata_to_recent_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "bronze-market-job")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job"
    )
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/bronze-market-job",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "name": "bronze-market-job-exec-1",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:10:00Z",
                    },
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    class FakeAzureLogAnalyticsClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
            assert workspace_id == "workspace"
            assert "Retry-on-next-run candidates (not promoted):" in query
            return {
                "tables": [
                    {
                        "columns": [
                            {"name": "TimeGenerated"},
                            {"name": "executionName"},
                            {"name": "msg"},
                        ],
                        "rows": [
                            [
                                "2024-01-01T00:10:00Z",
                                "bronze-market-job-exec-1",
                                "Retry-on-next-run candidates (not promoted): count=3 symbols=AAPL, MSFT",
                            ],
                        ],
                    }
                ]
            }

        def close(self) -> None:
            return None

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)
    monkeypatch.setattr(system_health, "AzureLogAnalyticsClient", FakeAzureLogAnalyticsClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert payload["recentJobs"][0]["metadata"] == {
        "retrySymbols": ["AAPL", "MSFT"],
        "retrySymbolCount": 3,
        "retrySymbolsTruncated": False,
        "retrySymbolsUpdatedAt": "2024-01-01T00:10:00Z",
    }


def test_system_health_does_not_add_retry_symbol_metadata_to_gold_jobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "gold-market-job")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    job_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/gold-market-job"
    )
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/gold-market-job",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "name": "gold-market-job-exec-1",
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:10:00Z",
                    },
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    class FakeAzureLogAnalyticsClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.query_calls = 0

        def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
            self.query_calls += 1
            raise AssertionError("gold jobs should not request retry-on-next-run metadata")

        def close(self) -> None:
            return None

    fake_log_client = FakeAzureLogAnalyticsClient()
    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)
    monkeypatch.setattr(system_health, "AzureLogAnalyticsClient", lambda *args, **kwargs: fake_log_client)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert "metadata" not in payload["recentJobs"][0]
    assert fake_log_client.query_calls == 0


def test_system_health_critical_on_resource_health_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    resource_health_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
        "/providers/Microsoft.ResourceHealth/availabilityStatuses/current"
    )
    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        },
        resource_health_url: {
            "properties": {"availabilityState": "Unavailable", "summary": "Outage", "reasonType": "Incident"}
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert payload["resources"][0]["status"] == "error"
    assert any(alert["title"] == "Azure resource health" and alert["severity"] == "error" for alert in payload["alerts"])
