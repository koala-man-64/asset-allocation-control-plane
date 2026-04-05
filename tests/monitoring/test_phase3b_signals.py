from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pytest

from monitoring import system_health


class FakeAzureArmClient:
    def __init__(self, cfg: Any, *, responses: Dict[str, Dict[str, Any]]) -> None:
        self._cfg = cfg
        self._responses = responses

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
        return self._responses[url]


class FakeAzureLogAnalyticsClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.closed = False

    def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
        return {"tables": [{"rows": [[12]]}]}

    def close(self) -> None:
        self.closed = True


def test_system_health_degraded_on_monitor_metrics_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    # Prevent jobs from polluting the test (since FakeAzureArmClient doesn't mock them)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)

    monkeypatch.setenv("SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS", "CpuUsage")
    monkeypatch.setenv(
        "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON", '{"CpuUsage":{"warn_above":80,"error_above":95}}'
    )

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    resource_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    metrics_url = f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"

    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": resource_id,
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        },
        metrics_url: {
            "value": [
                {
                    "name": {"value": "CpuUsage"},
                    "unit": "Percent",
                    "timeseries": [{"data": [{"timeStamp": "2024-01-01T00:00:00Z", "average": 90.0}]}],
                }
            ]
        },
    }

    monkeypatch.setattr(system_health, "AzureArmClient", lambda cfg: FakeAzureArmClient(cfg, responses=responses))

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "degraded"
    assert payload["resources"][0]["status"] == "warning"
    assert any(alert["title"] == "Azure resource health" and alert["severity"] == "warning" for alert in payload["alerts"])


def test_system_health_critical_on_log_analytics_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")

    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace")
    monkeypatch.setenv(
        "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON",
        '[{"resourceType":"Microsoft.App/containerApps","name":"errors_15m","query":"X {resourceName}","warnAbove":1,"errorAbove":10,"unit":"count"}]',
    )

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])
    monkeypatch.setattr(system_health, "AzureLogAnalyticsClient", FakeAzureLogAnalyticsClient)

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    resource_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )

    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": resource_id,
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        }
    }

    monkeypatch.setattr(system_health, "AzureArmClient", lambda cfg: FakeAzureArmClient(cfg, responses=responses))

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert payload["resources"][0]["status"] == "error"
    assert any(alert["title"] == "Azure resource health" and alert["severity"] == "error" for alert in payload["alerts"])

