from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from monitoring.monitor_metrics import collect_monitor_metrics, parse_metric_thresholds_json


class FakeArmClient:
    def __init__(self, *, payload: Dict[str, Any]) -> None:
        self._payload = payload
        self.last_url: Optional[str] = None
        self.last_params: Optional[Dict[str, Any]] = None

    def get_json(self, url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        self.last_url = url
        self.last_params = params
        return self._payload


def test_collect_monitor_metrics_extracts_latest_datapoint_and_applies_thresholds() -> None:
    payload = {
        "value": [
            {
                "name": {"value": "CpuUsage"},
                "unit": "Percent",
                "timeseries": [
                    {
                        "data": [
                            {"timeStamp": "2024-01-01T00:00:00Z", "average": 10.0},
                            {"timeStamp": "2024-01-01T00:01:00Z", "average": 90.0},
                        ]
                    }
                ],
            }
        ]
    }
    arm = FakeArmClient(payload=payload)
    thresholds = parse_metric_thresholds_json('{"CpuUsage":{"warn_above":80,"error_above":95}}')

    signals, worst = collect_monitor_metrics(
        arm,  # type: ignore[arg-type]
        resource_id="/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
        metric_names=["CpuUsage"],
        end_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        timespan_minutes=15,
        thresholds=thresholds,
    )

    assert worst == "warning"
    assert len(signals) == 1
    assert signals[0]["name"] == "CpuUsage"
    assert signals[0]["value"] == 90.0
    assert signals[0]["unit"] == "Percent"
    assert signals[0]["status"] == "warning"
    assert arm.last_url is not None and "/providers/microsoft.insights/metrics" in arm.last_url
    assert arm.last_params is not None and "metricnames" in arm.last_params

