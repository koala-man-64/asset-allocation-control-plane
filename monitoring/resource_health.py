from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from monitoring.arm_client import AzureArmClient


DEFAULT_RESOURCE_HEALTH_API_VERSION = "2022-10-01"
RESOURCE_HEALTH_PROVIDER_PATH = "/providers/Microsoft.ResourceHealth/availabilityStatuses/current"


@dataclass(frozen=True)
class AvailabilitySignal:
    state: str
    summary: str
    reason_type: str

    def to_details_fragment(self) -> str:
        state_text = self.state or "Unknown"
        parts = [f"availabilityState={state_text}"]
        if self.reason_type:
            parts.append(f"reasonType={self.reason_type}")
        if self.summary:
            parts.append(f"summary={self.summary}")
        return ", ".join(parts)


def _map_availability_state(state: str) -> str:
    normalized = (state or "").strip().lower()
    if normalized == "available":
        return "healthy"
    if normalized == "unavailable":
        return "error"
    if normalized == "degraded":
        return "warning"
    if not normalized or normalized == "unknown":
        return "unknown"
    return "warning"


def _parse_availability_signal(payload: Dict[str, Any]) -> Optional[AvailabilitySignal]:
    props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    state = str(props.get("availabilityState") or "")
    summary = str(props.get("summary") or "")
    reason_type = str(props.get("reasonType") or "")
    if not (state or summary or reason_type):
        return None
    return AvailabilitySignal(state=state, summary=summary, reason_type=reason_type)


def get_current_availability(
    arm: AzureArmClient,
    *,
    resource_id: str,
    api_version: str = DEFAULT_RESOURCE_HEALTH_API_VERSION,
) -> Tuple[Optional[AvailabilitySignal], str]:
    """
    Best-effort runtime availability signal from Azure Resource Health.

    Returns (signal, mapped_status) where mapped_status is one of:
    healthy|warning|error|unknown
    """
    rid = (resource_id or "").strip()
    if not rid.startswith("/"):
        return None, "unknown"

    url = f"https://management.azure.com{rid.rstrip('/')}{RESOURCE_HEALTH_PROVIDER_PATH}"
    try:
        payload = arm.get_json(url, params={"api-version": api_version})
    except Exception:
        return None, "unknown"

    signal = _parse_availability_signal(payload)
    if signal is None:
        return None, "unknown"
    return signal, _map_availability_state(signal.state)

