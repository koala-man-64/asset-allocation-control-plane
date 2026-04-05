from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
from azure.identity import DefaultAzureCredential


LOG_ANALYTICS_SCOPE = "https://api.loganalytics.io/.default"
LOG_ANALYTICS_ENDPOINT = "https://api.loganalytics.io/v1/workspaces"


@dataclass(frozen=True)
class LogAnalyticsQuerySpec:
    resource_type: str
    name: str
    query: str
    warn_above: Optional[float] = None
    error_above: Optional[float] = None
    unit: str = "count"


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_log_analytics_queries_json(raw: str) -> List[LogAnalyticsQuerySpec]:
    """
    Expected format (JSON array):
      [{"resourceType":"Microsoft.App/containerApps","name":"errors_15m","query":"...","warnAbove":1,"errorAbove":10,"unit":"count"}]
    """
    text = (raw or "").strip()
    if not text:
        return []
    data = json.loads(text)
    if not isinstance(data, list):
        raise ValueError("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON must be a JSON array.")

    out: List[LogAnalyticsQuerySpec] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        resource_type = str(item.get("resourceType") or "").strip()
        name = str(item.get("name") or "").strip()
        query = str(item.get("query") or "").strip()
        if not (resource_type and name and query):
            continue
        warn_above = item.get("warnAbove")
        error_above = item.get("errorAbove")
        unit = str(item.get("unit") or "count")
        out.append(
            LogAnalyticsQuerySpec(
                resource_type=resource_type,
                name=name,
                query=query,
                warn_above=float(warn_above) if warn_above is not None else None,
                error_above=float(error_above) if error_above is not None else None,
                unit=unit,
            )
        )
    return out


def _status_for_value(value: Optional[float], *, warn_above: Optional[float], error_above: Optional[float]) -> str:
    if value is None:
        return "unknown"
    if error_above is not None and value >= error_above:
        return "error"
    if warn_above is not None and value >= warn_above:
        return "warning"
    if warn_above is None and error_above is None:
        return "unknown"
    return "healthy"


def _worse_status(primary: str, secondary: str) -> str:
    status_order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if status_order.get(secondary, 0) > status_order.get(primary, 0) else primary


def _escape_kql_literal(value: str) -> str:
    return (value or "").replace("'", "''")


def render_query(template: str, *, resource_name: str, resource_id: Optional[str]) -> str:
    rendered = template or ""
    rendered = rendered.replace("{resourceName}", _escape_kql_literal(resource_name))
    rendered = rendered.replace("{resourceId}", _escape_kql_literal(resource_id or ""))
    return rendered


def extract_primary_scalar(payload: Dict[str, Any]) -> Optional[float]:
    tables = payload.get("tables") if isinstance(payload.get("tables"), list) else []
    if not tables:
        return None
    table = tables[0] if isinstance(tables[0], dict) else {}
    rows = table.get("rows") if isinstance(table.get("rows"), list) else []
    if not rows:
        return 0.0
    first_row = rows[0] if isinstance(rows[0], list) else []
    if not first_row:
        return 0.0
    cell = first_row[0]
    if cell is None:
        return 0.0
    try:
        return float(cell)
    except (TypeError, ValueError):
        return None


def extract_first_table_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    tables = payload.get("tables") if isinstance(payload.get("tables"), list) else []
    if not tables:
        return []
    table = tables[0] if isinstance(tables[0], dict) else {}
    columns = table.get("columns") if isinstance(table.get("columns"), list) else []
    rows = table.get("rows") if isinstance(table.get("rows"), list) else []

    names: List[str] = []
    for idx, column in enumerate(columns):
        if not isinstance(column, dict):
            names.append(f"col_{idx}")
            continue
        name = str(column.get("name") or "").strip()
        names.append(name or f"col_{idx}")

    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        out.append({name: row[idx] if idx < len(row) else None for idx, name in enumerate(names)})
    return out


class AzureLogAnalyticsClient:
    def __init__(
        self,
        *,
        credential: Optional[Any] = None,
        http_client: Optional[httpx.Client] = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        self._credential = credential or DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self._http = http_client or httpx.Client(timeout=httpx.Timeout(timeout_seconds))
        self._owns_http = http_client is None
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> "AzureLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        self.close()

    def _get_bearer(self) -> str:
        now = time.time()
        if self._token and now < (self._token_expires_at - 60):
            return self._token
        token = self._credential.get_token(LOG_ANALYTICS_SCOPE)
        self._token = token.token
        self._token_expires_at = float(getattr(token, "expires_on", 0) or 0)
        return self._token

    def query(self, *, workspace_id: str, query: str, timespan: Optional[str] = None) -> Dict[str, Any]:
        ws = (workspace_id or "").strip()
        if not ws:
            raise ValueError("workspace_id is required")
        url = f"{LOG_ANALYTICS_ENDPOINT}/{ws}/query"
        body: Dict[str, Any] = {"query": query}
        if timespan:
            body["timespan"] = timespan
        resp = self._http.post(url, headers={"Authorization": f"Bearer {self._get_bearer()}"}, json=body)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict):
            raise ValueError("Log Analytics response was not a JSON object.")
        return payload


def collect_log_analytics_signals(
    client: AzureLogAnalyticsClient,
    *,
    workspace_id: str,
    specs: Sequence[LogAnalyticsQuerySpec],
    resource_type: str,
    resource_name: str,
    resource_id: Optional[str],
    end_time: datetime,
    timespan_minutes: int = 15,
) -> Tuple[List[Dict[str, Any]], str]:
    matched = [s for s in specs if s.resource_type == resource_type]
    if not matched:
        return [], "unknown"

    end = _utc(end_time)
    start = end - timedelta(minutes=max(timespan_minutes, 1))
    timespan = f"{start.isoformat()}/{end.isoformat()}"

    signals: List[Dict[str, Any]] = []
    worst = "unknown"

    for spec in matched:
        rendered = render_query(spec.query, resource_name=resource_name, resource_id=resource_id)
        try:
            payload = client.query(workspace_id=workspace_id, query=rendered, timespan=timespan)
            value = extract_primary_scalar(payload)
        except Exception:
            value = None

        status = _status_for_value(value, warn_above=spec.warn_above, error_above=spec.error_above)
        worst = _worse_status(worst, status)

        signals.append(
            {
                "name": spec.name,
                "value": value,
                "unit": spec.unit,
                "timestamp": end.isoformat(),
                "status": status,
                "source": "logs",
            }
        )

    return signals, worst

