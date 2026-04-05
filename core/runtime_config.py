from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

from core.postgres import PostgresError, connect

logger = logging.getLogger(__name__)

_LOCAL_RUNTIME_MARKER_ENV_VARS = (
    # Set by Azure Container Apps at runtime.
    "CONTAINER_APP_ENV_DNS_SUFFIX",
    "CONTAINER_APP_JOB_EXECUTION_NAME",
    "CONTAINER_APP_REPLICA_NAME",
    # Set inside Kubernetes pods.
    "KUBERNETES_SERVICE_HOST",
)

_DB_CONNECTIVITY_ERROR_SNIPPETS = (
    "connection failed",
    "could not connect to server",
    "connection refused",
    "could not receive data from server",
    "could not send ssl negotiation packet",
    "network is unreachable",
    "name or service not known",
    "temporary failure in name resolution",
    "no route to host",
    "timeout expired",
    "timed out",
    "socket is not connected",
    "connection reset by peer",
)


@dataclass(frozen=True)
class RuntimeConfigItem:
    scope: str
    key: str
    value: str
    description: Optional[str]
    updated_at: Optional[datetime]
    updated_by: Optional[str]


# Allowlist of non-secret operational knobs that are safe to override via DB.
DEFAULT_ENV_OVERRIDE_KEYS: set[str] = {
    # Symbol universe refresh tuning (core/core.py).
    "SYMBOLS_REFRESH_INTERVAL_HOURS",
    # Debug symbol filtering (core/debug_symbols.py + tasks/*).
    "DEBUG_SYMBOLS",
    # Alpha Vantage tuning (alpha_vantage/* + tasks/*).
    "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN",
    "ALPHA_VANTAGE_TIMEOUT_SECONDS",
    "ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS",
    "ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS",
    "ALPHA_VANTAGE_GATEWAY_RETRY_ATTEMPTS",
    "ALPHA_VANTAGE_GATEWAY_RETRY_BASE_SECONDS",
    "ALPHA_VANTAGE_GATEWAY_RETRY_MAX_SECONDS",
    "ALPHA_VANTAGE_MAX_WORKERS",
    "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS",
    "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON",
    "ALPHA_VANTAGE_FINANCE_FRESH_DAYS",
    # Massive tuning (massive_provider/* + tasks/market_data + tasks/finance_data).
    "MASSIVE_TIMEOUT_SECONDS",
    "MASSIVE_MAX_WORKERS",
    "MASSIVE_FINANCE_FRESH_DAYS",
    "FINANCE_PIPELINE_SHARED_LOCK_NAME",
    "BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS",
    "SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS",
    "TRIGGER_NEXT_JOB_NAME",
    "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS",
    "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS",
    "SYSTEM_HEALTH_TTL_SECONDS",
    "SYSTEM_HEALTH_MAX_AGE_SECONDS",
    "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON",
    "SYSTEM_HEALTH_VERBOSE_IDS",
    "SYSTEM_HEALTH_MARKERS_CONTAINER",
    "SYSTEM_HEALTH_MARKERS_PREFIX",
    # System health: Azure control-plane probes (monitoring/system_health.py).
    "SYSTEM_HEALTH_ARM_API_VERSION",
    "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS",
    "SYSTEM_HEALTH_ARM_CONTAINERAPPS",
    "SYSTEM_HEALTH_ARM_JOBS",
    "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB",
    # System health: Azure Monitor Metrics (monitoring/system_health.py + monitoring/monitor_metrics.py).
    "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION",
    "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES",
    "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL",
    "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION",
    "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS",
    "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS",
    "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON",
    # System health: Azure Log Analytics query tuning. Connectivity bootstrap is deploy-time config.
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS",
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES",
    "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON",
    # System health: Azure Resource Health (monitoring/system_health.py).
    "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION",
    "DOMAIN_METADATA_MAX_SCANNED_BLOBS",
}

_INT_KEYS = {
    "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN",
    "ALPHA_VANTAGE_GATEWAY_RETRY_ATTEMPTS",
    "ALPHA_VANTAGE_MAX_WORKERS",
    "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS",
    "ALPHA_VANTAGE_EARNINGS_CALENDAR_HORIZON",
    "ALPHA_VANTAGE_FINANCE_FRESH_DAYS",
    "MASSIVE_MAX_WORKERS",
    "MASSIVE_FINANCE_FRESH_DAYS",
    "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS",
    "SYSTEM_HEALTH_MAX_AGE_SECONDS",
    "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB",
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES",
    "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES",
    "DOMAIN_METADATA_MAX_SCANNED_BLOBS",
}
_FLOAT_KEYS = {
    "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS",
    "SYSTEM_HEALTH_TTL_SECONDS",
    "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS",
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS",
    "SYMBOLS_REFRESH_INTERVAL_HOURS",
    "ALPHA_VANTAGE_TIMEOUT_SECONDS",
    "ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS",
    "ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS",
    "ALPHA_VANTAGE_GATEWAY_RETRY_BASE_SECONDS",
    "ALPHA_VANTAGE_GATEWAY_RETRY_MAX_SECONDS",
    "MASSIVE_TIMEOUT_SECONDS",
    "BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS",
    "SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS",
}
_BOOL_KEYS: set[str] = set()
_REQUIRED_NONEMPTY_KEYS = {
    "DEBUG_SYMBOLS",
    "SYSTEM_HEALTH_TTL_SECONDS",
    "SYSTEM_HEALTH_MAX_AGE_SECONDS",
    "DOMAIN_METADATA_MAX_SCANNED_BLOBS",
}
_JSON_ARRAY_KEYS = {"SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON"}
_JSON_OBJECT_KEYS = {
    "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON",
    "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON",
}


def normalize_env_override(key: str, value: object) -> str:
    resolved_key = str(key or "").strip()
    raw = "" if value is None else str(value)
    text = raw.strip()

    if resolved_key in _REQUIRED_NONEMPTY_KEYS and not text:
        raise ValueError(f"{resolved_key} cannot be empty when set via DB override.")

    if resolved_key in _INT_KEYS:
        if not text:
            return ""
        try:
            parsed = int(text)
        except Exception as exc:
            raise ValueError(f"{resolved_key} must be an integer.") from exc
        return str(parsed)

    if resolved_key in _FLOAT_KEYS:
        if not text:
            return ""
        try:
            parsed = float(text)
        except Exception as exc:
            raise ValueError(f"{resolved_key} must be a float.") from exc
        return str(parsed)

    if resolved_key in _BOOL_KEYS:
        if not text:
            return ""
        lowered = text.lower()
        if lowered in {"1", "true", "t", "yes", "y", "on"}:
            return "true"
        if lowered in {"0", "false", "f", "no", "n", "off"}:
            return "false"
        raise ValueError(f"{resolved_key} must be a boolean (true/false).")

    if resolved_key == "DEBUG_SYMBOLS":
        from core.config import parse_debug_symbols

        return ",".join(parse_debug_symbols(text))

    if resolved_key in _JSON_ARRAY_KEYS:
        if not text:
            return ""
        try:
            decoded = json.loads(text)
        except Exception as exc:
            raise ValueError(f"{resolved_key} must be valid JSON.") from exc
        if not isinstance(decoded, list):
            raise ValueError(f"{resolved_key} must be a JSON array.")
        return text

    if resolved_key in _JSON_OBJECT_KEYS:
        if not text:
            return ""
        try:
            decoded = json.loads(text)
        except Exception as exc:
            raise ValueError(f"{resolved_key} must be valid JSON.") from exc
        if not isinstance(decoded, dict):
            raise ValueError(f"{resolved_key} must be a JSON object.")
        return text

    return text


def _resolve_dsn(dsn: Optional[str]) -> Optional[str]:
    raw = dsn or os.environ.get("POSTGRES_DSN")
    value = (raw or "").strip()
    return value or None


def _is_local_runtime() -> bool:
    return not any((os.environ.get(key) or "").strip() for key in _LOCAL_RUNTIME_MARKER_ENV_VARS)


def _looks_like_db_connectivity_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    return any(snippet in text for snippet in _DB_CONNECTIVITY_ERROR_SNIPPETS)


def list_runtime_config(
    dsn: Optional[str] = None,
    *,
    scopes: Optional[Iterable[str]] = None,
    keys: Optional[Iterable[str]] = None,
) -> list[RuntimeConfigItem]:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    scope_list = [str(s).strip() for s in (scopes or []) if str(s).strip()]
    key_list = [str(k).strip() for k in (keys or []) if str(k).strip()]

    clauses: list[str] = []
    params: list[object] = []
    if scope_list:
        clauses.append("scope = ANY(%s)")
        params.append(scope_list)
    if key_list:
        clauses.append("key = ANY(%s)")
        params.append(key_list)

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT scope, key, value, description, updated_at, updated_by
                FROM core.runtime_config
                {where}
                ORDER BY scope, key
                """,
                tuple(params),
            )
            rows = cur.fetchall()

    out: list[RuntimeConfigItem] = []
    for row in rows:
        out.append(
            RuntimeConfigItem(
                scope=str(row[0] or ""),
                key=str(row[1] or ""),
                value=str(row[2] or ""),
                description=str(row[3]) if row[3] is not None else None,
                updated_at=row[4],
                updated_by=str(row[5]) if row[5] is not None else None,
            )
        )
    return out


def get_effective_runtime_config(
    dsn: Optional[str] = None,
    *,
    scopes_by_precedence: list[str],
    keys: Optional[Iterable[str]] = None,
) -> dict[str, RuntimeConfigItem]:
    """
    Returns runtime config entries merged by precedence.

    Precedence is defined by order in `scopes_by_precedence` (first wins).
    """
    scopes = [str(s).strip() for s in scopes_by_precedence if str(s).strip()]
    if not scopes:
        return {}

    rows = list_runtime_config(dsn, scopes=scopes, keys=keys)
    by_scope: dict[str, list[RuntimeConfigItem]] = {}
    for item in rows:
        by_scope.setdefault(item.scope, []).append(item)

    out: dict[str, RuntimeConfigItem] = {}
    for scope in scopes:
        for item in by_scope.get(scope, []):
            if item.key in out:
                continue
            out[item.key] = item
    return out


def upsert_runtime_config(
    *,
    dsn: Optional[str],
    scope: str = "global",
    key: str,
    value: str,
    description: Optional[str] = None,
    actor: Optional[str] = None,
) -> RuntimeConfigItem:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    scope_value = str(scope or "").strip() or "global"
    key_value = str(key or "").strip()
    if not key_value:
        raise ValueError("key is required")

    with connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.runtime_config(scope, key, value, description, updated_at, updated_by)
                VALUES (%s, %s, %s, %s, now(), %s)
                ON CONFLICT (scope, key) DO UPDATE
                SET value = EXCLUDED.value,
                    description = COALESCE(EXCLUDED.description, core.runtime_config.description),
                    updated_at = now(),
                    updated_by = EXCLUDED.updated_by
                RETURNING scope, key, value, description, updated_at, updated_by
                """,
                (scope_value, key_value, str(value or ""), description, actor),
            )
            row = cur.fetchone()

    if not row:
        raise RuntimeError("Failed to upsert runtime config row.")

    return RuntimeConfigItem(
        scope=str(row[0] or ""),
        key=str(row[1] or ""),
        value=str(row[2] or ""),
        description=str(row[3]) if row[3] is not None else None,
        updated_at=row[4],
        updated_by=str(row[5]) if row[5] is not None else None,
    )


def delete_runtime_config(*, dsn: Optional[str], scope: str, key: str) -> bool:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    scope_value = str(scope or "").strip() or "global"
    key_value = str(key or "").strip()
    if not key_value:
        raise ValueError("key is required")

    with connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM core.runtime_config WHERE scope=%s AND key=%s;",
                (scope_value, key_value),
            )
            return cur.rowcount > 0


def apply_runtime_config_to_env(
    *,
    dsn: Optional[str] = None,
    scopes_by_precedence: Optional[list[str]] = None,
    keys: Optional[Iterable[str]] = None,
    raise_on_error: bool = False,
) -> dict[str, str]:
    """
    Applies runtime config values as process env var overrides (os.environ).

    Returns a map of key -> value for keys applied.
    """
    scopes = scopes_by_precedence or ["global"]
    requested_keys = list(keys) if keys is not None else sorted(DEFAULT_ENV_OVERRIDE_KEYS)
    try:
        effective = get_effective_runtime_config(dsn, scopes_by_precedence=scopes, keys=requested_keys)
    except Exception as exc:
        if _is_local_runtime() and _looks_like_db_connectivity_error(exc):
            logger.info("Runtime config load skipped (db unavailable?): %s", exc)
        else:
            logger.warning("Runtime config load skipped (db unavailable?): %s", exc)
        if raise_on_error:
            raise
        return {}

    applied: dict[str, str] = {}
    for key, item in effective.items():
        try:
            normalized = normalize_env_override(key, item.value)
        except Exception as exc:
            logger.warning("Skipping invalid runtime config override: key=%s error=%s", key, exc)
            continue
        os.environ[key] = normalized
        applied[key] = normalized

    if applied:
        logger.info("Runtime config applied: scopes=%s keys=%s", scopes, sorted(applied.keys()))
    return applied


def default_scopes_by_precedence() -> list[str]:
    job_name = (os.environ.get("CONTAINER_APP_JOB_NAME") or "").strip()
    scopes: list[str] = []
    if job_name:
        scopes.append(f"job:{job_name}")
    scopes.append("global")
    return scopes
