from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from asset_allocation_runtime_common.foundation.postgres import PostgresError, connect
_ALLOWED_OPERATORS = {
    "gt",
    "gte",
    "lt",
    "lte",
    "eq",
    "ne",
    "bottom_percent",
    "top_percent",
}

_PERCENTAGE_OPERATORS = {"bottom_percent", "top_percent"}


_OPERATOR_ALIASES = {
    "greater_than": "gt",
    "greater-than": "gt",
    "greater": "gt",
    "gte": "gte",
    "at_least": "gte",
    "minimum": "gte",
    "greater_or_equal": "gte",
    "less_than": "lt",
    "less-than": "lt",
    "less": "lt",
    "lte": "lte",
    "at_most": "lte",
    "less_or_equal": "lte",
    "eq": "eq",
    "equals": "eq",
    "equal": "eq",
    "ne": "ne",
    "not_equal": "ne",
    "not_equal_to": "ne",
    "bottom_percent": "bottom_percent",
    "bottom_percentile": "bottom_percent",
    "bottom_pct": "bottom_percent",
    "bottom-percent": "bottom_percent",
    "top_percent": "top_percent",
    "top_percentile": "top_percent",
    "top_pct": "top_percent",
    "top-percent": "top_percent",
}


@dataclass(frozen=True)
class PurgeRule:
    id: int
    name: str
    layer: str
    domain: str
    column_name: str
    operator: str
    threshold: float
    run_interval_minutes: int
    next_run_at: Optional[datetime]
    last_run_at: Optional[datetime]
    last_status: Optional[str]
    last_error: Optional[str]
    last_match_count: Optional[int]
    last_purge_count: Optional[int]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    created_by: Optional[str]
    updated_by: Optional[str]


def supported_purge_rule_operators() -> list[str]:
    return sorted(_ALLOWED_OPERATORS)


def normalize_purge_rule_operator(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if not normalized:
        raise ValueError("operator is required.")
    resolved = _OPERATOR_ALIASES.get(normalized, normalized)
    if resolved not in _ALLOWED_OPERATORS:
        raise ValueError(
            f"Unsupported operator '{value}'. Supported: {', '.join(supported_purge_rule_operators())}"
        )
    return resolved


def is_percent_operator(operator: str) -> bool:
    return normalize_purge_rule_operator(operator) in _PERCENTAGE_OPERATORS


def _resolve_dsn(dsn: Optional[str]) -> Optional[str]:
    raw = dsn or ""
    value = raw.strip()
    return value or None


def _coerce_threshold(raw: object) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("threshold must be a number.") from exc
    if value != value:
        raise ValueError("threshold must be a real number.")
    return value


def _coerce_interval_minutes(raw: object) -> int:
    try:
        minutes = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("run_interval_minutes must be an integer.") from exc
    if minutes < 1:
        raise ValueError("run_interval_minutes must be >= 1.")
    return minutes


def _ensure_identifier(value: object, label: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"{label} is required.")
    return raw


def _coerce_next_run_after(interval_minutes: int, now: Optional[datetime] = None) -> datetime:
    resolved_now = now or datetime.now(timezone.utc)
    return resolved_now + timedelta(minutes=interval_minutes)


def _row_to_rule(row: tuple[object, ...]) -> PurgeRule:
    return PurgeRule(
        id=int(row[0]),
        name=str(row[1] or ""),
        layer=str(row[2] or ""),
        domain=str(row[3] or ""),
        column_name=str(row[4] or ""),
        operator=str(row[5] or ""),
        threshold=float(row[6]) if row[6] is not None else 0.0,
        run_interval_minutes=int(row[7] or 0),
        next_run_at=row[8],
        last_run_at=row[9],
        last_status=str(row[10]) if row[10] is not None else None,
        last_error=str(row[11]) if row[11] is not None else None,
        last_match_count=int(row[12]) if row[12] is not None else None,
        last_purge_count=int(row[13]) if row[13] is not None else None,
        created_at=row[14],
        updated_at=row[15],
        created_by=str(row[16]) if row[16] is not None else None,
        updated_by=str(row[17]) if row[17] is not None else None,
    )


def _fetch_rows(dsn: Optional[str], query: str, params: tuple = ()) -> List[tuple[object, ...]]:
    resolved_dsn = _resolve_dsn(dsn)
    if not resolved_dsn:
        raise PostgresError("POSTGRES_DSN is not configured.")
    with connect(resolved_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def _execute(dsn: Optional[str], query: str, params: tuple = ()) -> Optional[tuple[object, ...]]:
    resolved_dsn = _resolve_dsn(dsn)
    if not resolved_dsn:
        raise PostgresError("POSTGRES_DSN is not configured.")
    with connect(resolved_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()


def list_purge_rules(
    dsn: Optional[str],
    *,
    layer: Optional[str] = None,
    domain: Optional[str] = None,
) -> list[PurgeRule]:
    where: List[str] = []
    params: list[object] = []
    if layer:
        where.append("layer = %s")
        params.append(layer)
    if domain:
        where.append("domain = %s")
        params.append(domain)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    query = f"""
        SELECT
          id, name, layer, domain, column_name, operator, threshold,
          run_interval_minutes, next_run_at, last_run_at, last_status,
          last_error, last_match_count, last_purge_count,
          created_at, updated_at, created_by, updated_by
        FROM core.purge_rules
        {where_sql}
        ORDER BY updated_at DESC, id DESC
    """
    rows = _fetch_rows(dsn, query, tuple(params))
    return [_row_to_rule(row) for row in rows]


def get_purge_rule(dsn: Optional[str], rule_id: int) -> Optional[PurgeRule]:
    if rule_id <= 0:
        return None
    query = """
        SELECT
          id, name, layer, domain, column_name, operator, threshold,
          run_interval_minutes, next_run_at, last_run_at, last_status,
          last_error, last_match_count, last_purge_count,
          created_at, updated_at, created_by, updated_by
        FROM core.purge_rules
        WHERE id = %s
    """
    row = _execute(dsn, query, (rule_id,))
    return _row_to_rule(row) if row else None


def list_due_purge_rules(dsn: Optional[str], *, now: Optional[datetime] = None) -> list[PurgeRule]:
    resolved_now = now or datetime.now(timezone.utc)
    query = """
        SELECT
          id, name, layer, domain, column_name, operator, threshold,
          run_interval_minutes, next_run_at, last_run_at, last_status,
          last_error, last_match_count, last_purge_count,
          created_at, updated_at, created_by, updated_by
        FROM core.purge_rules
        WHERE next_run_at IS NULL OR next_run_at <= %s
        ORDER BY COALESCE(next_run_at, %s), id
    """
    rows = _fetch_rows(dsn, query, (resolved_now, resolved_now))
    return [_row_to_rule(row) for row in rows]


def create_purge_rule(
    *,
    dsn: Optional[str],
    name: str,
    layer: str,
    domain: str,
    column_name: str,
    operator: str,
    threshold: object,
    run_interval_minutes: object,
    actor: Optional[str] = None,
) -> PurgeRule:
    validated = PurgeRule(
        id=0,
        name=_ensure_identifier(name, "name"),
        layer=_ensure_identifier(layer, "layer").lower(),
        domain=_ensure_identifier(domain, "domain").lower(),
        column_name=_ensure_identifier(column_name, "column_name").strip(),
        operator=normalize_purge_rule_operator(operator),
        threshold=_coerce_threshold(threshold),
        run_interval_minutes=_coerce_interval_minutes(run_interval_minutes),
        next_run_at=None,
        last_run_at=None,
        last_status=None,
        last_error=None,
        last_match_count=None,
        last_purge_count=None,
        created_at=None,
        updated_at=None,
        created_by=actor,
        updated_by=actor,
    )

    if validated.threshold < 0 and is_percent_operator(validated.operator):
        raise ValueError("Percentile operators require a non-negative threshold.")
    if is_percent_operator(validated.operator) and validated.threshold > 100:
        raise ValueError("Percentile threshold must be between 0 and 100.")

    now = datetime.now(timezone.utc)
    next_run_at = _coerce_next_run_after(validated.run_interval_minutes, now)
    query = """
        INSERT INTO core.purge_rules(
          name, layer, domain, column_name, operator, threshold, run_interval_minutes,
          next_run_at, created_by, updated_by
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING
          id, name, layer, domain, column_name, operator, threshold,
          run_interval_minutes, next_run_at, last_run_at, last_status,
          last_error, last_match_count, last_purge_count,
          created_at, updated_at, created_by, updated_by
    """
    row = _execute(
        dsn,
        query,
        (
            validated.name,
            validated.layer,
            validated.domain,
            validated.column_name,
            validated.operator,
            validated.threshold,
            validated.run_interval_minutes,
            next_run_at,
            actor,
            actor,
        ),
    )
    if not row:
        raise PostgresError("Failed to create purge rule.")
    created = _row_to_rule(row)
    return created


def update_purge_rule(
    *,
    dsn: Optional[str],
    rule_id: int,
    name: Optional[str] = None,
    layer: Optional[str] = None,
    domain: Optional[str] = None,
    column_name: Optional[str] = None,
    operator: Optional[str] = None,
    threshold: Optional[object] = None,
    run_interval_minutes: Optional[object] = None,
    actor: Optional[str] = None,
) -> PurgeRule:
    existing = get_purge_rule(dsn, rule_id)
    if not existing:
        raise KeyError(f"rule_id={rule_id} not found.")

    values: list[object] = []
    updates: list[str] = []

    if name is not None:
        updates.append("name = %s")
        values.append(_ensure_identifier(name, "name"))
    if layer is not None:
        updates.append("layer = %s")
        values.append(_ensure_identifier(layer, "layer").lower())
    if domain is not None:
        updates.append("domain = %s")
        values.append(_ensure_identifier(domain, "domain").lower())
    if column_name is not None:
        updates.append("column_name = %s")
        values.append(_ensure_identifier(column_name, "column_name"))
    if operator is not None:
        updates.append("operator = %s")
        values.append(normalize_purge_rule_operator(operator))
    if threshold is not None:
        updates.append("threshold = %s")
        values.append(_coerce_threshold(threshold))
    if run_interval_minutes is not None:
        updates.append("run_interval_minutes = %s")
        values.append(_coerce_interval_minutes(run_interval_minutes))

    if not updates:
        raise ValueError("No fields provided for update.")

    updates.extend(["updated_by = %s", "updated_at = now()"])
    values.extend([actor, rule_id])
    query = f"""
        UPDATE core.purge_rules
        SET {", ".join(updates)}
        WHERE id = %s
        RETURNING
          id, name, layer, domain, column_name, operator, threshold,
          run_interval_minutes, next_run_at, last_run_at, last_status,
          last_error, last_match_count, last_purge_count,
          created_at, updated_at, created_by, updated_by
    """
    row = _execute(dsn, query, tuple(values))
    if not row:
        raise PostgresError("Failed to update purge rule.")
    return _row_to_rule(row)


def delete_purge_rule(dsn: Optional[str], rule_id: int) -> bool:
    if rule_id <= 0:
        return False
    row = _execute(dsn, "DELETE FROM core.purge_rules WHERE id = %s RETURNING id", (rule_id,))
    return bool(row)


def claim_purge_rule_for_run(
    *,
    dsn: Optional[str],
    rule_id: int,
    now: datetime,
    require_due: bool,
    actor: Optional[str] = None,
) -> bool:
    resolved_dsn = _resolve_dsn(dsn)
    if not resolved_dsn:
        raise PostgresError("POSTGRES_DSN is not configured.")

    if require_due:
        query = """
            UPDATE core.purge_rules
            SET last_status = 'running',
                last_error = NULL,
                updated_by = COALESCE(%s, updated_by),
                updated_at = %s
            WHERE id = %s
              AND (next_run_at IS NULL OR next_run_at <= %s)
              AND (last_status IS DISTINCT FROM 'running')
            RETURNING id
        """
        params = (actor, now, rule_id, now)
    else:
        query = """
            UPDATE core.purge_rules
            SET last_status = 'running',
                last_error = NULL,
                updated_by = COALESCE(%s, updated_by),
                updated_at = %s
            WHERE id = %s
              AND (last_status IS DISTINCT FROM 'running')
            RETURNING id
        """
        params = (actor, now, rule_id)

    row = _execute(resolved_dsn, query, params)
    return bool(row)


def complete_purge_rule_execution(
    *,
    dsn: Optional[str],
    rule_id: int,
    status: str,
    error: Optional[str],
    matched_count: Optional[int],
    purged_count: Optional[int],
    run_interval_minutes: int,
    actor: Optional[str],
    now: Optional[datetime] = None,
) -> None:
    resolved_now = now or datetime.now(timezone.utc)
    next_run_at = _coerce_next_run_after(run_interval_minutes, resolved_now)

    query = """
        UPDATE core.purge_rules
        SET last_run_at = %s,
            last_status = %s,
            last_error = %s,
            last_match_count = %s,
            last_purge_count = %s,
            next_run_at = %s,
            updated_at = %s,
            updated_by = %s
        WHERE id = %s
        RETURNING id
    """
    row = _execute(
        dsn,
        query,
        (
            resolved_now,
            status,
            error,
            matched_count,
            purged_count,
            next_run_at,
            resolved_now,
            actor,
            rule_id,
        ),
    )
    if not row:
        raise PostgresError(f"Failed to persist purge rule execution result for rule_id={rule_id}.")
