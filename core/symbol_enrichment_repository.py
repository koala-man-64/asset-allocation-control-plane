from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from asset_allocation_contracts.symbol_enrichment import (
    SymbolCleanupRunSummary,
    SymbolCleanupWorkItem,
    SymbolEnrichmentSummaryResponse,
    SymbolEnrichmentSymbolDetailResponse,
    SymbolEnrichmentSymbolListItem,
    SymbolProfileCurrent,
    SymbolProfileHistoryEntry,
    SymbolProfileOverride,
    SymbolProfileValues,
    SymbolProviderFacts,
)
from asset_allocation_runtime_common.foundation.postgres import connect


logger = logging.getLogger(__name__)

_RUNS_TABLE = "core.symbol_cleanup_runs"
_WORK_TABLE = "core.symbol_cleanup_work_queue"
_PROFILES_TABLE = "core.symbol_profiles"
_HISTORY_TABLE = "core.symbol_profile_history"
_OVERRIDES_TABLE = "core.symbol_profile_overrides"
_CATALOG_VIEW = "core.symbol_catalog_current"
_SYMBOLS_TABLE = "core.symbols"
_PROFILE_FIELDS = (
    "security_type_norm",
    "exchange_mic",
    "country_of_risk",
    "sector_norm",
    "industry_group_norm",
    "industry_norm",
    "is_adr",
    "is_etf",
    "is_cef",
    "is_preferred",
    "share_class",
    "listing_status_norm",
    "issuer_summary_short",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _normalize_symbol(value: str) -> str:
    text = str(value or "").strip().upper()
    if not text:
        raise ValueError("symbol is required.")
    return text


def _normalize_symbols(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = _normalize_symbol(value)
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    return deduped


def _requested_fields(fields: Sequence[str] | None = None) -> list[str]:
    if not fields:
        return list(_PROFILE_FIELDS)
    normalized = [str(field).strip() for field in fields if str(field).strip()]
    invalid = sorted(field for field in normalized if field not in _PROFILE_FIELDS)
    if invalid:
        raise ValueError(f"Unsupported symbol enrichment fields: {', '.join(invalid)}.")
    return normalized


def _row_to_run_summary(row: Sequence[Any]) -> SymbolCleanupRunSummary:
    columns = [
        "runId",
        "status",
        "mode",
        "queuedCount",
        "claimedCount",
        "completedCount",
        "failedCount",
        "acceptedUpdateCount",
        "rejectedUpdateCount",
        "lockedSkipCount",
        "overwriteCount",
        "startedAt",
        "completedAt",
    ]
    return SymbolCleanupRunSummary.model_validate(dict(zip(columns, row)))


def _row_to_work_item(row: Sequence[Any]) -> SymbolCleanupWorkItem:
    columns = [
        "workId",
        "runId",
        "symbol",
        "status",
        "requestedFields",
        "attemptCount",
        "executionName",
        "claimedAt",
        "lastError",
    ]
    payload = dict(zip(columns, row))
    payload["requestedFields"] = list(payload.get("requestedFields") or [])
    return SymbolCleanupWorkItem.model_validate(payload)


def _row_to_override(row: Sequence[Any]) -> SymbolProfileOverride:
    columns = ["symbol", "fieldName", "value", "isLocked", "updatedBy", "updatedAt"]
    return SymbolProfileOverride.model_validate(dict(zip(columns, row)))


def _row_to_history(row: Sequence[Any]) -> SymbolProfileHistoryEntry:
    columns = [
        "historyId",
        "symbol",
        "fieldName",
        "previousValue",
        "newValue",
        "sourceKind",
        "aiModel",
        "aiConfidence",
        "changeReason",
        "runId",
        "updatedAt",
    ]
    return SymbolProfileHistoryEntry.model_validate(dict(zip(columns, row)))


def _build_current_profile(row: Sequence[Any] | None) -> SymbolProfileCurrent | None:
    if not row:
        return None
    payload = dict(
        zip(
            [
                "symbol",
                *_PROFILE_FIELDS,
                "sourceKind",
                "sourceFingerprint",
                "aiModel",
                "aiConfidence",
                "validationStatus",
                "marketCapUsd",
                "marketCapBucket",
                "avgDollarVolume20d",
                "liquidityBucket",
                "isTradeableCommonEquity",
                "dataCompletenessScore",
                "updatedAt",
            ],
            row,
        )
    )
    return SymbolProfileCurrent.model_validate(payload)


def _build_provider_facts(row: Sequence[Any] | None) -> SymbolProviderFacts:
    if not row:
        raise LookupError("Symbol not found.")
    payload = dict(
        zip(
            [
                "symbol",
                "name",
                "description",
                "sector",
                "industry",
                "industry2",
                "country",
                "exchange",
                "assetType",
                "ipoDate",
                "delistingDate",
                "status",
                "isOptionable",
                "sourceNasdaq",
                "sourceMassive",
                "sourceAlphaVantage",
            ],
            row,
        )
    )
    return SymbolProviderFacts.model_validate(payload)


def _non_null_profile_payload(profile: SymbolProfileValues) -> dict[str, Any]:
    data = profile.model_dump(mode="json")
    return {field: value for field, value in data.items() if value is not None}


def _refresh_run_summary(conn, run_id: str) -> SymbolCleanupRunSummary:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH queue AS (
                SELECT
                    run_id,
                    COUNT(*) FILTER (WHERE status = 'queued') AS queued_count,
                    COUNT(*) FILTER (WHERE status = 'claimed') AS claimed_count,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
                    COALESCE(SUM(accepted_update_count), 0) AS accepted_update_count,
                    COALESCE(SUM(rejected_update_count), 0) AS rejected_update_count,
                    COALESCE(SUM(locked_skip_count), 0) AS locked_skip_count,
                    COALESCE(SUM(overwrite_count), 0) AS overwrite_count,
                    COUNT(*) AS total_count
                FROM {_WORK_TABLE}
                WHERE run_id = %s
                GROUP BY run_id
            )
            UPDATE {_RUNS_TABLE} AS runs
            SET
                queued_count = COALESCE(queue.queued_count, 0),
                claimed_count = COALESCE(queue.claimed_count, 0),
                completed_count = COALESCE(queue.completed_count, 0),
                failed_count = COALESCE(queue.failed_count, 0),
                accepted_update_count = COALESCE(queue.accepted_update_count, 0),
                rejected_update_count = COALESCE(queue.rejected_update_count, 0),
                locked_skip_count = COALESCE(queue.locked_skip_count, 0),
                overwrite_count = COALESCE(queue.overwrite_count, 0),
                status = CASE
                    WHEN COALESCE(queue.total_count, 0) = 0 THEN 'completed'
                    WHEN COALESCE(queue.claimed_count, 0) > 0 OR COALESCE(queue.completed_count, 0) > 0 OR COALESCE(queue.failed_count, 0) > 0 THEN
                        CASE
                            WHEN COALESCE(queue.queued_count, 0) = 0 AND COALESCE(queue.claimed_count, 0) = 0 AND COALESCE(queue.failed_count, 0) > 0 THEN 'failed'
                            WHEN COALESCE(queue.queued_count, 0) = 0 AND COALESCE(queue.claimed_count, 0) = 0 THEN 'completed'
                            ELSE 'running'
                        END
                    ELSE 'queued'
                END,
                started_at = CASE
                    WHEN runs.started_at IS NOT NULL THEN runs.started_at
                    WHEN COALESCE(queue.claimed_count, 0) > 0 OR COALESCE(queue.completed_count, 0) > 0 OR COALESCE(queue.failed_count, 0) > 0 THEN now()
                    ELSE NULL
                END,
                completed_at = CASE
                    WHEN COALESCE(queue.queued_count, 0) = 0 AND COALESCE(queue.claimed_count, 0) = 0 THEN now()
                    ELSE NULL
                END,
                updated_at = now()
            FROM queue
            WHERE runs.run_id = %s
            RETURNING
                runs.run_id,
                runs.status,
                runs.mode,
                runs.queued_count,
                runs.claimed_count,
                runs.completed_count,
                runs.failed_count,
                runs.accepted_update_count,
                runs.rejected_update_count,
                runs.locked_skip_count,
                runs.overwrite_count,
                runs.started_at,
                runs.completed_at
            """,
            (run_id, run_id),
        )
        row = cur.fetchone()
        if row:
            return _row_to_run_summary(row)

        cur.execute(
            f"""
            SELECT
                run_id,
                status,
                mode,
                queued_count,
                claimed_count,
                completed_count,
                failed_count,
                accepted_update_count,
                rejected_update_count,
                locked_skip_count,
                overwrite_count,
                started_at,
                completed_at
            FROM {_RUNS_TABLE}
            WHERE run_id = %s
            """,
            (run_id,),
        )
        final_row = cur.fetchone()
        if not final_row:
            raise LookupError(f"Symbol cleanup run '{run_id}' not found.")
        return _row_to_run_summary(final_row)


def _resolve_symbols_for_enqueue(conn, *, symbols: Sequence[str], full_scan: bool, max_symbols: int | None) -> list[str]:
    limit = int(max_symbols or 0) if max_symbols is not None else None
    with conn.cursor() as cur:
        if full_scan:
            sql = f"SELECT symbol FROM {_SYMBOLS_TABLE} ORDER BY symbol"
            if limit is not None:
                sql += " LIMIT %s"
                cur.execute(sql, (limit,))
            else:
                cur.execute(sql)
            return [str(row[0]) for row in cur.fetchall()]

        normalized = _normalize_symbols(symbols)
        if not normalized:
            return []
        cur.execute(
            f"""
            SELECT symbol
            FROM {_SYMBOLS_TABLE}
            WHERE symbol = ANY(%s)
            ORDER BY symbol
            """,
            (normalized,),
        )
        resolved = [str(row[0]) for row in cur.fetchall()]
        if limit is not None:
            resolved = resolved[:limit]
        return resolved


def enqueue_symbol_cleanup_run(
    dsn: str,
    *,
    symbols: Sequence[str],
    full_scan: bool,
    overwrite_mode: str,
    max_symbols: int | None,
) -> SymbolCleanupRunSummary:
    with connect(dsn) as conn:
        resolved_symbols = _resolve_symbols_for_enqueue(
            conn,
            symbols=symbols,
            full_scan=full_scan,
            max_symbols=max_symbols,
        )
        if not resolved_symbols:
            raise ValueError("No symbols matched the requested enqueue scope.")

        run_id = uuid.uuid4().hex
        requested_fields = _requested_fields()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_RUNS_TABLE} (run_id, status, mode, created_at, updated_at)
                VALUES (%s, 'queued', %s, now(), now())
                """,
                (run_id, overwrite_mode),
            )
            work_rows = [
                (
                    uuid.uuid4().hex,
                    run_id,
                    symbol,
                    _json_dumps(requested_fields),
                )
                for symbol in resolved_symbols
            ]
            cur.executemany(
                f"""
                INSERT INTO {_WORK_TABLE} (
                    work_id,
                    run_id,
                    symbol,
                    requested_fields
                )
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id, symbol) DO NOTHING
                """,
                work_rows,
            )
        return _refresh_run_summary(conn, run_id)


def claim_next_symbol_cleanup_work(dsn: str, *, execution_name: str | None = None) -> SymbolCleanupWorkItem | None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH next_work AS (
                    SELECT work_id
                    FROM {_WORK_TABLE}
                    WHERE status = 'queued'
                    ORDER BY created_at, work_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE {_WORK_TABLE} AS work
                SET
                    status = 'claimed',
                    attempt_count = work.attempt_count + 1,
                    execution_name = %s,
                    claimed_at = now(),
                    updated_at = now()
                FROM next_work
                WHERE work.work_id = next_work.work_id
                RETURNING
                    work.work_id,
                    work.run_id,
                    work.symbol,
                    work.status,
                    work.requested_fields,
                    work.attempt_count,
                    work.execution_name,
                    work.claimed_at,
                    work.last_error
                """,
                (execution_name,),
            )
            row = cur.fetchone()
            if not row:
                return None
            item = _row_to_work_item(row)
        _refresh_run_summary(conn, item.runId)
        return item


def _locked_override_fields(conn, symbol: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT field_name
            FROM {_OVERRIDES_TABLE}
            WHERE symbol = %s AND is_locked = TRUE
            """,
            (symbol,),
        )
        return {str(row[0]) for row in cur.fetchall()}


def _load_existing_profile(conn, symbol: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                symbol,
                {", ".join(_PROFILE_FIELDS)},
                source_kind,
                source_fingerprint,
                ai_model,
                ai_confidence,
                validation_status,
                updated_at
            FROM {_PROFILES_TABLE}
            WHERE symbol = %s
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        columns = [
            "symbol",
            *_PROFILE_FIELDS,
            "source_kind",
            "source_fingerprint",
            "ai_model",
            "ai_confidence",
            "validation_status",
            "updated_at",
        ]
        return dict(zip(columns, row))


def _insert_history_entry(
    conn,
    *,
    symbol: str,
    field_name: str,
    previous_value: Any,
    new_value: Any,
    ai_model: str | None,
    ai_confidence: float | None,
    run_id: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_HISTORY_TABLE} (
                history_id,
                symbol,
                field_name,
                previous_value,
                new_value,
                source_kind,
                ai_model,
                ai_confidence,
                change_reason,
                run_id,
                updated_at
            )
            VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, 'ai', %s, %s, %s, %s, now())
            """,
            (
                uuid.uuid4().hex,
                symbol,
                field_name,
                json.dumps(previous_value),
                json.dumps(new_value),
                ai_model,
                ai_confidence,
                "symbol_cleanup",
                run_id,
            ),
        )


def _upsert_profile(
    conn,
    *,
    symbol: str,
    updates: dict[str, Any],
    source_fingerprint: str | None,
    ai_model: str | None,
    ai_confidence: float | None,
) -> None:
    if not updates:
        return
    columns = ["symbol", *updates.keys(), "source_kind", "source_fingerprint", "ai_model", "ai_confidence", "validation_status"]
    placeholders = ["%s", *["%s" for _ in updates], "%s", "%s", "%s", "%s", "%s"]
    update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in [*updates.keys(), "source_kind", "source_fingerprint", "ai_model", "ai_confidence", "validation_status"])
    values = [
        symbol,
        *updates.values(),
        "ai",
        source_fingerprint,
        ai_model,
        ai_confidence,
        "accepted",
    ]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_PROFILES_TABLE} ({", ".join(columns)}, updated_at)
            VALUES ({", ".join(placeholders)}, now())
            ON CONFLICT (symbol)
            DO UPDATE SET
                {update_sql},
                updated_at = now()
            """,
            values,
        )


def _apply_profile_result(
    conn,
    *,
    symbol: str,
    run_id: str | None,
    result: dict[str, Any],
) -> tuple[int, int, int]:
    payload = SymbolProfileValues.model_validate(result.get("profile") or {})
    locked_fields = _locked_override_fields(conn, symbol)
    existing = _load_existing_profile(conn, symbol)
    existing_values = {field: existing.get(field) for field in _PROFILE_FIELDS}
    incoming_values = _non_null_profile_payload(payload)
    accepted = 0
    locked = 0
    overwrite = 0
    updates: dict[str, Any] = {}
    for field_name, new_value in incoming_values.items():
        if field_name in locked_fields:
            locked += 1
            continue
        previous_value = existing_values.get(field_name)
        if previous_value == new_value:
            continue
        updates[field_name] = new_value
        accepted += 1
        if previous_value is not None:
            overwrite += 1
        _insert_history_entry(
            conn,
            symbol=symbol,
            field_name=field_name,
            previous_value=previous_value,
            new_value=new_value,
            ai_model=result.get("model"),
            ai_confidence=result.get("confidence"),
            run_id=run_id,
        )
    _upsert_profile(
        conn,
        symbol=symbol,
        updates=updates,
        source_fingerprint=result.get("sourceFingerprint"),
        ai_model=result.get("model"),
        ai_confidence=result.get("confidence"),
    )
    return accepted, locked, overwrite


def complete_symbol_cleanup_work(dsn: str, *, work_id: str, result: dict[str, Any] | None) -> SymbolCleanupRunSummary:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT run_id, symbol, status
                FROM {_WORK_TABLE}
                WHERE work_id = %s
                FOR UPDATE
                """,
                (work_id,),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError(f"Symbol cleanup work '{work_id}' not found.")
            run_id, symbol, status = str(row[0]), str(row[1]), str(row[2])
            if status not in {"queued", "claimed"}:
                raise LookupError(f"Symbol cleanup work '{work_id}' is already {status}.")

        accepted, locked, overwrite = (0, 0, 0)
        if result is not None:
            resolved_symbol = _normalize_symbol(result.get("symbol") or symbol)
            if resolved_symbol != symbol:
                raise ValueError(f"Resolved symbol mismatch for work '{work_id}': expected {symbol}, got {resolved_symbol}.")
            accepted, locked, overwrite = _apply_profile_result(
                conn,
                symbol=symbol,
                run_id=run_id,
                result=result,
            )

        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {_WORK_TABLE}
                SET
                    status = 'completed',
                    completed_at = now(),
                    accepted_update_count = %s,
                    locked_skip_count = %s,
                    overwrite_count = %s,
                    result_json = %s::jsonb,
                    updated_at = now()
                WHERE work_id = %s
                """,
                (
                    accepted,
                    locked,
                    overwrite,
                    json.dumps(result) if result is not None else None,
                    work_id,
                ),
            )
        return _refresh_run_summary(conn, run_id)


def fail_symbol_cleanup_work(dsn: str, *, work_id: str, error: str) -> SymbolCleanupRunSummary:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT run_id, status
                FROM {_WORK_TABLE}
                WHERE work_id = %s
                FOR UPDATE
                """,
                (work_id,),
            )
            row = cur.fetchone()
            if not row:
                raise LookupError(f"Symbol cleanup work '{work_id}' not found.")
            run_id, status = str(row[0]), str(row[1])
            if status not in {"queued", "claimed"}:
                raise LookupError(f"Symbol cleanup work '{work_id}' is already {status}.")
            cur.execute(
                f"""
                UPDATE {_WORK_TABLE}
                SET
                    status = 'failed',
                    completed_at = now(),
                    last_error = %s,
                    updated_at = now()
                WHERE work_id = %s
                """,
                (error, work_id),
            )
        return _refresh_run_summary(conn, run_id)


def get_symbol_cleanup_run(dsn: str, run_id: str) -> SymbolCleanupRunSummary | None:
    with connect(dsn) as conn:
        try:
            return _refresh_run_summary(conn, run_id)
        except LookupError:
            return None


def list_symbol_cleanup_runs(dsn: str, *, limit: int = 50, offset: int = 0) -> list[SymbolCleanupRunSummary]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    run_id,
                    status,
                    mode,
                    queued_count,
                    claimed_count,
                    completed_count,
                    failed_count,
                    accepted_update_count,
                    rejected_update_count,
                    locked_skip_count,
                    overwrite_count,
                    started_at,
                    completed_at
                FROM {_RUNS_TABLE}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (int(limit), int(offset)),
            )
            return [_row_to_run_summary(row) for row in cur.fetchall()]


def get_symbol_enrichment_summary(dsn: str) -> SymbolEnrichmentSummaryResponse:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {_WORK_TABLE} WHERE status IN ('queued', 'claimed')")
            backlog_count = int(cur.fetchone()[0] or 0)
            cur.execute(f"SELECT COUNT(*) FROM {_WORK_TABLE} WHERE status = 'failed'")
            validation_failure_count = int(cur.fetchone()[0] or 0)
            cur.execute(f"SELECT COUNT(*) FROM {_OVERRIDES_TABLE} WHERE is_locked = TRUE")
            lock_count = int(cur.fetchone()[0] or 0)
            cur.execute(
                f"""
                SELECT
                    run_id,
                    status,
                    mode,
                    queued_count,
                    claimed_count,
                    completed_count,
                    failed_count,
                    accepted_update_count,
                    rejected_update_count,
                    locked_skip_count,
                    overwrite_count,
                    started_at,
                    completed_at
                FROM {_RUNS_TABLE}
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            last_run_row = cur.fetchone()
            cur.execute(
                f"""
                SELECT
                    run_id,
                    status,
                    mode,
                    queued_count,
                    claimed_count,
                    completed_count,
                    failed_count,
                    accepted_update_count,
                    rejected_update_count,
                    locked_skip_count,
                    overwrite_count,
                    started_at,
                    completed_at
                FROM {_RUNS_TABLE}
                WHERE status = 'running'
                ORDER BY started_at DESC NULLS LAST, created_at DESC
                LIMIT 1
                """
            )
            active_run_row = cur.fetchone()
    return SymbolEnrichmentSummaryResponse(
        backlogCount=backlog_count,
        lastRun=_row_to_run_summary(last_run_row) if last_run_row else None,
        activeRun=_row_to_run_summary(active_run_row) if active_run_row else None,
        validationFailureCount=validation_failure_count,
        lockCount=lock_count,
    )


def list_symbol_enrichment_symbols(
    dsn: str,
    *,
    q: str | None,
    limit: int,
    offset: int,
) -> tuple[int, list[SymbolEnrichmentSymbolListItem]]:
    query_text = str(q or "").strip().upper()
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            base_sql = f"""
                SELECT
                    symbol,
                    name,
                    validation_status,
                    COALESCE(source_kind, 'provider') AS source_kind,
                    updated_at,
                    data_completeness_score,
                    {", ".join(_PROFILE_FIELDS)}
                FROM {_CATALOG_VIEW}
            """
            params: list[Any] = []
            if query_text:
                base_sql += " WHERE upper(symbol) LIKE %s OR upper(COALESCE(name, '')) LIKE %s"
                params.extend([f"%{query_text}%", f"%{query_text}%"])

            count_sql = f"SELECT COUNT(*) FROM ({base_sql}) AS filtered"
            cur.execute(count_sql, params)
            total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                {base_sql}
                ORDER BY symbol
                LIMIT %s OFFSET %s
                """,
                [*params, int(limit), int(offset)],
            )
            rows = cur.fetchall()

        with conn.cursor() as cur:
            items: list[SymbolEnrichmentSymbolListItem] = []
            for row in rows:
                missing_field_count = sum(1 for value in row[6:] if value is None)
                cur.execute(
                    f"SELECT COUNT(*) FROM {_OVERRIDES_TABLE} WHERE symbol = %s AND is_locked = TRUE",
                    (row[0],),
                )
                locked_count = int(cur.fetchone()[0] or 0)
                items.append(
                    SymbolEnrichmentSymbolListItem(
                        symbol=str(row[0]),
                        name=row[1],
                        status=row[2] or "accepted",
                        sourceKind=row[3],
                        updatedAt=row[4],
                        missingFieldCount=missing_field_count,
                        lockedFieldCount=locked_count,
                        dataCompletenessScore=row[5],
                    )
                )
    return total, items


def get_symbol_enrichment_symbol_detail(dsn: str, symbol: str) -> SymbolEnrichmentSymbolDetailResponse:
    resolved_symbol = _normalize_symbol(symbol)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    symbol,
                    name,
                    description,
                    sector,
                    industry,
                    industry_2,
                    country,
                    exchange,
                    asset_type,
                    ipo_date,
                    delisting_date,
                    status,
                    COALESCE(
                        is_optionable,
                        CASE
                            WHEN upper(trim(COALESCE(optionable, ''))) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
                            WHEN upper(trim(COALESCE(optionable, ''))) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
                            ELSE NULL
                        END
                    ) AS is_optionable,
                    source_nasdaq,
                    source_massive,
                    source_alpha_vantage
                FROM {_SYMBOLS_TABLE}
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
            provider_row = cur.fetchone()
            provider_facts = _build_provider_facts(provider_row)
            cur.execute(
                f"""
                SELECT
                    symbol,
                    {", ".join(_PROFILE_FIELDS)},
                    source_kind,
                    source_fingerprint,
                    ai_model,
                    ai_confidence,
                    validation_status,
                    market_cap_usd,
                    market_cap_bucket,
                    avg_dollar_volume_20d,
                    liquidity_bucket,
                    is_tradeable_common_equity,
                    data_completeness_score,
                    updated_at
                FROM {_CATALOG_VIEW}
                WHERE symbol = %s
                """,
                (resolved_symbol,),
            )
            current_profile = _build_current_profile(cur.fetchone())
            cur.execute(
                f"""
                SELECT
                    symbol,
                    field_name,
                    value_json,
                    is_locked,
                    updated_by,
                    updated_at
                FROM {_OVERRIDES_TABLE}
                WHERE symbol = %s
                ORDER BY field_name
                """,
                (resolved_symbol,),
            )
            overrides = [_row_to_override(row) for row in cur.fetchall()]
            cur.execute(
                f"""
                SELECT
                    history_id,
                    symbol,
                    field_name,
                    previous_value,
                    new_value,
                    source_kind,
                    ai_model,
                    ai_confidence,
                    change_reason,
                    run_id,
                    updated_at
                FROM {_HISTORY_TABLE}
                WHERE symbol = %s
                ORDER BY updated_at DESC, history_id DESC
                LIMIT 100
                """,
                (resolved_symbol,),
            )
            history = [_row_to_history(row) for row in cur.fetchall()]
    return SymbolEnrichmentSymbolDetailResponse(
        providerFacts=provider_facts,
        currentProfile=current_profile,
        overrides=overrides,
        history=history,
    )


def upsert_symbol_profile_overrides(
    dsn: str,
    *,
    symbol: str,
    overrides: Sequence[SymbolProfileOverride],
) -> list[SymbolProfileOverride]:
    resolved_symbol = _normalize_symbol(symbol)
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {_SYMBOLS_TABLE} WHERE symbol = %s", (resolved_symbol,))
            if cur.fetchone() is None:
                raise LookupError(f"Symbol '{resolved_symbol}' not found.")
            for override in overrides:
                cur.execute(
                    f"""
                    INSERT INTO {_OVERRIDES_TABLE} (
                        symbol,
                        field_name,
                        value_json,
                        is_locked,
                        updated_by,
                        updated_at
                    )
                    VALUES (%s, %s, %s::jsonb, %s, %s, now())
                    ON CONFLICT (symbol, field_name)
                    DO UPDATE SET
                        value_json = EXCLUDED.value_json,
                        is_locked = EXCLUDED.is_locked,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = now()
                    """,
                    (
                        resolved_symbol,
                        override.fieldName,
                        json.dumps(override.value),
                        override.isLocked,
                        override.updatedBy,
                    ),
                )
            cur.execute(
                f"""
                SELECT
                    symbol,
                    field_name,
                    value_json,
                    is_locked,
                    updated_by,
                    updated_at
                FROM {_OVERRIDES_TABLE}
                WHERE symbol = %s
                ORDER BY field_name
                """,
                (resolved_symbol,),
            )
            return [_row_to_override(row) for row in cur.fetchall()]


def resolve_symbol_enrichment_max_symbols(default: int = 500) -> int:
    raw = str(os.environ.get("SYMBOL_ENRICHMENT_MAX_SYMBOLS_PER_RUN") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, min(value, 50_000))
