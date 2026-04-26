from __future__ import annotations

import json
import uuid
from datetime import datetime, time, timezone
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

from asset_allocation_runtime_common.foundation.postgres import connect

from api.service.intraday_contracts_compat import (
    INTRADAY_WATCHLIST_SYMBOLS_MAX,
    IntradayMonitorEvent,
    IntradayMonitorRunSummary,
    IntradayRefreshBatchSummary,
    IntradaySymbolStatus,
    IntradayWatchlistDetail,
    IntradayWatchlistSymbolAppendRequest,
    IntradayWatchlistSymbolAppendResponse,
    IntradayWatchlistSummary,
    IntradayWatchlistUpsertRequest,
)
from core.bronze_bucketing import bucket_letter

_WATCHLISTS_TABLE = "core.intraday_watchlists"
_WATCHLIST_SYMBOLS_TABLE = "core.intraday_watchlist_symbols"
_WATCHLIST_EVENTS_TABLE = "core.intraday_watchlist_events"
_MONITOR_RUNS_TABLE = "core.intraday_monitor_runs"
_MONITOR_EVENTS_TABLE = "core.intraday_monitor_events"
_SYMBOL_STATUS_TABLE = "core.intraday_symbol_status"
_REFRESH_BATCHES_TABLE = "core.intraday_refresh_batches"
_SYMBOLS_TABLE = "core.symbols"
_NEW_YORK_TZ = ZoneInfo("America/New_York")
_US_EQUITIES_REGULAR_OPEN = time(9, 30)
_US_EQUITIES_REGULAR_CLOSE = time(16, 0)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _normalize_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise ValueError("Symbol values must be non-empty.")
    if len(symbol) > 32:
        raise ValueError("Symbol values must be 32 characters or fewer.")
    return symbol


def _normalize_symbols(values: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        symbol = _normalize_symbol(raw)
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_market_session_open(market_session: str, *, reference_utc: datetime | None = None) -> bool:
    if str(market_session or "").strip().lower() != "us_equities_regular":
        return True

    localized = (reference_utc or _utc_now()).astimezone(_NEW_YORK_TZ)
    if localized.weekday() >= 5:
        return False

    local_time = localized.timetz().replace(tzinfo=None)
    return _US_EQUITIES_REGULAR_OPEN <= local_time < _US_EQUITIES_REGULAR_CLOSE


def _list_currently_due_watchlists(conn, *, reference_utc: datetime | None = None) -> list[tuple[str, int, str]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                w.watchlist_id,
                COUNT(ws.symbol)::integer AS symbol_count,
                w.market_session
            FROM {_WATCHLISTS_TABLE} AS w
            JOIN {_WATCHLIST_SYMBOLS_TABLE} AS ws
                ON ws.watchlist_id = w.watchlist_id
            WHERE w.enabled = TRUE
              AND w.next_due_at IS NOT NULL
              AND w.next_due_at <= now()
              AND NOT EXISTS (
                  SELECT 1
                  FROM {_MONITOR_RUNS_TABLE} AS runs
                  WHERE runs.watchlist_id = w.watchlist_id
                    AND runs.status IN ('queued', 'claimed')
              )
            GROUP BY w.watchlist_id, w.market_session
            ORDER BY w.next_due_at, w.watchlist_id
            """
        )
        rows = cur.fetchall()

    now_utc = reference_utc or _utc_now()
    due_watchlists: list[tuple[str, int, str]] = []
    for row in rows:
        watchlist_id = str(row[0])
        symbol_count = int(row[1])
        market_session = str(row[2] or "")
        if _is_market_session_open(market_session, reference_utc=now_utc):
            due_watchlists.append((watchlist_id, symbol_count, market_session))
    return due_watchlists


def _row_to_watchlist_summary(row: Sequence[Any]) -> IntradayWatchlistSummary:
    columns = [
        "watchlistId",
        "name",
        "description",
        "enabled",
        "symbolCount",
        "pollIntervalMinutes",
        "refreshCooldownMinutes",
        "autoRefreshEnabled",
        "marketSession",
        "nextDueAt",
        "lastRunAt",
        "updatedAt",
    ]
    return IntradayWatchlistSummary.model_validate(dict(zip(columns, row)))


def _row_to_watchlist_detail(row: Sequence[Any]) -> IntradayWatchlistDetail:
    columns = [
        "watchlistId",
        "name",
        "description",
        "enabled",
        "symbolCount",
        "pollIntervalMinutes",
        "refreshCooldownMinutes",
        "autoRefreshEnabled",
        "marketSession",
        "nextDueAt",
        "lastRunAt",
        "updatedAt",
        "symbols",
        "createdAt",
    ]
    payload = dict(zip(columns, row))
    payload["symbols"] = list(payload.get("symbols") or [])
    return IntradayWatchlistDetail.model_validate(payload)


def _row_to_monitor_run_summary(row: Sequence[Any]) -> IntradayMonitorRunSummary:
    columns = [
        "runId",
        "watchlistId",
        "watchlistName",
        "triggerKind",
        "status",
        "forceRefresh",
        "symbolCount",
        "observedSymbolCount",
        "eligibleRefreshCount",
        "refreshBatchCount",
        "executionName",
        "dueAt",
        "queuedAt",
        "claimedAt",
        "completedAt",
        "lastError",
    ]
    return IntradayMonitorRunSummary.model_validate(dict(zip(columns, row)))


def _row_to_monitor_event(row: Sequence[Any]) -> IntradayMonitorEvent:
    columns = [
        "eventId",
        "runId",
        "watchlistId",
        "symbol",
        "eventType",
        "severity",
        "message",
        "details",
        "createdAt",
    ]
    payload = dict(zip(columns, row))
    payload["details"] = payload.get("details") or {}
    return IntradayMonitorEvent.model_validate(payload)


def _row_to_symbol_status(row: Sequence[Any]) -> IntradaySymbolStatus:
    columns = [
        "watchlistId",
        "symbol",
        "monitorStatus",
        "lastSnapshotAt",
        "lastObservedPrice",
        "lastSuccessfulMarketRefreshAt",
        "lastRunId",
        "lastError",
        "updatedAt",
    ]
    payload = dict(zip(columns, row))
    payload["monitorStatus"] = payload.get("monitorStatus") or "idle"
    return IntradaySymbolStatus.model_validate(payload)


def _row_to_refresh_batch_summary(row: Sequence[Any]) -> IntradayRefreshBatchSummary:
    columns = [
        "batchId",
        "runId",
        "watchlistId",
        "watchlistName",
        "domain",
        "bucketLetter",
        "status",
        "symbols",
        "symbolCount",
        "executionName",
        "claimedAt",
        "completedAt",
        "createdAt",
        "updatedAt",
        "lastError",
    ]
    payload = dict(zip(columns, row))
    payload["symbols"] = list(payload.get("symbols") or [])
    return IntradayRefreshBatchSummary.model_validate(payload)


def _require_watchlist_exists(conn, watchlist_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {_WATCHLISTS_TABLE} WHERE watchlist_id = %s", (watchlist_id,))
        if cur.fetchone() is None:
            raise LookupError(f"Intraday watchlist '{watchlist_id}' not found.")


def _assert_symbols_exist(conn, symbols: Sequence[str]) -> None:
    normalized = _normalize_symbols(symbols)
    if not normalized:
        raise ValueError("At least one symbol is required.")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol
            FROM {_SYMBOLS_TABLE}
            WHERE symbol = ANY(%s)
            """,
            (normalized,),
        )
        existing = {str(row[0]).upper() for row in cur.fetchall()}
    missing = [symbol for symbol in normalized if symbol not in existing]
    if missing:
        raise ValueError(f"Unknown symbols: {', '.join(missing)}.")


def get_intraday_watchlist(dsn: str, watchlist_id: str) -> IntradayWatchlistDetail | None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    w.watchlist_id,
                    w.name,
                    w.description,
                    w.enabled,
                    COUNT(ws.symbol)::integer AS symbol_count,
                    w.poll_interval_minutes,
                    w.refresh_cooldown_minutes,
                    w.auto_refresh_enabled,
                    w.market_session,
                    w.next_due_at,
                    w.last_run_at,
                    w.updated_at,
                    COALESCE(array_agg(ws.symbol ORDER BY ws.symbol) FILTER (WHERE ws.symbol IS NOT NULL), '{{}}') AS symbols,
                    w.created_at
                FROM {_WATCHLISTS_TABLE} AS w
                LEFT JOIN {_WATCHLIST_SYMBOLS_TABLE} AS ws
                    ON ws.watchlist_id = w.watchlist_id
                WHERE w.watchlist_id = %s
                GROUP BY w.watchlist_id
                """,
                (watchlist_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_watchlist_detail(row)


def list_intraday_watchlists(dsn: str, *, limit: int = 100, offset: int = 0) -> list[IntradayWatchlistSummary]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    w.watchlist_id,
                    w.name,
                    w.description,
                    w.enabled,
                    COUNT(ws.symbol)::integer AS symbol_count,
                    w.poll_interval_minutes,
                    w.refresh_cooldown_minutes,
                    w.auto_refresh_enabled,
                    w.market_session,
                    w.next_due_at,
                    w.last_run_at,
                    w.updated_at
                FROM {_WATCHLISTS_TABLE} AS w
                LEFT JOIN {_WATCHLIST_SYMBOLS_TABLE} AS ws
                    ON ws.watchlist_id = w.watchlist_id
                GROUP BY w.watchlist_id
                ORDER BY lower(w.name), w.watchlist_id
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
    return [_row_to_watchlist_summary(row) for row in rows]


def upsert_intraday_watchlist(
    dsn: str,
    *,
    watchlist_id: str | None,
    payload: IntradayWatchlistUpsertRequest,
) -> IntradayWatchlistDetail:
    symbols = _normalize_symbols(payload.symbols)
    if not symbols:
        raise ValueError("At least one symbol is required.")

    with connect(dsn) as conn:
        _assert_symbols_exist(conn, symbols)

        resolved_watchlist_id = watchlist_id or uuid.uuid4().hex
        with conn.cursor() as cur:
            if watchlist_id is None:
                cur.execute(
                    f"""
                    INSERT INTO {_WATCHLISTS_TABLE} (
                        watchlist_id,
                        name,
                        description,
                        enabled,
                        poll_interval_minutes,
                        refresh_cooldown_minutes,
                        auto_refresh_enabled,
                        market_session,
                        next_due_at,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        CASE WHEN %s THEN now() ELSE NULL END,
                        now(),
                        now()
                    )
                    """,
                    (
                        resolved_watchlist_id,
                        payload.name,
                        payload.description,
                        payload.enabled,
                        payload.pollIntervalMinutes,
                        payload.refreshCooldownMinutes,
                        payload.autoRefreshEnabled,
                        payload.marketSession,
                        payload.enabled,
                    ),
                )
            else:
                cur.execute(
                    f"""
                    UPDATE {_WATCHLISTS_TABLE}
                    SET
                        name = %s,
                        description = %s,
                        enabled = %s,
                        poll_interval_minutes = %s,
                        refresh_cooldown_minutes = %s,
                        auto_refresh_enabled = %s,
                        market_session = %s,
                        next_due_at = CASE
                            WHEN %s = FALSE THEN NULL
                            WHEN next_due_at IS NULL THEN now()
                            ELSE next_due_at
                        END,
                        updated_at = now()
                    WHERE watchlist_id = %s
                    """,
                    (
                        payload.name,
                        payload.description,
                        payload.enabled,
                        payload.pollIntervalMinutes,
                        payload.refreshCooldownMinutes,
                        payload.autoRefreshEnabled,
                        payload.marketSession,
                        payload.enabled,
                        resolved_watchlist_id,
                    ),
                )
                if cur.rowcount == 0:
                    raise LookupError(f"Intraday watchlist '{resolved_watchlist_id}' not found.")

            cur.execute(
                f"DELETE FROM {_WATCHLIST_SYMBOLS_TABLE} WHERE watchlist_id = %s",
                (resolved_watchlist_id,),
            )
            cur.executemany(
                f"""
                INSERT INTO {_WATCHLIST_SYMBOLS_TABLE} (watchlist_id, symbol, created_at)
                VALUES (%s, %s, now())
                """,
                [(resolved_watchlist_id, symbol) for symbol in symbols],
            )
            cur.execute(
                f"""
                DELETE FROM {_SYMBOL_STATUS_TABLE}
                WHERE watchlist_id = %s
                  AND NOT (symbol = ANY(%s))
                """,
                (resolved_watchlist_id, symbols),
            )

    detail = get_intraday_watchlist(dsn, resolved_watchlist_id)
    if detail is None:
        raise LookupError(f"Intraday watchlist '{resolved_watchlist_id}' not found after upsert.")
    return detail


def delete_intraday_watchlist(dsn: str, watchlist_id: str) -> None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {_WATCHLISTS_TABLE} WHERE watchlist_id = %s", (watchlist_id,))
            if cur.rowcount == 0:
                raise LookupError(f"Intraday watchlist '{watchlist_id}' not found.")


def _insert_intraday_watchlist_run(
    conn,
    *,
    watchlist_id: str,
    trigger_kind: str,
    force_refresh: bool,
) -> str:
    run_id = uuid.uuid4().hex
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_MONITOR_RUNS_TABLE} (
                run_id,
                watchlist_id,
                trigger_kind,
                status,
                force_refresh,
                symbol_count,
                due_at,
                queued_at,
                created_at,
                updated_at
            )
            SELECT
                %s,
                %s,
                %s,
                'queued',
                %s,
                COUNT(ws.symbol)::integer,
                now(),
                now(),
                now(),
                now()
            FROM {_WATCHLIST_SYMBOLS_TABLE} AS ws
            WHERE ws.watchlist_id = %s
            """,
            (run_id, watchlist_id, trigger_kind, force_refresh, watchlist_id),
        )
    return run_id


def append_intraday_watchlist_symbols(
    dsn: str,
    *,
    watchlist_id: str,
    payload: IntradayWatchlistSymbolAppendRequest,
    actor: str | None = None,
    request_id: str | None = None,
) -> IntradayWatchlistSymbolAppendResponse:
    symbols = _normalize_symbols(payload.symbols)
    if not symbols:
        raise ValueError("At least one symbol is required.")

    added_symbols: list[str] = []
    already_present_symbols: list[str] = []
    run_id: str | None = None
    run_skipped_reason: str | None = None

    with connect(dsn) as conn:
        _assert_symbols_exist(conn, symbols)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watchlist_id, enabled
                FROM {_WATCHLISTS_TABLE}
                WHERE watchlist_id = %s
                FOR UPDATE
                """,
                (watchlist_id,),
            )
            watchlist_row = cur.fetchone()
            if watchlist_row is None:
                raise LookupError(f"Intraday watchlist '{watchlist_id}' not found.")

            enabled = bool(watchlist_row[1])
            cur.execute(
                f"""
                SELECT symbol
                FROM {_WATCHLIST_SYMBOLS_TABLE}
                WHERE watchlist_id = %s
                ORDER BY symbol
                """,
                (watchlist_id,),
            )
            existing_symbols = [str(row[0]).upper() for row in cur.fetchall()]

        existing_symbol_set = set(existing_symbols)
        for symbol in symbols:
            if symbol in existing_symbol_set:
                already_present_symbols.append(symbol)
            else:
                added_symbols.append(symbol)

        final_symbol_count = len(existing_symbol_set) + len(added_symbols)
        if final_symbol_count > INTRADAY_WATCHLIST_SYMBOLS_MAX:
            raise ValueError(
                "Intraday watchlists are limited to "
                f"{INTRADAY_WATCHLIST_SYMBOLS_MAX} symbols; append would produce {final_symbol_count}."
            )

        with conn.cursor() as cur:
            if added_symbols:
                cur.executemany(
                    f"""
                    INSERT INTO {_WATCHLIST_SYMBOLS_TABLE} (watchlist_id, symbol, created_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (watchlist_id, symbol) DO NOTHING
                    """,
                    [(watchlist_id, symbol) for symbol in added_symbols],
                )
                cur.execute(
                    f"""
                    UPDATE {_WATCHLISTS_TABLE}
                    SET updated_at = now()
                    WHERE watchlist_id = %s
                    """,
                    (watchlist_id,),
                )

        if not added_symbols:
            run_skipped_reason = "no_new_symbols"
        elif not payload.queueRun:
            run_skipped_reason = "queue_run_disabled"
        elif not enabled:
            run_skipped_reason = "watchlist_disabled"
        else:
            run_id = _insert_intraday_watchlist_run(
                conn,
                watchlist_id=watchlist_id,
                trigger_kind="manual",
                force_refresh=False,
            )

        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_WATCHLIST_EVENTS_TABLE} (
                    event_id,
                    watchlist_id,
                    event_type,
                    actor,
                    request_id,
                    reason,
                    symbols_added,
                    symbols_already_present,
                    symbol_count_before,
                    symbol_count_after,
                    event_payload,
                    created_at
                )
                VALUES (%s, %s, 'symbols_appended', %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s::jsonb, now())
                """,
                (
                    uuid.uuid4().hex,
                    watchlist_id,
                    actor,
                    request_id,
                    payload.reason,
                    _json_dumps(added_symbols),
                    _json_dumps(already_present_symbols),
                    len(existing_symbol_set),
                    final_symbol_count,
                    _json_dumps(
                        {
                            "queueRun": payload.queueRun,
                            "queuedRunId": run_id,
                            "runSkippedReason": run_skipped_reason,
                        }
                    ),
                ),
            )

    watchlist = get_intraday_watchlist(dsn, watchlist_id)
    if watchlist is None:
        raise LookupError(f"Intraday watchlist '{watchlist_id}' not found after append.")
    queued_run = get_intraday_monitor_run(dsn, run_id) if run_id else None
    return IntradayWatchlistSymbolAppendResponse(
        watchlist=watchlist,
        addedSymbols=added_symbols,
        alreadyPresentSymbols=already_present_symbols,
        queuedRun=queued_run,
        runSkippedReason=run_skipped_reason,
    )


def enqueue_intraday_watchlist_run(
    dsn: str,
    *,
    watchlist_id: str,
    trigger_kind: str = "manual",
    force_refresh: bool = True,
) -> IntradayMonitorRunSummary:
    with connect(dsn) as conn:
        _require_watchlist_exists(conn, watchlist_id)
        run_id = _insert_intraday_watchlist_run(
            conn,
            watchlist_id=watchlist_id,
            trigger_kind=trigger_kind,
            force_refresh=force_refresh,
        )
    run = get_intraday_monitor_run(dsn, run_id)
    if run is None:
        raise LookupError(f"Intraday monitor run '{run_id}' not found after enqueue.")
    return run


def _enqueue_due_monitor_runs(conn) -> None:
    due_watchlists = _list_currently_due_watchlists(conn)
    if not due_watchlists:
        return

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {_MONITOR_RUNS_TABLE} (
                run_id,
                watchlist_id,
                trigger_kind,
                status,
                force_refresh,
                symbol_count,
                due_at,
                queued_at,
                created_at,
                updated_at
            )
            SELECT %s, %s, 'scheduled', 'queued', FALSE, %s, now(), now(), now(), now()
            WHERE NOT EXISTS (
                SELECT 1
                FROM {_MONITOR_RUNS_TABLE} AS runs
                WHERE runs.watchlist_id = %s
                  AND runs.status IN ('queued', 'claimed')
            )
            """,
            [
                (uuid.uuid4().hex, watchlist_id, symbol_count, watchlist_id)
                for watchlist_id, symbol_count, _market_session in due_watchlists
            ],
        )


def get_intraday_monitor_run(dsn: str, run_id: str) -> IntradayMonitorRunSummary | None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    runs.run_id,
                    runs.watchlist_id,
                    watchlists.name,
                    runs.trigger_kind,
                    runs.status,
                    runs.force_refresh,
                    runs.symbol_count,
                    runs.observed_symbol_count,
                    runs.eligible_refresh_count,
                    runs.refresh_batch_count,
                    runs.execution_name,
                    runs.due_at,
                    runs.queued_at,
                    runs.claimed_at,
                    runs.completed_at,
                    runs.last_error
                FROM {_MONITOR_RUNS_TABLE} AS runs
                JOIN {_WATCHLISTS_TABLE} AS watchlists
                    ON watchlists.watchlist_id = runs.watchlist_id
                WHERE runs.run_id = %s
                """,
                (run_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_monitor_run_summary(row)


def list_intraday_monitor_runs(
    dsn: str,
    *,
    limit: int = 100,
    offset: int = 0,
    watchlist_id: str | None = None,
) -> list[IntradayMonitorRunSummary]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    runs.run_id,
                    runs.watchlist_id,
                    watchlists.name,
                    runs.trigger_kind,
                    runs.status,
                    runs.force_refresh,
                    runs.symbol_count,
                    runs.observed_symbol_count,
                    runs.eligible_refresh_count,
                    runs.refresh_batch_count,
                    runs.execution_name,
                    runs.due_at,
                    runs.queued_at,
                    runs.claimed_at,
                    runs.completed_at,
                    runs.last_error
                FROM {_MONITOR_RUNS_TABLE} AS runs
                JOIN {_WATCHLISTS_TABLE} AS watchlists
                    ON watchlists.watchlist_id = runs.watchlist_id
                WHERE (%s IS NULL OR runs.watchlist_id = %s)
                ORDER BY COALESCE(runs.completed_at, runs.claimed_at, runs.queued_at) DESC, runs.run_id DESC
                LIMIT %s OFFSET %s
                """,
                (watchlist_id, watchlist_id, limit, offset),
            )
            rows = cur.fetchall()
    return [_row_to_monitor_run_summary(row) for row in rows]


def list_intraday_monitor_events(
    dsn: str,
    *,
    limit: int = 100,
    offset: int = 0,
    watchlist_id: str | None = None,
    run_id: str | None = None,
) -> list[IntradayMonitorEvent]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    event_id,
                    run_id,
                    watchlist_id,
                    symbol,
                    event_type,
                    severity,
                    message,
                    details_json,
                    created_at
                FROM {_MONITOR_EVENTS_TABLE}
                WHERE (%s IS NULL OR watchlist_id = %s)
                  AND (%s IS NULL OR run_id = %s)
                ORDER BY created_at DESC, event_id DESC
                LIMIT %s OFFSET %s
                """,
                (watchlist_id, watchlist_id, run_id, run_id, limit, offset),
            )
            rows = cur.fetchall()
    return [_row_to_monitor_event(row) for row in rows]


def list_intraday_refresh_batches(
    dsn: str,
    *,
    limit: int = 100,
    offset: int = 0,
    watchlist_id: str | None = None,
) -> list[IntradayRefreshBatchSummary]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    batches.batch_id,
                    batches.run_id,
                    batches.watchlist_id,
                    watchlists.name,
                    batches.domain,
                    batches.bucket_letter,
                    batches.status,
                    batches.symbols,
                    batches.symbol_count,
                    batches.execution_name,
                    batches.claimed_at,
                    batches.completed_at,
                    batches.created_at,
                    batches.updated_at,
                    batches.last_error
                FROM {_REFRESH_BATCHES_TABLE} AS batches
                JOIN {_WATCHLISTS_TABLE} AS watchlists
                    ON watchlists.watchlist_id = batches.watchlist_id
                WHERE (%s IS NULL OR batches.watchlist_id = %s)
                ORDER BY COALESCE(batches.completed_at, batches.claimed_at, batches.created_at) DESC, batches.batch_id DESC
                LIMIT %s OFFSET %s
                """,
                (watchlist_id, watchlist_id, limit, offset),
            )
            rows = cur.fetchall()
    return [_row_to_refresh_batch_summary(row) for row in rows]


def list_intraday_symbol_status(
    dsn: str,
    *,
    watchlist_id: str | None = None,
    q: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[int, list[IntradaySymbolStatus]]:
    like_query = f"%{str(q or '').strip().upper()}%" if q else None
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {_WATCHLIST_SYMBOLS_TABLE} AS ws
                JOIN {_WATCHLISTS_TABLE} AS watchlists
                    ON watchlists.watchlist_id = ws.watchlist_id
                WHERE (%s IS NULL OR ws.watchlist_id = %s)
                  AND (%s IS NULL OR ws.symbol LIKE %s OR upper(watchlists.name) LIKE %s)
                """,
                (watchlist_id, watchlist_id, like_query, like_query, like_query),
            )
            total = int(cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT
                    ws.watchlist_id,
                    ws.symbol,
                    status.monitor_status,
                    status.last_snapshot_at,
                    status.last_observed_price,
                    status.last_successful_market_refresh_at,
                    status.last_run_id,
                    status.last_error,
                    status.updated_at
                FROM {_WATCHLIST_SYMBOLS_TABLE} AS ws
                JOIN {_WATCHLISTS_TABLE} AS watchlists
                    ON watchlists.watchlist_id = ws.watchlist_id
                LEFT JOIN {_SYMBOL_STATUS_TABLE} AS status
                    ON status.watchlist_id = ws.watchlist_id
                   AND status.symbol = ws.symbol
                WHERE (%s IS NULL OR ws.watchlist_id = %s)
                  AND (%s IS NULL OR ws.symbol LIKE %s OR upper(watchlists.name) LIKE %s)
                ORDER BY COALESCE(status.updated_at, watchlists.updated_at) DESC NULLS LAST, ws.symbol
                LIMIT %s OFFSET %s
                """,
                (watchlist_id, watchlist_id, like_query, like_query, like_query, limit, offset),
            )
            rows = cur.fetchall()
    return total, [_row_to_symbol_status(row) for row in rows]


def claim_next_intraday_monitor_run(
    dsn: str,
    *,
    execution_name: str | None = None,
) -> tuple[IntradayMonitorRunSummary, IntradayWatchlistDetail, str] | None:
    with connect(dsn) as conn:
        _enqueue_due_monitor_runs(conn)
        claim_token = uuid.uuid4().hex
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH next_run AS (
                    SELECT run_id
                    FROM {_MONITOR_RUNS_TABLE}
                    WHERE status = 'queued'
                    ORDER BY force_refresh DESC, queued_at, run_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE {_MONITOR_RUNS_TABLE} AS runs
                SET
                    status = 'claimed',
                    claim_token = %s,
                    execution_name = %s,
                    claimed_at = now(),
                    updated_at = now()
                FROM next_run
                WHERE runs.run_id = next_run.run_id
                RETURNING runs.run_id
                """,
                (claim_token, execution_name),
            )
            row = cur.fetchone()
            if row is None:
                return None
            run_id = str(row[0])

    run = get_intraday_monitor_run(dsn, run_id)
    if run is None:
        raise LookupError(f"Intraday monitor run '{run_id}' disappeared after claim.")
    watchlist = get_intraday_watchlist(dsn, run.watchlistId)
    if watchlist is None:
        raise LookupError(f"Intraday watchlist '{run.watchlistId}' disappeared after run claim.")
    return run, watchlist, claim_token


def _upsert_symbol_statuses(conn, *, run_id: str, watchlist_id: str, symbol_statuses: Sequence[IntradaySymbolStatus]) -> None:
    if not symbol_statuses:
        return

    rows = []
    for item in symbol_statuses:
        rows.append(
            (
                watchlist_id,
                _normalize_symbol(item.symbol),
                item.monitorStatus,
                item.lastSnapshotAt,
                item.lastObservedPrice,
                item.lastSuccessfulMarketRefreshAt,
                run_id,
                item.lastError,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {_SYMBOL_STATUS_TABLE} (
                watchlist_id,
                symbol,
                monitor_status,
                last_snapshot_at,
                last_observed_price,
                last_successful_market_refresh_at,
                last_run_id,
                last_error,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (watchlist_id, symbol) DO UPDATE
            SET
                monitor_status = EXCLUDED.monitor_status,
                last_snapshot_at = EXCLUDED.last_snapshot_at,
                last_observed_price = EXCLUDED.last_observed_price,
                last_successful_market_refresh_at = COALESCE(
                    EXCLUDED.last_successful_market_refresh_at,
                    {_SYMBOL_STATUS_TABLE}.last_successful_market_refresh_at
                ),
                last_run_id = EXCLUDED.last_run_id,
                last_error = EXCLUDED.last_error,
                updated_at = now()
            """,
            rows,
        )


def _insert_monitor_events(conn, *, run_id: str, watchlist_id: str, events: Sequence[IntradayMonitorEvent]) -> None:
    if not events:
        return

    rows = []
    for event in events:
        rows.append(
            (
                event.eventId or uuid.uuid4().hex,
                run_id,
                watchlist_id,
                _normalize_symbol(event.symbol) if event.symbol else None,
                event.eventType,
                event.severity,
                event.message,
                _json_dumps(event.details),
                event.createdAt,
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {_MONITOR_EVENTS_TABLE} (
                event_id,
                run_id,
                watchlist_id,
                symbol,
                event_type,
                severity,
                message,
                details_json,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, COALESCE(%s, now()))
            """,
            rows,
        )


def _enqueue_refresh_batches(conn, *, run_id: str, watchlist_id: str, refresh_symbols: Sequence[str]) -> int:
    normalized = _normalize_symbols(refresh_symbols)
    if not normalized:
        return 0

    bucketed: dict[str, list[str]] = {}
    for symbol in normalized:
        bucketed.setdefault(bucket_letter(symbol), []).append(symbol)

    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {_REFRESH_BATCHES_TABLE} (
                batch_id,
                run_id,
                watchlist_id,
                domain,
                bucket_letter,
                status,
                symbols,
                symbol_count,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, 'market', %s, 'queued', %s::jsonb, %s, now(), now())
            """,
            [
                (
                    uuid.uuid4().hex,
                    run_id,
                    watchlist_id,
                    bucket,
                    _json_dumps(symbols),
                    len(symbols),
                )
                for bucket, symbols in sorted(bucketed.items())
            ],
        )
        cur.executemany(
            f"""
            INSERT INTO {_SYMBOL_STATUS_TABLE} (
                watchlist_id,
                symbol,
                monitor_status,
                last_run_id,
                updated_at
            )
            VALUES (%s, %s, 'refresh_queued', %s, now())
            ON CONFLICT (watchlist_id, symbol) DO UPDATE
            SET
                monitor_status = 'refresh_queued',
                last_run_id = EXCLUDED.last_run_id,
                updated_at = now()
            """,
            [(watchlist_id, symbol, run_id) for symbol in normalized],
        )
    return len(bucketed)


def complete_intraday_monitor_run(
    dsn: str,
    *,
    run_id: str,
    claim_token: str,
    symbol_statuses: Sequence[IntradaySymbolStatus],
    events: Sequence[IntradayMonitorEvent],
    refresh_symbols: Sequence[str],
) -> IntradayMonitorRunSummary:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watchlist_id, trigger_kind, status
                FROM {_MONITOR_RUNS_TABLE}
                WHERE run_id = %s
                  AND claim_token = %s
                FOR UPDATE
                """,
                (run_id, claim_token),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError("Intraday monitor run claim token is invalid.")
            watchlist_id, trigger_kind, status = str(row[0]), str(row[1]), str(row[2])
            if status != "claimed":
                raise LookupError(f"Intraday monitor run '{run_id}' is not currently claimed.")

        _upsert_symbol_statuses(conn, run_id=run_id, watchlist_id=watchlist_id, symbol_statuses=symbol_statuses)
        _insert_monitor_events(conn, run_id=run_id, watchlist_id=watchlist_id, events=events)
        refresh_batch_count = _enqueue_refresh_batches(
            conn,
            run_id=run_id,
            watchlist_id=watchlist_id,
            refresh_symbols=refresh_symbols,
        )

        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {_MONITOR_RUNS_TABLE}
                SET
                    status = 'completed',
                    observed_symbol_count = %s,
                    eligible_refresh_count = %s,
                    refresh_batch_count = %s,
                    completed_at = now(),
                    last_error = NULL,
                    updated_at = now()
                WHERE run_id = %s
                """,
                (len(symbol_statuses), len(_normalize_symbols(refresh_symbols)), refresh_batch_count, run_id),
            )
            if trigger_kind == "scheduled":
                cur.execute(
                    f"""
                    UPDATE {_WATCHLISTS_TABLE}
                    SET
                        last_run_at = now(),
                        next_due_at = now() + make_interval(mins => poll_interval_minutes),
                        updated_at = now()
                    WHERE watchlist_id = %s
                    """,
                    (watchlist_id,),
                )
            else:
                cur.execute(
                    f"""
                    UPDATE {_WATCHLISTS_TABLE}
                    SET
                        last_run_at = now(),
                        next_due_at = CASE
                            WHEN enabled = TRUE AND next_due_at IS NULL THEN now() + make_interval(mins => poll_interval_minutes)
                            ELSE next_due_at
                        END,
                        updated_at = now()
                    WHERE watchlist_id = %s
                    """,
                    (watchlist_id,),
                )

    run = get_intraday_monitor_run(dsn, run_id)
    if run is None:
        raise LookupError(f"Intraday monitor run '{run_id}' not found after completion.")
    return run


def fail_intraday_monitor_run(
    dsn: str,
    *,
    run_id: str,
    claim_token: str,
    error: str,
) -> IntradayMonitorRunSummary:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watchlist_id, trigger_kind, status
                FROM {_MONITOR_RUNS_TABLE}
                WHERE run_id = %s
                  AND claim_token = %s
                FOR UPDATE
                """,
                (run_id, claim_token),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError("Intraday monitor run claim token is invalid.")
            watchlist_id, trigger_kind, status = str(row[0]), str(row[1]), str(row[2])
            if status != "claimed":
                raise LookupError(f"Intraday monitor run '{run_id}' is not currently claimed.")

            cur.execute(
                f"""
                UPDATE {_MONITOR_RUNS_TABLE}
                SET
                    status = 'failed',
                    last_error = %s,
                    completed_at = now(),
                    updated_at = now()
                WHERE run_id = %s
                """,
                (error, run_id),
            )
            cur.execute(
                f"""
                INSERT INTO {_MONITOR_EVENTS_TABLE} (
                    event_id,
                    run_id,
                    watchlist_id,
                    event_type,
                    severity,
                    message,
                    details_json,
                    created_at
                )
                VALUES (%s, %s, %s, 'run_failed', 'error', %s, '{{}}'::jsonb, now())
                """,
                (uuid.uuid4().hex, run_id, watchlist_id, error),
            )
            if trigger_kind == "scheduled":
                cur.execute(
                    f"""
                    UPDATE {_WATCHLISTS_TABLE}
                    SET
                        last_run_at = now(),
                        next_due_at = now() + make_interval(mins => poll_interval_minutes),
                        updated_at = now()
                    WHERE watchlist_id = %s
                    """,
                    (watchlist_id,),
                )
            else:
                cur.execute(
                    f"""
                    UPDATE {_WATCHLISTS_TABLE}
                    SET
                        last_run_at = now(),
                        next_due_at = CASE
                            WHEN enabled = TRUE AND next_due_at IS NULL THEN now() + make_interval(mins => poll_interval_minutes)
                            ELSE next_due_at
                        END,
                        updated_at = now()
                    WHERE watchlist_id = %s
                    """,
                    (watchlist_id,),
                )

    run = get_intraday_monitor_run(dsn, run_id)
    if run is None:
        raise LookupError(f"Intraday monitor run '{run_id}' not found after failure.")
    return run


def claim_next_intraday_refresh_batch(
    dsn: str,
    *,
    execution_name: str | None = None,
) -> tuple[IntradayRefreshBatchSummary, str] | None:
    with connect(dsn) as conn:
        claim_token = uuid.uuid4().hex
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH next_batch AS (
                    SELECT batch_id
                    FROM {_REFRESH_BATCHES_TABLE}
                    WHERE status = 'queued'
                    ORDER BY created_at, batch_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE {_REFRESH_BATCHES_TABLE} AS batches
                SET
                    status = 'claimed',
                    claim_token = %s,
                    execution_name = %s,
                    claimed_at = now(),
                    updated_at = now()
                FROM next_batch
                WHERE batches.batch_id = next_batch.batch_id
                RETURNING batches.batch_id
                """,
                (claim_token, execution_name),
            )
            row = cur.fetchone()
            if row is None:
                return None
            batch_id = str(row[0])

    batch = get_intraday_refresh_batch(dsn, batch_id)
    if batch is None:
        raise LookupError(f"Intraday refresh batch '{batch_id}' disappeared after claim.")
    return batch, claim_token


def get_intraday_refresh_batch(dsn: str, batch_id: str) -> IntradayRefreshBatchSummary | None:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    batches.batch_id,
                    batches.run_id,
                    batches.watchlist_id,
                    watchlists.name,
                    batches.domain,
                    batches.bucket_letter,
                    batches.status,
                    batches.symbols,
                    batches.symbol_count,
                    batches.execution_name,
                    batches.claimed_at,
                    batches.completed_at,
                    batches.created_at,
                    batches.updated_at,
                    batches.last_error
                FROM {_REFRESH_BATCHES_TABLE} AS batches
                JOIN {_WATCHLISTS_TABLE} AS watchlists
                    ON watchlists.watchlist_id = batches.watchlist_id
                WHERE batches.batch_id = %s
                """,
                (batch_id,),
            )
            row = cur.fetchone()
    if row is None:
        return None
    return _row_to_refresh_batch_summary(row)


def complete_intraday_refresh_batch(
    dsn: str,
    *,
    batch_id: str,
    claim_token: str,
) -> IntradayRefreshBatchSummary:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watchlist_id, symbols, status
                FROM {_REFRESH_BATCHES_TABLE}
                WHERE batch_id = %s
                  AND claim_token = %s
                FOR UPDATE
                """,
                (batch_id, claim_token),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError("Intraday refresh batch claim token is invalid.")
            watchlist_id = str(row[0])
            symbols = list(row[1] or [])
            status = str(row[2])
            if status != "claimed":
                raise LookupError(f"Intraday refresh batch '{batch_id}' is not currently claimed.")

            cur.execute(
                f"""
                UPDATE {_REFRESH_BATCHES_TABLE}
                SET
                    status = 'completed',
                    completed_at = now(),
                    last_error = NULL,
                    updated_at = now()
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            cur.executemany(
                f"""
                INSERT INTO {_SYMBOL_STATUS_TABLE} (
                    watchlist_id,
                    symbol,
                    monitor_status,
                    last_successful_market_refresh_at,
                    updated_at,
                    last_error
                )
                VALUES (%s, %s, 'refreshed', now(), now(), NULL)
                ON CONFLICT (watchlist_id, symbol) DO UPDATE
                SET
                    monitor_status = 'refreshed',
                    last_successful_market_refresh_at = now(),
                    last_error = NULL,
                    updated_at = now()
                """,
                [(watchlist_id, _normalize_symbol(symbol)) for symbol in symbols],
            )

    batch = get_intraday_refresh_batch(dsn, batch_id)
    if batch is None:
        raise LookupError(f"Intraday refresh batch '{batch_id}' not found after completion.")
    return batch


def fail_intraday_refresh_batch(
    dsn: str,
    *,
    batch_id: str,
    claim_token: str,
    error: str,
) -> IntradayRefreshBatchSummary:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT watchlist_id, symbols, status
                FROM {_REFRESH_BATCHES_TABLE}
                WHERE batch_id = %s
                  AND claim_token = %s
                FOR UPDATE
                """,
                (batch_id, claim_token),
            )
            row = cur.fetchone()
            if row is None:
                raise LookupError("Intraday refresh batch claim token is invalid.")
            watchlist_id = str(row[0])
            symbols = list(row[1] or [])
            status = str(row[2])
            if status != "claimed":
                raise LookupError(f"Intraday refresh batch '{batch_id}' is not currently claimed.")

            cur.execute(
                f"""
                UPDATE {_REFRESH_BATCHES_TABLE}
                SET
                    status = 'failed',
                    completed_at = now(),
                    last_error = %s,
                    updated_at = now()
                WHERE batch_id = %s
                """,
                (error, batch_id),
            )
            cur.executemany(
                f"""
                INSERT INTO {_SYMBOL_STATUS_TABLE} (
                    watchlist_id,
                    symbol,
                    monitor_status,
                    last_error,
                    updated_at
                )
                VALUES (%s, %s, 'failed', %s, now())
                ON CONFLICT (watchlist_id, symbol) DO UPDATE
                SET
                    monitor_status = 'failed',
                    last_error = %s,
                    updated_at = now()
                """,
                [(watchlist_id, _normalize_symbol(symbol), error, error) for symbol in symbols],
            )

    batch = get_intraday_refresh_batch(dsn, batch_id)
    if batch is None:
        raise LookupError(f"Intraday refresh batch '{batch_id}' not found after failure.")
    return batch


def get_intraday_health_summary(dsn: str) -> dict[str, Any]:
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*), COUNT(*) FILTER (WHERE enabled) FROM {_WATCHLISTS_TABLE}")
            watchlist_count, enabled_watchlist_count = cur.fetchone()
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {_MONITOR_RUNS_TABLE} AS runs
                WHERE runs.status = 'queued'
                  AND runs.due_at <= now()
                """
            )
            queued_backlog_count = int(cur.fetchone()[0])
            latent_due_count = len(_list_currently_due_watchlists(conn))
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {_MONITOR_RUNS_TABLE}
                WHERE status = 'failed'
                  AND completed_at >= now() - interval '1 day'
                """
            )
            failed_run_count = int(cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {_WATCHLISTS_TABLE} AS watchlists
                JOIN {_WATCHLIST_SYMBOLS_TABLE} AS symbols
                  ON symbols.watchlist_id = watchlists.watchlist_id
                LEFT JOIN {_SYMBOL_STATUS_TABLE} AS status
                  ON status.watchlist_id = symbols.watchlist_id
                 AND status.symbol = symbols.symbol
                WHERE watchlists.auto_refresh_enabled = TRUE
                  AND (
                      status.last_snapshot_at IS NOT NULL
                      AND (
                          status.last_successful_market_refresh_at IS NULL
                          OR status.last_successful_market_refresh_at
                             < now() - make_interval(mins => watchlists.refresh_cooldown_minutes)
                      )
                  )
                """
            )
            stale_symbol_count = int(cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at)))
                FROM {_REFRESH_BATCHES_TABLE}
                WHERE status IN ('queued', 'claimed')
                """
            )
            oldest_refresh_age_seconds = cur.fetchone()[0]

    latest_run = next(iter(list_intraday_monitor_runs(dsn, limit=1)), None)
    latest_batch = next(iter(list_intraday_refresh_batches(dsn, limit=1)), None)
    return {
        "watchlistCount": int(watchlist_count or 0),
        "enabledWatchlistCount": int(enabled_watchlist_count or 0),
        "dueRunBacklogCount": queued_backlog_count + latent_due_count,
        "failedRunCount": failed_run_count,
        "staleSymbolCount": stale_symbol_count,
        "refreshBatchBacklogAgeSeconds": float(oldest_refresh_age_seconds or 0.0),
        "latestMonitorRun": latest_run.model_dump(mode="json") if latest_run is not None else None,
        "latestRefreshBatch": latest_batch.model_dump(mode="json") if latest_batch is not None else None,
    }
