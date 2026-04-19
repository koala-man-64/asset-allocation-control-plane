from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from asset_allocation_runtime_common.foundation.postgres import connect
logger = logging.getLogger(__name__)

_RUN_COLUMNS = [
    "run_id",
    "status",
    "submitted_at",
    "started_at",
    "completed_at",
    "run_name",
    "start_date",
    "end_date",
    "error",
    "config_json",
    "effective_config_json",
    "strategy_name",
    "strategy_version",
    "ranking_schema_name",
    "ranking_schema_version",
    "universe_name",
    "universe_version",
    "regime_model_name",
    "regime_model_version",
    "start_ts",
    "end_ts",
    "bar_size",
    "execution_name",
    "heartbeat_at",
    "attempt_count",
    "results_ready_at",
    "results_schema_version",
    "canonical_target_id",
    "canonical_fingerprint",
    "config_fingerprint",
    "request_fingerprint",
    "submitted_by",
]
_RUN_SELECT_SQL = """
    SELECT
        run_id,
        status,
        submitted_at,
        started_at,
        completed_at,
        run_name,
        start_date,
        end_date,
        error,
        config_json,
        effective_config_json,
        strategy_name,
        strategy_version,
        ranking_schema_name,
        ranking_schema_version,
        universe_name,
        universe_version,
        regime_model_name,
        regime_model_version,
        start_ts,
        end_ts,
        bar_size,
        execution_name,
        heartbeat_at,
        attempt_count,
        results_ready_at,
        results_schema_version,
        canonical_target_id,
        canonical_fingerprint,
        config_fingerprint,
        request_fingerprint,
        submitted_by
"""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, separators=(",", ":"))


def _parse_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _hydrate_run_payload(row: tuple[Any, ...] | list[Any]) -> dict[str, Any]:
    payload = dict(zip(_RUN_COLUMNS, row))
    payload["config"] = _parse_json(payload.pop("config_json", None))
    payload["effective_config"] = _parse_json(payload.pop("effective_config_json", None))
    return payload


def _serialize_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _map_summary_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "run_id": row[0],
        "run_name": row[1],
        "start_date": row[2],
        "end_date": row[3],
        "total_return": row[4],
        "annualized_return": row[5],
        "annualized_volatility": row[6],
        "sharpe_ratio": row[7],
        "max_drawdown": row[8],
        "trades": row[9],
        "initial_cash": row[10],
        "final_equity": row[11],
        "gross_total_return": row[12],
        "gross_annualized_return": row[13],
        "total_commission": row[14],
        "total_slippage_cost": row[15],
        "total_transaction_cost": row[16],
        "cost_drag_bps": row[17],
        "avg_gross_exposure": row[18],
        "avg_net_exposure": row[19],
        "sortino_ratio": row[20],
        "calmar_ratio": row[21],
        "closed_positions": row[22],
        "winning_positions": row[23],
        "losing_positions": row[24],
        "hit_rate": row[25],
        "avg_win_pnl": row[26],
        "avg_loss_pnl": row[27],
        "avg_win_return": row[28],
        "avg_loss_return": row[29],
        "payoff_ratio": row[30],
        "profit_factor": row[31],
        "expectancy_pnl": row[32],
        "expectancy_return": row[33],
    }


def _map_timeseries_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "date": _serialize_timestamp(row[0]),
        "portfolio_value": row[1],
        "drawdown": row[2],
        "daily_return": row[3],
        "period_return": row[4],
        "cumulative_return": row[5],
        "cash": row[6],
        "gross_exposure": row[7],
        "net_exposure": row[8],
        "turnover": row[9],
        "commission": row[10],
        "slippage_cost": row[11],
        "trade_count": row[12],
    }


def _map_rolling_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "date": _serialize_timestamp(row[0]),
        "window_days": row[1],
        "window_periods": row[2],
        "rolling_return": row[3],
        "rolling_volatility": row[4],
        "rolling_sharpe": row[5],
        "rolling_max_drawdown": row[6],
        "turnover_sum": row[7],
        "commission_sum": row[8],
        "slippage_cost_sum": row[9],
        "n_trades_sum": row[10],
        "gross_exposure_avg": row[11],
        "net_exposure_avg": row[12],
    }


def _map_trade_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "execution_date": _serialize_timestamp(row[0]),
        "symbol": row[1],
        "quantity": row[2],
        "price": row[3],
        "notional": row[4],
        "commission": row[5],
        "slippage_cost": row[6],
        "cash_after": row[7],
        "position_id": row[8],
        "trade_role": row[9],
    }


def _map_closed_position_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "position_id": row[0],
        "symbol": row[1],
        "opened_at": _serialize_timestamp(row[2]),
        "closed_at": _serialize_timestamp(row[3]),
        "holding_period_bars": row[4],
        "average_cost": row[5],
        "exit_price": row[6],
        "max_quantity": row[7],
        "resize_count": row[8],
        "realized_pnl": row[9],
        "realized_return": row[10],
        "total_commission": row[11],
        "total_slippage_cost": row[12],
        "total_transaction_cost": row[13],
        "exit_reason": row[14],
        "exit_rule_id": row[15],
    }


class BacktestResultsNotReadyError(RuntimeError):
    """Raised when a run exists but its Postgres result set is not fully published."""


class BacktestRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. BacktestRepository will not function.")

    def _require_dsn(self) -> str:
        if not self.dsn:
            raise ValueError("Database connection not configured")
        return self.dsn

    def create_run(
        self,
        *,
        config: dict[str, Any],
        effective_config: dict[str, Any],
        status: str = "queued",
        run_name: str | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
        bar_size: str | None = None,
        strategy_name: str | None = None,
        strategy_version: int | None = None,
        ranking_schema_name: str | None = None,
        ranking_schema_version: int | None = None,
        universe_name: str | None = None,
        universe_version: int | None = None,
        regime_model_name: str | None = None,
        regime_model_version: int | None = None,
        canonical_target_id: str | None = None,
        canonical_fingerprint: str | None = None,
        config_fingerprint: str | None = None,
        request_fingerprint: str | None = None,
        submitted_by: str | None = None,
    ) -> dict[str, Any]:
        dsn = self._require_dsn()
        run_id = uuid.uuid4().hex
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.runs (
                        run_id,
                        status,
                        run_name,
                        start_date,
                        end_date,
                        config_json,
                        effective_config_json,
                        strategy_name,
                        strategy_version,
                        ranking_schema_name,
                        ranking_schema_version,
                        universe_name,
                        universe_version,
                        regime_model_name,
                        regime_model_version,
                        start_ts,
                        end_ts,
                        bar_size,
                        canonical_target_id,
                        canonical_fingerprint,
                        config_fingerprint,
                        request_fingerprint,
                        submitted_by
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        run_id,
                        status,
                        run_name,
                        start_ts.date().isoformat() if start_ts else None,
                        end_ts.date().isoformat() if end_ts else None,
                        _serialize_json(config),
                        _serialize_json(effective_config),
                        strategy_name,
                        strategy_version,
                        ranking_schema_name,
                        ranking_schema_version,
                        universe_name,
                        universe_version,
                        regime_model_name,
                        regime_model_version,
                        start_ts,
                        end_ts,
                        bar_size,
                        canonical_target_id,
                        canonical_fingerprint,
                        config_fingerprint,
                        request_fingerprint,
                        submitted_by,
                    ),
                )
        return self.get_run(run_id) or {"run_id": run_id, "status": status}

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_RUN_SELECT_SQL}
                    FROM core.runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
        return _hydrate_run_payload(row)

    def list_runs(
        self,
        *,
        status: str | None = None,
        query: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        predicates: list[str] = []
        params: list[Any] = []
        if status:
            predicates.append("status = %s")
            params.append(status)
        if query:
            predicates.append("(run_id ILIKE %s OR COALESCE(run_name, '') ILIKE %s OR COALESCE(strategy_name, '') ILIKE %s)")
            like = f"%{query.strip()}%"
            params.extend([like, like, like])
        where_sql = f"WHERE {' AND '.join(predicates)}" if predicates else ""
        params.extend([max(1, min(int(limit), 500)), max(0, int(offset))])
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        run_id,
                        status,
                        submitted_at,
                        started_at,
                        completed_at,
                        run_name,
                        start_date,
                        end_date,
                        error,
                        strategy_name,
                        strategy_version,
                        bar_size,
                        execution_name
                    FROM core.runs
                    {where_sql}
                    ORDER BY submitted_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    params,
                )
                rows = cur.fetchall()
        columns = [
            "run_id",
            "status",
            "submitted_at",
            "started_at",
            "completed_at",
            "run_name",
            "start_date",
            "end_date",
            "error",
            "strategy_name",
            "strategy_version",
            "bar_size",
            "execution_name",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def list_queued_runs_without_execution(self, *, older_than_seconds: int, limit: int) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_RUN_SELECT_SQL}
                    FROM core.runs
                    WHERE status = 'queued'
                      AND submitted_at <= (NOW() - (%s * INTERVAL '1 second'))
                      AND COALESCE(NULLIF(BTRIM(execution_name), ''), NULL) IS NULL
                    ORDER BY submitted_at ASC
                    LIMIT %s
                    """,
                    (max(0, int(older_than_seconds)), max(1, int(limit))),
                )
                rows = cur.fetchall()
        return [_hydrate_run_payload(row) for row in rows]

    def list_queued_runs_with_execution(self, *, older_than_seconds: int, limit: int) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_RUN_SELECT_SQL}
                    FROM core.runs
                    WHERE status = 'queued'
                      AND submitted_at <= (NOW() - (%s * INTERVAL '1 second'))
                      AND COALESCE(NULLIF(BTRIM(execution_name), ''), NULL) IS NOT NULL
                    ORDER BY submitted_at ASC
                    LIMIT %s
                    """,
                    (max(0, int(older_than_seconds)), max(1, int(limit))),
                )
                rows = cur.fetchall()
        return [_hydrate_run_payload(row) for row in rows]

    def list_stale_running_runs(self, *, heartbeat_timeout_seconds: int, limit: int) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_RUN_SELECT_SQL}
                    FROM core.runs
                    WHERE status = 'running'
                      AND COALESCE(heartbeat_at, started_at, submitted_at) <= (NOW() - (%s * INTERVAL '1 second'))
                    ORDER BY COALESCE(heartbeat_at, started_at, submitted_at) ASC
                    LIMIT %s
                    """,
                    (max(0, int(heartbeat_timeout_seconds)), max(1, int(limit))),
                )
                rows = cur.fetchall()
        return [_hydrate_run_payload(row) for row in rows]

    def find_latest_canonical_run(self, *, target_id: str, fingerprint: str) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_RUN_SELECT_SQL}
                    FROM core.runs
                    WHERE canonical_target_id = %s
                      AND canonical_fingerprint = %s
                    ORDER BY submitted_at DESC
                    LIMIT 1
                    """,
                    (target_id, fingerprint),
                )
                row = cur.fetchone()
                if not row:
                    return None
        return _hydrate_run_payload(row)

    def find_latest_completed_request_run(self, *, request_fingerprint: str) -> Optional[dict[str, Any]]:
        return self._find_latest_request_run(
            request_fingerprint=request_fingerprint,
            where_sql="status = 'completed' AND results_ready_at IS NOT NULL",
        )

    def find_latest_inflight_request_run(self, *, request_fingerprint: str) -> Optional[dict[str, Any]]:
        return self._find_latest_request_run(
            request_fingerprint=request_fingerprint,
            where_sql="status IN ('queued', 'running')",
        )

    def find_latest_failed_request_run(self, *, request_fingerprint: str) -> Optional[dict[str, Any]]:
        return self._find_latest_request_run(
            request_fingerprint=request_fingerprint,
            where_sql="status = 'failed'",
        )

    def _find_latest_request_run(
        self,
        *,
        request_fingerprint: str,
        where_sql: str,
    ) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    {_RUN_SELECT_SQL}
                    FROM core.runs
                    WHERE request_fingerprint = %s
                      AND {where_sql}
                    ORDER BY submitted_at DESC
                    LIMIT 1
                    """,
                    (request_fingerprint,),
                )
                row = cur.fetchone()
                if not row:
                    return None
        return _hydrate_run_payload(row)

    def get_operational_summary(
        self,
        *,
        queue_dispatch_grace_seconds: int,
        heartbeat_timeout_seconds: int,
        duration_window_hours: int = 24,
    ) -> dict[str, Any]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH queued AS (
                        SELECT
                            COUNT(*)::int AS queued_count,
                            COALESCE(
                                MAX(EXTRACT(EPOCH FROM (NOW() - submitted_at))),
                                0
                            )::double precision AS oldest_queued_age_seconds,
                            COUNT(*) FILTER (
                                WHERE COALESCE(NULLIF(BTRIM(execution_name), ''), NULL) IS NULL
                                  AND submitted_at <= (NOW() - (%s * INTERVAL '1 second'))
                            )::int AS dispatch_failure_count
                        FROM core.runs
                        WHERE status = 'queued'
                    ),
                    running AS (
                        SELECT
                            COUNT(*)::int AS running_count,
                            COUNT(*) FILTER (
                                WHERE COALESCE(heartbeat_at, started_at, submitted_at) <= (NOW() - (%s * INTERVAL '1 second'))
                            )::int AS stale_heartbeat_count
                        FROM core.runs
                        WHERE status = 'running'
                    ),
                    durations AS (
                        SELECT
                            EXTRACT(EPOCH FROM (completed_at - started_at))::double precision AS duration_seconds
                        FROM core.runs
                        WHERE status = 'completed'
                          AND started_at IS NOT NULL
                          AND completed_at IS NOT NULL
                          AND completed_at >= (NOW() - (%s * INTERVAL '1 hour'))
                    )
                    SELECT
                        queued.queued_count,
                        queued.oldest_queued_age_seconds,
                        queued.dispatch_failure_count,
                        running.running_count,
                        running.stale_heartbeat_count,
                        COALESCE(
                            (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_seconds) FROM durations),
                            0
                        )::double precision AS duration_p95_seconds
                    FROM queued
                    CROSS JOIN running
                    """,
                    (
                        max(0, int(queue_dispatch_grace_seconds)),
                        max(0, int(heartbeat_timeout_seconds)),
                        max(1, int(duration_window_hours)),
                    ),
                )
                row = cur.fetchone()
        if not row:
            return {
                "queuedCount": 0,
                "oldestQueuedAgeSeconds": 0.0,
                "dispatchFailureCount": 0,
                "runningCount": 0,
                "staleHeartbeatCount": 0,
                "durationP95Seconds": 0.0,
            }
        return {
            "queuedCount": int(row[0] or 0),
            "oldestQueuedAgeSeconds": float(row[1] or 0.0),
            "dispatchFailureCount": int(row[2] or 0),
            "runningCount": int(row[3] or 0),
            "staleHeartbeatCount": int(row[4] or 0),
            "durationP95Seconds": float(row[5] or 0.0),
        }

    def claim_next_run(self, *, execution_name: str | None = None) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id
                    FROM core.runs
                    WHERE status = 'queued'
                    ORDER BY submitted_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if not row:
                    return None
                run_id = str(row[0])
                cur.execute(
                    """
                    UPDATE core.runs
                    SET
                        status = 'running',
                        started_at = COALESCE(started_at, NOW()),
                        heartbeat_at = NOW(),
                        execution_name = COALESCE(%s, execution_name),
                        attempt_count = COALESCE(attempt_count, 0) + 1
                    WHERE run_id = %s
                    """,
                    (execution_name, run_id),
                )
        return self.get_run(run_id)

    def update_heartbeat(self, run_id: str) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE core.runs SET heartbeat_at = NOW() WHERE run_id = %s",
                    (run_id,),
                )

    def start_run(self, run_id: str, *, execution_name: str | None = None) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.runs
                    SET
                        status = 'running',
                        started_at = COALESCE(started_at, NOW()),
                        heartbeat_at = NOW(),
                        execution_name = COALESCE(%s, execution_name),
                        attempt_count = COALESCE(attempt_count, 0) + 1
                    WHERE run_id = %s
                    """,
                    (execution_name, run_id),
                )

    def complete_run(
        self,
        run_id: str,
        *,
        summary: dict[str, Any] | None = None,
    ) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                # The completion payload still accepts summary for backward-compatible
                # worker signaling, but typed Postgres tables are the canonical store.
                _ = summary
                cur.execute(
                    """
                    SELECT canonical_target_id, canonical_fingerprint
                    FROM core.runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                metadata_row = cur.fetchone()
                if not metadata_row:
                    raise LookupError(f"Backtest run '{run_id}' not found.")
                canonical_target_id = str(metadata_row[0] or "").strip() or None
                canonical_fingerprint = str(metadata_row[1] or "").strip() or None
                cur.execute(
                    """
                    UPDATE core.runs
                    SET
                        status = 'completed',
                        completed_at = NOW(),
                        heartbeat_at = NOW(),
                        error = NULL
                    WHERE run_id = %s
                      AND results_ready_at IS NOT NULL
                    RETURNING run_id
                    """,
                    (run_id,),
                )
                if not cur.fetchone():
                    raise BacktestResultsNotReadyError(
                        f"Backtest run '{run_id}' cannot complete before Postgres results are published."
                    )
                if canonical_target_id and canonical_fingerprint:
                    cur.execute(
                        """
                        UPDATE core.canonical_backtest_targets
                        SET
                            last_applied_fingerprint = %s,
                            last_enqueued_fingerprint = %s,
                            last_run_id = %s,
                            last_completed_at = NOW(),
                            updated_at = NOW()
                        WHERE target_id = %s
                        """,
                        (canonical_fingerprint, canonical_fingerprint, run_id, canonical_target_id),
                    )

    def fail_run(self, run_id: str, *, error: str) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.runs
                    SET
                        status = 'failed',
                        completed_at = NOW(),
                        heartbeat_at = NOW(),
                        error = %s
                    WHERE run_id = %s
                    """,
                    (error[:4000], run_id),
                )

    def get_summary(self, run_id: str) -> dict[str, Any] | None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        r.run_id,
                        r.run_name,
                        r.start_date,
                        r.end_date,
                        s.total_return,
                        s.annualized_return,
                        s.annualized_volatility,
                        s.sharpe_ratio,
                        s.max_drawdown,
                        s.trades,
                        s.initial_cash,
                        s.final_equity,
                        s.gross_total_return,
                        s.gross_annualized_return,
                        s.total_commission,
                        s.total_slippage_cost,
                        s.total_transaction_cost,
                        s.cost_drag_bps,
                        s.avg_gross_exposure,
                        s.avg_net_exposure,
                        s.sortino_ratio,
                        s.calmar_ratio,
                        s.closed_positions,
                        s.winning_positions,
                        s.losing_positions,
                        s.hit_rate,
                        s.avg_win_pnl,
                        s.avg_loss_pnl,
                        s.avg_win_return,
                        s.avg_loss_return,
                        s.payoff_ratio,
                        s.profit_factor,
                        s.expectancy_pnl,
                        s.expectancy_return
                    FROM core.runs AS r
                    LEFT JOIN core.backtest_run_summary AS s
                      ON s.run_id = r.run_id
                    WHERE r.run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return _map_summary_row(row)

    def list_timeseries(
        self,
        run_id: str,
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        sql = """
            SELECT
                bar_ts,
                portfolio_value,
                drawdown,
                daily_return,
                period_return,
                cumulative_return,
                cash,
                gross_exposure,
                net_exposure,
                turnover,
                commission,
                slippage_cost,
                trade_count
            FROM core.backtest_timeseries
            WHERE run_id = %s
            ORDER BY bar_ts ASC
        """
        params: list[Any] = [run_id]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([max(1, int(limit)), max(0, int(offset))])
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [_map_timeseries_row(row) for row in rows]

    def count_timeseries(self, run_id: str) -> int:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM core.backtest_timeseries WHERE run_id = %s", (run_id,))
                row = cur.fetchone()
        return int((row or (0,))[0] or 0)

    def list_rolling_metrics(
        self,
        run_id: str,
        *,
        window_days: int,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        sql = """
            SELECT
                bar_ts,
                window_days,
                window_periods,
                rolling_return,
                rolling_volatility,
                rolling_sharpe,
                rolling_max_drawdown,
                turnover_sum,
                commission_sum,
                slippage_cost_sum,
                n_trades_sum,
                gross_exposure_avg,
                net_exposure_avg
            FROM core.backtest_rolling_metrics
            WHERE run_id = %s
              AND COALESCE(window_periods, window_days) = %s
            ORDER BY bar_ts ASC
        """
        params: list[Any] = [run_id, int(window_days)]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([max(1, int(limit)), max(0, int(offset))])
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        return [_map_rolling_row(row) for row in rows]

    def count_rolling_metrics(self, run_id: str, *, window_days: int) -> int:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM core.backtest_rolling_metrics
                    WHERE run_id = %s
                      AND COALESCE(window_periods, window_days) = %s
                    """,
                    (run_id, int(window_days)),
                )
                row = cur.fetchone()
        return int((row or (0,))[0] or 0)

    def list_trades(self, run_id: str, *, limit: int, offset: int) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        execution_ts,
                        symbol,
                        quantity,
                        price,
                        notional,
                        commission,
                        slippage_cost,
                        cash_after,
                        position_id,
                        trade_role
                    FROM core.backtest_trades
                    WHERE run_id = %s
                    ORDER BY trade_seq ASC
                    LIMIT %s OFFSET %s
                    """,
                    (run_id, max(1, int(limit)), max(0, int(offset))),
                )
                rows = cur.fetchall()
        return [_map_trade_row(row) for row in rows]

    def count_trades(self, run_id: str) -> int:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM core.backtest_trades WHERE run_id = %s", (run_id,))
                row = cur.fetchone()
        return int((row or (0,))[0] or 0)

    def list_closed_positions(self, run_id: str, *, limit: int, offset: int) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        position_id,
                        symbol,
                        opened_at,
                        closed_at,
                        holding_period_bars,
                        average_cost,
                        exit_price,
                        max_quantity,
                        resize_count,
                        realized_pnl,
                        realized_return,
                        total_commission,
                        total_slippage_cost,
                        total_transaction_cost,
                        exit_reason,
                        exit_rule_id
                    FROM core.backtest_closed_positions
                    WHERE run_id = %s
                    ORDER BY closed_at ASC, position_id ASC
                    LIMIT %s OFFSET %s
                    """,
                    (run_id, max(1, int(limit)), max(0, int(offset))),
                )
                rows = cur.fetchall()
        return [_map_closed_position_row(row) for row in rows]

    def count_closed_positions(self, run_id: str) -> int:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM core.backtest_closed_positions WHERE run_id = %s", (run_id,))
                row = cur.fetchone()
        return int((row or (0,))[0] or 0)

    def set_execution_name(self, run_id: str, execution_name: str) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.runs
                    SET execution_name = %s
                    WHERE run_id = %s
                    """,
                    (execution_name, run_id),
                )
