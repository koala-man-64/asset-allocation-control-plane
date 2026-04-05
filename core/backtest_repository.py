from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from core.postgres import connect

logger = logging.getLogger(__name__)


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
        submitted_by: str | None = None,
        output_dir: str | None = None,
        adls_container: str | None = None,
        adls_prefix: str | None = None,
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
                        output_dir,
                        adls_container,
                        adls_prefix,
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
                        submitted_by
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        run_id,
                        status,
                        run_name,
                        start_ts.date().isoformat() if start_ts else None,
                        end_ts.date().isoformat() if end_ts else None,
                        output_dir,
                        adls_container,
                        adls_prefix,
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
                        submitted_by,
                    ),
                )
        return self.get_run(run_id) or {"run_id": run_id, "status": status}

    def get_run(self, run_id: str) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id,
                        status,
                        submitted_at,
                        started_at,
                        completed_at,
                        run_name,
                        start_date,
                        end_date,
                        output_dir,
                        adls_container,
                        adls_prefix,
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
                        summary_json,
                        artifact_manifest_path,
                        submitted_by
                    FROM core.runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
        columns = [
            "run_id",
            "status",
            "submitted_at",
            "started_at",
            "completed_at",
            "run_name",
            "start_date",
            "end_date",
            "output_dir",
            "adls_container",
            "adls_prefix",
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
            "summary_json",
            "artifact_manifest_path",
            "submitted_by",
        ]
        payload = dict(zip(columns, row))
        payload["config"] = _parse_json(payload.pop("config_json", None))
        payload["effective_config"] = _parse_json(payload.pop("effective_config_json", None))
        if payload.get("summary_json") is None:
            payload["summary_json"] = {}
        return payload

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
                        output_dir,
                        adls_container,
                        adls_prefix,
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
            "output_dir",
            "adls_container",
            "adls_prefix",
            "error",
            "strategy_name",
            "strategy_version",
            "bar_size",
            "execution_name",
        ]
        return [dict(zip(columns, row)) for row in rows]

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
        artifact_manifest_path: str | None = None,
    ) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.runs
                    SET
                        status = 'completed',
                        completed_at = NOW(),
                        heartbeat_at = NOW(),
                        summary_json = %s,
                        artifact_manifest_path = COALESCE(%s, artifact_manifest_path),
                        error = NULL
                    WHERE run_id = %s
                    """,
                    (json.dumps(summary or {}), artifact_manifest_path, run_id),
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

    def attach_artifact_manifest(self, run_id: str, artifact_manifest_path: str) -> None:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE core.runs
                    SET artifact_manifest_path = %s, heartbeat_at = NOW()
                    WHERE run_id = %s
                    """,
                    (artifact_manifest_path, run_id),
                )

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
