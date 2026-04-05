from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import date
from typing import Any, Optional

from core.postgres import connect

logger = logging.getLogger(__name__)
REGIME_MODELS_TABLE = "core.regime_models"


def _stable_config_hash(config: dict[str, Any]) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


class RegimeRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. RegimeRepository will not function.")

    def _require_dsn(self) -> str:
        if not self.dsn:
            raise ValueError("Database connection not configured")
        return self.dsn

    def get_regime_model(self, name: str) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_activation AS (
                        SELECT DISTINCT ON (model_name)
                            model_name,
                            model_version,
                            activated_at,
                            activated_by
                        FROM core.regime_model_activations
                        WHERE model_name = %s
                        ORDER BY model_name, activated_at DESC, activation_id DESC
                    )
                    SELECT
                        m.name,
                        m.description,
                        m.version,
                        m.updated_at,
                        m.config,
                        a.model_version,
                        a.activated_at,
                        a.activated_by
                    FROM core.regime_models AS m
                    LEFT JOIN latest_activation AS a
                      ON a.model_name = m.name
                    WHERE m.name = %s
                    """,
                    (name, name),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [
                    "name",
                    "description",
                    "version",
                    "updated_at",
                    "config",
                    "active_version",
                    "activated_at",
                    "activated_by",
                ]
                return dict(zip(columns, row))

    def list_regime_models(self) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_activation AS (
                        SELECT DISTINCT ON (model_name)
                            model_name,
                            model_version,
                            activated_at,
                            activated_by
                        FROM core.regime_model_activations
                        ORDER BY model_name, activated_at DESC, activation_id DESC
                    )
                    SELECT
                        m.name,
                        m.description,
                        m.version,
                        m.updated_at,
                        a.model_version,
                        a.activated_at,
                        a.activated_by
                    FROM core.regime_models AS m
                    LEFT JOIN latest_activation AS a
                      ON a.model_name = m.name
                    ORDER BY m.name
                    """
                )
                columns = [
                    "name",
                    "description",
                    "version",
                    "updated_at",
                    "active_version",
                    "activated_at",
                    "activated_by",
                ]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def save_regime_model(self, *, name: str, config: dict[str, Any], description: str = "") -> dict[str, Any]:
        dsn = self._require_dsn()
        payload = json.dumps(config)
        config_hash = _stable_config_hash(config)
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT version FROM {REGIME_MODELS_TABLE} WHERE name = %s", (name,))
                row = cur.fetchone()
                next_version = (int(row[0]) + 1) if row else 1
                cur.execute(
                    f"""
                    INSERT INTO {REGIME_MODELS_TABLE} (name, description, version, config, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        version = EXCLUDED.version,
                        config = EXCLUDED.config,
                        updated_at = NOW()
                    """,
                    (name, description, next_version, payload),
                )
                cur.execute(
                    """
                    INSERT INTO core.regime_model_revisions (
                        model_name,
                        version,
                        description,
                        config,
                        status,
                        config_hash,
                        published_at,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, 'published', %s, NOW(), NOW())
                    ON CONFLICT (model_name, version) DO NOTHING
                    """,
                    (name, next_version, description, payload, config_hash),
                )
        return {"name": name, "version": next_version, "description": description, "config": config}

    def get_regime_model_revision(self, name: str, version: int | None = None) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                if version is None:
                    cur.execute(
                        """
                        SELECT
                            model_name,
                            version,
                            description,
                            config,
                            status,
                            config_hash,
                            published_at,
                            created_at
                        FROM core.regime_model_revisions
                        WHERE model_name = %s
                        ORDER BY version DESC
                        LIMIT 1
                        """,
                        (name,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            model_name,
                            version,
                            description,
                            config,
                            status,
                            config_hash,
                            published_at,
                            created_at
                        FROM core.regime_model_revisions
                        WHERE model_name = %s AND version = %s
                        """,
                        (name, int(version)),
                    )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [
                    "name",
                    "version",
                    "description",
                    "config",
                    "status",
                    "config_hash",
                    "published_at",
                    "created_at",
                ]
                return dict(zip(columns, row))

    def list_regime_model_revisions(self, name: str) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        model_name,
                        version,
                        description,
                        config,
                        status,
                        config_hash,
                        published_at,
                        created_at
                    FROM core.regime_model_revisions
                    WHERE model_name = %s
                    ORDER BY version DESC
                    """,
                    (name,),
                )
                columns = [
                    "name",
                    "version",
                    "description",
                    "config",
                    "status",
                    "config_hash",
                    "published_at",
                    "created_at",
                ]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def activate_regime_model(
        self,
        *,
        name: str,
        version: int | None = None,
        activated_by: str | None = None,
    ) -> dict[str, Any]:
        dsn = self._require_dsn()
        resolved = self.get_regime_model_revision(name, version=version)
        if not resolved:
            raise ValueError(f"Regime model '{name}' version '{version}' not found.")
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.regime_model_activations (model_name, model_version, activated_by, activated_at)
                    VALUES (%s, %s, %s, NOW())
                    """,
                    (name, int(resolved["version"]), activated_by),
                )
        resolved["activated_by"] = activated_by
        return resolved

    def get_active_regime_model_revision(self, name: str) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_activation AS (
                        SELECT model_name, model_version, activated_at, activated_by
                        FROM core.regime_model_activations
                        WHERE model_name = %s
                        ORDER BY activated_at DESC, activation_id DESC
                        LIMIT 1
                    )
                    SELECT
                        r.model_name,
                        r.version,
                        r.description,
                        r.config,
                        r.status,
                        r.config_hash,
                        r.published_at,
                        r.created_at,
                        a.activated_at,
                        a.activated_by
                    FROM core.regime_model_revisions AS r
                    JOIN latest_activation AS a
                      ON a.model_name = r.model_name
                     AND a.model_version = r.version
                    """,
                    (name,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [
                    "name",
                    "version",
                    "description",
                    "config",
                    "status",
                    "config_hash",
                    "published_at",
                    "created_at",
                    "activated_at",
                    "activated_by",
                ]
                return dict(zip(columns, row))

    def list_active_regime_model_revisions(self) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH latest_activations AS (
                        SELECT DISTINCT ON (model_name)
                            model_name,
                            model_version,
                            activated_at,
                            activated_by
                        FROM core.regime_model_activations
                        ORDER BY model_name, activated_at DESC, activation_id DESC
                    )
                    SELECT
                        r.model_name,
                        r.version,
                        r.description,
                        r.config,
                        r.status,
                        r.config_hash,
                        r.published_at,
                        r.created_at,
                        a.activated_at,
                        a.activated_by
                    FROM core.regime_model_revisions AS r
                    JOIN latest_activations AS a
                      ON a.model_name = r.model_name
                     AND a.model_version = r.version
                    ORDER BY r.model_name
                    """
                )
                columns = [
                    "name",
                    "version",
                    "description",
                    "config",
                    "status",
                    "config_hash",
                    "published_at",
                    "created_at",
                    "activated_at",
                    "activated_by",
                ]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_regime_latest(self, *, model_name: str, model_version: int | None = None) -> Optional[dict[str, Any]]:
        dsn = self._require_dsn()
        resolved_version = int(model_version) if model_version is not None else None
        if resolved_version is None:
            active = self.get_active_regime_model_revision(model_name)
            if not active:
                return None
            resolved_version = int(active["version"])
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        as_of_date,
                        effective_from_date,
                        model_name,
                        model_version,
                        regime_code,
                        regime_status,
                        matched_rule_id,
                        halt_flag,
                        halt_reason,
                        spy_return_20d,
                        rvol_10d_ann,
                        vix_spot_close,
                        vix3m_close,
                        vix_slope,
                        trend_state,
                        curve_state,
                        vix_gt_32_streak,
                        computed_at
                    FROM gold.regime_latest
                    WHERE model_name = %s AND model_version = %s
                    """,
                    (model_name, resolved_version),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [
                    "as_of_date",
                    "effective_from_date",
                    "model_name",
                    "model_version",
                    "regime_code",
                    "regime_status",
                    "matched_rule_id",
                    "halt_flag",
                    "halt_reason",
                    "spy_return_20d",
                    "rvol_10d_ann",
                    "vix_spot_close",
                    "vix3m_close",
                    "vix_slope",
                    "trend_state",
                    "curve_state",
                    "vix_gt_32_streak",
                    "computed_at",
                ]
                return dict(zip(columns, row))

    def list_regime_inputs(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        predicates: list[str] = []
        params: list[Any] = []
        if start_date is not None:
            predicates.append("as_of_date >= %s")
            params.append(start_date)
        if end_date is not None:
            predicates.append("as_of_date <= %s")
            params.append(end_date)
        params.append(max(1, int(limit)))

        where_sql = f"WHERE {' AND '.join(predicates)}" if predicates else ""
        sql = f"""
            SELECT
                as_of_date,
                spy_close,
                return_1d,
                return_20d,
                rvol_10d_ann,
                vix_spot_close,
                vix3m_close,
                vix_slope,
                trend_state,
                curve_state,
                vix_gt_32_streak,
                inputs_complete_flag,
                computed_at
            FROM gold.regime_inputs_daily
            {where_sql}
            ORDER BY as_of_date DESC
            LIMIT %s
        """
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        columns = [
            "as_of_date",
            "spy_close",
            "return_1d",
            "return_20d",
            "rvol_10d_ann",
            "vix_spot_close",
            "vix3m_close",
            "vix_slope",
            "trend_state",
            "curve_state",
            "vix_gt_32_streak",
            "inputs_complete_flag",
            "computed_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def list_regime_history(
        self,
        *,
        model_name: str,
        model_version: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        resolved_version = int(model_version) if model_version is not None else None
        if resolved_version is None:
            active = self.get_active_regime_model_revision(model_name)
            if not active:
                return []
            resolved_version = int(active["version"])

        predicates = ["model_name = %s", "model_version = %s"]
        params: list[Any] = [model_name, resolved_version]
        if start_date is not None:
            predicates.append("as_of_date >= %s")
            params.append(start_date)
        if end_date is not None:
            predicates.append("as_of_date <= %s")
            params.append(end_date)
        params.append(max(1, int(limit)))

        sql = f"""
            SELECT
                as_of_date,
                effective_from_date,
                model_name,
                model_version,
                regime_code,
                regime_status,
                matched_rule_id,
                halt_flag,
                halt_reason,
                spy_return_20d,
                rvol_10d_ann,
                vix_spot_close,
                vix3m_close,
                vix_slope,
                trend_state,
                curve_state,
                vix_gt_32_streak,
                computed_at
            FROM gold.regime_history
            WHERE {" AND ".join(predicates)}
            ORDER BY as_of_date DESC
            LIMIT %s
        """
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        columns = [
            "as_of_date",
            "effective_from_date",
            "model_name",
            "model_version",
            "regime_code",
            "regime_status",
            "matched_rule_id",
            "halt_flag",
            "halt_reason",
            "spy_return_20d",
            "rvol_10d_ann",
            "vix_spot_close",
            "vix3m_close",
            "vix_slope",
            "trend_state",
            "curve_state",
            "vix_gt_32_streak",
            "computed_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def list_regime_transitions(
        self,
        *,
        model_name: str,
        model_version: int | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        dsn = self._require_dsn()
        resolved_version = int(model_version) if model_version is not None else None
        if resolved_version is None:
            active = self.get_active_regime_model_revision(model_name)
            if not active:
                return []
            resolved_version = int(active["version"])

        predicates = ["model_name = %s", "model_version = %s"]
        params: list[Any] = [model_name, resolved_version]
        if start_date is not None:
            predicates.append("effective_from_date >= %s")
            params.append(start_date)
        if end_date is not None:
            predicates.append("effective_from_date <= %s")
            params.append(end_date)
        params.append(max(1, int(limit)))

        sql = f"""
            SELECT
                model_name,
                model_version,
                effective_from_date,
                prior_regime_code,
                new_regime_code,
                trigger_rule_id,
                computed_at
            FROM gold.regime_transitions
            WHERE {" AND ".join(predicates)}
            ORDER BY effective_from_date DESC
            LIMIT %s
        """
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        columns = [
            "model_name",
            "model_version",
            "effective_from_date",
            "prior_regime_code",
            "new_regime_code",
            "trigger_rule_id",
            "computed_at",
        ]
        return [dict(zip(columns, row)) for row in rows]
