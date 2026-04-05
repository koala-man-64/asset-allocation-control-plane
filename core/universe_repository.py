from __future__ import annotations

import json
import logging
import os
import hashlib
from typing import Any, Optional

from core.postgres import connect

logger = logging.getLogger(__name__)
UNIVERSE_CONFIGS_TABLE = "core.universe_configs"


def _stable_config_hash(config: dict[str, Any]) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


class UniverseRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. UniverseRepository will not function.")

    def get_universe_config(self, name: str) -> Optional[dict[str, Any]]:
        if not self.dsn:
            return None
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT name, description, version, updated_at, config
                    FROM {UNIVERSE_CONFIGS_TABLE}
                    WHERE name = %s
                    """,
                    (name,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = ["name", "description", "version", "updated_at", "config"]
                return dict(zip(columns, row))

    def list_universe_configs(self) -> list[dict[str, Any]]:
        if not self.dsn:
            return []
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT name, description, version, updated_at
                    FROM {UNIVERSE_CONFIGS_TABLE}
                    ORDER BY name
                    """
                )
                columns = ["name", "description", "version", "updated_at"]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def save_universe_config(self, *, name: str, config: dict[str, Any], description: str = "") -> dict[str, Any]:
        if not self.dsn:
            raise ValueError("Database connection not configured")
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT version FROM {UNIVERSE_CONFIGS_TABLE} WHERE name = %s",
                    (name,),
                )
                row = cur.fetchone()
                next_version = (int(row[0]) + 1) if row else 1
                payload = json.dumps(config)
                config_hash = _stable_config_hash(config)
                cur.execute(
                    f"""
                    INSERT INTO {UNIVERSE_CONFIGS_TABLE} (name, description, version, config, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
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
                    INSERT INTO core.universe_config_revisions (
                        universe_name,
                        version,
                        description,
                        config,
                        status,
                        config_hash,
                        published_at,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, 'published', %s, NOW(), NOW())
                    ON CONFLICT (universe_name, version) DO NOTHING
                    """,
                    (name, next_version, description, payload, config_hash),
                )
        return {"name": name, "version": next_version, "description": description, "config": config}

    def get_universe_config_revision(self, name: str, version: int | None = None) -> Optional[dict[str, Any]]:
        if not self.dsn:
            return None
        resolved_version = int(version) if version is not None else None
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                if resolved_version is None:
                    cur.execute(
                        """
                        SELECT universe_name, version, description, config, status, config_hash, published_at, created_at
                        FROM core.universe_config_revisions
                        WHERE universe_name = %s
                        ORDER BY version DESC
                        LIMIT 1
                        """,
                        (name,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT universe_name, version, description, config, status, config_hash, published_at, created_at
                        FROM core.universe_config_revisions
                        WHERE universe_name = %s AND version = %s
                        """,
                        (name, resolved_version),
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

    def get_universe_config_references(self, name: str) -> dict[str, list[str]]:
        if not self.dsn:
            return {"strategies": [], "rankingSchemas": []}
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT name
                    FROM core.strategies
                    WHERE COALESCE(NULLIF(BTRIM(config ->> 'universeConfigName'), ''), '') = %s
                    ORDER BY name
                    """,
                    (name,),
                )
                strategies = [str(row[0]) for row in cur.fetchall()]
                cur.execute(
                    """
                    SELECT name
                    FROM core.ranking_schemas
                    WHERE COALESCE(NULLIF(BTRIM(config ->> 'universeConfigName'), ''), '') = %s
                    ORDER BY name
                    """,
                    (name,),
                )
                ranking_schemas = [str(row[0]) for row in cur.fetchall()]
        return {"strategies": strategies, "rankingSchemas": ranking_schemas}

    def delete_universe_config(self, name: str) -> bool:
        if not self.dsn:
            raise ValueError("Database connection not configured")
        with connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {UNIVERSE_CONFIGS_TABLE} WHERE name = %s RETURNING name",
                    (name,),
                )
                return cur.fetchone() is not None
