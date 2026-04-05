import os
import json
import hashlib
import logging
from typing import Any, Optional, Dict

from core.postgres import connect
from core.ranking_engine.naming import slugify_strategy_output_table

logger = logging.getLogger(__name__)
STRATEGIES_TABLE = "core.strategies"


def _stable_config_hash(config: Dict) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def normalize_strategy_config_document(config: Any) -> Dict:
    if not isinstance(config, dict):
        return {}

    normalized = json.loads(json.dumps(config))

    regime_policy = normalized.get("regimePolicy")
    if isinstance(regime_policy, dict):
        if regime_policy.get("enabled") is False:
            normalized.pop("regimePolicy", None)
        else:
            regime_policy.pop("enabled", None)
            normalized["regimePolicy"] = regime_policy

    raw_exits = normalized.get("exits")
    if isinstance(raw_exits, list):
        cleaned_exits: list[dict[str, Any]] = []
        for raw_rule in raw_exits:
            if not isinstance(raw_rule, dict):
                continue
            if raw_rule.get("enabled") is False:
                continue
            cleaned_rule = dict(raw_rule)
            cleaned_rule.pop("enabled", None)
            cleaned_exits.append(cleaned_rule)
        normalized["exits"] = cleaned_exits

    return normalized

class StrategyRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. StrategyRepository will not function.")

    def get_strategy_config(self, name: str) -> Optional[Dict]:
        """
        Retrieves a strategy configuration by name.
        Returns the 'config' JSONB content as a dictionary.
        """
        if not self.dsn:
            return None
            
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT config FROM {STRATEGIES_TABLE} WHERE name = %s",
                        (name,)
                    )
                    row = cur.fetchone()
                    if row:
                        return normalize_strategy_config_document(row[0])
                    return None
        except Exception as e:
            logger.error(f"Failed to fetch strategy '{name}': {e}")
            raise

    def get_strategy(self, name: str) -> Optional[Dict]:
        """
        Retrieves metadata and configuration for a strategy by name.
        """
        if not self.dsn:
            return None

        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT name, type, description, output_table_name, updated_at, config
                        FROM {STRATEGIES_TABLE}
                        WHERE name = %s
                        """,
                        (name,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    columns = ["name", "type", "description", "output_table_name", "updated_at", "config"]
                    payload = dict(zip(columns, row))
                    payload["config"] = normalize_strategy_config_document(payload.get("config"))
                    return payload
        except Exception as e:
            logger.error(f"Failed to fetch strategy detail '{name}': {e}")
            raise

    def save_strategy(self, name: str, config: Dict, strategy_type: str = "configured", description: str = "") -> None:
        """
        Upserts a strategy configuration.
        """
        if not self.dsn:
            raise ValueError("Database connection not configured")
        normalized_config = normalize_strategy_config_document(config)

        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    output_table_name = slugify_strategy_output_table(name)
                    config_hash = _stable_config_hash(normalized_config)
                    cur.execute(
                        f"""
                        INSERT INTO {STRATEGIES_TABLE} (name, config, type, description, output_table_name, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (name) 
                        DO UPDATE SET 
                            config = EXCLUDED.config,
                            type = EXCLUDED.type,
                            description = EXCLUDED.description,
                            output_table_name = EXCLUDED.output_table_name,
                            updated_at = NOW()
                        """,
                        (name, json.dumps(normalized_config), strategy_type, description, output_table_name)
                    )
                    ranking_schema_name = str(normalized_config.get("rankingSchemaName") or "").strip() or None
                    universe_name = str(normalized_config.get("universeConfigName") or "").strip() or None
                    ranking_schema_version: int | None = None
                    universe_version: int | None = None

                    if ranking_schema_name:
                        cur.execute(
                            "SELECT version, config FROM core.ranking_schemas WHERE name = %s",
                            (ranking_schema_name,),
                        )
                        ranking_row = cur.fetchone()
                        if ranking_row:
                            ranking_schema_version = int(ranking_row[0])
                            ranking_config = ranking_row[1] if isinstance(ranking_row[1], dict) else {}
                            if universe_name is None and isinstance(ranking_config, dict):
                                universe_name = str(ranking_config.get("universeConfigName") or "").strip() or None

                    if universe_name:
                        cur.execute(
                            "SELECT version FROM core.universe_configs WHERE name = %s",
                            (universe_name,),
                        )
                        universe_row = cur.fetchone()
                        if universe_row:
                            universe_version = int(universe_row[0])

                    cur.execute(
                        """
                        SELECT COALESCE(MAX(version), 0) + 1
                        FROM core.strategy_revisions
                        WHERE strategy_name = %s
                        """,
                        (name,),
                    )
                    revision_row = cur.fetchone()
                    next_version = int(revision_row[0]) if revision_row and revision_row[0] else 1
                    cur.execute(
                        """
                        INSERT INTO core.strategy_revisions (
                            strategy_name,
                            version,
                            description,
                            config,
                            ranking_schema_name,
                            ranking_schema_version,
                            universe_name,
                            universe_version,
                            status,
                            config_hash,
                            published_at,
                            created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'published', %s, NOW(), NOW())
                        """,
                        (
                            name,
                            next_version,
                            description,
                            json.dumps(normalized_config),
                            ranking_schema_name,
                            ranking_schema_version,
                            universe_name,
                            universe_version,
                            config_hash,
                        ),
                    )
        except Exception as e:
            logger.error(f"Failed to save strategy '{name}': {e}")
            raise

    def delete_strategy(self, name: str) -> bool:
        """
        Deletes a strategy by name.
        Returns True when a row was removed.
        """
        if not self.dsn:
            raise ValueError("Database connection not configured")

        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {STRATEGIES_TABLE} WHERE name = %s RETURNING name",
                        (name,),
                    )
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to delete strategy '{name}': {e}")
            raise

    def list_strategies(self) -> list[Dict]:
        """
        Returns a list of all strategies metadata.
        """
        if not self.dsn:
            return []
            
        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT name, type, description, output_table_name, updated_at FROM {STRATEGIES_TABLE} ORDER BY name"
                    )
                    columns = ["name", "type", "description", "output_table_name", "updated_at"]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Failed to list strategies: {e}")
            raise

    def get_strategy_revision(self, name: str, version: int | None = None) -> Optional[Dict]:
        """
        Retrieves a strategy revision with pinned ranking/universe references.
        """
        if not self.dsn:
            return None

        try:
            with connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    if version is None:
                        cur.execute(
                            """
                            SELECT
                                strategy_name,
                                version,
                                description,
                                config,
                                ranking_schema_name,
                                ranking_schema_version,
                                universe_name,
                                universe_version,
                                status,
                                config_hash,
                                published_at,
                                created_at
                            FROM core.strategy_revisions
                            WHERE strategy_name = %s
                            ORDER BY version DESC
                            LIMIT 1
                            """,
                            (name,),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT
                                strategy_name,
                                version,
                                description,
                                config,
                                ranking_schema_name,
                                ranking_schema_version,
                                universe_name,
                                universe_version,
                                status,
                                config_hash,
                                published_at,
                                created_at
                            FROM core.strategy_revisions
                            WHERE strategy_name = %s AND version = %s
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
                        "ranking_schema_name",
                        "ranking_schema_version",
                        "universe_name",
                        "universe_version",
                        "status",
                        "config_hash",
                        "published_at",
                        "created_at",
                    ]
                    payload = dict(zip(columns, row))
                    payload["config"] = normalize_strategy_config_document(payload.get("config"))
                    return payload
        except Exception as e:
            logger.error(f"Failed to fetch strategy revision '{name}': {e}")
            raise
