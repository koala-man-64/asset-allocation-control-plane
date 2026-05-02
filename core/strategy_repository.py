import os
import json
import hashlib
import logging
from typing import Any, Optional, Dict

from asset_allocation_runtime_common.foundation.postgres import connect
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


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _resolve_revision(
    cur,
    *,
    table: str,
    name_column: str,
    label: str,
    name: str,
    version: int | None,
) -> dict[str, Any]:
    if version is None:
        cur.execute(
            f"""
            SELECT {name_column}, version, description, config
            FROM {table}
            WHERE {name_column} = %s
            ORDER BY version DESC
            LIMIT 1
            """,
            (name,),
        )
    else:
        cur.execute(
            f"""
            SELECT {name_column}, version, description, config
            FROM {table}
            WHERE {name_column} = %s AND version = %s
            """,
            (name, int(version)),
        )
    row = cur.fetchone()
    if not row:
        suffix = f" version {version}" if version is not None else ""
        raise ValueError(f"{label} '{name}'{suffix} not found.")
    return dict(zip(["name", "version", "description", "config"], row))


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
                    ranking_schema_name = str(normalized_config.get("rankingSchemaName") or "").strip() or None
                    universe_name = str(normalized_config.get("universeConfigName") or "").strip() or None
                    ranking_schema_version = _optional_int(normalized_config.get("rankingSchemaVersion"))
                    universe_version = _optional_int(normalized_config.get("universeConfigVersion"))

                    if ranking_schema_name:
                        ranking_revision = _resolve_revision(
                            cur,
                            table="core.ranking_schema_revisions",
                            name_column="schema_name",
                            label="Ranking schema",
                            name=ranking_schema_name,
                            version=ranking_schema_version,
                        )
                        ranking_schema_version = int(ranking_revision["version"])
                        normalized_config["rankingSchemaVersion"] = ranking_schema_version
                        ranking_config = _as_mapping(ranking_revision.get("config"))
                        if universe_name is None:
                            universe_name = str(ranking_config.get("universeConfigName") or "").strip() or None
                            if universe_name:
                                normalized_config["universeConfigName"] = universe_name

                    if universe_name:
                        universe_revision = _resolve_revision(
                            cur,
                            table="core.universe_config_revisions",
                            name_column="universe_name",
                            label="Universe config",
                            name=universe_name,
                            version=universe_version,
                        )
                        universe_version = int(universe_revision["version"])
                        normalized_config["universeConfigVersion"] = universe_version

                    regime_policy_name = str(normalized_config.get("regimePolicyConfigName") or "").strip() or None
                    regime_policy_version = _optional_int(normalized_config.get("regimePolicyConfigVersion"))
                    if regime_policy_name:
                        regime_policy_revision = _resolve_revision(
                            cur,
                            table="core.regime_policy_config_revisions",
                            name_column="policy_name",
                            label="Regime policy config",
                            name=regime_policy_name,
                            version=regime_policy_version,
                        )
                        regime_policy_version = int(regime_policy_revision["version"])
                        normalized_config["regimePolicyConfigVersion"] = regime_policy_version
                        normalized_config["regimePolicy"] = _as_mapping(regime_policy_revision.get("config"))

                    risk_policy_name = str(normalized_config.get("riskPolicyName") or "").strip() or None
                    risk_policy_version = _optional_int(normalized_config.get("riskPolicyVersion"))
                    if risk_policy_name:
                        risk_policy_revision = _resolve_revision(
                            cur,
                            table="core.risk_policy_config_revisions",
                            name_column="policy_name",
                            label="Risk policy config",
                            name=risk_policy_name,
                            version=risk_policy_version,
                        )
                        risk_policy_version = int(risk_policy_revision["version"])
                        normalized_config["riskPolicyVersion"] = risk_policy_version
                        risk_policy_config = _as_mapping(risk_policy_revision.get("config"))
                        resolved_policy = risk_policy_config.get("policy", risk_policy_config)
                        normalized_config["strategyRiskPolicy"] = resolved_policy

                    exit_rule_set_name = str(normalized_config.get("exitRuleSetName") or "").strip() or None
                    exit_rule_set_version = _optional_int(normalized_config.get("exitRuleSetVersion"))
                    if exit_rule_set_name:
                        exit_rule_set_revision = _resolve_revision(
                            cur,
                            table="core.exit_rule_set_revisions",
                            name_column="rule_set_name",
                            label="Exit rule set",
                            name=exit_rule_set_name,
                            version=exit_rule_set_version,
                        )
                        exit_rule_set_version = int(exit_rule_set_revision["version"])
                        normalized_config["exitRuleSetVersion"] = exit_rule_set_version
                        exit_rule_set_config = _as_mapping(exit_rule_set_revision.get("config"))
                        normalized_config["intrabarConflictPolicy"] = (
                            str(exit_rule_set_config.get("intrabarConflictPolicy") or "").strip() or "stop_first"
                        )
                        normalized_config["exits"] = (
                            exit_rule_set_config.get("exits")
                            if isinstance(exit_rule_set_config.get("exits"), list)
                            else []
                        )

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
                            regime_policy_name,
                            regime_policy_version,
                            risk_policy_name,
                            risk_policy_version,
                            exit_rule_set_name,
                            exit_rule_set_version,
                            status,
                            config_hash,
                            published_at,
                            created_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            'published', %s, NOW(), NOW()
                        )
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
                            regime_policy_name,
                            regime_policy_version,
                            risk_policy_name,
                            risk_policy_version,
                            exit_rule_set_name,
                            exit_rule_set_version,
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
                                regime_policy_name,
                                regime_policy_version,
                                risk_policy_name,
                                risk_policy_version,
                                exit_rule_set_name,
                                exit_rule_set_version,
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
                                regime_policy_name,
                                regime_policy_version,
                                risk_policy_name,
                                risk_policy_version,
                                exit_rule_set_name,
                                exit_rule_set_version,
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
                        "regime_policy_name",
                        "regime_policy_version",
                        "risk_policy_name",
                        "risk_policy_version",
                        "exit_rule_set_name",
                        "exit_rule_set_version",
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
