from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

from asset_allocation_runtime_common.foundation.postgres import connect

logger = logging.getLogger(__name__)


def stable_config_hash(config: dict[str, Any]) -> str:
    payload = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ConfigFamily:
    key: str
    table: str
    revision_table: str
    revision_name_column: str
    strategy_name_field: str | None
    component_ref_field: str


REGIME_POLICY_FAMILY = ConfigFamily(
    key="regimePolicy",
    table="core.regime_policy_configs",
    revision_table="core.regime_policy_config_revisions",
    revision_name_column="policy_name",
    strategy_name_field="regimePolicyConfigName",
    component_ref_field="regimePolicy",
)
RISK_POLICY_FAMILY = ConfigFamily(
    key="riskPolicy",
    table="core.risk_policy_configs",
    revision_table="core.risk_policy_config_revisions",
    revision_name_column="policy_name",
    strategy_name_field="riskPolicyName",
    component_ref_field="riskPolicy",
)
EXIT_RULE_SET_FAMILY = ConfigFamily(
    key="exitRuleSet",
    table="core.exit_rule_sets",
    revision_table="core.exit_rule_set_revisions",
    revision_name_column="rule_set_name",
    strategy_name_field="exitRuleSetName",
    component_ref_field="exitPolicy",
)
REBALANCE_POLICY_FAMILY = ConfigFamily(
    key="rebalancePolicy",
    table="core.rebalance_policy_configs",
    revision_table="core.rebalance_policy_config_revisions",
    revision_name_column="policy_name",
    strategy_name_field=None,
    component_ref_field="rebalance",
)

FAMILIES_BY_KEY = {
    REGIME_POLICY_FAMILY.key: REGIME_POLICY_FAMILY,
    RISK_POLICY_FAMILY.key: RISK_POLICY_FAMILY,
    EXIT_RULE_SET_FAMILY.key: EXIT_RULE_SET_FAMILY,
    REBALANCE_POLICY_FAMILY.key: REBALANCE_POLICY_FAMILY,
}


class ConfigLibraryRepository:
    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            logger.warning("POSTGRES_DSN not set. ConfigLibraryRepository will not function.")

    def _require_dsn(self) -> str:
        if not self.dsn:
            raise ValueError("Database connection not configured")
        return self.dsn

    def _family(self, key: str) -> ConfigFamily:
        try:
            return FAMILIES_BY_KEY[key]
        except KeyError as exc:
            raise ValueError(f"Unknown config family '{key}'.") from exc

    def _usage_name_expression(self, family: ConfigFamily) -> str:
        ref_expr = f"NULLIF(BTRIM(config #>> '{{componentRefs,{family.component_ref_field},name}}'), '')"
        if not family.strategy_name_field:
            return ref_expr
        return f"COALESCE(NULLIF(BTRIM(config ->> '{family.strategy_name_field}'), ''), {ref_expr})"

    def list_configs(self, family_key: str, *, include_archived: bool = False) -> list[dict[str, Any]]:
        family = self._family(family_key)
        archived_filter = "" if include_archived else "WHERE c.archived = FALSE"
        usage_name_expression = self._usage_name_expression(family)
        sql = f"""
            WITH usage AS (
                SELECT
                    {usage_name_expression} AS name,
                    COUNT(*) AS usage_count
                FROM core.strategies
                WHERE {usage_name_expression} IS NOT NULL
                GROUP BY {usage_name_expression}
            )
            SELECT
                c.name,
                c.description,
                c.version,
                c.archived,
                COALESCE(usage.usage_count, 0),
                c.status,
                c.intended_use,
                c.thesis,
                c.what_to_monitor,
                c.updated_at,
                c.config
            FROM {family.table} AS c
            LEFT JOIN usage
              ON usage.name = c.name
            {archived_filter}
            ORDER BY c.name
        """
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                columns = [
                    "name",
                    "description",
                    "version",
                    "archived",
                    "usageCount",
                    "status",
                    "intendedUse",
                    "thesis",
                    "whatToMonitor",
                    "updatedAt",
                    "config",
                ]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_config(self, family_key: str, name: str) -> dict[str, Any] | None:
        family = self._family(family_key)
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        name,
                        description,
                        version,
                        archived,
                        status,
                        intended_use,
                        thesis,
                        what_to_monitor,
                        updated_at,
                        config
                    FROM {family.table}
                    WHERE name = %s
                    """,
                    (name,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                columns = [
                    "name",
                    "description",
                    "version",
                    "archived",
                    "status",
                    "intendedUse",
                    "thesis",
                    "whatToMonitor",
                    "updatedAt",
                    "config",
                ]
                return dict(zip(columns, row))

    def save_config(
        self,
        family_key: str,
        *,
        name: str,
        config: dict[str, Any],
        description: str = "",
        status: str = "active",
        intended_use: str = "research",
        thesis: str = "",
        what_to_monitor: list[str] | None = None,
    ) -> dict[str, Any]:
        family = self._family(family_key)
        payload = json.dumps(config)
        monitor_payload = json.dumps(what_to_monitor or [])
        config_hash = stable_config_hash(config)
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT version FROM {family.table} WHERE name = %s", (name,))
                row = cur.fetchone()
                next_version = (int(row[0]) + 1) if row else 1
                cur.execute(
                    f"""
                    INSERT INTO {family.table} (
                        name,
                        description,
                        version,
                        config,
                        archived,
                        status,
                        intended_use,
                        thesis,
                        what_to_monitor,
                        created_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, FALSE, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (name)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        version = EXCLUDED.version,
                        config = EXCLUDED.config,
                        archived = FALSE,
                        status = EXCLUDED.status,
                        intended_use = EXCLUDED.intended_use,
                        thesis = EXCLUDED.thesis,
                        what_to_monitor = EXCLUDED.what_to_monitor,
                        updated_at = NOW()
                    """,
                    (name, description, next_version, payload, status, intended_use, thesis, monitor_payload),
                )
                cur.execute(
                    f"""
                    INSERT INTO {family.revision_table} (
                        {family.revision_name_column},
                        version,
                        description,
                        config,
                        status,
                        intended_use,
                        thesis,
                        what_to_monitor,
                        config_hash,
                        published_at,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT ({family.revision_name_column}, version) DO NOTHING
                    """,
                    (
                        name,
                        next_version,
                        description,
                        payload,
                        status,
                        intended_use,
                        thesis,
                        monitor_payload,
                        config_hash,
                    ),
                )
        return {
            "name": name,
            "description": description,
            "version": next_version,
            "archived": False,
            "status": status,
            "intendedUse": intended_use,
            "thesis": thesis,
            "whatToMonitor": what_to_monitor or [],
            "config": config,
        }

    def get_revision(self, family_key: str, name: str, version: int | None = None) -> dict[str, Any] | None:
        family = self._family(family_key)
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                if version is None:
                    cur.execute(
                        f"""
                        SELECT
                            {family.revision_name_column},
                            version,
                            description,
                            config,
                            status,
                            intended_use,
                            thesis,
                            what_to_monitor,
                            config_hash,
                            published_at,
                            created_at
                        FROM {family.revision_table}
                        WHERE {family.revision_name_column} = %s
                        ORDER BY version DESC
                        LIMIT 1
                        """,
                        (name,),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT
                            {family.revision_name_column},
                            version,
                            description,
                            config,
                            status,
                            intended_use,
                            thesis,
                            what_to_monitor,
                            config_hash,
                            published_at,
                            created_at
                        FROM {family.revision_table}
                        WHERE {family.revision_name_column} = %s AND version = %s
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
                    "intendedUse",
                    "thesis",
                    "whatToMonitor",
                    "configHash",
                    "publishedAt",
                    "createdAt",
                ]
                return dict(zip(columns, row))

    def list_revisions(self, family_key: str, name: str) -> list[dict[str, Any]]:
        family = self._family(family_key)
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        {family.revision_name_column},
                        version,
                        description,
                        config,
                        status,
                        intended_use,
                        thesis,
                        what_to_monitor,
                        config_hash,
                        published_at,
                        created_at
                    FROM {family.revision_table}
                    WHERE {family.revision_name_column} = %s
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
                    "intendedUse",
                    "thesis",
                    "whatToMonitor",
                    "configHash",
                    "publishedAt",
                    "createdAt",
                ]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def archive_config(self, family_key: str, name: str) -> bool:
        family = self._family(family_key)
        with connect(self._require_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {family.table}
                    SET archived = TRUE, updated_at = NOW()
                    WHERE name = %s
                    RETURNING name
                    """,
                    (name,),
                )
                return cur.fetchone() is not None
