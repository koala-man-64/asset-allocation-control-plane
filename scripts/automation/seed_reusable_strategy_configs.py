from __future__ import annotations

import argparse
import json
import os
import sys
from itertools import product
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config_library_repository import ConfigLibraryRepository
from core.ranking_repository import RankingRepository
from core.universe_repository import UniverseRepository


UNIVERSES: dict[str, dict[str, Any]] = {
    "us_large_liquid": {
        "description": "US primary-listed large-cap liquid equities.",
        "config": {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {"kind": "condition", "field": "security.country", "operator": "eq", "value": "US"},
                    {"kind": "condition", "field": "security.primary_listing", "operator": "eq", "value": True},
                    {"kind": "condition", "field": "security.market_cap", "operator": "gte", "value": 10000000000},
                    {"kind": "condition", "field": "market.dollar_volume_20d", "operator": "gte", "value": 50000000},
                    {"kind": "condition", "field": "security.is_price_liquidity_eligible", "operator": "eq", "value": True},
                ],
            },
        },
    },
    "us_mid_large_liquid": {
        "description": "US primary-listed mid and large-cap liquid equities.",
        "config": {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {"kind": "condition", "field": "security.country", "operator": "eq", "value": "US"},
                    {"kind": "condition", "field": "security.primary_listing", "operator": "eq", "value": True},
                    {"kind": "condition", "field": "security.market_cap", "operator": "gte", "value": 2000000000},
                    {"kind": "condition", "field": "market.dollar_volume_20d", "operator": "gte", "value": 25000000},
                    {"kind": "condition", "field": "security.is_price_liquidity_eligible", "operator": "eq", "value": True},
                ],
            },
        },
    },
    "sector_balanced_large": {
        "description": "Documented starter universe for later sector-balanced large-cap experiments.",
        "config": {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {"kind": "condition", "field": "security.country", "operator": "eq", "value": "US"},
                    {"kind": "condition", "field": "security.primary_listing", "operator": "eq", "value": True},
                    {"kind": "condition", "field": "security.market_cap", "operator": "gte", "value": 10000000000},
                    {"kind": "condition", "field": "security.is_price_liquidity_eligible", "operator": "eq", "value": True},
                ],
            },
        },
    },
}

RANKINGS: dict[str, dict[str, Any]] = {
    "momentum_12_1": {
        "description": "12 minus 1 month momentum starter ranking.",
        "config": {
            "universeConfigName": "us_large_liquid",
            "groups": [
                {
                    "name": "momentum",
                    "weight": 1.0,
                    "factors": [
                        {
                            "name": "return_252d",
                            "table": "market_data",
                            "column": "return_252d",
                            "weight": 1.0,
                            "direction": "desc",
                            "missingValuePolicy": "exclude",
                            "transforms": [],
                        },
                        {
                            "name": "return_21d_reversal",
                            "table": "market_data",
                            "column": "return_21d",
                            "weight": 0.35,
                            "direction": "asc",
                            "missingValuePolicy": "neutral",
                            "transforms": [],
                        },
                    ],
                    "transforms": [],
                }
            ],
            "overallTransforms": [],
        },
    },
    "quality_momentum": {
        "description": "Momentum with profitability and balance-sheet quality tilt.",
        "config": {
            "universeConfigName": "us_large_liquid",
            "groups": [
                {
                    "name": "momentum",
                    "weight": 0.6,
                    "factors": [
                        {
                            "name": "return_252d",
                            "table": "market_data",
                            "column": "return_252d",
                            "weight": 1.0,
                            "direction": "desc",
                            "missingValuePolicy": "exclude",
                            "transforms": [],
                        }
                    ],
                    "transforms": [],
                },
                {
                    "name": "quality",
                    "weight": 0.4,
                    "factors": [
                        {
                            "name": "return_on_equity",
                            "table": "fundamentals",
                            "column": "return_on_equity",
                            "weight": 0.6,
                            "direction": "desc",
                            "missingValuePolicy": "neutral",
                            "transforms": [],
                        },
                        {
                            "name": "debt_to_equity",
                            "table": "fundamentals",
                            "column": "debt_to_equity",
                            "weight": 0.4,
                            "direction": "asc",
                            "missingValuePolicy": "neutral",
                            "transforms": [],
                        },
                    ],
                    "transforms": [],
                },
            ],
            "overallTransforms": [],
        },
    },
    "value_quality_momentum": {
        "description": "Value, quality, and momentum blend for validation.",
        "config": {
            "universeConfigName": "us_large_liquid",
            "groups": [
                {
                    "name": "value",
                    "weight": 0.3,
                    "factors": [
                        {
                            "name": "earnings_yield",
                            "table": "fundamentals",
                            "column": "earnings_yield",
                            "weight": 1.0,
                            "direction": "desc",
                            "missingValuePolicy": "neutral",
                            "transforms": [],
                        }
                    ],
                    "transforms": [],
                },
                {
                    "name": "quality",
                    "weight": 0.3,
                    "factors": [
                        {
                            "name": "return_on_equity",
                            "table": "fundamentals",
                            "column": "return_on_equity",
                            "weight": 1.0,
                            "direction": "desc",
                            "missingValuePolicy": "neutral",
                            "transforms": [],
                        }
                    ],
                    "transforms": [],
                },
                {
                    "name": "momentum",
                    "weight": 0.4,
                    "factors": [
                        {
                            "name": "return_252d",
                            "table": "market_data",
                            "column": "return_252d",
                            "weight": 1.0,
                            "direction": "desc",
                            "missingValuePolicy": "exclude",
                            "transforms": [],
                        }
                    ],
                    "transforms": [],
                },
            ],
            "overallTransforms": [],
        },
    },
}

REBALANCE_POLICIES: dict[str, dict[str, Any]] = {
    "monthly_last_trading_day": {
        "description": "Monthly rebalance on last trading day at close, next-bar execution.",
        "config": {
            "cadence": "monthly",
            "dayRule": "last_trading_day",
            "anchor": "close",
            "tradeDelayBars": 1,
            "driftThresholdBps": 100,
            "maxTurnoverPerRebalance": 0.25,
        },
    },
    "quarterly_last_trading_day": {
        "description": "Quarterly rebalance on last trading day at close, next-bar execution.",
        "config": {
            "cadence": "quarterly",
            "dayRule": "last_trading_day",
            "anchor": "close",
            "tradeDelayBars": 1,
            "driftThresholdBps": 150,
            "maxTurnoverPerRebalance": 0.35,
        },
    },
}

REGIME_POLICIES = {
    "observe_only_default": {
        "description": "Observe default-regime signals without trading action.",
        "config": {"modelName": "default-regime", "modelVersion": 3, "mode": "observe_only"},
    }
}

RISK_POLICIES = {
    "balanced_long_only": {
        "description": "Balanced long-only strategy stop policy.",
        "config": {
            "policy": {
                "enabled": True,
                "scope": "strategy",
                "stopLoss": {
                    "thresholdPct": 8,
                    "action": "reduce_exposure",
                    "reductionPct": 50,
                },
                "reentry": {"cooldownBars": 5, "requireApproval": False},
            }
        },
    }
}

EXIT_POLICIES = {
    "rebalance_only": {
        "description": "No explicit exits; portfolio changes happen through scheduled rebalance.",
        "config": {"intrabarConflictPolicy": "stop_first", "exits": []},
    },
    "rank_decay_exit": {
        "description": "Exit held names when rank decays past the configured threshold.",
        "config": {
            "intrabarConflictPolicy": "priority_order",
            "exits": [{"id": "rank-decay", "type": "rank_decay", "rankThreshold": 40, "priority": 0}],
        },
    },
}


def _same_config(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return json.dumps(left, sort_keys=True, separators=(",", ":")) == json.dumps(
        right,
        sort_keys=True,
        separators=(",", ":"),
    )


def _ensure_universe(repo: UniverseRepository, name: str, preset: dict[str, Any]) -> None:
    existing = repo.get_universe_config(name)
    if existing:
        if not _same_config(dict(existing.get("config") or {}), preset["config"]):
            raise RuntimeError(f"Universe preset '{name}' already exists with different config.")
        return
    repo.save_universe_config(name=name, description=preset["description"], config=preset["config"])


def _ensure_ranking(repo: RankingRepository, name: str, preset: dict[str, Any]) -> None:
    existing = repo.get_ranking_schema(name)
    if existing:
        if not _same_config(dict(existing.get("config") or {}), preset["config"]):
            raise RuntimeError(f"Ranking preset '{name}' already exists with different config.")
        return
    repo.save_ranking_schema(name=name, description=preset["description"], config=preset["config"])


def _ensure_library_config(repo: ConfigLibraryRepository, family: str, name: str, preset: dict[str, Any]) -> None:
    existing = repo.get_config(family, name)
    if existing:
        if not _same_config(dict(existing.get("config") or {}), preset["config"]):
            raise RuntimeError(f"{family} preset '{name}' already exists with different config.")
        return
    repo.save_config(
        family,
        name=name,
        description=preset["description"],
        config=preset["config"],
        status="active",
        intended_use="research",
        thesis="Starter preset for reusable strategy-config backtests.",
        what_to_monitor=["validation metrics", "turnover", "drawdown", "data coverage"],
    )


def seed_presets(dsn: str) -> None:
    universe_repo = UniverseRepository(dsn)
    ranking_repo = RankingRepository(dsn)
    library_repo = ConfigLibraryRepository(dsn)
    for name, preset in UNIVERSES.items():
        _ensure_universe(universe_repo, name, preset)
    for name, preset in RANKINGS.items():
        _ensure_ranking(ranking_repo, name, preset)
    for name, preset in REBALANCE_POLICIES.items():
        _ensure_library_config(library_repo, "rebalancePolicy", name, preset)
    for name, preset in REGIME_POLICIES.items():
        _ensure_library_config(library_repo, "regimePolicy", name, preset)
    for name, preset in RISK_POLICIES.items():
        _ensure_library_config(library_repo, "riskPolicy", name, preset)
    for name, preset in EXIT_POLICIES.items():
        _ensure_library_config(library_repo, "exitRuleSet", name, preset)


def first_pass_matrix() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for universe, ranking, rebalance, exit_policy in product(
        ["us_large_liquid", "us_mid_large_liquid"],
        ["momentum_12_1", "quality_momentum", "value_quality_momentum"],
        ["monthly_last_trading_day", "quarterly_last_trading_day"],
        ["rebalance_only", "rank_decay_exit"],
    ):
        rows.append(
            {
                "name": f"{universe}__{ranking}__{rebalance}__{exit_policy}",
                "config": {
                    "componentRefs": {
                        "universe": {"name": universe, "version": 1},
                        "ranking": {"name": ranking, "version": 1},
                        "rebalance": {"name": rebalance, "version": 1},
                        "regimePolicy": {"name": "observe_only_default", "version": 1},
                        "riskPolicy": {"name": "balanced_long_only", "version": 1},
                        "exitPolicy": {"name": exit_policy, "version": 1},
                    },
                    "universeConfigName": universe,
                    "universeConfigVersion": 1,
                    "rankingSchemaName": ranking,
                    "rankingSchemaVersion": 1,
                    "regimePolicyConfigName": "observe_only_default",
                    "regimePolicyConfigVersion": 1,
                    "riskPolicyName": "balanced_long_only",
                    "riskPolicyVersion": 1,
                    "exitRuleSetName": exit_policy,
                    "exitRuleSetVersion": 1,
                    "rebalance": "monthly" if rebalance.startswith("monthly") else "quarterly",
                    "longOnly": True,
                    "topN": 25,
                    "lookbackWindow": 252,
                    "holdingPeriod": 21,
                    "costModel": "default",
                },
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed reusable strategy config starters and emit the 24-run matrix.")
    parser.add_argument("--dsn", default=os.environ.get("POSTGRES_DSN"))
    parser.add_argument("--seed", action="store_true", help="Seed starter presets into Postgres.")
    parser.add_argument("--emit-matrix", action="store_true", help="Print the first-pass 24-run matrix as JSON.")
    args = parser.parse_args()

    if args.seed:
        if not args.dsn:
            raise SystemExit("--dsn or POSTGRES_DSN is required when --seed is used.")
        seed_presets(str(args.dsn))
    if args.emit_matrix or not args.seed:
        print(json.dumps(first_pass_matrix(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
