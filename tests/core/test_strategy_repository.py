from __future__ import annotations

import json

from core.strategy_repository import StrategyRepository, normalize_strategy_config_document


class _FakeCursor:
    def __init__(self, *, fetchone_result=None, fetchone_results=None, fetchall_result=None) -> None:
        self.fetchone_result = fetchone_result
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_result = fetchall_result or []
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_get_strategy_config_reads_from_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_result=(
            {
                "universe": {
                    "source": "postgres_gold",
                    "root": {
                        "kind": "group",
                        "operator": "and",
                        "clauses": [
                            {
                                "kind": "condition",
                                "table": "market_data",
                                "column": "close",
                                "operator": "gt",
                                "value": 10,
                            }
                        ],
                    },
                }
            },
        )
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.get_strategy_config("momentum") == {
        "universe": {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {
                        "kind": "condition",
                        "table": "market_data",
                        "column": "close",
                        "operator": "gt",
                        "value": 10,
                    }
                ],
            },
        }
    }
    sql, params = cursor.execute_calls[0]
    assert "FROM core.strategies" in sql
    assert params == ("momentum",)


def test_save_strategy_writes_to_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor()

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")
    repo.save_strategy(
        name="momentum",
        config={"rebalance": "monthly"},
        strategy_type="configured",
        description="Monthly momentum",
    )

    sql, params = cursor.execute_calls[0]
    assert "INSERT INTO core.strategies" in sql
    assert params == (
        "momentum",
        json.dumps({"rebalance": "monthly"}),
        "configured",
        "Monthly momentum",
        "momentum",
    )


def test_get_strategy_reads_metadata_and_config(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_result=(
            "momentum",
            "configured",
            "Monthly momentum",
            "momentum",
            "2026-03-07T00:00:00Z",
            {"rebalance": "monthly"},
        )
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.get_strategy("momentum") == {
        "name": "momentum",
        "type": "configured",
        "description": "Monthly momentum",
        "output_table_name": "momentum",
        "updated_at": "2026-03-07T00:00:00Z",
        "config": {"rebalance": "monthly"},
    }
    sql, params = cursor.execute_calls[0]
    assert "SELECT name, type, description, output_table_name, updated_at, config" in sql
    assert params == ("momentum",)


def test_list_strategies_reads_from_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchall_result=[
            ("momentum", "configured", "Monthly momentum", "momentum", "2026-03-07T00:00:00Z"),
        ]
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.list_strategies() == [
        {
            "name": "momentum",
            "type": "configured",
            "description": "Monthly momentum",
            "output_table_name": "momentum",
            "updated_at": "2026-03-07T00:00:00Z",
        }
    ]
    sql, params = cursor.execute_calls[0]
    assert "FROM core.strategies" in sql
    assert params is None


def test_delete_strategy_deletes_from_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor(fetchone_result=("momentum",))

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.delete_strategy("momentum") is True
    sql, params = cursor.execute_calls[0]
    assert "DELETE FROM core.strategies" in sql
    assert "RETURNING name" in sql
    assert params == ("momentum",)


def test_normalize_strategy_config_document_removes_disabled_structures() -> None:
    normalized = normalize_strategy_config_document(
        {
            "rebalance": "monthly",
            "regimePolicy": {
                "enabled": False,
                "modelName": "legacy-regime",
            },
            "exits": [
                {"enabled": False, "kind": "stop_loss"},
                {"enabled": True, "kind": "take_profit", "threshold": 0.1},
            ],
        }
    )

    assert "regimePolicy" not in normalized
    assert normalized["exits"] == [{"kind": "take_profit", "threshold": 0.1}]


def test_save_strategy_strips_legacy_enabled_fields(monkeypatch) -> None:
    cursor = _FakeCursor()

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")
    repo.save_strategy(
        name="momentum",
        config={
            "rebalance": "monthly",
            "regimePolicy": {"enabled": True, "modelName": "steady"},
            "exits": [
                {"enabled": False, "kind": "stop_loss"},
                {"enabled": True, "kind": "take_profit", "threshold": 0.2},
            ],
        },
        strategy_type="configured",
        description="Monthly momentum",
    )

    _sql, params = cursor.execute_calls[0]
    assert params is not None
    persisted = json.loads(params[1])
    assert persisted["regimePolicy"] == {"modelName": "steady"}
    assert persisted["exits"] == [{"kind": "take_profit", "threshold": 0.2}]


def test_save_strategy_round_trips_structured_policy_fields(monkeypatch) -> None:
    cursor = _FakeCursor()

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")
    repo.save_strategy(
        name="momentum",
        config={
            "rebalance": "weekly",
            "rebalancePolicy": {
                "frequency": "every_bar",
                "executionTiming": "next_bar_open",
                "driftThresholdPct": 2.0,
                "minTradeNotional": 100.0,
            },
            "strategyRiskPolicy": {
                "stopLoss": {
                    "thresholdPct": 8.0,
                    "action": "reduce_exposure",
                    "reductionPct": 50.0,
                },
                "reentry": {"cooldownBars": 3, "requireApproval": True},
            },
        },
        strategy_type="configured",
        description="Policy momentum",
    )

    _sql, params = cursor.execute_calls[0]
    assert params is not None
    persisted = json.loads(params[1])
    assert persisted["rebalancePolicy"]["frequency"] == "every_bar"
    assert persisted["rebalancePolicy"]["driftThresholdPct"] == 2.0
    assert persisted["strategyRiskPolicy"]["stopLoss"]["thresholdPct"] == 8.0
    assert persisted["strategyRiskPolicy"]["reentry"]["requireApproval"] is True


def test_save_strategy_resolves_pinned_configuration_libraries(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_results=[
            ("quality-ranking", 7, "Ranking", {"universeConfigName": "large-cap-quality"}),
            ("large-cap-quality", 5, "Universe", {"source": "postgres_gold"}),
            ("observe-default", 2, "Regime policy", {"modelName": "default-regime", "modelVersion": 3, "mode": "observe_only"}),
            (
                "balanced-risk",
                4,
                "Risk policy",
                {
                    "policy": {
                        "scope": "strategy",
                        "stopLoss": {"thresholdPct": 8, "action": "reduce_exposure", "reductionPct": 50},
                    }
                },
            ),
            (
                "standard-exits",
                6,
                "Exit rules",
                {
                    "intrabarConflictPolicy": "priority_order",
                    "exits": [{"id": "stop-8", "type": "stop_loss_fixed", "value": 0.08}],
                },
            ),
            (2,),
        ]
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")
    repo.save_strategy(
        name="momentum",
        config={
            "rankingSchemaName": "quality-ranking",
            "regimePolicyConfigName": "observe-default",
            "riskPolicyName": "balanced-risk",
            "exitRuleSetName": "standard-exits",
        },
        strategy_type="configured",
        description="Pinned momentum",
    )

    insert_sql, insert_params = cursor.execute_calls[5]
    assert "INSERT INTO core.strategies" in insert_sql
    assert insert_params is not None
    persisted = json.loads(insert_params[1])
    assert persisted["rankingSchemaVersion"] == 7
    assert persisted["universeConfigName"] == "large-cap-quality"
    assert persisted["universeConfigVersion"] == 5
    assert persisted["regimePolicyConfigVersion"] == 2
    assert persisted["regimePolicy"]["modelVersion"] == 3
    assert persisted["riskPolicyVersion"] == 4
    assert persisted["strategyRiskPolicy"]["stopLoss"]["thresholdPct"] == 8
    assert persisted["exitRuleSetVersion"] == 6
    assert persisted["intrabarConflictPolicy"] == "priority_order"
    assert persisted["exits"][0]["id"] == "stop-8"
