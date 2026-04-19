from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core.backtest_request_resolution import resolve_backtest_request
from core.backtest_runtime import (
    ResolvedBacktestDefinition,
    resolve_backtest_definition_for_run,
)
from core.ranking_engine.contracts import RankingSchemaConfig
from core.strategy_engine.contracts import StrategyConfig, UniverseDefinition


def _sample_universe() -> UniverseDefinition:
    return UniverseDefinition.model_validate(
        {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {
                        "kind": "condition",
                        "field": "market.close",
                        "operator": "gt",
                        "value": 1,
                    }
                ],
            },
        }
    )


def _sample_definition(*, strategy_name: str | None = "mom-spy-res", strategy_version: int | None = 3) -> ResolvedBacktestDefinition:
    universe = _sample_universe()
    return ResolvedBacktestDefinition(
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        strategy_config=StrategyConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "rebalance": "weekly",
                "longOnly": True,
                "topN": 2,
                "lookbackWindow": 20,
                "holdingPeriod": 5,
                "costModel": "default",
                "rankingSchemaName": "quality",
                "intrabarConflictPolicy": "stop_first",
                "exits": [],
            }
        ),
        strategy_config_raw={
            "universeConfigName": "large-cap-quality",
            "rebalance": "weekly",
            "longOnly": True,
            "topN": 2,
            "lookbackWindow": 20,
            "holdingPeriod": 5,
            "costModel": "default",
            "rankingSchemaName": "quality",
            "intrabarConflictPolicy": "stop_first",
            "exits": [],
        },
        strategy_universe=universe,
        ranking_schema_name="quality",
        ranking_schema_version=7,
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "f1",
                                "table": "market_data",
                                "column": "return_20d",
                                "weight": 1,
                                "direction": "desc",
                                "missingValuePolicy": "exclude",
                                "transforms": [],
                            }
                        ],
                        "transforms": [],
                    }
                ],
                "overallTransforms": [],
            }
        ),
        ranking_universe_name="large-cap-quality",
        ranking_universe_version=5,
        ranking_universe=universe,
        regime_model_name="default-regime",
        regime_model_version=1,
        regime_model_config={"highVolEnterThreshold": 28.0},
    )


def test_resolve_backtest_request_supports_inline_strategy_config(monkeypatch: pytest.MonkeyPatch) -> None:
    inline_definition = _sample_definition(strategy_name=None, strategy_version=None)
    monkeypatch.setattr(
        "core.backtest_request_resolution.resolve_backtest_definition_from_config",
        lambda *args, **kwargs: inline_definition,
    )
    monkeypatch.setattr(
        "core.backtest_request_resolution.validate_backtest_submission",
        lambda *args, **kwargs: [
            datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
            datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        ],
    )

    resolved = resolve_backtest_request(
        "postgresql://test:test@localhost:5432/asset_allocation",
        strategy_ref=None,
        strategy_config=_sample_definition().strategy_config_raw,
        start_ts=datetime(2026, 3, 3, 14, 30, tzinfo=timezone.utc),
        end_ts=datetime(2026, 3, 3, 14, 35, tzinfo=timezone.utc),
        bar_size="5m",
    )

    assert resolved.input_mode == "inline"
    assert resolved.definition.strategy_name is None
    assert resolved.effective_config["inputMode"] == "inline"
    assert resolved.effective_config["pins"]["rankingSchemaVersion"] == 7
    assert resolved.request_payload["strategyConfig"]["rankingSchemaName"] == "quality"
    assert resolved.config_fingerprint
    assert resolved.request_fingerprint
    assert resolved.config_fingerprint != resolved.request_fingerprint


def test_resolve_backtest_definition_for_run_replays_inline_effective_config(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    inline_definition = _sample_definition(strategy_name=None, strategy_version=None)

    def fake_resolve_from_config(*args, **kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return inline_definition

    monkeypatch.setattr("core.backtest_runtime.resolve_backtest_definition_from_config", fake_resolve_from_config)

    resolved = resolve_backtest_definition_for_run(
        "postgresql://test:test@localhost:5432/asset_allocation",
        run={
            "run_id": "run-inline-1",
            "strategy_name": None,
            "effective_config": {
                "inputMode": "inline",
                "strategy": _sample_definition().strategy_config_raw,
                "pins": {
                    "rankingSchemaName": "quality",
                    "rankingSchemaVersion": 7,
                    "universeName": "large-cap-quality",
                    "universeVersion": 5,
                    "regimeModelName": "default-regime",
                    "regimeModelVersion": 1,
                },
            },
        },
    )

    assert resolved is inline_definition
    assert captured["ranking_schema_name"] == "quality"
    assert captured["ranking_schema_version"] == 7
    assert captured["universe_name"] == "large-cap-quality"
    assert captured["regime_model_name"] == "default-regime"


def test_resolve_backtest_definition_for_run_keeps_saved_strategy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = _sample_definition()

    def fake_resolve(*args, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["strategy_name"] == "quality-trend"
        assert kwargs["strategy_version"] == 4
        return expected

    monkeypatch.setattr("core.backtest_runtime.resolve_backtest_definition", fake_resolve)

    resolved = resolve_backtest_definition_for_run(
        "postgresql://test:test@localhost:5432/asset_allocation",
        run={
            "run_id": "run-saved-1",
            "strategy_name": "quality-trend",
            "strategy_version": 4,
            "regime_model_name": "default-regime",
            "regime_model_version": 1,
            "effective_config": {},
        },
    )

    assert resolved is expected
