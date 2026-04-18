from __future__ import annotations

from datetime import date

import pandas as pd

from core.ranking_engine.naming import build_scoped_identifier, slugify_strategy_output_table
from core.ranking_engine.service import (
    _apply_transforms,
    _compute_rankings_dataframe,
    _evaluate_universe_mask,
    _write_rankings_to_platinum,
)
from core.ranking_engine.contracts import RankingSchemaConfig
from core.strategy_engine import universe as universe_service
from core.strategy_engine.contracts import StrategyConfig


def test_slugify_strategy_output_table_normalizes_invalid_characters() -> None:
    assert slugify_strategy_output_table("My Strategy / 2026") == "my_strategy_2026"


def test_build_scoped_identifier_stays_within_postgres_limit() -> None:
    identifier = build_scoped_identifier("x" * 63, "symbol", "date", "idx")

    assert len(identifier) <= 63
    assert identifier.endswith("_idx")


def test_apply_transforms_runs_in_order() -> None:
    series = pd.Series([1.0, 2.0, 3.0])
    dates = pd.Series([date(2026, 3, 7), date(2026, 3, 7), date(2026, 3, 7)])
    transformed = _apply_transforms(
        series,
        dates,
        [
            type("Transform", (), {"type": "negate", "params": {}})(),
            type("Transform", (), {"type": "abs", "params": {}})(),
            type("Transform", (), {"type": "percentile_rank", "params": {}})(),
        ],
    )

    assert transformed.tolist() == [1 / 3, 2 / 3, 1.0]


def test_evaluate_universe_mask_handles_nested_groups() -> None:
    universe = {
        "source": "postgres_gold",
        "root": {
            "kind": "group",
            "operator": "and",
            "clauses": [
                {
                    "kind": "condition",
                    "field": "market.close",
                    "operator": "gt",
                    "value": 10,
                },
                {
                    "kind": "group",
                    "operator": "or",
                    "clauses": [
                        {
                            "kind": "condition",
                            "field": "quality.piotroski_f_score",
                            "operator": "gte",
                            "value": 7,
                        },
                        {
                            "kind": "condition",
                            "field": "returns.return_20d",
                            "operator": "gt",
                            "value": 0.1,
                        },
                    ],
                },
            ],
        },
    }
    frame = pd.DataFrame(
        {
            "market_data__close": [12.0, 9.0, 15.0],
            "finance_data__piotroski_f_score": [5, 8, 6],
            "market_data__return_20d": [0.05, 0.2, 0.15],
        }
    )

    mask = _evaluate_universe_mask(frame, universe["root"])

    assert mask.tolist() == [False, False, True]


def test_compute_rankings_dataframe_intersects_strategy_and_ranking_universes(monkeypatch) -> None:
    strategy_universe = universe_service._normalize_universe_definition(
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
                        "value": 10,
                    }
                ],
            },
        }
    )
    ranking_universe = universe_service._normalize_universe_definition(
        {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {
                        "kind": "condition",
                        "field": "quality.piotroski_f_score",
                        "operator": "gte",
                        "value": 7,
                    }
                ],
            },
        }
    )
    monkeypatch.setattr(
        "core.ranking_engine.service.universe_service._load_gold_table_specs",
        lambda _dsn: {},
    )
    monkeypatch.setattr("core.ranking_engine.service._resolve_strategy_universe", lambda _dsn, _config: strategy_universe)
    monkeypatch.setattr("core.ranking_engine.service._resolve_ranking_universe", lambda _dsn, _schema: ranking_universe)
    monkeypatch.setattr(
        "core.ranking_engine.service._load_table_frames",
        lambda _dsn, **kwargs: {
            "market_data": pd.DataFrame(
                {
                    "date": [date(2026, 3, 7), date(2026, 3, 7), date(2026, 3, 7)],
                    "symbol": ["AAPL", "MSFT", "TSLA"],
                    "market_data__close": [20.0, 22.0, 5.0],
                    "market_data__return_20d": [0.3, 0.2, 0.9],
                }
            ),
            "finance_data": pd.DataFrame(
                {
                    "date": [date(2026, 3, 7), date(2026, 3, 7), date(2026, 3, 7)],
                    "symbol": ["AAPL", "MSFT", "TSLA"],
                    "finance_data__piotroski_f_score": [8, 6, 9],
                }
            ),
        },
    )

    ranked = _compute_rankings_dataframe(
        "postgresql://test:test@localhost:5432/asset_allocation",
        strategy_config=StrategyConfig.model_validate(
            {
                "universeConfigName": "large-cap-quality",
                "rebalance": "monthly",
                "longOnly": True,
                "topN": 20,
                "lookbackWindow": 63,
                "holdingPeriod": 21,
                "costModel": "default",
                "intrabarConflictPolicy": "stop_first",
                "exits": [],
            }
        ),
        ranking_schema=RankingSchemaConfig.model_validate(
            {
                "universeConfigName": "quality-universe",
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "momentum",
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
        start_date=date(2026, 3, 7),
        end_date=date(2026, 3, 7),
    )

    assert ranked.to_dict("records") == [
        {
            "date": date(2026, 3, 7),
            "symbol": "AAPL",
            "score": 0.3,
            "rank": 1,
        }
    ]


class _FakeCursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_write_rankings_to_platinum_copies_score_and_rank(monkeypatch) -> None:
    cursor = _FakeCursor()
    copied: dict[str, object] = {}
    monkeypatch.setattr("core.ranking_engine.service.connect", lambda _dsn: _FakeConnection(cursor))
    monkeypatch.setattr(
        "core.ranking_engine.service.copy_rows",
        lambda _cur, *, table, columns, rows: copied.update(
            {
                "table": table,
                "columns": columns,
                "rows": list(rows),
            }
        ),
    )

    row_count = _write_rankings_to_platinum(
        "postgresql://test:test@localhost:5432/asset_allocation",
        table_name="mom_spy_res",
        ranked=pd.DataFrame(
            [
                {
                    "date": date(2026, 3, 7),
                    "symbol": "AAPL",
                    "score": 0.91,
                    "rank": 1,
                }
            ]
        ),
        start_date=date(2026, 3, 7),
        end_date=date(2026, 3, 7),
    )

    assert row_count == 1
    assert copied["columns"] == ("date", "symbol", "rank", "score", "last_updated_date")
    assert copied["rows"][0][:4] == (date(2026, 3, 7), "AAPL", 1, 0.91)
