from __future__ import annotations

import pytest

from core.ranking_engine.contracts import RankingSchemaConfig


def test_ranking_schema_rejects_duplicate_group_names() -> None:
    with pytest.raises(ValueError, match="Duplicate group name"):
        RankingSchemaConfig.model_validate(
            {
                "groups": [
                    {"name": "quality", "weight": 1, "factors": [{"name": "f1", "table": "market_data", "column": "close", "weight": 1}]},
                    {"name": "quality", "weight": 1, "factors": [{"name": "f2", "table": "market_data", "column": "volume", "weight": 1}]},
                ]
            }
        )


def test_ranking_schema_validates_transform_params() -> None:
    with pytest.raises(ValueError, match="winsorize requires"):
        RankingSchemaConfig.model_validate(
            {
                "groups": [
                    {
                        "name": "quality",
                        "weight": 1,
                        "factors": [
                            {
                                "name": "f1",
                                "table": "market_data",
                                "column": "close",
                                "weight": 1,
                                "transforms": [{"type": "winsorize", "params": {}}],
                            }
                        ],
                    }
                ]
            }
        )


def test_ranking_schema_accepts_group_and_factor_transforms() -> None:
    schema = RankingSchemaConfig.model_validate(
        {
            "groups": [
                {
                    "name": "quality",
                    "weight": 1,
                    "transforms": [{"type": "percentile_rank", "params": {}}],
                    "factors": [
                        {
                            "name": "f1",
                            "table": "market_data",
                            "column": "close",
                            "weight": 1,
                            "direction": "desc",
                            "missingValuePolicy": "exclude",
                            "transforms": [{"type": "zscore", "params": {}}],
                        }
                    ],
                }
            ],
            "overallTransforms": [{"type": "clip", "params": {"lower": 0, "upper": 1}}],
        }
    )

    assert schema.groups[0].factors[0].table == "market_data"
    assert schema.overallTransforms[0].type == "clip"
