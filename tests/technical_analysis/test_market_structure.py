from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from tasks.technical_analysis.market_structure import add_market_structure_features


def _make_structure_df(close_values: list[float], *, atr_value: float = 2.0) -> pd.DataFrame:
    start = datetime(2024, 1, 1)
    return pd.DataFrame(
        {
            "date": [start + timedelta(days=index) for index in range(len(close_values))],
            "symbol": ["TEST"] * len(close_values),
            "high": [value + 0.5 for value in close_values],
            "low": [value - 0.5 for value in close_values],
            "close": close_values,
            "atr_14d": [atr_value] * len(close_values),
        }
    )


def test_donchian_channels_use_completed_history_only() -> None:
    out = add_market_structure_features(_make_structure_df([float(value) for value in range(100, 126)]))

    assert pd.isna(out.iloc[19]["donchian_high_20d"])
    assert out.iloc[20]["donchian_high_20d"] == pytest.approx(119.5)
    assert out.iloc[20]["crosses_above_donchian_high_20d"] == 1


def test_support_zone_waits_for_pivot_confirmation() -> None:
    closes = [
        30.0,
        29.0,
        28.0,
        27.0,
        26.0,
        25.0,
        24.0,
        23.0,
        22.0,
        21.0,
        20.0,
        19.0,
        18.0,
        17.0,
        16.0,
        10.0,
        17.0,
        18.0,
        19.0,
        20.0,
        21.0,
        22.0,
        23.0,
        24.0,
        25.0,
        26.0,
        27.0,
        28.0,
        29.0,
        30.0,
    ]
    out = add_market_structure_features(_make_structure_df(closes))

    assert pd.isna(out.iloc[17]["sr_support_1_mid"])
    assert out.iloc[17]["sr_support_1_touches"] == 0

    assert out.iloc[18]["sr_support_1_mid"] == pytest.approx(9.5)
    assert out.iloc[18]["sr_support_1_touches"] == 1


def test_fibonacci_levels_use_latest_confirmed_opposite_pivots() -> None:
    closes = [
        20.0,
        19.0,
        18.0,
        17.0,
        16.0,
        15.0,
        14.0,
        13.0,
        12.0,
        11.0,
        10.0,
        11.0,
        12.0,
        13.0,
        14.0,
        15.0,
        16.0,
        17.0,
        18.0,
        19.0,
        20.0,
        19.0,
        18.0,
        17.0,
        16.0,
        15.0,
        14.0,
        13.0,
    ]
    out = add_market_structure_features(_make_structure_df(closes))

    row = out.iloc[23]
    assert row["fib_swing_direction"] == 1
    assert row["fib_anchor_low"] == pytest.approx(9.5)
    assert row["fib_anchor_high"] == pytest.approx(20.5)
    assert row["fib_level_500"] == pytest.approx(15.0)
