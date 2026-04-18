from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from asset_allocation_runtime_common.domain.regime import build_regime_outputs, classify_regime_row, compute_curve_state, compute_trend_state
def test_compute_states_use_deadbands() -> None:
    assert compute_trend_state(0.03) == "positive"
    assert compute_trend_state(-0.03) == "negative"
    assert compute_trend_state(0.01) == "near_zero"

    assert compute_curve_state(0.6) == "contango"
    assert compute_curve_state(-0.6) == "inverted"
    assert compute_curve_state(0.1) == "flat"


def test_classify_regime_row_handles_transition_band_and_cold_start() -> None:
    cold_start = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": 0.0,
            "vix_slope": 0.0,
            "rvol_10d_ann": 26.5,
            "vix_spot_close": 24.0,
            "vix_gt_32_streak": 0,
        }
    )
    assert cold_start["regime_code"] == "unclassified"
    assert cold_start["regime_status"] == "unclassified"
    assert cold_start["matched_rule_id"] == "transition_band"

    follow_on = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": 0.0,
            "vix_slope": 0.2,
            "rvol_10d_ann": 26.0,
            "vix_spot_close": 24.0,
            "vix_gt_32_streak": 0,
        },
        prev_confirmed_regime="trending_bear",
    )
    assert follow_on["regime_code"] == "trending_bear"
    assert follow_on["regime_status"] == "transition"


def test_classify_regime_row_sets_high_vol_and_halt_overlay() -> None:
    row = classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": -0.04,
            "vix_slope": -1.1,
            "rvol_10d_ann": 30.2,
            "vix_spot_close": 35.0,
            "vix_gt_32_streak": 2,
        }
    )

    assert row["regime_code"] == "high_vol"
    assert row["regime_status"] == "confirmed"
    assert row["halt_flag"] is True
    assert row["halt_reason"] == "vix_spot_close_gt_32_for_2_days"


def test_build_regime_outputs_uses_next_input_date_as_effective_date() -> None:
    inputs = pd.DataFrame(
        [
            {
                "as_of_date": "2026-03-02",
                "return_1d": 0.01,
                "return_20d": 0.04,
                "rvol_10d_ann": 12.0,
                "vix_spot_close": 18.0,
                "vix3m_close": 18.7,
                "vix_slope": 0.7,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
            {
                "as_of_date": "2026-03-03",
                "return_1d": -0.02,
                "return_20d": -0.05,
                "rvol_10d_ann": 18.0,
                "vix_spot_close": 24.0,
                "vix3m_close": 23.2,
                "vix_slope": -0.8,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
        ]
    )

    history, latest, transitions = build_regime_outputs(
        inputs,
        model_name="default-regime",
        model_version=1,
        computed_at=datetime(2026, 3, 8, tzinfo=timezone.utc),
    )

    assert history["effective_from_date"].tolist()[0].isoformat() == "2026-03-03"
    assert latest.iloc[0]["as_of_date"].isoformat() == "2026-03-03"
    assert transitions["new_regime_code"].tolist() == ["trending_bull", "trending_bear"]
