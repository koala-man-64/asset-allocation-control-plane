from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

import core.regime as local_regime
from asset_allocation_runtime_common.domain import regime as shared_regime


def _signal_by_code(classification: dict[str, Any], regime_code: str) -> dict[str, Any]:
    return next(signal for signal in classification["signals"] if signal["regime_code"] == regime_code)


def test_compute_states_use_deadbands() -> None:
    assert shared_regime.compute_trend_state(0.03) == "positive"
    assert shared_regime.compute_trend_state(-0.03) == "negative"
    assert shared_regime.compute_trend_state(0.01) == "near_zero"

    assert shared_regime.compute_curve_state(0.6) == "contango"
    assert shared_regime.compute_curve_state(-0.6) == "inverted"
    assert shared_regime.compute_curve_state(0.1) == "flat"


def test_classify_regime_row_uses_canonical_default_without_transition_band() -> None:
    cold_start = shared_regime.classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": 0.0,
            "vix_slope": 0.0,
            "rvol_10d_ann": 26.5,
            "vix_spot_close": 24.0,
            "vix_gt_32_streak": 0,
        }
    )

    unclassified = _signal_by_code(cold_start, "unclassified")

    assert cold_start["active_regimes"] == ["unclassified"]
    assert unclassified["signal_state"] == "active"
    assert unclassified["matched_rule_id"] == "unclassified"
    assert "regime_code" not in cold_start


def test_classify_regime_row_can_use_activation_threshold_override() -> None:
    row = {
        "inputs_complete_flag": True,
        "return_20d": 0.0,
        "vix_slope": 0.0,
        "vix_spot_close": 26.0,
        "vix_gt_32_streak": 0,
        "spy_close": 100.0,
        "atr_14d": 2.0,
        "gap_atr": 0.0,
    }
    config = {"activationThreshold": 0.3}

    default_result = shared_regime.classify_regime_row(row)
    overridden = shared_regime.classify_regime_row(row, config=config)

    default_high_vol = _signal_by_code(default_result, "high_volatility")
    overridden_high_vol = _signal_by_code(overridden, "high_volatility")
    assert default_result["active_regimes"] == ["unclassified"]
    assert default_high_vol["signal_state"] == "inactive"
    assert overridden["active_regimes"] == ["high_volatility"]
    assert overridden_high_vol["signal_state"] == "active"
    assert overridden_high_vol["matched_rule_id"] == "high_volatility"


def test_local_regime_facade_matches_shared_runtime_defaults() -> None:
    row = {
        "inputs_complete_flag": True,
        "return_20d": -0.01,
        "vix_slope": 0.1,
        "rvol_10d_ann": 26.5,
        "vix_spot_close": 24.0,
        "vix_gt_32_streak": 0,
    }

    assert local_regime.classify_regime_row(row) == shared_regime.classify_regime_row(row)


def test_local_regime_facade_matches_shared_runtime_activation_threshold_override() -> None:
    row = {
        "inputs_complete_flag": True,
        "return_20d": 0.0,
        "vix_slope": 0.0,
        "vix_spot_close": 26.0,
        "vix_gt_32_streak": 0,
        "spy_close": 100.0,
        "atr_14d": 2.0,
        "gap_atr": 0.0,
    }
    config = {"activationThreshold": 0.3}

    assert local_regime.classify_regime_row(row, config=config) == shared_regime.classify_regime_row(
        row,
        config=config,
    )


def test_classify_regime_row_sets_high_vol_and_halt_overlay() -> None:
    row = shared_regime.classify_regime_row(
        {
            "inputs_complete_flag": True,
            "return_20d": -0.04,
            "vix_slope": -1.1,
            "rvol_10d_ann": 30.2,
            "vix_spot_close": 35.0,
            "vix_gt_32_streak": 2,
            "spy_close": 100.0,
            "atr_14d": 5.0,
            "gap_atr": 0.75,
        }
    )

    high_vol = _signal_by_code(row, "high_volatility")

    assert row["active_regimes"] == ["high_volatility"]
    assert high_vol["signal_state"] == "active"
    assert high_vol["matched_rule_id"] == "high_volatility"
    assert row["halt_flag"] is True
    assert row["halt_reason"] == "high_volatility_and_stress_cluster"


def test_build_regime_outputs_uses_next_input_date_as_effective_date() -> None:
    inputs = pd.DataFrame(
        [
            {
                "as_of_date": "2026-03-02",
                "return_1d": 0.01,
                "return_20d": 0.04,
                "spy_close": 120.0,
                "sma_200d": 100.0,
                "qqq_close": 110.0,
                "qqq_sma_200d": 100.0,
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
                "spy_close": 80.0,
                "sma_200d": 100.0,
                "qqq_close": 90.0,
                "qqq_sma_200d": 100.0,
                "rvol_10d_ann": 18.0,
                "vix_spot_close": 24.0,
                "vix3m_close": 23.2,
                "vix_slope": -0.8,
                "vix_gt_32_streak": 0,
                "inputs_complete_flag": True,
            },
        ]
    )

    history, latest, transitions = shared_regime.build_regime_outputs(
        inputs,
        model_name="default-regime",
        model_version=1,
        computed_at=datetime(2026, 3, 8, tzinfo=timezone.utc),
    )

    assert history["effective_from_date"].tolist()[0].isoformat() == "2026-03-03"
    assert latest.iloc[0]["as_of_date"].isoformat() == "2026-03-03"
    assert transitions[["regime_code", "transition_type"]].to_dict("records") == [
        {"regime_code": "trending_up", "transition_type": "entered"},
        {"regime_code": "trending_up", "transition_type": "exited"},
        {"regime_code": "trending_down", "transition_type": "entered"},
    ]
