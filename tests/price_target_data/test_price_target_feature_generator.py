import numpy as np
import pandas as pd
import pytest

from tasks.price_target_data.gold_price_target_data import compute_features


def _make_price_target_df(rows: int = 200, freq: str = "D") -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq=freq)
    mean_target = 100.0 + 0.1 * np.arange(rows, dtype=float)

    return pd.DataFrame(
        {
            "symbol": ["AAPL"] * rows,
            "obs_date": dates.astype(str),
            "tp_mean_est": mean_target,
            "tp_std_dev_est": mean_target * 0.05,
            "tp_high_est": mean_target * 1.1,
            "tp_low_est": mean_target * 0.9,
            "tp_cnt_est": [10] * rows,
            "tp_cnt_est_rev_up": [2] * rows,
            "tp_cnt_est_rev_down": [1] * rows,
        }
    )


def test_compute_features_adds_expected_columns():
    df = _make_price_target_df(200)
    out = compute_features(df)

    expected = {
        "disp_abs",
        "disp_norm",
        "disp_std_norm",
        "rev_net",
        "rev_ratio",
        "rev_intensity",
        "disp_norm_change_30d",
        "tp_mean_change_30d",
        "tp_mean_slope_90d",
    }

    assert expected.issubset(set(out.columns))
    assert len(out) == 200


def test_compute_features_sanity_on_linear_series():
    df = _make_price_target_df(200)
    out = compute_features(df)
    last = out.iloc[-1]

    assert float(last["disp_norm"]) == pytest.approx(0.2, abs=1e-9)
    assert float(last["disp_std_norm"]) == pytest.approx(0.05, abs=1e-9)
    assert float(last["rev_ratio"]) == pytest.approx(1.5, abs=1e-9)
    assert float(last["rev_intensity"]) == pytest.approx(0.1, abs=1e-9)

    assert float(last["disp_norm_change_30d"]) == pytest.approx(0.0, abs=1e-9)
    assert float(last["tp_mean_change_30d"]) == pytest.approx(3.0, abs=1e-9)

    assert float(last["tp_mean_slope_90d"]) == pytest.approx(0.1, abs=1e-9)


def test_compute_features_resamples_irregular_updates_to_daily():
    df = _make_price_target_df(rows=10, freq="2D")
    out = compute_features(df)

    assert len(out) == 19
    obs_date = pd.to_datetime(out["obs_date"])
    diffs = obs_date.diff().dropna()
    assert diffs.min() == pd.Timedelta(days=1)


def test_compute_features_requires_expected_columns():
    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(pd.DataFrame({"symbol": ["AAPL"], "obs_date": ["2020-01-01"]}))


def test_compute_features_requires_symbol_column():
    df = _make_price_target_df(10).rename(columns={"symbol": "ticker"})

    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(df)
