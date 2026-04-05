import numpy as np
import pandas as pd
import pytest

from tasks.earnings_data.gold_earnings_data import compute_features


def _make_earnings_df(rows: int = 12) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq="3MS")
    estimate = pd.Series(np.ones(rows))
    reported = estimate + 0.1

    return pd.DataFrame(
        {
            "Date": dates.astype(str),
            "Symbol": ["AAPL"] * rows,
            "Reported EPS": reported,
            "EPS Estimate": estimate,
        }
    )


def test_compute_features_adds_expected_columns():
    df = _make_earnings_df(12)
    out = compute_features(df)

    expected = {"surprise_pct", "surprise_mean_4q", "surprise_std_8q", "beat_rate_8q"}
    assert expected.issubset(set(out.columns))
    # daily resampling should expand the 12 quarterly rows into a contiguous date range
    expected_days = (out["date"].max() - out["date"].min()).days + 1
    assert len(out) == expected_days
    # ensure earnings flags align with the original quarterly dates
    earnings_days = out.loc[out["is_earnings_day"] == 1.0]
    assert earnings_days["days_since_earnings"].eq(0).all()


def test_compute_features_rolls_over_quarters():
    rows = 8
    dates = pd.date_range("2020-01-01", periods=rows, freq="3MS")
    estimate = pd.Series(np.ones(rows))
    surprises = pd.Series([0.1] * 7 + [-0.1])
    reported = estimate + surprises

    df = pd.DataFrame(
        {
            "Date": dates.astype(str),
            "Symbol": ["AAPL"] * rows,
            "Reported EPS": reported,
            "EPS Estimate": estimate,
        }
    )

    out = compute_features(df)
    last = out.iloc[-1]

    assert float(last["surprise_mean_4q"]) == pytest.approx(0.05, abs=1e-9)
    assert float(last["beat_rate_8q"]) == pytest.approx(0.875, abs=1e-9)
    assert float(last["surprise_std_8q"]) == pytest.approx(np.sqrt(0.005), abs=1e-9)


def test_compute_features_handles_divide_by_zero():
    df = pd.DataFrame(
        {
            "Date": ["2020-01-01"],
            "Symbol": ["AAPL"],
            "Reported EPS": [1.0],
            "EPS Estimate": [0.0],
        }
    )

    out = compute_features(df)
    assert pd.isna(out.iloc[0]["surprise_pct"])


def test_compute_features_requires_expected_columns():
    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(pd.DataFrame({"Date": ["2020-01-01"], "Symbol": ["AAPL"]}))
