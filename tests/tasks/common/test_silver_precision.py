import pandas as pd

from tasks.common.silver_precision import apply_precision_policy, round_series_half_up


def test_round_series_half_up_handles_ties_for_positive_and_negative_values():
    series = pd.Series([1.005, 1.0049, -1.005, -1.0049])

    out = round_series_half_up(series, scale=2)

    assert [f"{value:.2f}" for value in out] == ["1.01", "1.00", "-1.01", "-1.00"]


def test_round_series_half_up_supports_four_decimal_places():
    series = pd.Series([0.12345, 0.12344, -0.12345])

    out = round_series_half_up(series, scale=4)

    assert [f"{value:.4f}" for value in out] == ["0.1235", "0.1234", "-0.1235"]


def test_apply_precision_policy_rounds_only_target_columns_and_preserves_non_targets():
    df = pd.DataFrame(
        {
            "open": ["1.005", "2.334", None],
            "tp_std_dev_est": ["0.12345", "bad-value", None],
            "volume": [10, 20, 30],
            "symbol": ["A", "B", "C"],
        }
    )

    out = apply_precision_policy(
        df,
        price_columns={"open"},
        calculated_columns={"tp_std_dev_est"},
        price_scale=2,
        calculated_scale=4,
    )

    assert [f"{value:.2f}" if pd.notna(value) else "nan" for value in out["open"]] == ["1.01", "2.33", "nan"]
    assert [f"{value:.4f}" if pd.notna(value) else "nan" for value in out["tp_std_dev_est"]] == [
        "0.1235",
        "nan",
        "nan",
    ]
    assert out["volume"].tolist() == [10, 20, 30]
    assert out["symbol"].tolist() == ["A", "B", "C"]
