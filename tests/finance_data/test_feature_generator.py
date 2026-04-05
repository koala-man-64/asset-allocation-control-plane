import numpy as np
import pandas as pd
import pytest

from tasks.finance_data.gold_finance_data import _preflight_feature_schema, compute_features


def _make_finance_df(rows: int = 8) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=rows, freq="3MS")
    revenue = np.linspace(100, 200, rows)

    return pd.DataFrame(
        {
            "Date": dates,
            "Symbol": ["AAPL"] * rows,
            "Total Revenue": revenue,
            "Gross Profit": revenue * 0.4,
            "Net Income": revenue * 0.12,
            "Operating Cash Flow": revenue * 0.25,
            "Long Term Debt": np.full(rows, 250.0),
            "Total Assets": np.full(rows, 1000.0),
            "Current Assets": np.full(rows, 500.0),
            "Current Liabilities": np.full(rows, 250.0),
            "Shares Outstanding": np.full(rows, 100.0),
        }
    )


def test_compute_features_adds_expected_columns() -> None:
    df = _make_finance_df(8)

    out = compute_features(df)

    expected_cols = {
        "rev_qoq",
        "rev_yoy",
        "net_inc_yoy",
        "gross_margin",
        "current_ratio_stmt",
        "piotroski_f_score",
        "roa_ttm",
    }
    assert expected_cols.issubset(set(out.columns))
    assert len(out) == 8


def test_compute_features_accepts_iso_date_strings() -> None:
    df = _make_finance_df(4)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

    out = compute_features(df)

    assert len(out) == 4
    assert out.iloc[-1]["date"] == pd.Timestamp("2020-10-01")


def test_compute_features_preserves_optional_valuation_metrics() -> None:
    df = _make_finance_df(8)
    df["market_cap"] = np.linspace(1_000_000, 1_100_000, len(df))
    df["pe_ratio"] = np.linspace(20.0, 21.0, len(df))
    df["current_ratio"] = np.linspace(1.2, 1.6, len(df))

    out = compute_features(df)

    assert out.iloc[-1]["market_cap"] == pytest.approx(1_100_000.0)
    assert out.iloc[-1]["pe_ratio"] == pytest.approx(21.0)
    assert out.iloc[-1]["current_ratio"] == pytest.approx(1.6)


def test_piotroski_score_calculation() -> None:
    dates = pd.date_range("2020-01-01", periods=8, freq="3MS")
    df = pd.DataFrame(
        {
            "Date": dates,
            "Symbol": ["TEST"] * 8,
            "Total Revenue": [100] * 8,
            "Gross Profit": [50, 50, 50, 50, 60, 60, 60, 60],
            "Net Income": [10] * 8,
            "Operating Cash Flow": [20] * 8,
            "Long Term Debt": [500, 500, 500, 500, 400, 400, 400, 400],
            "Total Assets": [1000] * 8,
            "Current Assets": [200, 200, 200, 200, 300, 300, 300, 300],
            "Current Liabilities": [100] * 8,
            "Shares Outstanding": [100] * 8,
        }
    )

    out = compute_features(df)
    last = out.iloc[-1]

    assert last["piotroski_roa_pos"] == 1
    assert last["piotroski_cfo_pos"] == 1
    assert last["piotroski_leverage_decrease"] == 1
    assert last["piotroski_liquidity_increase"] == 1
    assert 0 <= last["piotroski_f_score"] <= 9


def test_missing_required_columns() -> None:
    with pytest.raises(ValueError, match="Missing required columns"):
        compute_features(pd.DataFrame({"Date": ["2020-01-01"]}))


def test_parse_human_number_integration() -> None:
    df = pd.DataFrame(
        {
            "Date": ["01/01/2020"],
            "Symbol": ["AAPL"],
            "Total Revenue": ["10M"],
            "Gross Profit": ["4M"],
            "Net Income": ["1M"],
            "Operating Cash Flow": ["1.5M"],
            "Long Term Debt": [80],
            "Total Assets": [500],
            "Current Assets": [200],
            "Current Liabilities": [100],
            "Shares Outstanding": [100],
        }
    )

    out = compute_features(df)

    assert out.iloc[0]["total_revenue"] == 10_000_000.0


def test_preflight_accepts_minimal_piotroski_contract() -> None:
    preflight = _preflight_feature_schema(_make_finance_df(4))

    assert preflight["missing_requirements"] == []


def test_preflight_reports_missing_piotroski_inputs() -> None:
    df = _make_finance_df(4).drop(columns=["Shares Outstanding"])

    preflight = _preflight_feature_schema(df)

    assert any("shares_outstanding" in item for item in preflight["missing_requirements"])
