import pandas as pd

from tasks.common.gold_output_contracts import (
    GOLD_EARNINGS_OUTPUT_COLUMNS,
    GOLD_MARKET_OUTPUT_COLUMNS,
    GOLD_PRICE_TARGET_OUTPUT_COLUMNS,
    empty_gold_output_frame,
    project_gold_output_frame,
)


def test_project_gold_output_frame_projects_earnings_contract_and_drops_intermediate_columns() -> None:
    projected = project_gold_output_frame(
        pd.DataFrame(
            [
                {
                    "Date": "2026-03-01",
                    "Symbol": "aapl",
                    "Reported EPS": "1.23",
                    "EPS Estimate": "1.11",
                    "Calendar Time Of Day": "post-market",
                    "Next Earnings Time Of Day": "post-market",
                    "Has Upcoming Earnings": 1,
                    "Is Scheduled Earnings Day": 0,
                    "helper_column": "drop-me",
                }
            ]
        ),
        domain="earnings",
    )

    assert list(projected.columns) == list(GOLD_EARNINGS_OUTPUT_COLUMNS)
    assert projected.loc[0, "symbol"] == "AAPL"
    assert projected.loc[0, "date"] == pd.Timestamp("2026-03-01")
    assert projected.loc[0, "reported_eps"] == 1.23
    assert projected.loc[0, "next_earnings_time_of_day"] == "post-market"
    assert projected.loc[0, "has_upcoming_earnings"] == 1
    assert projected.loc[0, "is_scheduled_earnings_day"] == 0
    assert "calendar_time_of_day" not in projected.columns
    assert "helper_column" not in projected.columns
    assert str(projected["symbol"].dtype) == "string"
    assert str(projected["has_upcoming_earnings"].dtype) == "Int64"


def test_project_gold_output_frame_projects_market_contract_and_coerces_types() -> None:
    projected = project_gold_output_frame(
        pd.DataFrame(
            [
                {
                    "Date": "2026-01-02",
                    "Symbol": "msft",
                    "Close": "100.5",
                    "PAT Doji": "1",
                    "helper_flag": True,
                }
            ]
        ),
        domain="market",
    )

    assert list(projected.columns) == list(GOLD_MARKET_OUTPUT_COLUMNS)
    assert projected.loc[0, "symbol"] == "MSFT"
    assert projected.loc[0, "date"] == pd.Timestamp("2026-01-02")
    assert projected.loc[0, "close"] == 100.5
    assert projected.loc[0, "pat_doji"] == 1
    assert str(projected["symbol"].dtype) == "string"
    assert str(projected["pat_doji"].dtype) == "Int64"
    assert pd.isna(projected.loc[0, "open"])
    assert "helper_flag" not in projected.columns


def test_project_gold_output_frame_projects_price_target_contract() -> None:
    projected = project_gold_output_frame(
        pd.DataFrame(
            [
                {
                    "Obs Date": "2026-02-14",
                    "Symbol": "nvda",
                    "TP Mean Est": "220.5",
                    "TP Cnt Est": "17",
                    "extra_metric": 99,
                }
            ]
        ),
        domain="price-target",
    )

    assert list(projected.columns) == list(GOLD_PRICE_TARGET_OUTPUT_COLUMNS)
    assert projected.loc[0, "symbol"] == "NVDA"
    assert projected.loc[0, "obs_date"] == pd.Timestamp("2026-02-14")
    assert projected.loc[0, "tp_mean_est"] == 220.5
    assert projected.loc[0, "tp_cnt_est"] == 17.0
    assert "extra_metric" not in projected.columns


def test_empty_gold_output_frame_returns_typed_contract_frame() -> None:
    empty_frame = empty_gold_output_frame(domain="earnings")

    assert empty_frame.empty
    assert list(empty_frame.columns) == list(GOLD_EARNINGS_OUTPUT_COLUMNS)
    assert str(empty_frame["date"].dtype) == "datetime64[ns]"
    assert str(empty_frame["symbol"].dtype) == "string"
    assert str(empty_frame["has_upcoming_earnings"].dtype) == "Int64"
