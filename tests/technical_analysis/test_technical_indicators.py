import pytest
import pandas as pd


from datetime import datetime

from tasks.technical_analysis import technical_indicators as gc


@pytest.fixture
def sample_ohlcv_doji():
    """Returns a DataFrame that produces a Doji candle."""
    data = {
        "date": [datetime(2023, 1, 1)],
        "symbol": ["TEST"],
        "open": [100.0],
        "high": [105.0],
        "low": [95.0],
        "close": [100.05],  # Very small body
        "volume": [1000],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_ohlcv_engulfing():
    """Returns a DataFrame with a Bullish Engulfing pattern."""
    data = {
        "date": [
            datetime(2022, 12, 30),  # Context: Downtrend (added history)
            datetime(2022, 12, 31),  # Context: Downtrend (added history)
            datetime(2023, 1, 1),  # Context: Downtrend
            datetime(2023, 1, 2),  # Context: Downtrend
            datetime(2023, 1, 3),  # Candle 1: Bearish
            datetime(2023, 1, 4),  # Candle 2: Bullish Engulfing
        ],
        "symbol": ["TEST"] * 6,
        "open": [115.0, 112.0, 110.0, 108.0, 105.0, 100.0],
        "high": [116.0, 114.0, 112.0, 110.0, 106.0, 107.0],
        "low": [112.0, 110.0, 108.0, 100.0, 100.0, 99.0],
        "close": [112.0, 110.0, 108.0, 105.0, 101.0, 106.0],
        # Candle 1 (1/3): Open 105, Close 101 (Bearish)
        # Candle 2 (1/4): Open 100, Close 106 (Bullish) -> Fully engulfs
        "volume": [1000] * 6,
    }
    return pd.DataFrame(data)


def test_compute_features_doji(sample_ohlcv_doji):
    df = gc.add_candlestick_patterns(sample_ohlcv_doji)
    row = df.iloc[0]

    # Assert Doji flag is set
    assert row["pat_doji"] == 1
    assert row["range"] == 10.0
    assert row["body"] == pytest.approx(0.05)


def test_compute_features_bullish_engulfing(sample_ohlcv_engulfing):
    df = gc.add_candlestick_patterns(sample_ohlcv_engulfing)

    # Check last row for pattern
    row = df.iloc[-1]

    assert row["pat_bullish_engulfing"] == 1
    assert row["is_bull"] == 1


def test_snake_case_conversion():
    assert gc._to_snake_case("Adj Close") == "adj_close"
    assert gc._to_snake_case("Volume") == "volume"
    assert gc._to_snake_case("Typical Price") == "typical_price"


def _make_ohlc_rows(rows: int = 80) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=rows, freq="D")
    close = pd.Series(range(100, 100 + rows), dtype=float)
    open_ = close - 0.5
    high = close + 1.0
    low = close - 1.5
    return pd.DataFrame(
        {
            "date": dates,
            "symbol": ["TEST"] * rows,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": [1000.0] * rows,
        }
    )


def test_add_heikin_ashi_and_ichimoku_adds_expected_columns():
    out = gc.add_heikin_ashi_and_ichimoku(_make_ohlc_rows(80))

    expected = {
        "ha_open",
        "ha_high",
        "ha_low",
        "ha_close",
        "ichimoku_tenkan_sen_9",
        "ichimoku_kijun_sen_26",
        "ichimoku_senkou_span_a",
        "ichimoku_senkou_span_b",
        "ichimoku_senkou_span_a_26",
        "ichimoku_senkou_span_b_26",
        "ichimoku_chikou_span_26",
    }
    assert expected.issubset(set(out.columns))


def test_heikin_ashi_seed_and_recurrence():
    df = pd.DataFrame(
        {
            "date": [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
            "symbol": ["TEST", "TEST", "TEST"],
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.5, 13.0],
            "low": [9.0, 10.0, 11.5],
            "close": [10.5, 12.0, 12.5],
            "volume": [1000, 1000, 1000],
        }
    )
    out = gc.add_heikin_ashi_and_ichimoku(df)

    row0 = out.iloc[0]
    expected_ha_close_0 = (10.0 + 11.0 + 9.0 + 10.5) / 4.0
    expected_ha_open_0 = (10.0 + 10.5) / 2.0
    assert row0["ha_close"] == pytest.approx(expected_ha_close_0)
    assert row0["ha_open"] == pytest.approx(expected_ha_open_0)

    row1 = out.iloc[1]
    expected_ha_open_1 = (expected_ha_open_0 + expected_ha_close_0) / 2.0
    assert row1["ha_open"] == pytest.approx(expected_ha_open_1)


def test_ichimoku_shifted_columns_are_past_aligned():
    out = gc.add_heikin_ashi_and_ichimoku(_make_ohlc_rows(80))

    idx = 79
    assert out.iloc[idx]["ichimoku_chikou_span_26"] == pytest.approx(out.iloc[idx - 26]["close"])
    assert out.iloc[idx]["ichimoku_senkou_span_a_26"] == pytest.approx(out.iloc[idx - 26]["ichimoku_senkou_span_a"])
    assert out.iloc[idx]["ichimoku_senkou_span_b_26"] == pytest.approx(out.iloc[idx - 26]["ichimoku_senkou_span_b"])
