from __future__ import annotations

import pandas as pd

from tasks.common import backfill


def test_get_backfill_range_defaults_to_2016_floor(monkeypatch) -> None:
    monkeypatch.delenv("BACKFILL_START_DATE", raising=False)

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2016-01-01")
    assert end is None


def test_get_backfill_range_applies_explicit_start(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "2020-05-03")

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2020-05-03")
    assert end is None


def test_get_backfill_range_clamps_start_before_floor(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "2010-01-01")

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2016-01-01")
    assert end is None


def test_get_backfill_range_ignores_invalid_values(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "not-a-date")

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2016-01-01")
    assert end is None


def test_filter_by_date_resets_index_after_filter() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
            "value": [1, 2, 3],
        }
    )
    df.index = pd.Index([10, 11, 12])

    filtered = backfill.filter_by_date(
        df,
        date_col="date",
        start=pd.Timestamp("2020-01-02"),
        end=None,
    )

    assert isinstance(filtered.index, pd.RangeIndex)
    assert filtered.index.start == 0
    assert filtered.index.step == 1
    assert filtered["value"].tolist() == [2, 3]
