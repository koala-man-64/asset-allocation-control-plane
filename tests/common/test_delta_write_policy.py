from __future__ import annotations

import pandas as pd

from tasks.common.delta_write_policy import prepare_delta_write_frame


def test_prepare_delta_write_frame_skips_empty_frame_without_existing_schema(monkeypatch):
    monkeypatch.setattr(
        "tasks.common.delta_write_policy.delta_core.get_delta_schema_columns",
        lambda _container, _path: None,
    )

    decision = prepare_delta_write_frame(
        pd.DataFrame(columns=["Date", "Symbol"]),
        container="gold",
        path="market/buckets/A",
    )

    assert decision.action == "skip_empty_no_schema"
    assert decision.reason == "empty_bucket_no_existing_schema"
    assert list(decision.frame.columns) == ["date", "symbol"]


def test_prepare_delta_write_frame_aligns_empty_frame_to_existing_schema(monkeypatch):
    existing_cols = ["date", "symbol", "feature_x"]
    monkeypatch.setattr(
        "tasks.common.delta_write_policy.delta_core.get_delta_schema_columns",
        lambda _container, _path: existing_cols,
    )

    decision = prepare_delta_write_frame(
        pd.DataFrame(columns=["date", "symbol"]),
        container="gold",
        path="market/buckets/A",
    )

    assert decision.action == "write"
    assert decision.reason == "aligned_to_existing_schema"
    assert decision.existing_schema_columns == tuple(existing_cols)
    assert decision.frame.empty
    assert list(decision.frame.columns) == existing_cols


def test_prepare_delta_write_frame_aligns_non_empty_frame_to_existing_schema(monkeypatch):
    existing_cols = ["date", "symbol", "feature_x"]
    monkeypatch.setattr(
        "tasks.common.delta_write_policy.delta_core.get_delta_schema_columns",
        lambda _container, _path: existing_cols,
    )

    frame = pd.DataFrame(
        {
            "Symbol": ["AAPL"],
            "Date": ["2026-01-02"],
            "extra_field": [7],
        },
        index=[10],
    )
    decision = prepare_delta_write_frame(frame, container="gold", path="market/buckets/A")

    assert decision.action == "write"
    assert list(decision.frame.columns) == ["date", "symbol", "feature_x", "extra_field"]
    assert decision.frame.index.tolist() == [0]
    assert decision.frame.loc[0, "symbol"] == "AAPL"
    assert pd.isna(decision.frame.loc[0, "feature_x"])


def test_prepare_delta_write_frame_normalizes_non_empty_frame_without_existing_schema(monkeypatch):
    monkeypatch.setattr(
        "tasks.common.delta_write_policy.delta_core.get_delta_schema_columns",
        lambda _container, _path: None,
    )

    frame = pd.DataFrame({"Date": ["2026-01-02"], "Symbol": ["AAPL"]}, index=[4])
    decision = prepare_delta_write_frame(frame, container="gold", path="market/buckets/A")

    assert decision.action == "write"
    assert decision.reason == "no_existing_schema"
    assert list(decision.frame.columns) == ["date", "symbol"]
    assert decision.frame.index.tolist() == [0]


def test_prepare_delta_write_frame_preserves_empty_wider_schema_for_existing_table(monkeypatch):
    existing_cols = ["date", "symbol", "close", "return_1d"]
    monkeypatch.setattr(
        "tasks.common.delta_write_policy.delta_core.get_delta_schema_columns",
        lambda _container, _path: existing_cols,
    )

    decision = prepare_delta_write_frame(
        pd.DataFrame(columns=["date", "symbol"]),
        container="gold",
        path="market/buckets/Z",
    )

    assert decision.action == "write"
    assert decision.frame.empty
    assert list(decision.frame.columns) == existing_cols


def test_prepare_delta_write_frame_enforces_declared_schema_over_legacy_table_columns(monkeypatch):
    existing_cols = ["date", "symbol", "shares_outstanding", "timeframe"]
    monkeypatch.setattr(
        "tasks.common.delta_write_policy.delta_core.get_delta_schema_columns",
        lambda _container, _path: existing_cols,
    )

    decision = prepare_delta_write_frame(
        pd.DataFrame({"Date": ["2026-01-02"], "Symbol": ["AAPL"]}),
        container="silver",
        path="finance-data/balance_sheet/buckets/A",
        enforced_schema_columns=("date", "symbol", "total_assets", "timeframe"),
    )

    assert decision.action == "write"
    assert decision.reason == "aligned_to_enforced_schema"
    assert list(decision.frame.columns) == ["date", "symbol", "total_assets", "timeframe"]
    assert pd.isna(decision.frame.loc[0, "total_assets"])
