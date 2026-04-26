from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from asset_allocation_contracts.portfolio import PortfolioHistoryPoint

from core.portfolio_analytics import derive_next_rebalance, derive_portfolio_forecast


def test_derive_portfolio_forecast_returns_regime_conditioned_metrics() -> None:
    start = date(2026, 1, 1)
    history = [
        PortfolioHistoryPoint(
            asOf=start + timedelta(days=index),
            nav=100_000 + (index * 1_000),
            cash=5_000,
            grossExposure=0.95,
            netExposure=0.95,
        )
        for index in range(30)
    ]
    benchmark_rows = [
        {"date": (start + timedelta(days=index)).isoformat(), "close": 400 + index}
        for index in range(30)
    ]
    regime_history_rows = [
        {
            "as_of_date": (start + timedelta(days=index)).isoformat(),
            "effective_from_date": (start + timedelta(days=index)).isoformat(),
            "active_regimes": ["trending_up"],
            "model_version": 3,
        }
        for index in range(30)
    ]

    forecast = derive_portfolio_forecast(
        account_id="acct-core",
        history_points=history,
        benchmark_rows=benchmark_rows,
        regime_history_rows=regime_history_rows,
        current_regime_code="trending_up",
        benchmark_symbol="SPY",
        model_name="default-regime",
        model_version=3,
        horizon="1M",
        assumption="current",
        cost_drag_override_bps=12,
    )

    assert forecast.appliedRegimeCode == "trending_up"
    assert forecast.sampleMode == "regime-conditioned"
    assert forecast.sampleSize == 9
    assert forecast.expectedReturnPct is not None
    assert forecast.expectedActiveReturnPct is not None


def test_derive_next_rebalance_returns_anchor_aligned_weekly_window() -> None:
    response = derive_next_rebalance(
        account_id="acct-core",
        rebalance_cadence="weekly",
        anchor_text="Monday close",
        last_materialized_at=datetime(2026, 4, 17, 15, 30, tzinfo=timezone.utc),
        snapshot_as_of=date(2026, 4, 17),
        effective_from=date(2026, 1, 2),
        as_of=date(2026, 4, 18),
    )

    assert response.nextDate == date(2026, 4, 20)
    assert response.inferred is False
    assert response.basis == "anchor"
