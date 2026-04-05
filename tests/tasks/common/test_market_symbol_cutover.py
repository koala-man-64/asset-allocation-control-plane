from __future__ import annotations

import pandas as pd

from tasks.common import market_symbol_cutover


def test_prepare_provider_native_vix_cutover_resets_gold_index_and_watermarks(monkeypatch) -> None:
    deleted_paths: list[str] = []
    warnings: list[str] = []

    class _FakeCommonClient:
        def delete_prefix(self, path: str) -> int:
            deleted_paths.append(str(path))
            return 1

    monkeypatch.setattr(market_symbol_cutover.mdc, "common_storage_client", _FakeCommonClient())
    monkeypatch.setattr(
        market_symbol_cutover.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["^VIX"], "bucket": ["V"]}),
    )
    monkeypatch.setattr(market_symbol_cutover.mdc, "write_warning", lambda msg: warnings.append(str(msg)))

    assert market_symbol_cutover.prepare_provider_native_vix_cutover(layer="gold") is True
    assert deleted_paths == [
        "system/gold-index/market/latest.parquet",
        "system/watermarks/gold_market_features.json",
    ]
    assert any("legacy_symbols=['^VIX']" in message for message in warnings)
