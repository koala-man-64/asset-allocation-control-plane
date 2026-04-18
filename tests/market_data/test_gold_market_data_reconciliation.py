from __future__ import annotations

import pandas as pd

from asset_allocation_runtime_common.market_data import delta_core
from asset_allocation_runtime_common.market_data.pipeline import DataPaths
from tasks.market_data import gold_market_data as gold

def test_run_market_reconciliation_cutoff_store_path_sanitizes_index_artifacts(monkeypatch, tmp_path):
    class _FakeGoldClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_gold = _FakeGoldClient()

    def _fake_get_storage_client(container: str):
        if container == "silver":
            return object()
        if container == "gold":
            return fake_gold
        return None

    monkeypatch.setattr("core.core.get_storage_client", _fake_get_storage_client)
    monkeypatch.setattr(gold, "collect_delta_market_symbols", lambda *, client, root_prefix: {"AAPL"})
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(tmp_path / "gold_market"))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})
    monkeypatch.setattr(delta_core, "_get_existing_delta_schema_columns", lambda _uri, _opts: None)

    captured: dict[str, object] = {}

    def fake_write_deltalake(_uri, df: pd.DataFrame, **kwargs) -> None:
        captured["df"] = df.copy()
        captured["kwargs"] = dict(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

    def _fake_enforce_backfill_cutoff_on_bucket_tables(**kwargs):
        dirty = pd.DataFrame(
            {
                "date": [pd.Timestamp("2024-01-10")],
                "symbol": ["AAPL"],
                "close": [101.0],
                "__index_level_0__": [8],
            }
        )
        dirty.index = pd.Index([11])
        kwargs["store_table"](dirty, DataPaths.get_gold_market_bucket_path("A"))
        return type(
            "_Stats",
            (),
            {"tables_scanned": 1, "tables_rewritten": 1, "deleted_blobs": 0, "rows_dropped": 1, "errors": 0},
        )()

    monkeypatch.setattr(
        gold,
        "enforce_backfill_cutoff_on_bucket_tables",
        _fake_enforce_backfill_cutoff_on_bucket_tables,
    )

    gold._run_market_reconciliation(silver_container="silver", gold_container="gold")

    assert "__index_level_0__" not in captured["df"].columns
    assert isinstance(captured["df"].index, pd.RangeIndex)
    assert captured["df"].index.start == 0
    assert captured["df"].index.step == 1
