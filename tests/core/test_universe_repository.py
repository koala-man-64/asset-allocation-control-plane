from __future__ import annotations

import json

from core.universe_repository import UniverseRepository


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, fetchall_result=None, fetchall_results=None) -> None:
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or ([] if fetchall_result is None else [fetchall_result]))
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_save_universe_config_increments_version(monkeypatch) -> None:
    cursor = _FakeCursor(fetchone_results=[(2,)])
    monkeypatch.setattr("core.universe_repository.connect", lambda _dsn: _FakeConnection(cursor))

    repo = UniverseRepository("postgresql://user:pass@localhost/db")
    saved = repo.save_universe_config(
        name="large-cap-quality",
        description="Universe",
        config={"source": "postgres_gold"},
    )

    assert saved["version"] == 3
    upsert_sql, upsert_params = cursor.execute_calls[1]
    assert "INSERT INTO core.universe_configs" in upsert_sql
    assert upsert_params == (
        "large-cap-quality",
        "Universe",
        3,
        json.dumps({"source": "postgres_gold"}),
    )


def test_get_universe_config_reads_detail(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_results=[
            ("large-cap-quality", "Universe", 2, "2026-03-08T00:00:00Z", {"source": "postgres_gold"})
        ]
    )
    monkeypatch.setattr("core.universe_repository.connect", lambda _dsn: _FakeConnection(cursor))

    repo = UniverseRepository("postgresql://user:pass@localhost/db")
    result = repo.get_universe_config("large-cap-quality")

    assert result == {
        "name": "large-cap-quality",
        "description": "Universe",
        "version": 2,
        "updated_at": "2026-03-08T00:00:00Z",
        "config": {"source": "postgres_gold"},
    }


def test_get_universe_config_references_reads_strategy_and_ranking_links(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchall_results=[
            [("mom-spy-res",), ("swing-quality",)],
            [("quality-rank",)],
        ]
    )
    monkeypatch.setattr("core.universe_repository.connect", lambda _dsn: _FakeConnection(cursor))

    repo = UniverseRepository("postgresql://user:pass@localhost/db")
    result = repo.get_universe_config_references("large-cap-quality")

    assert result == {
        "strategies": ["mom-spy-res", "swing-quality"],
        "rankingSchemas": ["quality-rank"],
    }
