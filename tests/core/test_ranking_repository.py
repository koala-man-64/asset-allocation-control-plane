from __future__ import annotations

import json

from core.ranking_repository import RankingRepository


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, fetchall_result=None) -> None:
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_result = fetchall_result or []
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
        return self.fetchall_result


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_save_ranking_schema_increments_version(monkeypatch) -> None:
    cursor = _FakeCursor(fetchone_results=[(2,)])
    monkeypatch.setattr("core.ranking_repository.connect", lambda _dsn: _FakeConnection(cursor))

    repo = RankingRepository("postgresql://user:pass@localhost/db")
    saved = repo.save_ranking_schema(
        name="quality-momentum",
        description="Composite schema",
        config={"groups": []},
    )

    assert saved["version"] == 3
    upsert_sql, upsert_params = cursor.execute_calls[1]
    assert "INSERT INTO core.ranking_schemas" in upsert_sql
    assert upsert_params == (
        "quality-momentum",
        "Composite schema",
        3,
        json.dumps({"groups": []}),
    )


def test_get_ranking_schema_reads_detail(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_results=[
            ("quality-momentum", "Composite schema", 4, "2026-03-08T00:00:00Z", {"groups": []})
        ]
    )
    monkeypatch.setattr("core.ranking_repository.connect", lambda _dsn: _FakeConnection(cursor))

    repo = RankingRepository("postgresql://user:pass@localhost/db")
    result = repo.get_ranking_schema("quality-momentum")

    assert result == {
        "name": "quality-momentum",
        "description": "Composite schema",
        "version": 4,
        "updated_at": "2026-03-08T00:00:00Z",
        "config": {"groups": []},
    }


def test_delete_ranking_schema_returns_true_when_row_removed(monkeypatch) -> None:
    cursor = _FakeCursor(fetchone_results=[("quality-momentum",)])
    monkeypatch.setattr("core.ranking_repository.connect", lambda _dsn: _FakeConnection(cursor))

    repo = RankingRepository("postgresql://user:pass@localhost/db")

    assert repo.delete_ranking_schema("quality-momentum") is True
    sql, params = cursor.execute_calls[0]
    assert "DELETE FROM core.ranking_schemas" in sql
    assert params == ("quality-momentum",)
