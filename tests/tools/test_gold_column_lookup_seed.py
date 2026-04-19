from __future__ import annotations

import json
from pathlib import Path

from core.gold_column_lookup_catalog import SUPPORTED_GOLD_LOOKUP_TABLES, expected_gold_lookup_columns


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _seed_path() -> Path:
    return _repo_root() / "core" / "metadata" / "gold_column_lookup_seed.json"


def _load_seed_entries() -> list[dict[str, object]]:
    payload = json.loads(_seed_path().read_text(encoding="utf-8"))
    entries = payload.get("entries") or []
    assert isinstance(entries, list), "gold_column_lookup_seed.json must contain an entries array."
    return [entry for entry in entries if isinstance(entry, dict)]


def test_gold_column_lookup_seed_contains_expected_tables() -> None:
    entries = _load_seed_entries()
    observed_tables = {str(entry.get("table_name") or "").strip() for entry in entries}
    for table_name in SUPPORTED_GOLD_LOOKUP_TABLES:
        assert table_name in observed_tables, f"Missing seed entries for table {table_name!r}."


def test_gold_column_lookup_seed_has_full_column_coverage() -> None:
    entries = _load_seed_entries()
    observed = {
        (str(entry.get("table_name") or "").strip(), str(entry.get("column_name") or "").strip())
        for entry in entries
    }

    missing: list[tuple[str, str]] = []
    for table_name, columns in expected_gold_lookup_columns().items():
        for column_name in columns:
            key = (table_name, column_name)
            if key not in observed:
                missing.append(key)

    assert not missing, f"Seed coverage is missing lookup rows: {missing[:10]}"


def test_gold_column_lookup_seed_approved_rows_do_not_use_placeholders() -> None:
    entries = _load_seed_entries()
    offenders: list[str] = []

    for entry in entries:
        schema_name = str(entry.get("schema_name") or "").strip()
        status = str(entry.get("status") or "").strip().lower()
        description = str(entry.get("description") or "").strip()
        table_name = str(entry.get("table_name") or "").strip()
        column_name = str(entry.get("column_name") or "").strip()

        assert schema_name == "gold", f"Seed schema_name must be 'gold' for {table_name}.{column_name}"
        if status == "approved" and description.lower().startswith("todo: describe"):
            offenders.append(f"{table_name}.{column_name}")

    assert not offenders, f"Approved rows cannot keep placeholder descriptions: {offenders[:10]}"
