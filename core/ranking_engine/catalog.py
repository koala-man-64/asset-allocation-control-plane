from __future__ import annotations

from typing import Any

from core.strategy_engine import universe as universe_service


def list_gold_ranking_catalog(dsn: str) -> dict[str, Any]:
    specs = universe_service._load_gold_table_specs(dsn)
    tables: list[dict[str, Any]] = []
    for spec in specs.values():
        columns = [
            {
                "name": column.name,
                "dataType": column.data_type,
                "valueKind": column.value_kind,
            }
            for column in spec.columns.values()
            if column.value_kind in {"number", "boolean"}
            and column.name not in {spec.as_of_column, "symbol"}
        ]
        if not columns:
            continue
        tables.append(
            {
                "name": spec.name,
                "asOfColumn": spec.as_of_column,
                "asOfKind": spec.as_of_kind,
                "columns": columns,
            }
        )
    return {"source": "postgres_gold", "tables": tables}
