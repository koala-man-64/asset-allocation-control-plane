from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from core.gold_column_lookup_catalog import TABLE_SOURCE_JOBS


class PostgresError(RuntimeError):
    """Raised when generated gold-column lookup metadata is invalid."""


def _text_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _first_present(*values: object) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _metadata_value(
    *,
    field_name: str,
    existing: Mapping[str, Any] | None,
    seed: Mapping[str, Any] | None,
    default: Any = None,
    force_metadata: bool,
) -> Any:
    if force_metadata:
        return _first_present(seed.get(field_name) if seed else None, existing.get(field_name) if existing else None, default)
    return _first_present(existing.get(field_name) if existing else None, seed.get(field_name) if seed else None, default)


def _build_lookup_row(
    *,
    live_row: Mapping[str, Any],
    existing: Mapping[str, Any] | None,
    seed: Mapping[str, Any] | None,
    updated_by: str,
    force_metadata: bool,
) -> dict[str, Any]:
    schema_name = _text_or_none(live_row.get("schema_name")) or "gold"
    table_name = _text_or_none(live_row.get("table_name"))
    column_name = _text_or_none(live_row.get("column_name"))
    data_type = _text_or_none(live_row.get("data_type"))
    if not table_name or not column_name or not data_type:
        raise PostgresError("Live column metadata must include table_name, column_name, and data_type.")

    placeholder_description = f"TODO: Describe {schema_name}.{table_name}.{column_name}."
    description = _text_or_none(
        _metadata_value(
            field_name="description",
            existing=existing,
            seed=seed,
            default=placeholder_description,
            force_metadata=force_metadata,
        )
    )
    status = _text_or_none(
        _metadata_value(
            field_name="status",
            existing=existing,
            seed=seed,
            default="draft",
            force_metadata=force_metadata,
        )
    ) or "draft"

    if status == "approved" and (description or "").lower().startswith("todo: describe"):
        raise PostgresError("Approved metadata cannot use placeholder description.")

    dependencies = _metadata_value(
        field_name="calculation_dependencies",
        existing=existing,
        seed=seed,
        default=[],
        force_metadata=force_metadata,
    )
    if dependencies is None:
        dependencies = []

    return {
        "schema_name": schema_name,
        "table_name": table_name,
        "column_name": column_name,
        "data_type": data_type,
        "description": description or placeholder_description,
        "is_nullable": bool(live_row.get("is_nullable", True)),
        "calculation_type": _text_or_none(
            _metadata_value(
                field_name="calculation_type",
                existing=existing,
                seed=seed,
                default="source",
                force_metadata=force_metadata,
            )
        )
        or "source",
        "calculation_notes": _metadata_value(
            field_name="calculation_notes",
            existing=existing,
            seed=seed,
            default=None,
            force_metadata=force_metadata,
        ),
        "calculation_expression": _metadata_value(
            field_name="calculation_expression",
            existing=existing,
            seed=seed,
            default=None,
            force_metadata=force_metadata,
        ),
        "calculation_dependencies": list(dependencies),
        "source_job": _metadata_value(
            field_name="source_job",
            existing=existing,
            seed=seed,
            default=TABLE_SOURCE_JOBS.get(table_name),
            force_metadata=force_metadata,
        ),
        "status": status,
        "updated_by": updated_by,
    }
