from __future__ import annotations

import importlib.util
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, Sequence

from fastapi import HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import MetaData, Table, create_engine, inspect, select, text

from api.endpoints.system_modules.domain_metadata import _read_cached_domain_metadata_snapshot
from api.service.dependencies import get_settings
from core.gold_column_lookup_catalog import SUPPORTED_GOLD_LOOKUP_TABLES
from core.strategy_engine.universe import UNIVERSE_FIELD_MAP, UniverseTableSpec, _load_gold_table_specs

_HIDDEN_EXPLORER_SCHEMAS = frozenset({"information_schema", "public"})
_SUPPORTED_GOLD_EXPLORER_TABLES = frozenset(SUPPORTED_GOLD_LOOKUP_TABLES)
_SUPPORTED_LOOKUP_STATUS = frozenset({"draft", "reviewed", "approved"})
_AS_OF_COLUMN_CANDIDATES = (
    "as_of_ts",
    "timestamp",
    "ts",
    "datetime",
    "date",
    "obs_date",
    "updated_at",
    "effective_from_date",
    "computed_at",
)
_GOLD_FRESHNESS_DOMAIN_BY_TABLE = {
    "market_data": "market",
    "finance_data": "finance",
    "earnings_data": "earnings",
    "price_target_data": "price-target",
    "government_signal_issuer_daily": "government-signals",
}
_LOGICAL_FIELD_IDS_BY_COLUMN: dict[tuple[str, str, str], list[str]] = {}

for _field_id, _field_spec in UNIVERSE_FIELD_MAP.items():
    _LOGICAL_FIELD_IDS_BY_COLUMN.setdefault(
        (_field_spec.schema, _field_spec.table, _field_spec.column),
        [],
    ).append(_field_id)


class PostgresColumnMetadata(BaseModel):
    name: str
    data_type: str
    description: Optional[str] = None
    nullable: bool
    primary_key: bool
    editable: bool
    edit_reason: Optional[str] = None


class TableMetadataResponse(BaseModel):
    schema_name: str
    table_name: str
    primary_key: List[str]
    can_edit: bool
    edit_reason: Optional[str] = None
    columns: List[PostgresColumnMetadata]


class GoldColumnLookupRecord(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(alias="schema")
    table: str
    column: str
    data_type: str
    description: str
    calculation_type: str
    calculation_notes: Optional[str] = None
    calculation_expression: Optional[str] = None
    calculation_dependencies: List[str] = Field(default_factory=list)
    source_job: Optional[str] = None
    status: str
    updated_at: Optional[str] = None


class GoldColumnLookupResponse(BaseModel):
    rows: List[GoldColumnLookupRecord] = Field(default_factory=list)
    limit: int
    offset: int
    has_more: bool


class DataDiscoveryDateRange(BaseModel):
    min: Optional[str] = None
    max: Optional[str] = None
    column: Optional[str] = None


class DataDiscoveryFreshness(BaseModel):
    computedAt: Optional[str] = None
    cachedAt: Optional[str] = None
    cacheSource: Optional[Literal["snapshot", "live-refresh"]] = None
    dateRange: Optional[DataDiscoveryDateRange] = None
    totalRows: Optional[int] = None


class DataDiscoverySortField(BaseModel):
    column: str
    direction: Literal["asc", "desc"]


class DataDiscoveryField(BaseModel):
    name: str
    dataType: str
    nullable: bool
    primaryKey: bool
    description: Optional[str] = None
    valueKind: str
    logicalFieldId: Optional[str] = None
    calculationType: Optional[str] = None
    calculationDependencies: List[str] = Field(default_factory=list)
    calculationNotes: Optional[str] = None


class DataDiscoveryDatasetSummary(BaseModel):
    scope: Literal["gold", "control_plane"]
    schemaName: str
    tableName: str
    label: str
    description: str
    primaryKey: List[str] = Field(default_factory=list)
    asOfColumn: Optional[str] = None
    fieldCount: int
    sampleSupported: bool
    freshness: Optional[DataDiscoveryFreshness] = None


class DataDiscoveryCatalogResponse(BaseModel):
    datasets: List[DataDiscoveryDatasetSummary] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class DataDiscoveryDatasetDetailResponse(BaseModel):
    scope: Literal["gold", "control_plane"]
    schemaName: str
    tableName: str
    label: str
    description: str
    primaryKey: List[str] = Field(default_factory=list)
    asOfColumn: Optional[str] = None
    defaultSort: List[DataDiscoverySortField] = Field(default_factory=list)
    fields: List[DataDiscoveryField] = Field(default_factory=list)
    freshness: Optional[DataDiscoveryFreshness] = None
    warnings: List[str] = Field(default_factory=list)


class DataDiscoverySampleResponse(BaseModel):
    scope: Literal["gold", "control_plane"]
    schemaName: str
    tableName: str
    limit: int
    sortApplied: List[DataDiscoverySortField] = Field(default_factory=list)
    rows: List[Dict[str, Any]] = Field(default_factory=list)


@dataclass(frozen=True)
class _GoldLookupFieldMetadata:
    description: Optional[str]
    calculation_type: Optional[str]
    calculation_dependencies: tuple[str, ...]
    calculation_notes: Optional[str]


_DISCOVERY_CATALOG_CACHE: dict[str, tuple[float, DataDiscoveryCatalogResponse]] = {}
_DISCOVERY_DETAIL_CACHE: dict[tuple[str, str, str, str], tuple[float, DataDiscoveryDatasetDetailResponse]] = {}
_DISCOVERY_CACHE_LOCK = threading.Lock()


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _has_module(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _normalize_sync_driver(value: str) -> str:
    has_psycopg = _has_module("psycopg")
    has_psycopg2 = _has_module("psycopg2")

    if value.startswith("postgresql+psycopg2://"):
        if has_psycopg2:
            return value
        if has_psycopg:
            return "postgresql+psycopg://" + value.removeprefix("postgresql+psycopg2://")
        return value

    if value.startswith("postgresql+psycopg://"):
        if has_psycopg:
            return value
        if has_psycopg2:
            return "postgresql+psycopg2://" + value.removeprefix("postgresql+psycopg://")
        return value

    if value.startswith("postgresql://"):
        if has_psycopg2:
            return value
        if has_psycopg:
            return "postgresql+psycopg://" + value.removeprefix("postgresql://")
        return value

    if value.startswith("postgres://"):
        if has_psycopg2:
            return value
        if has_psycopg:
            return "postgresql+psycopg://" + value.removeprefix("postgres://")
        return value

    return value


def _resolve_postgres_dsn(request: Request) -> Optional[str]:
    raw = os.environ.get("POSTGRES_DSN")
    dsn = _strip_or_none(raw) or _strip_or_none(get_settings(request).postgres_dsn)
    if not dsn:
        return None
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn.removeprefix("postgresql+asyncpg://")
    return _normalize_sync_driver(dsn)


def _require_postgres_dsn(request: Request) -> str:
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")
    return dsn


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _normalize_visible_schemas(values: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        schema_name = str(value or "").strip().lower()
        if not schema_name or schema_name in seen:
            continue
        seen.add(schema_name)
        normalized.append(schema_name)
    return normalized


def _is_hidden_explorer_table(*, schema_name: str, table_name: str) -> bool:
    normalized_schema = str(schema_name or "").strip().lower()
    normalized_table = str(table_name or "").strip().lower()
    return normalized_schema == "gold" and normalized_table not in _SUPPORTED_GOLD_EXPLORER_TABLES


def _visible_schema_names(insp: Any, *, visible_schemas: Sequence[str]) -> List[str]:
    allowed = set(_normalize_visible_schemas(visible_schemas))
    return sorted(
        schema_name
        for schema_name in insp.get_schema_names()
        if str(schema_name or "").strip().lower() in allowed
        and str(schema_name or "").strip().lower() not in _HIDDEN_EXPLORER_SCHEMAS
    )


def _visible_table_names(
    insp: Any,
    *,
    schema_name: str,
    visible_schemas: Sequence[str],
) -> List[str]:
    if str(schema_name or "").strip().lower() not in set(_normalize_visible_schemas(visible_schemas)):
        return []
    return sorted(
        table_name
        for table_name in insp.get_table_names(schema=schema_name)
        if not _is_hidden_explorer_table(schema_name=schema_name, table_name=str(table_name or ""))
    )


def _validate_table_target(
    insp: Any,
    *,
    schema_name: str,
    table_name: str,
    visible_schemas: Sequence[str],
) -> None:
    visible_schema_names = _visible_schema_names(insp, visible_schemas=visible_schemas)
    if schema_name not in visible_schema_names:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found.")

    if table_name not in _visible_table_names(insp, schema_name=schema_name, visible_schemas=visible_schemas):
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found in schema '{schema_name}'.",
        )


def _reflect_table(engine: Any, *, schema_name: str, table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, schema=schema_name, autoload_with=engine)


def _load_postgres_column_descriptions(
    engine: Any,
    *,
    schema_name: str,
    table_name: str,
) -> Dict[str, str]:
    query = text(
        """
        SELECT
            a.attname AS column_name,
            d.description AS description
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_catalog.pg_description d
            ON d.objoid = c.oid AND d.objsubid = a.attnum
        WHERE n.nspname = :schema_name
          AND c.relname = :table_name
          AND a.attnum > 0
          AND NOT a.attisdropped
        """
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                query,
                {"schema_name": schema_name, "table_name": table_name},
            )
            descriptions: Dict[str, str] = {}
            for row in rows:
                column_name = str(row.column_name or "").strip()
                description = str(row.description or "").strip()
                if column_name and description:
                    descriptions[column_name] = description
            return descriptions
    except Exception:
        return {}


def _load_postgres_table_description(
    engine: Any,
    *,
    schema_name: str,
    table_name: str,
) -> Optional[str]:
    query = text(
        """
        SELECT d.description
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_catalog.pg_description d
            ON d.objoid = c.oid AND d.objsubid = 0
        WHERE n.nspname = :schema_name
          AND c.relname = :table_name
        LIMIT 1
        """
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(query, {"schema_name": schema_name, "table_name": table_name}).first()
    except Exception:
        return None
    return _strip_or_none(row[0] if row else None)


def _load_table_metadata(
    engine: Any,
    insp: Any,
    *,
    schema_name: str,
    table_name: str,
    visible_schemas: Sequence[str],
) -> TableMetadataResponse:
    _validate_table_target(
        insp,
        schema_name=schema_name,
        table_name=table_name,
        visible_schemas=visible_schemas,
    )
    column_descriptions = _load_postgres_column_descriptions(
        engine,
        schema_name=schema_name,
        table_name=table_name,
    )

    pk_constraint = insp.get_pk_constraint(table_name, schema=schema_name) or {}
    primary_key = [
        str(name)
        for name in (pk_constraint.get("constrained_columns") or [])
        if str(name or "").strip()
    ]
    primary_key_set = set(primary_key)

    columns: List[PostgresColumnMetadata] = []
    has_editable_columns = False
    for column in insp.get_columns(table_name, schema=schema_name):
        name = str(column.get("name") or "").strip()
        if not name:
            continue

        is_generated = bool(column.get("computed")) or bool(column.get("identity"))
        editable = not is_generated
        if editable:
            has_editable_columns = True

        columns.append(
            PostgresColumnMetadata(
                name=name,
                data_type=str(column.get("type") or ""),
                description=column_descriptions.get(name),
                nullable=bool(column.get("nullable", True)),
                primary_key=name in primary_key_set,
                editable=editable,
                edit_reason=None if editable else "Generated or identity column is read-only.",
            )
        )

    can_edit = bool(primary_key) and has_editable_columns
    edit_reason: Optional[str] = None
    if not primary_key:
        edit_reason = "Table has no primary key; row editing is disabled."
    elif not has_editable_columns:
        edit_reason = "Table exposes no editable columns."

    return TableMetadataResponse(
        schema_name=schema_name,
        table_name=table_name,
        primary_key=primary_key,
        can_edit=can_edit,
        edit_reason=edit_reason,
        columns=columns,
    )


def _normalize_lookup_status(value: Optional[str]) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if normalized not in _SUPPORTED_LOOKUP_STATUS:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid status filter. Expected one of: "
                + ", ".join(sorted(_SUPPORTED_LOOKUP_STATUS))
            ),
        )
    return normalized


def _iso_datetime(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return _strip_or_none(value)


def _query_gold_lookup_rows(
    engine: Any,
    *,
    table_name: Optional[str],
    q: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
) -> GoldColumnLookupResponse:
    normalized_table = str(table_name or "").strip().lower() or None
    normalized_status = _normalize_lookup_status(status)
    query_text = str(q or "").strip()

    if normalized_table and normalized_table not in _SUPPORTED_GOLD_EXPLORER_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Gold table '{normalized_table}' is not supported by the lookup catalog.",
        )

    where_clauses = [
        "schema_name = 'gold'",
        "table_name = ANY(:supported_tables)",
    ]
    query_params: Dict[str, Any] = {
        "supported_tables": list(_SUPPORTED_GOLD_EXPLORER_TABLES),
        "limit": int(limit) + 1,
        "offset": int(offset),
    }

    if normalized_table:
        where_clauses.append("table_name = :table_name")
        query_params["table_name"] = normalized_table

    if normalized_status:
        where_clauses.append("status = :status")
        query_params["status"] = normalized_status

    if query_text:
        where_clauses.append(
            "(column_name ILIKE :search OR description ILIKE :search OR COALESCE(calculation_notes, '') ILIKE :search)"
        )
        query_params["search"] = f"%{query_text}%"

    statement = text(
        f"""
        SELECT
            schema_name,
            table_name,
            column_name,
            data_type,
            description,
            calculation_type,
            calculation_notes,
            calculation_expression,
            calculation_dependencies,
            source_job,
            status,
            updated_at
        FROM gold.column_lookup
        WHERE {" AND ".join(where_clauses)}
        ORDER BY table_name, column_name
        LIMIT :limit
        OFFSET :offset
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(statement, query_params).mappings().all()

    has_more = len(rows) > limit
    rows = rows[:limit]

    payload_rows: List[GoldColumnLookupRecord] = []
    for row in rows:
        dependencies = row.get("calculation_dependencies")
        if isinstance(dependencies, list):
            normalized_dependencies = [str(item).strip() for item in dependencies if str(item).strip()]
        elif isinstance(dependencies, tuple):
            normalized_dependencies = [str(item).strip() for item in dependencies if str(item).strip()]
        else:
            normalized_dependencies = []

        payload_rows.append(
            GoldColumnLookupRecord(
                schema_name=str(row.get("schema_name") or "gold"),
                table=str(row.get("table_name") or ""),
                column=str(row.get("column_name") or ""),
                data_type=str(row.get("data_type") or ""),
                description=str(row.get("description") or ""),
                calculation_type=str(row.get("calculation_type") or "source"),
                calculation_notes=str(row.get("calculation_notes") or "").strip() or None,
                calculation_expression=str(row.get("calculation_expression") or "").strip() or None,
                calculation_dependencies=normalized_dependencies,
                source_job=str(row.get("source_job") or "").strip() or None,
                status=str(row.get("status") or "draft"),
                updated_at=_iso_datetime(row.get("updated_at")),
            )
        )

    return GoldColumnLookupResponse(
        rows=payload_rows,
        limit=limit,
        offset=offset,
        has_more=has_more,
    )


def _load_gold_lookup_tables(engine: Any) -> list[str]:
    statement = text(
        """
        SELECT DISTINCT table_name
        FROM gold.column_lookup
        WHERE schema_name = 'gold'
          AND table_name = ANY(:supported_tables)
        ORDER BY table_name
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(statement, {"supported_tables": list(_SUPPORTED_GOLD_EXPLORER_TABLES)}).all()
    return [str(row[0]) for row in rows if str(row[0] or "").strip()]


def _dataset_scope(schema_name: str) -> Literal["gold", "control_plane"]:
    return "gold" if str(schema_name or "").strip().lower() == "gold" else "control_plane"


def _humanize_table_name(table_name: str) -> str:
    return " ".join(part.capitalize() for part in str(table_name or "").strip().split("_") if part)


def _default_dataset_description(
    *,
    scope: Literal["gold", "control_plane"],
    schema_name: str,
    table_name: str,
    label: str,
) -> str:
    if scope == "gold":
        return f"Gold serving dataset for {label.lower()}."
    return f"Control-plane table {schema_name}.{table_name}."


def _classify_value_kind(data_type: str) -> str:
    normalized = str(data_type or "").strip().lower()
    if any(token in normalized for token in ("smallint", "integer", "bigint", "numeric", "decimal", "real", "double", "float", "serial", "money")):
        return "number"
    if "bool" in normalized:
        return "boolean"
    if "timestamp" in normalized or "datetime" in normalized:
        return "datetime"
    if "date" in normalized and "time" not in normalized and "stamp" not in normalized:
        return "date"
    if any(token in normalized for token in ("char", "text", "uuid", "citext")):
        return "string"
    if normalized:
        return "unknown"
    return "string"


def _detect_as_of_column(columns: Sequence[PostgresColumnMetadata]) -> Optional[str]:
    by_name = {column.name: column for column in columns}
    for candidate in _AS_OF_COLUMN_CANDIDATES:
        column = by_name.get(candidate)
        if not column:
            continue
        value_kind = _classify_value_kind(column.data_type)
        if value_kind in {"date", "datetime"}:
            return column.name
    return None


def _logical_field_id(schema_name: str, table_name: str, column_name: str) -> Optional[str]:
    matches = _LOGICAL_FIELD_IDS_BY_COLUMN.get((schema_name, table_name, column_name), [])
    if len(matches) == 1:
        return matches[0]
    return None


def _normalize_gold_dependencies(value: Any) -> tuple[str, ...]:
    if isinstance(value, tuple):
        raw_values = value
    elif isinstance(value, list):
        raw_values = tuple(value)
    else:
        raw_values = tuple()
    return tuple(str(item).strip() for item in raw_values if str(item).strip())


def _load_gold_lookup_field_map(engine: Any, *, table_name: str) -> dict[str, _GoldLookupFieldMetadata]:
    if table_name not in _SUPPORTED_GOLD_EXPLORER_TABLES:
        return {}

    statement = text(
        """
        SELECT
            column_name,
            description,
            calculation_type,
            calculation_dependencies,
            calculation_notes
        FROM gold.column_lookup
        WHERE schema_name = 'gold'
          AND table_name = :table_name
        ORDER BY column_name
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(statement, {"table_name": table_name}).mappings().all()

    metadata_by_column: dict[str, _GoldLookupFieldMetadata] = {}
    for row in rows:
        column_name = str(row.get("column_name") or "").strip()
        if not column_name:
            continue
        metadata_by_column[column_name] = _GoldLookupFieldMetadata(
            description=_strip_or_none(row.get("description")),
            calculation_type=_strip_or_none(row.get("calculation_type")),
            calculation_dependencies=_normalize_gold_dependencies(row.get("calculation_dependencies")),
            calculation_notes=_strip_or_none(row.get("calculation_notes")),
        )
    return metadata_by_column


def _load_gold_lookup_field_map_safely(
    engine: Any,
    *,
    schema_name: str,
    table_name: str,
) -> tuple[dict[str, _GoldLookupFieldMetadata], list[str]]:
    if schema_name != "gold" or table_name not in _SUPPORTED_GOLD_EXPLORER_TABLES:
        return {}, []

    try:
        return _load_gold_lookup_field_map(engine, table_name=table_name), []
    except Exception as exc:
        return {}, [f"Gold semantic metadata unavailable for {schema_name}.{table_name}: {type(exc).__name__}: {exc}"]


def _load_gold_table_specs_safely(dsn: str) -> tuple[dict[str, UniverseTableSpec], list[str]]:
    try:
        return _load_gold_table_specs(dsn), []
    except Exception as exc:
        return {}, [f"Gold catalog metadata unavailable: {type(exc).__name__}: {exc}"]


def _load_gold_freshness(table_name: str) -> tuple[Optional[DataDiscoveryFreshness], list[str]]:
    domain_name = _GOLD_FRESHNESS_DOMAIN_BY_TABLE.get(table_name)
    if not domain_name:
        return None, []
    try:
        snapshot = _read_cached_domain_metadata_snapshot("gold", domain_name)
    except Exception as exc:
        return None, [f"Gold freshness metadata unavailable for {table_name}: {type(exc).__name__}: {exc}"]

    if not isinstance(snapshot, dict):
        return None, []

    date_range_raw = snapshot.get("dateRange")
    date_range = None
    if isinstance(date_range_raw, dict):
        date_range = DataDiscoveryDateRange(
            min=_strip_or_none(date_range_raw.get("min")),
            max=_strip_or_none(date_range_raw.get("max")),
            column=_strip_or_none(date_range_raw.get("column")),
        )

    return (
        DataDiscoveryFreshness(
            computedAt=_strip_or_none(snapshot.get("computedAt")),
            cachedAt=_strip_or_none(snapshot.get("cachedAt")),
            cacheSource=snapshot.get("cacheSource"),
            dateRange=date_range,
            totalRows=int(snapshot.get("totalRows")) if snapshot.get("totalRows") is not None else None,
        ),
        [],
    )


def _build_data_discovery_fields(
    *,
    schema_name: str,
    table_name: str,
    metadata: TableMetadataResponse,
    gold_lookup_by_column: dict[str, _GoldLookupFieldMetadata],
    gold_table_spec: UniverseTableSpec | None,
) -> list[DataDiscoveryField]:
    fields: list[DataDiscoveryField] = []
    gold_columns = gold_table_spec.columns if gold_table_spec is not None else {}

    for column in metadata.columns:
        gold_lookup = gold_lookup_by_column.get(column.name)
        gold_column = gold_columns.get(column.name)
        value_kind = gold_column.value_kind if gold_column is not None else _classify_value_kind(column.data_type)
        description = None
        if gold_lookup and gold_lookup.description:
            description = gold_lookup.description
        elif column.description:
            description = column.description

        fields.append(
            DataDiscoveryField(
                name=column.name,
                dataType=column.data_type,
                nullable=column.nullable,
                primaryKey=column.primary_key,
                description=description,
                valueKind=value_kind,
                logicalFieldId=_logical_field_id(schema_name, table_name, column.name),
                calculationType=gold_lookup.calculation_type if gold_lookup else None,
                calculationDependencies=list(gold_lookup.calculation_dependencies) if gold_lookup else [],
                calculationNotes=gold_lookup.calculation_notes if gold_lookup else None,
            )
        )
    return fields


def _default_sort_fields(
    *,
    as_of_column: Optional[str],
    fields: Sequence[DataDiscoveryField],
    primary_key: Sequence[str],
) -> list[DataDiscoverySortField]:
    available_columns = {field.name for field in fields}
    sort_fields: list[DataDiscoverySortField] = []

    if as_of_column and as_of_column in available_columns:
        sort_fields.append(DataDiscoverySortField(column=as_of_column, direction="desc"))
        if "symbol" in available_columns and "symbol" != as_of_column:
            sort_fields.append(DataDiscoverySortField(column="symbol", direction="asc"))
        return sort_fields

    if primary_key:
        return [
            DataDiscoverySortField(column=column_name, direction="asc")
            for column_name in primary_key
            if column_name in available_columns
        ]

    if "symbol" in available_columns:
        return [DataDiscoverySortField(column="symbol", direction="asc")]

    if fields:
        return [DataDiscoverySortField(column=fields[0].name, direction="asc")]

    return []


def _order_expression(column: Any, direction: Literal["asc", "desc"]) -> Any:
    if direction == "desc":
        return column.desc().nulls_last()
    return column.asc().nulls_last()


def _catalog_cache_key(dsn: str, visible_schemas: Sequence[str]) -> str:
    return f"{dsn}|{','.join(_normalize_visible_schemas(visible_schemas))}"


def _detail_cache_key(dsn: str, visible_schemas: Sequence[str], schema_name: str, table_name: str) -> tuple[str, str, str, str]:
    return (
        dsn,
        ",".join(_normalize_visible_schemas(visible_schemas)),
        schema_name,
        table_name,
    )


def _get_cached_catalog(cache_key: str) -> Optional[DataDiscoveryCatalogResponse]:
    with _DISCOVERY_CACHE_LOCK:
        entry = _DISCOVERY_CATALOG_CACHE.get(cache_key)
        if not entry:
            return None
        expires_at, payload = entry
        if time.monotonic() >= expires_at:
            _DISCOVERY_CATALOG_CACHE.pop(cache_key, None)
            return None
        return payload.model_copy(deep=True)


def _set_cached_catalog(cache_key: str, payload: DataDiscoveryCatalogResponse, ttl_seconds: float) -> None:
    if ttl_seconds <= 0:
        return
    with _DISCOVERY_CACHE_LOCK:
        _DISCOVERY_CATALOG_CACHE[cache_key] = (
            time.monotonic() + ttl_seconds,
            payload.model_copy(deep=True),
        )


def _get_cached_detail(cache_key: tuple[str, str, str, str]) -> Optional[DataDiscoveryDatasetDetailResponse]:
    with _DISCOVERY_CACHE_LOCK:
        entry = _DISCOVERY_DETAIL_CACHE.get(cache_key)
        if not entry:
            return None
        expires_at, payload = entry
        if time.monotonic() >= expires_at:
            _DISCOVERY_DETAIL_CACHE.pop(cache_key, None)
            return None
        return payload.model_copy(deep=True)


def _set_cached_detail(
    cache_key: tuple[str, str, str, str],
    payload: DataDiscoveryDatasetDetailResponse,
    ttl_seconds: float,
) -> None:
    if ttl_seconds <= 0:
        return
    with _DISCOVERY_CACHE_LOCK:
        _DISCOVERY_DETAIL_CACHE[cache_key] = (
            time.monotonic() + ttl_seconds,
            payload.model_copy(deep=True),
        )


def reset_data_discovery_caches() -> None:
    with _DISCOVERY_CACHE_LOCK:
        _DISCOVERY_CATALOG_CACHE.clear()
        _DISCOVERY_DETAIL_CACHE.clear()


def _build_dataset_detail_from_context(
    engine: Any,
    insp: Any,
    *,
    dsn: str,
    visible_schemas: Sequence[str],
    schema_name: str,
    table_name: str,
    gold_table_specs: dict[str, UniverseTableSpec],
    inherited_warnings: Sequence[str] | None = None,
) -> DataDiscoveryDatasetDetailResponse:
    metadata = _load_table_metadata(
        engine,
        insp,
        schema_name=schema_name,
        table_name=table_name,
        visible_schemas=visible_schemas,
    )
    gold_table_spec = gold_table_specs.get(table_name) if schema_name == "gold" else None
    gold_lookup_by_column, lookup_warnings = _load_gold_lookup_field_map_safely(
        engine,
        schema_name=schema_name,
        table_name=table_name,
    )
    fields = _build_data_discovery_fields(
        schema_name=schema_name,
        table_name=table_name,
        metadata=metadata,
        gold_lookup_by_column=gold_lookup_by_column,
        gold_table_spec=gold_table_spec,
    )
    as_of_column = gold_table_spec.as_of_column if gold_table_spec is not None else _detect_as_of_column(metadata.columns)
    default_sort = _default_sort_fields(
        as_of_column=as_of_column,
        fields=fields,
        primary_key=metadata.primary_key,
    )

    table_description = _load_postgres_table_description(
        engine,
        schema_name=schema_name,
        table_name=table_name,
    )
    scope = _dataset_scope(schema_name)
    label = _humanize_table_name(table_name)
    description = table_description or _default_dataset_description(
        scope=scope,
        schema_name=schema_name,
        table_name=table_name,
        label=label,
    )
    freshness, freshness_warnings = _load_gold_freshness(table_name) if scope == "gold" else (None, [])
    warnings = [
        *list(inherited_warnings or []),
        *lookup_warnings,
        *freshness_warnings,
    ]
    return DataDiscoveryDatasetDetailResponse(
        scope=scope,
        schemaName=schema_name,
        tableName=table_name,
        label=label,
        description=description,
        primaryKey=list(metadata.primary_key),
        asOfColumn=as_of_column,
        defaultSort=default_sort,
        fields=fields,
        freshness=freshness,
        warnings=warnings,
    )


def _dataset_summary_from_detail(detail: DataDiscoveryDatasetDetailResponse) -> DataDiscoveryDatasetSummary:
    return DataDiscoveryDatasetSummary(
        scope=detail.scope,
        schemaName=detail.schemaName,
        tableName=detail.tableName,
        label=detail.label,
        description=detail.description,
        primaryKey=list(detail.primaryKey),
        asOfColumn=detail.asOfColumn,
        fieldCount=len(detail.fields),
        sampleSupported=bool(detail.fields),
        freshness=detail.freshness.model_copy(deep=True) if detail.freshness else None,
    )


def build_data_discovery_catalog(request: Request) -> DataDiscoveryCatalogResponse:
    dsn = _require_postgres_dsn(request)
    settings = get_settings(request).data_discovery
    cache_key = _catalog_cache_key(dsn, settings.visible_schemas)
    cached = _get_cached_catalog(cache_key)
    if cached is not None:
        return cached

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        gold_table_specs, gold_spec_warnings = _load_gold_table_specs_safely(dsn)
        datasets: list[DataDiscoveryDatasetSummary] = []
        warnings: list[str] = list(gold_spec_warnings)
        for schema_name in _visible_schema_names(insp, visible_schemas=settings.visible_schemas):
            for table_name in _visible_table_names(
                insp,
                schema_name=schema_name,
                visible_schemas=settings.visible_schemas,
            ):
                detail = _build_dataset_detail_from_context(
                    engine,
                    insp,
                    dsn=dsn,
                    visible_schemas=settings.visible_schemas,
                    schema_name=schema_name,
                    table_name=table_name,
                    gold_table_specs=gold_table_specs,
                    inherited_warnings=gold_spec_warnings if schema_name == "gold" else [],
                )
                _set_cached_detail(
                    _detail_cache_key(dsn, settings.visible_schemas, schema_name, table_name),
                    detail,
                    settings.cache_ttl_seconds,
                )
                datasets.append(_dataset_summary_from_detail(detail))
                warnings.extend(detail.warnings)
        payload = DataDiscoveryCatalogResponse(
            datasets=sorted(
                datasets,
                key=lambda item: (item.scope, item.schemaName, item.tableName),
            ),
            warnings=list(dict.fromkeys(warnings)),
        )
        _set_cached_catalog(cache_key, payload, settings.cache_ttl_seconds)
        return payload
    finally:
        engine.dispose()


def build_data_discovery_dataset_detail(
    request: Request,
    *,
    schema_name: str,
    table_name: str,
) -> DataDiscoveryDatasetDetailResponse:
    dsn = _require_postgres_dsn(request)
    settings = get_settings(request).data_discovery
    cache_key = _detail_cache_key(dsn, settings.visible_schemas, schema_name, table_name)
    cached = _get_cached_detail(cache_key)
    if cached is not None:
        return cached

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        gold_table_specs, gold_spec_warnings = _load_gold_table_specs_safely(dsn)
        payload = _build_dataset_detail_from_context(
            engine,
            insp,
            dsn=dsn,
            visible_schemas=settings.visible_schemas,
            schema_name=schema_name,
            table_name=table_name,
            gold_table_specs=gold_table_specs,
            inherited_warnings=gold_spec_warnings if schema_name == "gold" else [],
        )
        _set_cached_detail(cache_key, payload, settings.cache_ttl_seconds)
        return payload
    finally:
        engine.dispose()


def build_data_discovery_sample(
    request: Request,
    *,
    schema_name: str,
    table_name: str,
    limit: int,
) -> DataDiscoverySampleResponse:
    settings = get_settings(request).data_discovery
    if int(limit) > settings.sample_max_limit:
        raise HTTPException(
            status_code=400,
            detail=f"Sample limit must be <= {settings.sample_max_limit}.",
        )

    detail = build_data_discovery_dataset_detail(
        request,
        schema_name=schema_name,
        table_name=table_name,
    )
    dsn = _require_postgres_dsn(request)
    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        _validate_table_target(
            insp,
            schema_name=schema_name,
            table_name=table_name,
            visible_schemas=settings.visible_schemas,
        )
        table = _reflect_table(engine, schema_name=schema_name, table_name=table_name)
        statement = select(table).limit(int(limit))
        order_by = []
        for sort_field in detail.defaultSort:
            column = table.c.get(sort_field.column)
            if column is None:
                continue
            order_by.append(_order_expression(column, sort_field.direction))
        if order_by:
            statement = statement.order_by(*order_by)

        with engine.connect() as conn:
            rows = conn.execute(statement).mappings().all()
        return DataDiscoverySampleResponse(
            scope=detail.scope,
            schemaName=schema_name,
            tableName=table_name,
            limit=int(limit),
            sortApplied=[field.model_copy(deep=True) for field in detail.defaultSort],
            rows=[dict(row) for row in rows],
        )
    finally:
        engine.dispose()
