from datetime import date, datetime, time as time_value
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import String as SqlString, Table, and_, cast, create_engine, inspect, select, text

from api.service.data_discovery import (
    GoldColumnLookupResponse,
    PostgresColumnMetadata,
    TableMetadataResponse,
    _load_gold_lookup_tables as _load_gold_lookup_tables,
    _load_table_metadata as _load_table_metadata,
    _query_gold_lookup_rows as _query_gold_lookup_rows,
    _quote_identifier as _quote_identifier,
    _reflect_table as _reflect_table,
    _require_postgres_dsn as _require_postgres_dsn,
    _resolve_postgres_dsn as _resolve_postgres_dsn,
    _validate_table_target as _validate_table_target,
    _visible_schema_names as _visible_schema_names,
    _visible_table_names as _visible_table_names,
)
from api.service.dependencies import (
    get_settings,
    require_data_discovery_read_access,
    require_data_discovery_write_access,
)

router = APIRouter()
_BOOLEAN_TRUE_VALUES = frozenset({"1", "true", "t", "yes", "y", "on"})
_BOOLEAN_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})


class QueryFilter(BaseModel):
    column_name: str = Field(min_length=1)
    operator: Literal[
        "eq",
        "neq",
        "contains",
        "starts_with",
        "ends_with",
        "gt",
        "gte",
        "lt",
        "lte",
        "is_null",
        "is_not_null",
    ]
    value: Optional[Any] = None


class QueryRequest(BaseModel):
    schema_name: str
    table_name: str
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    filters: List[QueryFilter] = Field(default_factory=list)


class TableRequest(BaseModel):
    schema_name: str
    table_name: str


class UpdateRowRequest(BaseModel):
    schema_name: str
    table_name: str
    match: Dict[str, Any] = Field(default_factory=dict)
    values: Dict[str, Any] = Field(default_factory=dict)


def _normalize_data_type(value: object) -> str:
    return str(value or "").strip().lower()


def _is_text_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return any(token in normalized for token in ("char", "text", "uuid", "citext"))


def _is_integer_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return any(token in normalized for token in ("smallint", "integer", "bigint", "serial"))


def _is_numeric_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return any(
        token in normalized
        for token in ("smallint", "integer", "bigint", "numeric", "decimal", "real", "double", "float", "serial", "money")
    )


def _is_boolean_type(data_type: str) -> bool:
    return "bool" in _normalize_data_type(data_type)


def _is_datetime_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return "timestamp" in normalized or "datetime" in normalized


def _is_date_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return "date" in normalized and "time" not in normalized and "stamp" not in normalized


def _is_time_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return "time" in normalized and "stamp" not in normalized


def _allowed_query_operators(data_type: str) -> set[str]:
    allowed = {"eq", "neq", "is_null", "is_not_null"}
    if _is_text_type(data_type):
        allowed.update({"contains", "starts_with", "ends_with"})
    if _is_numeric_type(data_type) or _is_datetime_type(data_type) or _is_date_type(data_type) or _is_time_type(data_type):
        allowed.update({"gt", "gte", "lt", "lte"})
    return allowed


def _parse_iso_datetime(raw_value: Any) -> datetime:
    candidate = str(raw_value or "").strip()
    if not candidate:
        raise ValueError("datetime value is required")
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    return datetime.fromisoformat(candidate)


def _coerce_filter_value(column: PostgresColumnMetadata, raw_value: Any) -> Any:
    if raw_value is None:
        raise ValueError(f'Filter "{column.name}" requires a value.')

    data_type = column.data_type
    normalized_value = str(raw_value).strip()

    if not normalized_value:
        raise ValueError(f'Filter "{column.name}" requires a value.')

    if _is_boolean_type(data_type):
        lowered = normalized_value.lower()
        if lowered in _BOOLEAN_TRUE_VALUES:
            return True
        if lowered in _BOOLEAN_FALSE_VALUES:
            return False
        raise ValueError(
            f'Filter "{column.name}" must be a boolean value (true/false, yes/no, 1/0).'
        )

    if _is_numeric_type(data_type):
        try:
            if _is_integer_type(data_type):
                return int(normalized_value)
            return float(normalized_value)
        except ValueError as exc:
            raise ValueError(f'Filter "{column.name}" must be numeric.') from exc

    if _is_datetime_type(data_type):
        try:
            return _parse_iso_datetime(normalized_value)
        except ValueError as exc:
            raise ValueError(
                f'Filter "{column.name}" must be an ISO-8601 timestamp.'
            ) from exc

    if _is_date_type(data_type):
        try:
            return date.fromisoformat(normalized_value)
        except ValueError as exc:
            raise ValueError(f'Filter "{column.name}" must be an ISO-8601 date.') from exc

    if _is_time_type(data_type):
        try:
            return time_value.fromisoformat(normalized_value)
        except ValueError as exc:
            raise ValueError(f'Filter "{column.name}" must be an ISO-8601 time.') from exc

    return normalized_value


def _build_query_conditions(
    table: Table,
    filters: List[QueryFilter],
    metadata: TableMetadataResponse,
) -> List[Any]:
    if not filters:
        return []

    metadata_by_name = {column.name: column for column in metadata.columns}
    conditions: List[Any] = []

    for filter_item in filters:
        column_metadata = metadata_by_name.get(filter_item.column_name)
        if not column_metadata:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown query filter column "{filter_item.column_name}".',
            )

        allowed_operators = _allowed_query_operators(column_metadata.data_type)
        if filter_item.operator not in allowed_operators:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'Operator "{filter_item.operator}" is not supported for column '
                    f'"{filter_item.column_name}" ({column_metadata.data_type}).'
                ),
            )

        column = table.c[filter_item.column_name]

        if filter_item.operator == "is_null":
            conditions.append(column.is_(None))
            continue
        if filter_item.operator == "is_not_null":
            conditions.append(column.is_not(None))
            continue

        try:
            coerced_value = _coerce_filter_value(column_metadata, filter_item.value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if filter_item.operator == "eq":
            conditions.append(column == coerced_value)
        elif filter_item.operator == "neq":
            conditions.append(column != coerced_value)
        elif filter_item.operator == "contains":
            conditions.append(cast(column, SqlString).ilike(f"%{coerced_value}%"))
        elif filter_item.operator == "starts_with":
            conditions.append(cast(column, SqlString).ilike(f"{coerced_value}%"))
        elif filter_item.operator == "ends_with":
            conditions.append(cast(column, SqlString).ilike(f"%{coerced_value}"))
        elif filter_item.operator == "gt":
            conditions.append(column > coerced_value)
        elif filter_item.operator == "gte":
            conditions.append(column >= coerced_value)
        elif filter_item.operator == "lt":
            conditions.append(column < coerced_value)
        elif filter_item.operator == "lte":
            conditions.append(column <= coerced_value)

    return conditions


@router.get("/gold-column-lookup/tables")
def list_gold_lookup_tables(request: Request) -> List[str]:
    require_data_discovery_read_access(request)
    dsn = _require_postgres_dsn(request)

    engine = create_engine(dsn)
    try:
        return _load_gold_lookup_tables(engine)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch gold lookup tables: {str(exc)}")
    finally:
        engine.dispose()


@router.get("/gold-column-lookup", response_model=GoldColumnLookupResponse)
def list_gold_column_lookup(
    request: Request,
    table: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    status: Optional[Literal["draft", "reviewed", "approved"]] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
) -> GoldColumnLookupResponse:
    require_data_discovery_read_access(request)
    dsn = _require_postgres_dsn(request)

    engine = create_engine(dsn)
    try:
        return _query_gold_lookup_rows(
            engine,
            table_name=table,
            q=q,
            status=status,
            limit=limit,
            offset=offset,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch gold column lookup rows: {str(exc)}")
    finally:
        engine.dispose()


@router.get("/schemas")
def list_schemas(request: Request) -> List[str]:
    require_data_discovery_read_access(request)
    dsn = _require_postgres_dsn(request)
    visible_schemas = get_settings(request).data_discovery.visible_schemas

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        return _visible_schema_names(insp, visible_schemas=visible_schemas)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch schemas: {str(exc)}")
    finally:
        engine.dispose()


@router.get("/schemas/{schema_name}/tables")
def list_tables(schema_name: str, request: Request) -> List[str]:
    require_data_discovery_read_access(request)
    dsn = _require_postgres_dsn(request)
    visible_schemas = get_settings(request).data_discovery.visible_schemas

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        if schema_name not in _visible_schema_names(insp, visible_schemas=visible_schemas):
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found.")
        return _visible_table_names(insp, schema_name=schema_name, visible_schemas=visible_schemas)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tables: {str(exc)}")
    finally:
        engine.dispose()


@router.get("/schemas/{schema_name}/tables/{table_name}/metadata")
def get_table_metadata(schema_name: str, table_name: str, request: Request) -> TableMetadataResponse:
    require_data_discovery_read_access(request)
    dsn = _require_postgres_dsn(request)
    visible_schemas = get_settings(request).data_discovery.visible_schemas

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        return _load_table_metadata(
            engine,
            insp,
            schema_name=schema_name,
            table_name=table_name,
            visible_schemas=visible_schemas,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load table metadata: {str(exc)}")
    finally:
        engine.dispose()


@router.post("/query")
def query_table(payload: QueryRequest, request: Request) -> List[Dict[str, Any]]:
    require_data_discovery_read_access(request)
    dsn = _require_postgres_dsn(request)
    visible_schemas = get_settings(request).data_discovery.visible_schemas

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        _validate_table_target(
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            visible_schemas=visible_schemas,
        )
        table = _reflect_table(
            engine,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )
        metadata = _load_table_metadata(
            engine,
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            visible_schemas=visible_schemas,
        )
        conditions = _build_query_conditions(table, payload.filters, metadata)
        statement = select(table).limit(payload.limit).offset(payload.offset)
        if conditions:
            statement = statement.where(and_(*conditions))

        with engine.connect() as conn:
            rows = conn.execute(statement).mappings().all()

        return [dict(row) for row in rows]
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(exc)}")
    finally:
        engine.dispose()


@router.post("/update")
def update_row(payload: UpdateRowRequest, request: Request) -> Dict[str, Any]:
    require_data_discovery_write_access(request)
    dsn = _require_postgres_dsn(request)
    visible_schemas = get_settings(request).data_discovery.visible_schemas

    if not payload.values:
        raise HTTPException(status_code=400, detail="At least one field value is required.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        metadata = _load_table_metadata(
            engine,
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            visible_schemas=visible_schemas,
        )
        if not metadata.can_edit:
            raise HTTPException(
                status_code=400,
                detail=metadata.edit_reason or "Row editing is disabled for this table.",
            )

        missing_match_columns = [
            column_name for column_name in metadata.primary_key if column_name not in payload.match
        ]
        if missing_match_columns:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Primary-key match values are required for row updates: "
                    + ", ".join(missing_match_columns)
                ),
            )

        column_lookup = {column.name: column for column in metadata.columns}
        unknown_columns = [
            column_name for column_name in payload.values.keys() if column_name not in column_lookup
        ]
        if unknown_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown columns requested for update: {', '.join(sorted(unknown_columns))}",
            )

        read_only_columns = [
            column_name
            for column_name in payload.values.keys()
            if not column_lookup[column_name].editable
        ]
        if read_only_columns:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Read-only columns cannot be updated: "
                    + ", ".join(sorted(read_only_columns))
                ),
            )

        table = _reflect_table(
            engine,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )
        conditions = []
        for column_name in metadata.primary_key:
            column = table.c[column_name]
            match_value = payload.match.get(column_name)
            if match_value is None:
                conditions.append(column.is_(None))
            else:
                conditions.append(column == match_value)

        statement = table.update().where(and_(*conditions)).values(**payload.values)
        with engine.begin() as conn:
            result = conn.execute(statement)

        row_count = max(int(result.rowcount or 0), 0)
        if row_count == 0:
            raise HTTPException(
                status_code=404,
                detail="No row matched the provided primary-key values.",
            )

        return {
            "schema_name": payload.schema_name,
            "table_name": payload.table_name,
            "row_count": row_count,
            "updated_columns": sorted(payload.values.keys()),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to update row: {str(exc)}")
    finally:
        engine.dispose()


@router.post("/purge")
def purge_table(payload: TableRequest, request: Request) -> Dict[str, Any]:
    require_data_discovery_write_access(request)
    dsn = _require_postgres_dsn(request)
    visible_schemas = get_settings(request).data_discovery.visible_schemas

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        _validate_table_target(
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
            visible_schemas=visible_schemas,
        )

        qualified_table = (
            f"{_quote_identifier(payload.schema_name)}.{_quote_identifier(payload.table_name)}"
        )
        with engine.begin() as conn:
            result = conn.execute(text(f"DELETE FROM {qualified_table}"))

        return {
            "schema_name": payload.schema_name,
            "table_name": payload.table_name,
            "row_count": max(int(result.rowcount or 0), 0),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to purge table: {str(exc)}")
    finally:
        engine.dispose()

