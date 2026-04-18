from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from asset_allocation_runtime_common.foundation.postgres import connect
from core.strategy_engine.contracts import (
    UniverseCondition,
    UniverseConditionOperator,
    UniverseDefinition,
    UniverseGroup,
)

logger = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FIELD_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")
_AS_OF_COLUMN_CANDIDATES = ("as_of_ts", "timestamp", "ts", "datetime", "date", "obs_date")
_NUMERIC_TYPES = {
    "smallint",
    "integer",
    "bigint",
    "numeric",
    "real",
    "double precision",
    "decimal",
}
_BOOLEAN_TYPES = {"boolean"}
_DATE_TYPES = {"date"}
_DATETIME_TYPES = {"timestamp without time zone", "timestamp with time zone"}
_STRING_TYPES = {"text", "character varying", "character", "uuid"}
_NUMBER_OPERATORS: tuple[UniverseConditionOperator, ...] = (
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
)
_STRING_OPERATORS: tuple[UniverseConditionOperator, ...] = (
    "eq",
    "ne",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
)
_BOOLEAN_OPERATORS: tuple[UniverseConditionOperator, ...] = (
    "eq",
    "ne",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
)


def _is_catalog_table_name(table_name: str) -> bool:
    normalized = str(table_name or "").strip().lower()
    return bool(normalized) and not normalized.endswith(("_backup", "_by_date"))


@dataclass(frozen=True)
class UniverseFieldSpec:
    field_id: str
    label: str
    table: str
    column: str
    value_kind: str
    operators: tuple[UniverseConditionOperator, ...]


UNIVERSE_FIELD_MAP: dict[str, UniverseFieldSpec] = {
    "market.close": UniverseFieldSpec(
        field_id="market.close",
        label="Market Close",
        table="market_data",
        column="close",
        value_kind="number",
        operators=_NUMBER_OPERATORS,
    ),
    "market.trade_date": UniverseFieldSpec(
        field_id="market.trade_date",
        label="Market Trade Date",
        table="market_data",
        column="date",
        value_kind="date",
        operators=_NUMBER_OPERATORS,
    ),
    "market.timestamp": UniverseFieldSpec(
        field_id="market.timestamp",
        label="Market Timestamp",
        table="market_data",
        column="date",
        value_kind="date",
        operators=_NUMBER_OPERATORS,
    ),
    "returns.return_20d": UniverseFieldSpec(
        field_id="returns.return_20d",
        label="20D Return",
        table="market_data",
        column="return_20d",
        value_kind="number",
        operators=_NUMBER_OPERATORS,
    ),
    "returns.return_126d": UniverseFieldSpec(
        field_id="returns.return_126d",
        label="126D Return",
        table="market_data",
        column="return_60d",
        value_kind="number",
        operators=_NUMBER_OPERATORS,
    ),
    "security.is_active": UniverseFieldSpec(
        field_id="security.is_active",
        label="Security Is Active",
        table="symbols",
        column="status",
        value_kind="string",
        operators=_STRING_OPERATORS,
    ),
    "security.sector": UniverseFieldSpec(
        field_id="security.sector",
        label="Security Sector",
        table="symbols",
        column="sector",
        value_kind="string",
        operators=_STRING_OPERATORS,
    ),
    "security.delisted_at": UniverseFieldSpec(
        field_id="security.delisted_at",
        label="Security Delisted At",
        table="symbols",
        column="delisting_date",
        value_kind="date",
        operators=_NUMBER_OPERATORS,
    ),
    "quality.piotroski_f_score": UniverseFieldSpec(
        field_id="quality.piotroski_f_score",
        label="Piotroski F Score",
        table="finance_data",
        column="piotroski_f_score",
        value_kind="number",
        operators=_NUMBER_OPERATORS,
    ),
    "earnings.surprise_pct": UniverseFieldSpec(
        field_id="earnings.surprise_pct",
        label="Earnings Surprise %",
        table="earnings_data",
        column="surprise_pct",
        value_kind="number",
        operators=_NUMBER_OPERATORS,
    ),
}

_UNIVERSE_TABLE_COLUMN_TO_FIELD: dict[tuple[str, str], str] = {
    (spec.table, spec.column): field_id for field_id, spec in UNIVERSE_FIELD_MAP.items()
}


@dataclass(frozen=True)
class UniverseColumnSpec:
    name: str
    data_type: str
    value_kind: str
    operators: tuple[UniverseConditionOperator, ...]


@dataclass(frozen=True)
class UniverseTableSpec:
    name: str
    as_of_column: str
    columns: dict[str, UniverseColumnSpec]
    as_of_kind: str = "slower"


def list_gold_universe_catalog(dsn: str) -> dict[str, Any]:
    fields = [
        {
            "id": spec.field_id,
            "label": spec.label,
            "valueKind": spec.value_kind,
            "operators": list(spec.operators),
        }
        for spec in sorted(UNIVERSE_FIELD_MAP.values(), key=lambda item: item.field_id)
    ]
    logger.info("Universe catalog loaded: fields=%d", len(fields))
    return {"source": "postgres_gold", "fields": fields}


def preview_gold_universe(
    dsn: str,
    universe: Any,
    *,
    sample_limit: int = 25,
) -> dict[str, Any]:
    if sample_limit < 1:
        raise ValueError("sample_limit must be >= 1.")

    normalized_universe = _normalize_universe_definition(universe)
    table_specs = _load_gold_table_specs(dsn)
    with connect(dsn) as conn:
        symbols, fields_used = _evaluate_node(conn, normalized_universe.root, table_specs)

    ordered_symbols = sorted(symbols)
    warnings: list[str] = []
    if not ordered_symbols:
        warnings.append("Universe preview matched zero symbols.")

    logger.info(
        "Universe preview resolved: fields=%s symbol_count=%d sample_limit=%d",
        ",".join(sorted(fields_used)),
        len(ordered_symbols),
        sample_limit,
    )
    return {
        "source": "postgres_gold",
        "symbolCount": len(ordered_symbols),
        "sampleSymbols": ordered_symbols[:sample_limit],
        "fieldsUsed": sorted(fields_used),
        "warnings": warnings,
    }


def _normalize_universe_definition(universe: Any) -> UniverseDefinition:
    payload = _payload_to_dict(universe)
    payload.setdefault("source", "postgres_gold")
    try:
        return UniverseDefinition.model_validate(payload)
    except Exception:
        translated = dict(payload)
        translated["root"], _ = _normalize_universe_node(payload.get("root"))
        try:
            return UniverseDefinition.model_validate(translated)
        except Exception:
            translated["root"] = _publicize_universe_node(payload.get("root"))
            return UniverseDefinition.model_validate(translated)


def _publicize_universe_definition(universe: Any) -> dict[str, Any]:
    payload = _normalize_universe_definition(universe).model_dump(exclude_none=True)
    root = payload.get("root")
    if isinstance(root, dict) and "field" in root:
        return payload
    payload["root"] = _publicize_universe_node(root)
    return payload


def _normalize_universe_node(node: Any) -> tuple[dict[str, Any], set[str]]:
    if not isinstance(node, dict):
        raise ValueError("Universe root must be an object.")

    kind = str(node.get("kind") or "").strip().lower()
    if kind == "condition":
        return _normalize_universe_condition(node)
    if kind == "group":
        clauses = node.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            raise ValueError("Universe groups must contain at least one clause.")
        normalized_clauses: list[dict[str, Any]] = []
        fields_used: set[str] = set()
        for clause in clauses:
            normalized_clause, clause_fields = _normalize_universe_node(clause)
            normalized_clauses.append(normalized_clause)
            fields_used.update(clause_fields)
        normalized_group: dict[str, Any] = {
            "kind": "group",
            "operator": node.get("operator", "and"),
            "clauses": normalized_clauses,
        }
        return normalized_group, fields_used
    raise ValueError("Universe nodes must declare kind='group' or kind='condition'.")


def _normalize_universe_condition(node: dict[str, Any]) -> tuple[dict[str, Any], set[str]]:
    field_id = str(node.get("field") or "").strip()
    table_name = str(node.get("table") or "").strip()
    column_name = str(node.get("column") or "").strip()
    if field_id:
        field_id = _normalize_field_id(field_id)
        spec = UNIVERSE_FIELD_MAP.get(field_id)
        if spec is None:
            raise ValueError(f"Unknown universe field '{field_id}'.")
        normalized = {
            "kind": "condition",
            "table": spec.table,
            "column": spec.column,
            "operator": node.get("operator"),
        }
        if "value" in node:
            normalized["value"] = node.get("value")
        if "values" in node:
            normalized["values"] = node.get("values")
        return normalized, {field_id}

    if table_name and column_name:
        field_id = _UNIVERSE_TABLE_COLUMN_TO_FIELD.get((_normalize_identifier(table_name, "table"), _normalize_identifier(column_name, "column")))
        normalized = {
            "kind": "condition",
            "table": table_name,
            "column": column_name,
            "operator": node.get("operator"),
        }
        if "value" in node:
            normalized["value"] = node.get("value")
        if "values" in node:
            normalized["values"] = node.get("values")
        return normalized, {field_id} if field_id else {f"{table_name}.{column_name}"}

    raise ValueError("Universe conditions must declare field or table/column.")


def _publicize_universe_node(node: Any) -> dict[str, Any]:
    if not isinstance(node, dict):
        raise ValueError("Universe root must be an object.")

    kind = str(node.get("kind") or "").strip().lower()
    if kind == "condition":
        return _publicize_universe_condition(node)
    if kind == "group":
        clauses = node.get("clauses")
        if not isinstance(clauses, list) or not clauses:
            raise ValueError("Universe groups must contain at least one clause.")
        return {
            "kind": "group",
            "operator": node.get("operator", "and"),
            "clauses": [_publicize_universe_node(clause) for clause in clauses],
        }
    raise ValueError("Universe nodes must declare kind='group' or kind='condition'.")


def _publicize_universe_condition(node: dict[str, Any]) -> dict[str, Any]:
    table_name = str(node.get("table") or "").strip()
    column_name = str(node.get("column") or "").strip()
    field_id = _UNIVERSE_TABLE_COLUMN_TO_FIELD.get(
        (_normalize_identifier(table_name, "table"), _normalize_identifier(column_name, "column"))
    ) if table_name and column_name else None
    public_node = {
        "kind": "condition",
        "operator": node.get("operator"),
    }
    if field_id:
        public_node["field"] = field_id
    elif table_name and column_name:
        public_node["table"] = table_name
        public_node["column"] = column_name
    if "value" in node:
        public_node["value"] = node.get("value")
    if "values" in node:
        public_node["values"] = node.get("values")
    return public_node


def _payload_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    raise ValueError("Universe payload must be an object.")


def _load_gold_table_specs(dsn: str) -> dict[str, UniverseTableSpec]:
    query = """
        SELECT table_name, column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'gold'
        ORDER BY table_name, ordinal_position
    """
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    rows = [row for row in rows if _is_catalog_table_name(str(row[0] or ""))]
    return _build_table_specs(rows)


def _build_table_specs(rows: list[tuple[Any, ...]]) -> dict[str, UniverseTableSpec]:
    by_table: dict[str, list[tuple[str, str, str]]] = {}
    for table_name_raw, column_name_raw, data_type_raw, udt_name_raw in rows:
        table_name = _normalize_identifier(str(table_name_raw or ""), "table")
        column_name = _normalize_identifier(str(column_name_raw or ""), "column")
        data_type = str(data_type_raw or "").strip().lower()
        udt_name = str(udt_name_raw or "").strip().lower()
        by_table.setdefault(table_name, []).append((column_name, data_type, udt_name))

    table_specs: dict[str, UniverseTableSpec] = {}
    for table_name, column_rows in sorted(by_table.items()):
        column_specs: dict[str, UniverseColumnSpec] = {}
        has_symbol = False
        as_of_column: str | None = None
        for column_name, data_type, udt_name in column_rows:
            if column_name == "symbol":
                has_symbol = True
            if as_of_column is None and column_name in _AS_OF_COLUMN_CANDIDATES:
                as_of_column = column_name

            value_kind = _classify_value_kind(data_type, udt_name)
            if value_kind is None:
                continue
            column_specs[column_name] = UniverseColumnSpec(
                name=column_name,
                data_type=data_type,
                value_kind=value_kind,
                operators=_operators_for_value_kind(value_kind),
            )

        if not has_symbol or not as_of_column:
            continue
        as_of_kind = "intraday" if _is_intraday_data_type(as_of_column, column_rows) else "slower"
        table_specs[table_name] = UniverseTableSpec(
            name=table_name,
            as_of_column=as_of_column,
            as_of_kind=as_of_kind,
            columns=column_specs,
        )
    return table_specs


def _is_intraday_data_type(as_of_column: str, column_rows: list[tuple[str, str, str]]) -> bool:
    for column_name, data_type, _udt_name in column_rows:
        if column_name != as_of_column:
            continue
        return data_type in _DATETIME_TYPES
    return False


def is_intraday_table_spec(spec: UniverseTableSpec) -> bool:
    return spec.as_of_kind == "intraday"


def _evaluate_node(
    conn: Any,
    node: UniverseGroup | UniverseCondition,
    table_specs: dict[str, UniverseTableSpec],
) -> tuple[set[str], set[str]]:
    if isinstance(node, UniverseCondition):
        field_id_value = getattr(node, "field", None)
        if field_id_value:
            field_id = _normalize_field_id(str(field_id_value))
            field_spec = UNIVERSE_FIELD_MAP.get(field_id)
            if field_spec is None:
                raise ValueError(f"Unknown universe field '{node.field}'.")
        else:
            table_name = _normalize_identifier(getattr(node, "table", ""), "table")
            column_name = _normalize_identifier(getattr(node, "column", ""), "column")
            field_id = _UNIVERSE_TABLE_COLUMN_TO_FIELD.get((table_name, column_name))
            if field_id is None:
                raise ValueError(f"Unknown gold column mapping '{table_name}.{column_name}'.")
            field_spec = UNIVERSE_FIELD_MAP[field_id]
        table_spec = table_specs.get(field_spec.table)
        if table_spec is None:
            raise ValueError(f"Unknown gold table '{field_spec.table}'.")
        symbols = _fetch_condition_symbols(conn, table_spec, field_spec, node)
        return symbols, {field_spec.field_id}

    child_symbols: list[set[str]] = []
    tables_used: set[str] = set()
    for clause in node.clauses:
        clause_symbols, clause_tables = _evaluate_node(conn, clause, table_specs)
        child_symbols.append(clause_symbols)
        tables_used.update(clause_tables)

    if node.operator == "and":
        resolved = set(child_symbols[0])
        for item in child_symbols[1:]:
            resolved.intersection_update(item)
        return resolved, tables_used

    resolved = set()
    for item in child_symbols:
        resolved.update(item)
    return resolved, tables_used


def _fetch_condition_symbols(
    conn: Any,
    table_spec: UniverseTableSpec,
    field_spec: UniverseFieldSpec,
    condition: UniverseCondition,
) -> set[str]:
    column_spec = table_spec.columns.get(_normalize_identifier(field_spec.column, "column"))
    if column_spec is None:
        raise ValueError(f"Unknown column '{field_spec.column}' for gold.{table_spec.name}.")
    if condition.operator not in column_spec.operators:
        raise ValueError(
            f"Operator '{condition.operator}' is not supported for field '{field_spec.field_id}'."
        )

    predicate_sql, params = _build_predicate(condition, column_spec)
    symbol_identifier = _quote_identifier("symbol")
    column_identifier = _quote_identifier(column_spec.name)
    as_of_identifier = _quote_identifier(table_spec.as_of_column)
    query = f"""
        WITH latest AS (
            SELECT DISTINCT ON ({symbol_identifier})
              {symbol_identifier} AS symbol,
              {column_identifier} AS candidate_value
            FROM "gold".{_quote_identifier(table_spec.name)}
            WHERE {symbol_identifier} IS NOT NULL
            ORDER BY {symbol_identifier}, {as_of_identifier} DESC NULLS LAST
        )
        SELECT symbol
        FROM latest
        WHERE {predicate_sql}
        ORDER BY symbol
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        return {str(row[0]).strip().upper() for row in cur.fetchall() if str(row[0]).strip()}


def _build_predicate(
    condition: UniverseCondition,
    column_spec: UniverseColumnSpec,
) -> tuple[str, list[Any]]:
    if condition.operator == "is_null":
        return "candidate_value IS NULL", []
    if condition.operator == "is_not_null":
        return "candidate_value IS NOT NULL", []

    if condition.operator in {"in", "not_in"}:
        assert condition.values is not None
        coerced = _coerce_values(condition.values, column_spec)
        placeholders = ", ".join(["%s"] * len(coerced))
        comparator = "IN" if condition.operator == "in" else "NOT IN"
        return f"candidate_value {comparator} ({placeholders})", coerced

    assert condition.value is not None
    coerced_value = _coerce_value(condition.value, column_spec)
    comparator = {
        "eq": "=",
        "ne": "<>",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
    }.get(condition.operator)
    if comparator is None:
        raise ValueError(f"Unsupported operator '{condition.operator}'.")
    return f"candidate_value {comparator} %s", [coerced_value]


def _coerce_values(values: list[Any], column_spec: UniverseColumnSpec) -> list[Any]:
    if not values:
        raise ValueError("values must not be empty.")
    return [_coerce_value(value, column_spec) for value in values]


def _coerce_value(value: Any, column_spec: UniverseColumnSpec) -> Any:
    if column_spec.value_kind == "number":
        if isinstance(value, bool):
            raise ValueError(f"{column_spec.name} expects a numeric value.")
        if isinstance(value, (int, float)):
            return value
        try:
            text = str(value or "").strip()
            if not text:
                raise ValueError
            return float(text)
        except ValueError as exc:
            raise ValueError(f"{column_spec.name} expects a numeric value.") from exc

    if column_spec.value_kind == "boolean":
        if isinstance(value, bool):
            return value
        normalized = str(value or "").strip().lower()
        if normalized in {"true", "1", "yes", "y", "on", "t"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", "f"}:
            return False
        raise ValueError(f"{column_spec.name} expects a boolean value.")

    if column_spec.value_kind == "date":
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{column_spec.name} expects a date value.")
        normalized = text.replace("Z", "+00:00")
        try:
            return date.fromisoformat(normalized)
        except ValueError:
            try:
                return datetime.fromisoformat(normalized).date()
            except ValueError as exc:
                raise ValueError(f"{column_spec.name} expects an ISO date value.") from exc

    if column_spec.value_kind == "datetime":
        if isinstance(value, datetime):
            return value
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{column_spec.name} expects a datetime value.")
        normalized = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(f"{column_spec.name} expects an ISO datetime value.") from exc

    return str(value or "")


def _classify_value_kind(data_type: str, udt_name: str) -> str | None:
    normalized_data_type = str(data_type or "").strip().lower()
    normalized_udt_name = str(udt_name or "").strip().lower()

    if normalized_data_type in _NUMERIC_TYPES:
        return "number"
    if normalized_data_type in _BOOLEAN_TYPES:
        return "boolean"
    if normalized_data_type in _DATE_TYPES:
        return "date"
    if normalized_data_type in _DATETIME_TYPES:
        return "datetime"
    if normalized_data_type in _STRING_TYPES or normalized_udt_name in {"varchar", "text", "bpchar", "uuid"}:
        return "string"
    return None


def _operators_for_value_kind(value_kind: str) -> tuple[UniverseConditionOperator, ...]:
    if value_kind == "number" or value_kind == "date" or value_kind == "datetime":
        return _NUMBER_OPERATORS
    if value_kind == "boolean":
        return _BOOLEAN_OPERATORS
    return _STRING_OPERATORS


def _normalize_identifier(value: str, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or not _IDENTIFIER_PATTERN.match(normalized):
        raise ValueError(f"Invalid {label} identifier '{value}'.")
    return normalized


def _normalize_field_id(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized or not _FIELD_ID_PATTERN.match(normalized):
        raise ValueError(f"Invalid field identifier '{value}'.")
    return normalized


def _quote_identifier(identifier: str) -> str:
    normalized = _normalize_identifier(identifier, "identifier")
    return '"' + normalized.replace('"', '""') + '"'
