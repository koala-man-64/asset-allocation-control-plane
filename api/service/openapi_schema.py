from __future__ import annotations

from copy import deepcopy
from typing import Any


_SCHEMA_ALIAS_ORDER: tuple[tuple[str, str], ...] = (
    ("StrategyConfigOutput", "StrategyConfig-Output"),
    ("StrategyConfig", "StrategyConfig-Output"),
    ("UniverseDefinitionOutput", "UniverseDefinition-Output"),
    ("UniverseDefinition", "UniverseDefinition-Output"),
    ("api__endpoints__strategies__UniversePreviewResponse", "UniversePreviewResponse"),
    ("asset_allocation_contracts__strategy__UniversePreviewResponse", "UniversePreviewResponse"),
    (
        "asset_allocation_contracts__strategy__UniverseCatalogResponse",
        "api__endpoints__universes__UniverseCatalogResponse",
    ),
)


def _rewrite_schema_refs(node: Any, aliases: dict[str, str]) -> None:
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            prefix = "#/components/schemas/"
            if ref.startswith(prefix):
                key = ref[len(prefix) :]
                target = aliases.get(key)
                if target:
                    node["$ref"] = f"{prefix}{target}"
        for value in node.values():
            _rewrite_schema_refs(value, aliases)
        return

    if isinstance(node, list):
        for item in node:
            _rewrite_schema_refs(item, aliases)


def _set_path_response_ref(
    schema: dict[str, Any],
    *,
    path: str,
    method: str,
    status_code: str,
    target: str,
) -> None:
    try:
        path_schema = schema["paths"][path][method]["responses"][status_code]["content"]["application/json"]["schema"]
    except KeyError:
        return
    path_schema["$ref"] = f"#/components/schemas/{target}"


def stabilize_openapi_schema(schema: dict[str, Any]) -> dict[str, Any]:
    components = schema.get("components", {})
    schemas = components.get("schemas")
    if not isinstance(schemas, dict):
        return schema

    aliases: dict[str, str] = {}
    for source, target in _SCHEMA_ALIAS_ORDER:
        if source not in schemas:
            continue
        if target not in schemas:
            schemas[target] = deepcopy(schemas[source])
        aliases[source] = target

    if not aliases:
        return schema

    _rewrite_schema_refs(schema, aliases)

    if "UniverseCatalogResponse" in schemas:
        shared_catalog = deepcopy(schemas["UniverseCatalogResponse"])
        schemas.setdefault("api__endpoints__universes__UniverseCatalogResponse", deepcopy(shared_catalog))
        schemas.setdefault("api__endpoints__strategies__UniverseCatalogResponse", deepcopy(shared_catalog))
        _set_path_response_ref(
            schema,
            path="/api/universes/catalog",
            method="get",
            status_code="200",
            target="api__endpoints__universes__UniverseCatalogResponse",
        )
        _set_path_response_ref(
            schema,
            path="/api/strategies/universe/catalog",
            method="get",
            status_code="200",
            target="api__endpoints__strategies__UniverseCatalogResponse",
        )

    for source, target in aliases.items():
        if source == target:
            continue
        if source in schemas and target in schemas:
            del schemas[source]

    return schema
