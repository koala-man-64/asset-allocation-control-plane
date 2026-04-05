from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

from core import core as mdc


DOMAIN_METADATA_CACHE_PATH_DEFAULT = "metadata/domain-metadata.json"
DOMAIN_METADATA_UI_CACHE_PATH_DEFAULT = "metadata/ui-cache/domain-metadata-snapshot.json"
_HISTORY_LIMIT = 200
_FINANCE_SUBFOLDERS: tuple[str, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
    "valuation",
)
_LAYER_CONTAINER_ENV: dict[str, str] = {
    "bronze": "AZURE_CONTAINER_BRONZE",
    "silver": "AZURE_CONTAINER_SILVER",
    "gold": "AZURE_CONTAINER_GOLD",
    "platinum": "AZURE_CONTAINER_PLATINUM",
}
_BLOB_PREFIXES: dict[tuple[str, str], str] = {
    ("bronze", "market"): "market-data/",
    ("bronze", "finance"): "finance-data/",
    ("bronze", "earnings"): "earnings-data/",
    ("bronze", "price-target"): "price-target-data/",
    ("bronze", "platinum"): "platinum/",
    ("silver", "market"): "market-data/",
    ("silver", "finance"): "finance-data/",
    ("silver", "earnings"): "earnings-data/",
    ("silver", "price-target"): "price-target-data/",
    ("gold", "market"): "market/",
    ("gold", "finance"): "finance/",
    ("gold", "earnings"): "earnings/",
    ("gold", "price-target"): "targets/",
    ("gold", "regime"): "regime/",
    ("platinum", "platinum"): "platinum/",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_layer(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def normalize_domain(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _normalize_columns(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        column = str(value or "").strip()
        if not column or column in seen:
            continue
        seen.add(column)
        normalized.append(column)
    return normalized


def _normalize_finance_subfolder_counts(raw: Any) -> Optional[dict[str, int]]:
    if not isinstance(raw, dict):
        return None

    out: dict[str, int] = {}
    for key, value in raw.items():
        normalized_key = str(key or "").strip().lower().replace("-", "_")
        if normalized_key not in _FINANCE_SUBFOLDERS:
            continue
        try:
            out[normalized_key] = int(value)
        except Exception:
            continue
    return out or None


def _container_name_for_layer(layer: str) -> str:
    normalized_layer = normalize_layer(layer)
    env_name = _LAYER_CONTAINER_ENV.get(normalized_layer)
    if not env_name:
        return normalized_layer
    return str(os.environ.get(env_name) or "").strip() or normalized_layer


def _blob_prefix(layer: str, domain: str) -> Optional[str]:
    return _BLOB_PREFIXES.get((normalize_layer(layer), normalize_domain(domain)))


def _domain_metadata_cache_key(layer: str, domain: str) -> str:
    return f"{normalize_layer(layer)}/{normalize_domain(domain)}"


def _default_domain_metadata_document() -> dict[str, Any]:
    return {"version": 1, "updatedAt": None, "entries": {}}


def _default_ui_snapshot_document() -> dict[str, Any]:
    return {"version": 1, "updatedAt": None, "entries": {}, "warnings": []}


def _load_common_document(path: str, factory) -> dict[str, Any]:
    payload = mdc.get_common_json_content(path)
    if not isinstance(payload, dict):
        payload = factory()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    return payload


def build_domain_metadata_snapshot_metadata_from_artifact(
    *,
    layer: str,
    domain: str,
    artifact: dict[str, Any],
    container: Optional[str] = None,
) -> dict[str, Any]:
    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    computed_at = str(artifact.get("updatedAt") or artifact.get("computedAt") or _utc_now_iso())
    columns = _normalize_columns(artifact.get("columns"))
    column_count = artifact.get("columnCount")
    if not isinstance(column_count, int):
        column_count = len(columns)
    total_bytes = artifact.get("totalBytes")
    if not isinstance(total_bytes, int):
        total_bytes = None

    warnings = artifact.get("warnings")
    if not isinstance(warnings, list):
        warnings = []
    prefix = str(artifact.get("activeDataPrefix") or artifact.get("prefix") or "").strip()
    file_count = artifact.get("fileCount")
    if not isinstance(file_count, int):
        file_count = None

    return {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "container": str(container or "").strip() or _container_name_for_layer(normalized_layer),
        "type": "blob",
        "prefix": prefix or _blob_prefix(normalized_layer, normalized_domain),
        "tablePath": None,
        "computedAt": computed_at,
        "folderLastModified": computed_at,
        "symbolCount": artifact.get("symbolCount"),
        "financeSubfolderSymbolCounts": _normalize_finance_subfolder_counts(
            artifact.get("financeSubfolderSymbolCounts")
        ),
        "blacklistedSymbolCount": None,
        "dateRange": artifact.get("dateRange"),
        "columns": columns,
        "columnCount": column_count,
        "totalRows": None,
        "fileCount": file_count,
        "totalBytes": total_bytes,
        "deltaVersion": None,
        "metadataPath": artifact.get("artifactPath") or artifact.get("metadataPath"),
        "metadataSource": "artifact",
        "warnings": [str(item) for item in warnings if str(item or "").strip()],
    }


def build_domain_metadata_snapshot_metadata_for_purge(
    *,
    layer: str,
    domain: str,
    container: Optional[str] = None,
    computed_at: Optional[str] = None,
) -> dict[str, Any]:
    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    purge_time = str(computed_at or _utc_now_iso())

    return {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "container": str(container or "").strip() or _container_name_for_layer(normalized_layer),
        "type": "blob",
        "prefix": _blob_prefix(normalized_layer, normalized_domain),
        "tablePath": None,
        "computedAt": purge_time,
        "folderLastModified": None,
        "symbolCount": 0,
        "financeSubfolderSymbolCounts": None,
        "blacklistedSymbolCount": 0,
        "dateRange": None,
        "columns": [],
        "columnCount": 0,
        "totalRows": None,
        "fileCount": 0,
        "totalBytes": 0,
        "deltaVersion": None,
        "metadataPath": None,
        "metadataSource": "scan",
        "warnings": [],
    }


def build_snapshot_miss_payload(
    *,
    layer: str,
    domain: str,
    warning: Optional[str] = None,
) -> dict[str, Any]:
    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    message = (
        str(warning).strip()
        if isinstance(warning, str) and warning.strip()
        else f"No cached domain metadata snapshot found for layer={normalized_layer} domain={normalized_domain}."
    )
    now = _utc_now_iso()
    return {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "container": "",
        "type": "blob",
        "computedAt": now,
        "folderLastModified": None,
        "cachedAt": None,
        "cacheSource": "snapshot",
        "symbolCount": None,
        "columns": [],
        "columnCount": None,
        "dateRange": None,
        "totalRows": None,
        "fileCount": None,
        "totalBytes": None,
        "deltaVersion": None,
        "tablePath": None,
        "prefix": None,
        "blacklistedSymbolCount": None,
        "metadataPath": None,
        "metadataSource": None,
        "warnings": [message],
    }


def write_domain_metadata_snapshot_documents(
    *,
    layer: str,
    domain: str,
    metadata: dict[str, Any],
    snapshot_path: str = DOMAIN_METADATA_CACHE_PATH_DEFAULT,
    ui_snapshot_path: str = DOMAIN_METADATA_UI_CACHE_PATH_DEFAULT,
) -> dict[str, Any]:
    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    key = _domain_metadata_cache_key(normalized_layer, normalized_domain)
    now = _utc_now_iso()

    metadata_payload = dict(metadata)
    metadata_payload["layer"] = normalized_layer
    metadata_payload["domain"] = normalized_domain
    metadata_payload["cachedAt"] = now
    metadata_payload["cacheSource"] = "snapshot"

    snapshot_doc = _load_common_document(snapshot_path, _default_domain_metadata_document)
    snapshot_entries = snapshot_doc.get("entries")
    previous_entry = snapshot_entries.get(key) if isinstance(snapshot_entries, dict) else None
    history: list[dict[str, Any]] = []
    if isinstance(previous_entry, dict):
        previous_history = previous_entry.get("history")
        if isinstance(previous_history, list):
            history.extend(dict(item) for item in previous_history[-(_HISTORY_LIMIT - 1):] if isinstance(item, dict))

    history.append(
        {
            "timestamp": now,
            "symbolCount": metadata_payload.get("symbolCount"),
            "columnCount": metadata_payload.get("columnCount"),
            "fileCount": metadata_payload.get("fileCount"),
            "totalRows": metadata_payload.get("totalRows"),
            "totalBytes": metadata_payload.get("totalBytes"),
            "deltaVersion": metadata_payload.get("deltaVersion"),
        }
    )
    snapshot_entries[key] = {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "cachedAt": now,
        "metadata": metadata_payload,
        "history": history[-_HISTORY_LIMIT:],
    }
    snapshot_doc["version"] = 1
    snapshot_doc["updatedAt"] = now
    mdc.save_common_json_content(snapshot_doc, snapshot_path)

    ui_doc = _load_common_document(ui_snapshot_path, _default_ui_snapshot_document)
    ui_entries = ui_doc.get("entries")
    if isinstance(ui_entries, dict):
        ui_entries[key] = dict(metadata_payload)
    ui_doc["version"] = 1
    ui_doc["updatedAt"] = now
    warnings = ui_doc.get("warnings")
    if not isinstance(warnings, list):
        ui_doc["warnings"] = []
    mdc.save_common_json_content(ui_doc, ui_snapshot_path)
    return metadata_payload


def update_domain_metadata_snapshots_from_artifact(
    *,
    layer: str,
    domain: str,
    artifact: dict[str, Any],
) -> dict[str, Any]:
    metadata = build_domain_metadata_snapshot_metadata_from_artifact(
        layer=layer,
        domain=domain,
        artifact=artifact,
    )
    return write_domain_metadata_snapshot_documents(layer=layer, domain=domain, metadata=metadata)


def refresh_domain_metadata_snapshots_from_saved_artifact(
    *,
    layer: str,
    domain: str,
) -> Optional[dict[str, Any]]:
    from core import domain_artifacts

    artifact = domain_artifacts.load_domain_artifact(layer=layer, domain=domain)
    if not isinstance(artifact, dict):
        return None
    return update_domain_metadata_snapshots_from_artifact(layer=layer, domain=domain, artifact=artifact)


def mark_domain_metadata_snapshot_purged(
    *,
    layer: str,
    domain: str,
    container: Optional[str] = None,
    computed_at: Optional[str] = None,
) -> dict[str, Any]:
    metadata = build_domain_metadata_snapshot_metadata_for_purge(
        layer=layer,
        domain=domain,
        container=container,
        computed_at=computed_at,
    )
    return write_domain_metadata_snapshot_documents(layer=layer, domain=domain, metadata=metadata)
