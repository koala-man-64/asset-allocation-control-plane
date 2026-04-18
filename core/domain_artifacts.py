from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation.blob_storage import BlobStorageClient
from asset_allocation_runtime_common.market_data import bronze_bucketing
from asset_allocation_runtime_common.market_data import domain_metadata_snapshots
logger = logging.getLogger(__name__)

ARTIFACT_VERSION = 1
DATE_RANGE_SOURCE = "artifact"
FINANCE_SUBDOMAINS: tuple[str, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
    "valuation",
)

_LAYER_CONTAINER_ENV: dict[str, str] = {
    "bronze": "AZURE_CONTAINER_BRONZE",
    "silver": "AZURE_CONTAINER_SILVER",
    "gold": "AZURE_CONTAINER_GOLD",
}
_ROOT_PREFIXES: dict[tuple[str, str], str] = {
    ("bronze", "market"): "market-data",
    ("bronze", "finance"): "finance-data",
    ("bronze", "earnings"): "earnings-data",
    ("bronze", "price-target"): "price-target-data",
    ("silver", "market"): "market-data",
    ("silver", "finance"): "finance-data",
    ("silver", "earnings"): "earnings-data",
    ("silver", "price-target"): "price-target-data",
    ("gold", "market"): "market",
    ("gold", "finance"): "finance",
    ("gold", "earnings"): "earnings",
    ("gold", "price-target"): "targets",
    ("gold", "regime"): "regime",
}
_FINANCE_SUBDOMAIN_ALIASES: dict[str, str] = {
    "balance_sheet": "balance_sheet",
    "balance-sheet": "balance_sheet",
    "income_statement": "income_statement",
    "income-statement": "income_statement",
    "cash_flow": "cash_flow",
    "cash-flow": "cash_flow",
    "valuation": "valuation",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_layer(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def normalize_domain(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def normalize_sub_domain(value: Optional[str]) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    return _FINANCE_SUBDOMAIN_ALIASES.get(normalized, normalized)


def root_prefix(*, layer: str, domain: str) -> str:
    key = (normalize_layer(layer), normalize_domain(domain))
    prefix = _ROOT_PREFIXES.get(key)
    if not prefix:
        raise ValueError(f"Unsupported layer/domain for metadata artifacts: layer={layer!r} domain={domain!r}")
    return prefix


def _storage_listing_prefix(*, layer: str, domain: str, sub_domain: Optional[str] = None) -> str:
    prefix = root_prefix(layer=layer, domain=domain).rstrip("/")
    normalized_domain = normalize_domain(domain)
    normalized_sub_domain = normalize_sub_domain(sub_domain)
    if normalized_domain == "finance" and normalized_sub_domain:
        prefix = f"{prefix}/{normalized_sub_domain}"
    return f"{prefix}/"


def domain_artifact_path(*, layer: str, domain: str, sub_domain: Optional[str] = None) -> str:
    prefix = root_prefix(layer=layer, domain=domain)
    normalized_sub_domain = normalize_sub_domain(sub_domain)
    if normalize_domain(domain) == "finance" and normalized_sub_domain:
        return f"{prefix}/_metadata/subdomains/{normalized_sub_domain}.json"
    return f"{prefix}/_metadata/domain.json"


def bucket_artifact_path(*, layer: str, domain: str, bucket: str, sub_domain: Optional[str] = None) -> str:
    clean_bucket = str(bucket or "").strip().upper()
    if clean_bucket not in bronze_bucketing.ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket {bucket!r}.")
    prefix = root_prefix(layer=layer, domain=domain)
    normalized_sub_domain = normalize_sub_domain(sub_domain)
    if normalize_domain(domain) == "finance" and normalized_sub_domain:
        return f"{prefix}/_metadata/subdomains/{normalized_sub_domain}/buckets/{clean_bucket}.json"
    return f"{prefix}/_metadata/buckets/{clean_bucket}.json"


def _client_for_layer(layer: str) -> Optional[BlobStorageClient]:
    env_name = _LAYER_CONTAINER_ENV.get(normalize_layer(layer))
    if not env_name:
        return None
    container = str(os.environ.get(env_name) or "").strip()
    if not container:
        return None
    return mdc.get_storage_client(container)


def _normalize_columns(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        column = str(value or "").strip()
        if not column or column in seen:
            continue
        seen.add(column)
        normalized.append(column)
    return normalized


def _coerce_timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce", utc=True)
    except Exception:
        return None
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.DatetimeIndex):
        if len(parsed) <= 0:
            return None
        return parsed[0]
    return parsed


def _min_max_iso(values: pd.Series) -> tuple[Optional[str], Optional[str]]:
    if values is None or values.empty:
        return None, None
    try:
        parsed = pd.to_datetime(values, errors="coerce", utc=True).dropna()
    except Exception:
        return None, None
    if parsed.empty:
        return None, None
    return parsed.min().isoformat(), parsed.max().isoformat()


def _base_summary_from_frame(
    df: Optional[pd.DataFrame],
    *,
    date_column: Optional[str],
) -> dict[str, Any]:
    frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    columns = _normalize_columns(frame.columns.tolist())
    symbol_count = 0
    if not frame.empty and "symbol" in frame.columns:
        symbols = (
            frame["symbol"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
        )
        symbol_count = int(symbols[symbols != ""].nunique())

    date_range = None
    clean_date_column = str(date_column or "").strip()
    if clean_date_column and clean_date_column in frame.columns:
        date_min, date_max = _min_max_iso(frame[clean_date_column])
        if date_min or date_max:
            date_range = {
                "min": date_min,
                "max": date_max,
                "column": clean_date_column,
                "source": DATE_RANGE_SOURCE,
            }

    return {
        "symbolCount": symbol_count,
        "columns": columns,
        "columnCount": len(columns),
        "dateRange": date_range,
    }


def _normalize_finance_report_type(value: Any) -> Optional[str]:
    normalized = normalize_sub_domain(value)
    if normalized in FINANCE_SUBDOMAINS:
        return normalized
    return None


def summarize_frame(
    df: Optional[pd.DataFrame],
    *,
    domain: str,
    date_column: Optional[str],
    sub_domain: Optional[str] = None,
) -> dict[str, Any]:
    summary = _base_summary_from_frame(df, date_column=date_column)
    frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    domain_key = normalize_domain(domain)
    normalized_sub_domain = normalize_sub_domain(sub_domain)
    if domain_key != "finance":
        return summary

    finance_summaries: dict[str, Any] = {}
    if normalized_sub_domain:
        finance_summaries[normalized_sub_domain] = _base_summary_from_frame(frame, date_column=date_column)
    elif not frame.empty and "report_type" in frame.columns:
        for report_type, group in frame.groupby("report_type", dropna=True):
            finance_key = _normalize_finance_report_type(report_type)
            if not finance_key:
                continue
            finance_summaries[finance_key] = _base_summary_from_frame(group, date_column=date_column)
    if finance_summaries:
        summary["subdomains"] = finance_summaries
    return summary


def _merge_date_ranges(
    payloads: Iterable[dict[str, Any]],
    *,
    date_column: Optional[str],
) -> Optional[dict[str, Any]]:
    min_ts: Optional[pd.Timestamp] = None
    max_ts: Optional[pd.Timestamp] = None
    column_name = str(date_column or "").strip() or None
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        raw_range = payload.get("dateRange")
        if not isinstance(raw_range, dict):
            continue
        if not column_name:
            raw_column = str(raw_range.get("column") or "").strip()
            if raw_column:
                column_name = raw_column
        min_candidate = _coerce_timestamp(raw_range.get("min"))
        max_candidate = _coerce_timestamp(raw_range.get("max"))
        if min_candidate is not None and (min_ts is None or min_candidate < min_ts):
            min_ts = min_candidate
        if max_candidate is not None and (max_ts is None or max_candidate > max_ts):
            max_ts = max_candidate

    if min_ts is None and max_ts is None:
        return None
    return {
        "min": min_ts.isoformat() if min_ts is not None else None,
        "max": max_ts.isoformat() if max_ts is not None else None,
        "column": column_name,
        "source": DATE_RANGE_SOURCE,
    }


def aggregate_summaries(
    payloads: Iterable[dict[str, Any]],
    *,
    symbol_count_override: Optional[int] = None,
    date_column: Optional[str] = None,
) -> dict[str, Any]:
    payload_list = [payload for payload in payloads if isinstance(payload, dict)]
    seen_columns: set[str] = set()
    columns: list[str] = []
    symbol_count = 0
    for payload in payload_list:
        for column in _normalize_columns(payload.get("columns") or []):
            if column in seen_columns:
                continue
            seen_columns.add(column)
            columns.append(column)
        raw_count = payload.get("symbolCount")
        if isinstance(raw_count, int):
            symbol_count += raw_count

    if symbol_count_override is not None:
        symbol_count = int(symbol_count_override)

    return {
        "symbolCount": symbol_count,
        "columns": columns,
        "columnCount": len(columns),
        "dateRange": _merge_date_ranges(payload_list, date_column=date_column),
    }


def _aggregate_finance_subdomains_from_payloads(payloads: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        subdomains = payload.get("subdomains")
        if not isinstance(subdomains, dict):
            continue
        for key, summary in subdomains.items():
            normalized_key = normalize_sub_domain(key)
            if normalized_key not in FINANCE_SUBDOMAINS or not isinstance(summary, dict):
                continue
            grouped.setdefault(normalized_key, []).append(summary)

    merged: dict[str, dict[str, Any]] = {}
    for key, summaries in grouped.items():
        merged[key] = aggregate_summaries(summaries, date_column="date")
    return merged


def _finance_subdomain_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbolCount": payload.get("symbolCount"),
        "columns": _normalize_columns(payload.get("columns") or []),
        "columnCount": extract_column_count(payload) or 0,
        "totalBytes": payload.get("totalBytes"),
        "dateRange": payload.get("dateRange"),
        "artifactPath": payload.get("artifactPath"),
        "updatedAt": payload.get("updatedAt"),
    }


def extract_column_count(payload: Optional[dict[str, Any]]) -> Optional[int]:
    if not isinstance(payload, dict):
        return None

    raw_count = payload.get("columnCount")
    if isinstance(raw_count, int):
        return raw_count

    columns = payload.get("columns")
    if isinstance(columns, list):
        return len(_normalize_columns(columns))

    return None


def _measure_domain_storage_bytes(
    client: Optional[BlobStorageClient],
    *,
    layer: str,
    domain: str,
    sub_domain: Optional[str] = None,
) -> Optional[int]:
    if client is None:
        return None

    container_client = getattr(client, "container_client", None)
    if container_client is None:
        return None

    total_bytes = 0
    prefix = _storage_listing_prefix(layer=layer, domain=domain, sub_domain=sub_domain)
    try:
        for blob in container_client.list_blobs(name_starts_with=prefix):
            size = getattr(blob, "size", None)
            if isinstance(size, int):
                total_bytes += size
    except Exception as exc:
        logger.warning(
            "Failed to measure domain storage bytes for artifact payload: layer=%s domain=%s prefix=%s err=%s",
            layer,
            domain,
            prefix,
            exc,
        )
        return None

    return total_bytes


def write_bucket_artifact(
    *,
    layer: str,
    domain: str,
    bucket: str,
    df: Optional[pd.DataFrame],
    date_column: Optional[str],
    client: Optional[BlobStorageClient] = None,
    job_name: Optional[str] = None,
    job_run_id: Optional[str] = None,
    sub_domain: Optional[str] = None,
    run_id: Optional[str] = None,
    manifest_path: Optional[str] = None,
    active_data_prefix: Optional[str] = None,
    data_path: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    storage_client = client or _client_for_layer(layer)
    if storage_client is None:
        return None

    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    normalized_sub_domain = normalize_sub_domain(sub_domain)
    artifact_path = bucket_artifact_path(
        layer=normalized_layer,
        domain=normalized_domain,
        bucket=bucket,
        sub_domain=normalized_sub_domain or None,
    )
    now = _utc_now_iso()
    payload = {
        "version": ARTIFACT_VERSION,
        "scope": "bucket",
        "layer": normalized_layer,
        "domain": normalized_domain,
        "subDomain": normalized_sub_domain or None,
        "bucket": str(bucket).strip().upper(),
        "rootPath": root_prefix(layer=normalized_layer, domain=normalized_domain),
        "artifactPath": artifact_path,
        "updatedAt": now,
        "computedAt": now,
        "producerJobName": str(job_name or "").strip() or None,
        "jobRunId": str(job_run_id or "").strip() or None,
        "runId": str(run_id or job_run_id or "").strip() or None,
        "manifestPath": str(manifest_path or "").strip() or None,
        "activeDataPrefix": str(active_data_prefix or "").strip().strip("/") or None,
        "dataPath": str(data_path or "").strip() or None,
        **summarize_frame(
            df,
            domain=normalized_domain,
            date_column=date_column,
            sub_domain=normalized_sub_domain or None,
        ),
    }
    mdc.save_json_content(payload, artifact_path, client=storage_client)
    return payload


def load_bucket_artifact(
    *,
    layer: str,
    domain: str,
    bucket: str,
    client: Optional[BlobStorageClient] = None,
    sub_domain: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    storage_client = client or _client_for_layer(layer)
    if storage_client is None:
        return None
    payload = mdc.get_json_content(
        bucket_artifact_path(layer=layer, domain=domain, bucket=bucket, sub_domain=sub_domain),
        client=storage_client,
    )
    if not isinstance(payload, dict):
        return None
    return payload


def load_domain_artifact(
    *,
    layer: str,
    domain: str,
    client: Optional[BlobStorageClient] = None,
    sub_domain: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    storage_client = client or _client_for_layer(layer)
    if storage_client is None:
        return None
    payload = mdc.get_json_content(
        domain_artifact_path(layer=layer, domain=domain, sub_domain=sub_domain),
        client=storage_client,
    )
    if not isinstance(payload, dict):
        return None
    return payload


def publish_domain_artifact_payload(
    *,
    payload: dict[str, Any],
    client: Optional[BlobStorageClient] = None,
) -> Optional[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("Domain artifact payload must be a mapping.")

    normalized_layer = normalize_layer(str(payload.get("layer") or ""))
    normalized_domain = normalize_domain(str(payload.get("domain") or ""))
    if not normalized_layer:
        raise ValueError("Domain artifact payload is missing layer.")
    if not normalized_domain:
        raise ValueError("Domain artifact payload is missing domain.")

    normalized_sub_domain = normalize_sub_domain(payload.get("subDomain"))
    storage_client = client or _client_for_layer(normalized_layer)
    if storage_client is None:
        return None

    artifact_path = str(payload.get("artifactPath") or "").strip() or domain_artifact_path(
        layer=normalized_layer,
        domain=normalized_domain,
        sub_domain=normalized_sub_domain or None,
    )
    published = dict(payload)
    published["layer"] = normalized_layer
    published["domain"] = normalized_domain
    published["subDomain"] = normalized_sub_domain or None
    published["artifactPath"] = artifact_path
    if not str(published.get("rootPath") or "").strip():
        published["rootPath"] = root_prefix(layer=normalized_layer, domain=normalized_domain)

    mdc.save_json_content(published, artifact_path, client=storage_client)
    if not normalized_sub_domain:
        domain_metadata_snapshots.update_domain_metadata_snapshots_from_artifact(
            layer=normalized_layer,
            domain=normalized_domain,
            artifact=published,
        )
    return published


def write_domain_artifact(
    *,
    layer: str,
    domain: str,
    date_column: Optional[str],
    client: Optional[BlobStorageClient] = None,
    symbol_count_override: Optional[int] = None,
    symbol_index_path: Optional[str] = None,
    job_name: Optional[str] = None,
    job_run_id: Optional[str] = None,
    sub_domain: Optional[str] = None,
    finance_subdomains: Optional[dict[str, dict[str, Any]]] = None,
    run_id: Optional[str] = None,
    manifest_path: Optional[str] = None,
    active_data_prefix: Optional[str] = None,
    total_bytes_override: Optional[int] = None,
    file_count_override: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    storage_client = client or _client_for_layer(layer)
    if storage_client is None:
        return None

    normalized_layer = normalize_layer(layer)
    normalized_domain = normalize_domain(domain)
    normalized_sub_domain = normalize_sub_domain(sub_domain)
    bucket_payloads: list[dict[str, Any]] = []
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        payload = load_bucket_artifact(
            layer=normalized_layer,
            domain=normalized_domain,
            bucket=bucket,
            client=storage_client,
            sub_domain=normalized_sub_domain or None,
        )
        if payload is not None:
            bucket_payloads.append(payload)

    if finance_subdomains:
        aggregated_finance_subdomains = {
            normalize_sub_domain(key): value
            for key, value in finance_subdomains.items()
            if isinstance(value, dict)
        }
        summary = aggregate_summaries(
            aggregated_finance_subdomains.values(),
            symbol_count_override=symbol_count_override,
            date_column=date_column,
        )
    else:
        aggregated_finance_subdomains = _aggregate_finance_subdomains_from_payloads(bucket_payloads)
        summary = aggregate_summaries(
            bucket_payloads,
            symbol_count_override=symbol_count_override,
            date_column=date_column,
        )

    artifact_path = domain_artifact_path(
        layer=normalized_layer,
        domain=normalized_domain,
        sub_domain=normalized_sub_domain or None,
    )
    total_bytes = total_bytes_override
    if total_bytes is None:
        total_bytes = _measure_domain_storage_bytes(
            storage_client,
            layer=normalized_layer,
            domain=normalized_domain,
            sub_domain=normalized_sub_domain or None,
        )
    now = _utc_now_iso()
    payload: dict[str, Any] = {
        "version": ARTIFACT_VERSION,
        "scope": "domain",
        "layer": normalized_layer,
        "domain": normalized_domain,
        "subDomain": normalized_sub_domain or None,
        "rootPath": root_prefix(layer=normalized_layer, domain=normalized_domain),
        "artifactPath": artifact_path,
        "updatedAt": now,
        "computedAt": now,
        "producerJobName": str(job_name or "").strip() or None,
        "jobRunId": str(job_run_id or "").strip() or None,
        "runId": str(run_id or job_run_id or "").strip() or None,
        "manifestPath": str(manifest_path or "").strip() or None,
        "activeDataPrefix": str(active_data_prefix or "").strip().strip("/") or None,
        "symbolIndexPath": str(symbol_index_path or "").strip() or None,
        "totalBytes": total_bytes,
        "fileCount": int(file_count_override) if isinstance(file_count_override, int) else None,
        **summary,
    }
    if normalized_domain == "finance" and not normalized_sub_domain:
        payload["financeSubfolderSymbolCounts"] = {
            key: int(value.get("symbolCount") or 0)
            for key, value in aggregated_finance_subdomains.items()
            if key in FINANCE_SUBDOMAINS
        } or None
        payload["subdomains"] = {
            key: _finance_subdomain_snapshot(value)
            for key, value in aggregated_finance_subdomains.items()
            if key in FINANCE_SUBDOMAINS
        } or None

    return publish_domain_artifact_payload(payload=payload, client=storage_client)
