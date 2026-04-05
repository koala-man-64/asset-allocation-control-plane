from __future__ import annotations

import csv
import io
import logging
import os
import time
from datetime import datetime, timezone
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from core import bronze_bucketing
from core import domain_artifacts
from core import layer_bucketing
from core.blob_storage import BlobStorageClient
from core import delta_core
from core.domain_metadata_snapshots import build_domain_metadata_snapshot_metadata_from_artifact
from core.finance_contracts import SILVER_FINANCE_SUBDOMAINS
from deltalake import DeltaTable

logger = logging.getLogger("asset_allocation.monitoring.domain_metadata")
_DOMAIN_METADATA_CACHE: Dict[Tuple[str, str], Tuple[float, Dict[str, Any]]] = {}


LayerKey = str
DomainKey = str
FinanceSubfolderKey = str

_FINANCE_SUBFOLDER_KEYS: Tuple[FinanceSubfolderKey, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
    "valuation",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_not_found_listing_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int) and status_code == 404:
        return True

    response = getattr(exc, "response", None)
    if response is not None:
        response_status = getattr(response, "status_code", None)
        if response_status is None:
            response_status = getattr(response, "status", None)
        if isinstance(response_status, int) and response_status == 404:
            return True

    error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
    if error_code in {"resourcenotfound", "containernotfound", "blobnotfound", "pathnotfound", "filesystemnotfound"}:
        return True

    class_name = exc.__class__.__name__.lower()
    if "notfound" in class_name:
        return True

    message = str(exc).lower()
    not_found_tokens = (
        "resource not found",
        "container not found",
        "blob not found",
        "path not found",
        "status code: 404",
        "http status code 404",
    )
    return any(token in message for token in not_found_tokens)


def _domain_metadata_cache_ttl_seconds() -> int:
    raw_ttl = os.environ.get("DOMAIN_METADATA_CACHE_TTL_SECONDS", "30").strip()
    try:
        ttl = int(raw_ttl)
    except ValueError:
        logger.warning(
            "Invalid DOMAIN_METADATA_CACHE_TTL_SECONDS=%s; defaulting to 30 seconds.",
            raw_ttl,
        )
        return 30
    if ttl < 0:
        return 0
    return ttl


def _read_cached_domain_metadata(layer_key: str, domain_key: str) -> Optional[Dict[str, Any]]:
    ttl = _domain_metadata_cache_ttl_seconds()
    if ttl <= 0:
        return None

    key = (layer_key, domain_key)
    cached = _DOMAIN_METADATA_CACHE.get(key)
    if not cached:
        return None

    cached_at, payload = cached
    if time.time() - cached_at > ttl:
        _DOMAIN_METADATA_CACHE.pop(key, None)
        return None

    return deepcopy(payload)


def _cache_domain_metadata(layer_key: str, domain_key: str, payload: Dict[str, Any]) -> None:
    ttl = _domain_metadata_cache_ttl_seconds()
    if ttl <= 0:
        return
    _DOMAIN_METADATA_CACHE[(layer_key, domain_key)] = (time.time(), deepcopy(payload))


def _normalize_key(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _require_container(env_name: str) -> str:
    raw = os.environ.get(env_name)
    container = raw.strip() if raw else ""
    if not container:
        raise ValueError(f"Missing required environment variable: {env_name}")
    return container


def _layer_container_env(layer: LayerKey) -> str:
    layer_key = _normalize_key(layer)
    if layer_key == "bronze":
        return "AZURE_CONTAINER_BRONZE"
    if layer_key == "silver":
        return "AZURE_CONTAINER_SILVER"
    if layer_key == "gold":
        return "AZURE_CONTAINER_GOLD"
    if layer_key == "platinum":
        return "AZURE_CONTAINER_PLATINUM"
    raise ValueError(f"Unsupported layer: {layer}")


def _delta_table_path(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    # Domain metadata now operates on regular bucket prefixes.
    return None


def _blob_prefix(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)

    if layer_key == "bronze":
        if domain_key in {"market", "finance", "earnings"}:
            return f"{domain_key}-data/"
        if domain_key == "price-target":
            return "price-target-data/"
        if domain_key == "platinum":
            return "platinum/"
    if layer_key == "silver":
        if domain_key in {"market", "finance", "earnings"}:
            return f"{domain_key}-data/"
        if domain_key == "price-target":
            return "price-target-data/"
    if layer_key == "gold":
        if domain_key in {"market", "finance", "earnings"}:
            return f"{domain_key}/"
        if domain_key == "price-target":
            return "targets/"
        if domain_key == "regime":
            return "regime/"
    if layer_key == "platinum":
        return "platinum/"
    return None


def _list_path(layer: LayerKey, domain: DomainKey, *, list_type: str) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)
    prefix = _blob_prefix(layer_key, domain_key)
    if not prefix:
        return None
    base = str(prefix).strip().strip("/")
    if not base:
        return None
    if list_type not in {"whitelist", "blacklist"}:
        return None
    return f"{base}/{list_type}.csv"


def _whitelist_path(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    return _list_path(layer, domain, list_type="whitelist")


def _blacklist_path(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    return _list_path(layer, domain, list_type="blacklist")


def _ticker_listing_prefix(layer: LayerKey, domain: DomainKey) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)

    if layer_key == "silver":
        if domain_key == "market":
            return "market-data/"
        if domain_key == "finance":
            return "finance-data/"
        if domain_key == "earnings":
            return "earnings-data/"
        if domain_key == "price-target":
            return "price-target-data/"

    if layer_key == "gold":
        if domain_key == "market":
            return "market/"
        if domain_key == "finance":
            return "finance/"
        if domain_key == "earnings":
            return "earnings/"
        if domain_key == "price-target":
            return "targets/"
        if domain_key == "regime":
            return None

    return None


def _is_alpha26_layout(layer: LayerKey) -> bool:
    layer_key = _normalize_key(layer)
    if layer_key == "bronze":
        bronze_bucketing.bronze_layout_mode()
        return True
    if layer_key == "silver":
        layer_bucketing.silver_layout_mode()
        return True
    if layer_key == "gold":
        layer_bucketing.gold_layout_mode()
        return True
    return True


def _load_alpha26_index_symbols(*, layer_key: str, domain_key: str) -> set[str]:
    if domain_key not in {"market", "earnings", "price-target", "finance"}:
        return set()
    if layer_key == "bronze":
        return bronze_bucketing.load_symbol_set(domain_key)
    if layer_key in {"silver", "gold"}:
        return layer_bucketing.load_layer_symbol_set(layer=layer_key, domain=domain_key)
    return set()


def _load_alpha26_bucket_artifact_symbol_count(
    *,
    layer_key: str,
    domain_key: str,
    client: BlobStorageClient,
) -> Optional[int]:
    if layer_key != "gold":
        return None
    if domain_key not in {"market", "finance", "earnings", "price-target"}:
        return None

    symbol_count = 0
    found_artifact = False
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        payload = domain_artifacts.load_bucket_artifact(
            layer=layer_key,
            domain=domain_key,
            bucket=bucket,
            client=client,
        )
        if not isinstance(payload, dict):
            continue
        found_artifact = True
        raw_count = payload.get("symbolCount")
        if isinstance(raw_count, int):
            symbol_count += raw_count
            continue
        try:
            symbol_count += int(raw_count or 0)
        except Exception:
            continue
    return symbol_count if found_artifact else None


def _normalize_finance_subfolder_counts(raw: Any) -> Optional[Dict[FinanceSubfolderKey, int]]:
    if not isinstance(raw, dict):
        return None
    out: Dict[FinanceSubfolderKey, int] = {}
    for key, value in raw.items():
        normalized_key = domain_artifacts.normalize_sub_domain(str(key or ""))
        if normalized_key not in _FINANCE_SUBFOLDER_KEYS:
            continue
        try:
            out[normalized_key] = int(value)
        except Exception:
            continue
    return out or None


def _artifact_domain_metadata_payload(
    *,
    layer_key: str,
    domain_key: str,
    container: str,
) -> Optional[Dict[str, Any]]:
    artifact = domain_artifacts.load_domain_artifact(layer=layer_key, domain=domain_key)
    if not isinstance(artifact, dict):
        return None
    return build_domain_metadata_snapshot_metadata_from_artifact(
        layer=layer_key,
        domain=domain_key,
        artifact=artifact,
        container=container,
    )


def _extract_ticker_from_blob_name(layer: LayerKey, domain: DomainKey, blob_name: str) -> Optional[str]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)
    parts = str(blob_name).strip("/").split("/")

    if layer_key == "silver" and domain_key == "market":
        # market-data/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "market-data" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "market":
        # market/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "market" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "silver" and domain_key == "earnings":
        # earnings-data/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "earnings-data" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "earnings":
        # earnings/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "earnings" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "silver" and domain_key == "price-target":
        # price-target-data/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "price-target-data" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "price-target":
        # targets/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "targets" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "gold" and domain_key == "finance":
        # finance/<ticker>/_delta_log/<file>
        if len(parts) >= 4 and parts[0] == "finance" and parts[2] == "_delta_log":
            return parts[1].strip() or None
        return None

    if layer_key == "silver" and domain_key == "finance":
        # finance-data/<folder>/<ticker>_<suffix>/_delta_log/<file>
        if len(parts) >= 5 and parts[0] == "finance-data" and parts[3] == "_delta_log":
            table_name = parts[2].strip()
            if "_" not in table_name:
                return None
            ticker = table_name.split("_", 1)[0].strip()
            return ticker or None
        return None

    return None


def _normalize_finance_subfolder(value: str) -> Optional[FinanceSubfolderKey]:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    compact = raw.replace("-", " ").replace("_", " ")
    compact = " ".join(compact.split())
    aliases: Dict[str, FinanceSubfolderKey] = {
        "balance sheet": "balance_sheet",
        "income statement": "income_statement",
        "cash flow": "cash_flow",
        "valuation": "valuation",
    }
    return aliases.get(compact)


def _extract_finance_subfolder_and_ticker(blob_name: str) -> Tuple[Optional[FinanceSubfolderKey], Optional[str]]:
    parts = str(blob_name).strip("/").split("/")
    if len(parts) < 3 or parts[0] != "finance-data":
        return None, None

    subfolder = _normalize_finance_subfolder(parts[1])
    if not subfolder:
        return None, None

    # Silver Delta layout: finance-data/<folder>/<ticker>_<suffix>/_delta_log/<file>
    if len(parts) >= 5 and parts[3] == "_delta_log":
        table_name = parts[2].strip()
        if "_" not in table_name:
            return subfolder, None
        ticker = table_name.split("_", 1)[0].strip()
        return subfolder, ticker or None

    # Bronze raw layout: finance-data/<folder>/<ticker>_<suffix>.json|csv
    file_stem = parts[2].strip()
    if "." in file_stem:
        file_stem = file_stem.rsplit(".", 1)[0]
    if "_" not in file_stem:
        return subfolder, None
    ticker = file_stem.split("_", 1)[0].strip()
    return subfolder, ticker or None


def _parse_symbol_list(blob_bytes: Optional[bytes]) -> Optional[set[str]]:
    if not blob_bytes:
        return None

    text = blob_bytes.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    symbols: set[str] = set()
    for row in reader:
        if not row:
            continue
        raw = str(row[0]).strip()
        if not raw:
            continue
        lowered = raw.lower()
        if lowered in {"symbol", "ticker", "tickers"}:
            continue
        symbols.add(raw.replace(".", "-"))
    return symbols


def _parse_list_size(blob_bytes: Optional[bytes]) -> Optional[int]:
    symbols = _parse_symbol_list(blob_bytes)
    if symbols is None:
        return None
    return len(symbols)


def _count_symbols_from_listing(
    client: BlobStorageClient,
    *,
    layer: LayerKey,
    domain: DomainKey,
    prefix: str,
    max_scanned_blobs: int,
) -> Tuple[Optional[int], bool]:
    tickers: set[str] = set()
    scanned = 0
    truncated = False

    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            scanned += 1
            if scanned > max_scanned_blobs:
                truncated = True
                break
            ticker = _extract_ticker_from_blob_name(layer, domain, getattr(blob, "name", ""))
            if ticker:
                tickers.add(ticker)
    except Exception as exc:
        if _is_not_found_listing_error(exc):
            logger.info(
                "Blob prefix missing for symbol count; treating as zero. container=%s prefix=%s err=%s",
                client.container_name,
                prefix,
                exc,
            )
            return 0, False
        logger.warning(
            "Failed to list blobs for symbol count: container=%s prefix=%s err=%s", client.container_name, prefix, exc
        )
        return None, False

    return len(tickers), truncated


def _count_finance_symbols_from_listing(
    client: BlobStorageClient,
    *,
    prefix: str,
    max_scanned_blobs: int,
) -> Tuple[Optional[int], Optional[Dict[FinanceSubfolderKey, int]], bool]:
    tickers: set[str] = set()
    by_subfolder: Dict[FinanceSubfolderKey, set[str]] = {key: set() for key in _FINANCE_SUBFOLDER_KEYS}
    scanned = 0
    truncated = False

    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            scanned += 1
            if scanned > max_scanned_blobs:
                truncated = True
                break
            subfolder, ticker = _extract_finance_subfolder_and_ticker(getattr(blob, "name", ""))
            if ticker:
                tickers.add(ticker)
            if subfolder and ticker:
                by_subfolder[subfolder].add(ticker)
    except Exception as exc:
        if _is_not_found_listing_error(exc):
            logger.info(
                "Finance blob prefix missing for symbol count; treating as zero. container=%s prefix=%s err=%s",
                client.container_name,
                prefix,
                exc,
            )
            return 0, {key: 0 for key in _FINANCE_SUBFOLDER_KEYS}, False
        logger.warning(
            "Failed to list blobs for finance subfolder symbol count: container=%s prefix=%s err=%s",
            client.container_name,
            prefix,
            exc,
        )
        return None, None, False

    subfolder_counts = {key: len(by_subfolder[key]) for key in _FINANCE_SUBFOLDER_KEYS}
    return len(tickers), subfolder_counts, truncated


def _summarize_blob_prefix(
    client: BlobStorageClient,
    *,
    prefix: str,
    max_scanned_blobs: int,
) -> Tuple[Optional[int], Optional[int], Optional[str], bool]:
    files = 0
    total_bytes = 0
    scanned = 0
    truncated = False
    latest_modified: Optional[datetime] = None

    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            scanned += 1
            if scanned > max_scanned_blobs:
                truncated = True
                break
            files += 1
            size = getattr(blob, "size", None)
            if isinstance(size, int):
                total_bytes += size
            modified_dt = _coerce_datetime(getattr(blob, "last_modified", None))
            if modified_dt is not None and (latest_modified is None or modified_dt > latest_modified):
                latest_modified = modified_dt
    except Exception as exc:
        if _is_not_found_listing_error(exc):
            logger.info(
                "Blob prefix missing for summary; treating as empty. container=%s prefix=%s err=%s",
                client.container_name,
                prefix,
                exc,
            )
            return 0, 0, None, False
        logger.warning(
            "Failed to list blobs for prefix summary: container=%s prefix=%s err=%s", client.container_name, prefix, exc
        )
        return None, None, None, False

    return files, total_bytes, _to_iso_datetime(latest_modified), truncated


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, bool):
        return None
    elif isinstance(value, (int, float)):
        try:
            raw = float(value)
        except (TypeError, ValueError):
            return None
        if raw <= 0:
            return None
        if raw > 1_000_000_000_000:
            dt = datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        elif raw > 1_000_000_000:
            dt = datetime.fromtimestamp(raw, tz=timezone.utc)
        else:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None

        parsed: Optional[datetime] = None
        for candidate in (text.replace("Z", "+00:00"), text):
            try:
                parsed = datetime.fromisoformat(candidate)
                break
            except ValueError:
                parsed = None

        if parsed is None:
            formats = [
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y/%m/%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S.%f",
            ]
            for fmt in formats:
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    parsed = None

        if parsed is None:
            return None
        dt = parsed

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _pick_date_like_column(candidates: List[str]) -> Optional[str]:
    if not candidates:
        return None

    date_like = [c for c in candidates if "date" in c.lower()]
    if date_like:
        if "Date" in date_like:
            return "Date"
        if "date" in date_like:
            return "date"
        date_like.sort(key=str.lower)
        return date_like[0]

    candidates.sort(key=str.lower)
    return candidates[0]


def _collect_partition_date_bounds(
    rows: List[Dict[str, Any]],
) -> tuple[
    Optional[str],
    Optional[datetime],
    Optional[datetime],
    bool,
]:
    """
    Extract date bounds from partition metadata in add-action rows.

    Supports both flattened and non-flattened delta action payloads.
    """
    partition_seen = False
    candidates: Dict[str, list[datetime]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        partition_payloads: Dict[str, Any] = {}

        if isinstance(row.get("partition"), dict):
            partition_seen = True
            partition_payloads.update(row.get("partition") or {})

        if isinstance(row.get("partition_values"), dict):
            partition_seen = True
            partition_payloads.update(row.get("partition_values") or {})

        for key, value in row.items():
            if not isinstance(key, str):
                continue
            if key.startswith("partition."):
                partition_seen = True
                partition_payloads[key.split(".", 1)[1]] = value

        for partition_key, raw_value in partition_payloads.items():
            parsed = _coerce_datetime(raw_value)
            if parsed is None:
                continue
            bucket = candidates.setdefault(str(partition_key), [])
            bucket.append(parsed)

    if not candidates:
        return None, None, None, partition_seen

    column = _pick_date_like_column(list(candidates.keys()))
    if column is None:
        return None, None, None, partition_seen

    values = candidates.get(column, [])
    if not values:
        return column, None, None, partition_seen

    return column, min(values), max(values), partition_seen


def _pick_date_column(rows: List[Dict[str, Any]]) -> Optional[str]:
    if not rows:
        return None

    candidates: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if not key.startswith("min."):
                continue
            column = key[len("min.") :]
            max_key = f"max.{column}"
            if _coerce_datetime(value) is None and _coerce_datetime(row.get(max_key)) is None:
                continue
            candidates.append(column)

    if not candidates:
        return None

    # Prefer columns that look like date/time.
    date_like = [c for c in candidates if "date" in c.lower()]
    if date_like:
        date_like.sort(key=lambda c: (0 if c.lower() in {"date", "asofdate"} else 1, c.lower()))
        return date_like[0]
    candidates.sort(key=str.lower)
    return candidates[0]


def collect_delta_table_metadata(
    container: str, table_path: str, warnings: Optional[List[str]] = None
) -> Dict[str, Any]:
    local_warnings = warnings if warnings is not None else []

    uri = delta_core.get_delta_table_uri(container, table_path)
    opts = delta_core.get_delta_storage_options(container)
    try:
        dt = DeltaTable(uri, storage_options=opts)
    except Exception as exc:
        message = str(exc).lower()
        is_no_files_error = "no files in log segment" in message
        is_table_not_found_error = exc.__class__.__name__ == "TableNotFoundError"
        if is_no_files_error or is_table_not_found_error:
            local_warnings.append(f"Delta table not readable at {table_path}; no commit files found in _delta_log yet.")
            return {
                "deltaVersion": None,
                "fileCount": 0,
                "totalBytes": 0,
                "totalRows": 0,
                "dateRange": None,
            }
        logger.exception(
            "Failed to open Delta table for metadata collection: container=%s table=%s",
            container,
            table_path,
        )
        raise

    version = int(dt.version())
    add_actions = dt.get_add_actions(flatten=True).to_struct_array().to_pylist()
    # Keep using flattened actions for min/max stats and partition.* fields.

    total_rows = 0
    total_bytes = 0
    for action in add_actions:
        num_records = action.get("num_records")
        if isinstance(num_records, int):
            total_rows += num_records
        size_bytes = action.get("size_bytes")
        if isinstance(size_bytes, int):
            total_bytes += size_bytes

    partition_date_range: Optional[Dict[str, Any]] = None
    partition_column, partition_min_dt, partition_max_dt, partition_seen = _collect_partition_date_bounds(add_actions)

    if partition_column is None and not partition_seen:
        raw_add_actions = dt.get_add_actions(flatten=False).to_struct_array().to_pylist()
        (
            partition_column,
            partition_min_dt,
            partition_max_dt,
            partition_seen,
        ) = _collect_partition_date_bounds(raw_add_actions)

    if partition_column:
        partition_date_range = {
            "min": _to_iso_datetime(partition_min_dt),
            "max": _to_iso_datetime(partition_max_dt),
            "column": partition_column,
            "source": "partition",
        }

    date_column = _pick_date_column(add_actions)
    min_dt: Optional[datetime] = None
    max_dt: Optional[datetime] = None
    if date_column:
        for action in add_actions:
            start = _coerce_datetime(action.get(f"min.{date_column}"))
            end = _coerce_datetime(action.get(f"max.{date_column}"))
            if start is not None:
                min_dt = start if min_dt is None or start < min_dt else min_dt
            if end is not None:
                max_dt = end if max_dt is None or end > max_dt else max_dt

    date_range = (
        {
            "min": _to_iso_datetime(min_dt),
            "max": _to_iso_datetime(max_dt),
            "column": date_column,
            "source": "stats",
        }
        if date_column and (min_dt is not None or max_dt is not None)
        else None
    )

    if partition_date_range is not None:
        date_range = partition_date_range

    if date_range is None:
        if partition_seen:
            local_warnings.append(
                f"Date range for table={table_path} was not parseable from partition and stats metadata."
            )
        elif date_column is None:
            local_warnings.append(f"Date range stats for table={table_path} were not found in table metadata.")
        else:
            local_warnings.append(f"Date range stats for table={table_path} could not be parsed from min/max metadata.")

    return {
        "deltaVersion": version,
        "fileCount": len(add_actions),
        "totalBytes": total_bytes,
        "totalRows": total_rows,
        "dateRange": date_range,
    }


def collect_domain_metadata(*, layer: str, domain: str, force_refresh: bool = False) -> Dict[str, Any]:
    layer_key = _normalize_key(layer)
    domain_key = _normalize_key(domain)
    if not force_refresh:
        cached = _read_cached_domain_metadata(layer_key, domain_key)
        if cached is not None:
            return cached

    container = _require_container(_layer_container_env(layer_key))
    computed_at = _utc_now_iso()
    max_scanned_blobs = int(os.environ.get("DOMAIN_METADATA_MAX_SCANNED_BLOBS", "200000"))

    artifact_payload = _artifact_domain_metadata_payload(
        layer_key=layer_key,
        domain_key=domain_key,
        container=container,
    )
    if artifact_payload is not None:
        warnings = artifact_payload.setdefault("warnings", [])
        prefix = _blob_prefix(layer_key, domain_key)
        if prefix:
            client = BlobStorageClient(container_name=container, ensure_container_exists=False)
            blacklist_path = _blacklist_path(layer_key, domain_key)
            if blacklist_path:
                try:
                    blacklist_blob_bytes = client.download_data(blacklist_path)
                    artifact_payload["blacklistedSymbolCount"] = _parse_list_size(blacklist_blob_bytes)
                except Exception as exc:
                    warnings.append(f"Unable to read blacklist.csv: {exc}")
        artifact_payload["computedAt"] = artifact_payload.get("computedAt") or computed_at
        _cache_domain_metadata(layer_key, domain_key, artifact_payload)
        return artifact_payload

    delta_path = _delta_table_path(layer_key, domain_key)
    if delta_path:
        client = BlobStorageClient(container_name=container, ensure_container_exists=False)
        prefix = _ticker_listing_prefix(layer_key, domain_key)
        symbol_count = None
        symbol_truncated = False
        warnings: List[str] = []

        if prefix:
            symbol_count, symbol_truncated = _count_symbols_from_listing(
                client,
                layer=layer_key,
                domain=domain_key,
                prefix=prefix,
                max_scanned_blobs=max_scanned_blobs,
            )

        if symbol_truncated:
            warnings.append(f"Symbol discovery truncated after {max_scanned_blobs} blobs.")

        metrics = collect_delta_table_metadata(container, delta_path, warnings=warnings)
        payload = {
            "layer": layer_key,
            "domain": domain_key,
            "container": container,
            "type": "delta",
            "tablePath": delta_path,
            "computedAt": computed_at,
            "folderLastModified": None,
            "symbolCount": symbol_count,
            "columns": [],
            "columnCount": None,
            "blacklistedSymbolCount": None,
            "metadataPath": None,
            "metadataSource": "scan",
            "warnings": warnings,
            **metrics,
        }
        _cache_domain_metadata(layer_key, domain_key, payload)
        return payload

    prefix = _blob_prefix(layer_key, domain_key)
    if prefix:
        client = BlobStorageClient(container_name=container, ensure_container_exists=False)
        files, total_bytes, folder_last_modified, truncated = _summarize_blob_prefix(
            client, prefix=prefix, max_scanned_blobs=max_scanned_blobs
        )
        warnings: List[str] = []
        if truncated:
            warnings.append(f"Blob listing truncated after {max_scanned_blobs} blobs.")

        symbol_count = None
        symbol_truncated = False
        finance_subfolder_symbol_counts: Optional[Dict[FinanceSubfolderKey, int]] = None
        listing_prefix = _ticker_listing_prefix(layer_key, domain_key)
        alpha26_layout = _is_alpha26_layout(layer_key)
        if domain_key == "finance" and prefix == "finance-data/":
            symbol_count, finance_subfolder_symbol_counts, symbol_truncated = _count_finance_symbols_from_listing(
                client,
                prefix=prefix,
                max_scanned_blobs=max_scanned_blobs,
            )
        elif listing_prefix:
            symbol_count, symbol_truncated = _count_symbols_from_listing(
                client,
                layer=layer_key,
                domain=domain_key,
                prefix=listing_prefix,
                max_scanned_blobs=max_scanned_blobs,
            )
        if symbol_truncated:
            warnings.append(f"Symbol discovery truncated after {max_scanned_blobs} blobs.")

        whitelist_blob_bytes: Optional[bytes] = None
        whitelist_path = _whitelist_path(layer_key, domain_key) if layer_key == "bronze" else None
        if whitelist_path:
            try:
                whitelist_blob_bytes = client.download_data(whitelist_path)
                whitelist_symbol_count = _parse_list_size(whitelist_blob_bytes)
                if not (layer_key == "bronze" and domain_key in {"market", "price-target", "finance", "earnings"}):
                    symbol_count = whitelist_symbol_count
            except Exception as exc:
                warnings.append(f"Unable to read whitelist.csv: {exc}")

        blacklisted_symbol_count = None
        blacklist_blob_bytes: Optional[bytes] = None
        blacklist_path = _blacklist_path(layer_key, domain_key)
        if blacklist_path:
            try:
                blacklist_blob_bytes = client.download_data(blacklist_path)
                blacklisted_symbol_count = _parse_list_size(blacklist_blob_bytes)
            except Exception as exc:
                warnings.append(f"Unable to read blacklist.csv: {exc}")

        if alpha26_layout:
            index_domain_key = domain_key if domain_key != "price_target" else "price-target"
            if layer_key == "bronze":
                index_symbols = bronze_bucketing.load_symbol_set(index_domain_key)
            else:
                index_symbols = _load_alpha26_index_symbols(layer_key=layer_key, domain_key=index_domain_key)
            if isinstance(files, int):
                prefix_is_empty = files == 0
                raw_symbol_count = symbol_count
                # If the target prefix has no blobs, report zero symbols even when the alpha26
                # index still contains stale entries.
                if prefix_is_empty:
                    symbol_count = 0
                elif index_symbols:
                    symbol_count = len(index_symbols)
                else:
                    bucket_artifact_symbol_count = _load_alpha26_bucket_artifact_symbol_count(
                        layer_key=layer_key,
                        domain_key=index_domain_key,
                        client=client,
                    )
                    if bucket_artifact_symbol_count is not None:
                        symbol_count = bucket_artifact_symbol_count
                        warnings.append(
                            f"{layer_key.title()} alpha26 index empty or unavailable for domain={index_domain_key}; "
                            "symbol count derived from bucket artifacts."
                        )
                    else:
                        symbol_count = raw_symbol_count
                        warnings.append(
                            f"{layer_key.title()} alpha26 index empty or unavailable for domain={index_domain_key}; "
                            "symbol count may be incomplete."
                        )
                if domain_key == "finance" and finance_subfolder_symbol_counts:
                    if layer_key == "silver":
                        finance_subfolder_symbol_counts = {
                            key: int(finance_subfolder_symbol_counts.get(key, 0))
                            for key in SILVER_FINANCE_SUBDOMAINS
                        }
                    if not prefix_is_empty and sum(int(v) for v in finance_subfolder_symbol_counts.values()) == 0:
                        finance_subfolder_symbol_counts = None
            else:
                warnings.append(
                    f"{layer_key.title()} blob listing unavailable for prefix={prefix}; symbol count set to unknown."
                )

        payload = {
            "layer": layer_key,
            "domain": domain_key,
            "container": container,
            "type": "blob",
            "prefix": prefix,
            "computedAt": computed_at,
            "folderLastModified": folder_last_modified,
            "symbolCount": symbol_count,
            "columns": [],
            "columnCount": None,
            "financeSubfolderSymbolCounts": finance_subfolder_symbol_counts,
            "blacklistedSymbolCount": blacklisted_symbol_count,
            "fileCount": files,
            "totalBytes": total_bytes,
            "metadataPath": None,
            "metadataSource": "scan",
            "warnings": warnings,
        }
        _cache_domain_metadata(layer_key, domain_key, payload)
        return payload

    raise ValueError(f"Unsupported layer/domain combination: layer={layer_key} domain={domain_key}")
