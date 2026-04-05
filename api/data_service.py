from __future__ import annotations

import math
import json
from io import BytesIO
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from core import config as cfg
from core import core as mdc
from core import delta_core
from core import bronze_bucketing
from core import layer_bucketing
from core.pipeline import DataPaths
from core.finance_contracts import SILVER_FINANCE_SUBDOMAINS


_FINANCE_BRONZE_FOLDERS: dict[str, tuple[str, str]] = {
    "balance_sheet": ("Balance Sheet", "quarterly_balance-sheet"),
    "income_statement": ("Income Statement", "quarterly_financials"),
    "cash_flow": ("Cash Flow", "quarterly_cash-flow"),
    "valuation": ("Valuation", "quarterly_valuation_measures"),
}
_FINANCE_SUBDOMAIN_TO_REPORT_TYPE: dict[str, str] = {
    "balance_sheet": "balance_sheet",
    "income_statement": "income_statement",
    "cash_flow": "cash_flow",
    "valuation": "valuation",
}
_FINANCE_LAYER_FOLDERS: dict[str, tuple[str, str]] = {
    sub_domain: _FINANCE_BRONZE_FOLDERS[sub_domain] for sub_domain in SILVER_FINANCE_SUBDOMAINS
}

_ADLS_TREE_SCAN_LIMIT_DEFAULT = 5_000
_ADLS_TREE_SCAN_LIMIT_MAX = 100_000
_ADLS_PREVIEW_MAX_BYTES_DEFAULT = 256 * 1024
_ADLS_PREVIEW_MAX_BYTES_MAX = 1_048_576
_ADLS_PREVIEW_MAX_DELTA_FILES_DEFAULT = 0
_ADLS_PREVIEW_MAX_DELTA_FILES_MAX = 99
_ADLS_TABLE_PREVIEW_ROW_LIMIT = 100
_PLAINTEXT_EXTENSIONS = {
    ".txt",
    ".csv",
    ".json",
    ".jsonl",
    ".log",
    ".yaml",
    ".yml",
    ".xml",
    ".md",
    ".py",
    ".sql",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".css",
    ".html",
    ".htm",
    ".env",
}


class DataService:
    """
    Service layer for accessing financial data from Delta Lake storage.
    Decouples API from direct pipeline script usage.
    """

    @staticmethod
    def _sanitize_json_value(value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, (bool, np.bool_)):
            return bool(value)

        if isinstance(value, (float, np.floating)):
            numeric = float(value)
            if not math.isfinite(numeric):
                return None
            return numeric

        if isinstance(value, (int, np.integer)):
            return int(value)

        if isinstance(value, dict):
            return {str(k): DataService._sanitize_json_value(v) for k, v in value.items()}

        if isinstance(value, list):
            return [DataService._sanitize_json_value(v) for v in value]

        if isinstance(value, tuple):
            return [DataService._sanitize_json_value(v) for v in value]

        return value

    @staticmethod
    def _df_to_records_json_safe(df: pd.DataFrame, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if limit:
            df = df.head(int(limit))

        # Starlette's JSONResponse enforces RFC-compliant JSON and rejects NaN/Inf.
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object).where(pd.notnull(df), None)

        records: Any = df.to_dict(orient="records")
        sanitized = DataService._sanitize_json_value(records)
        return sanitized

    @staticmethod
    def _normalize_date_sort_direction(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if not normalized:
            return None
        if normalized not in {"asc", "desc"}:
            raise ValueError("date sort direction must be 'asc' or 'desc'.")
        return normalized

    @staticmethod
    def _coerce_sortable_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value is None:
            return None
        try:
            parsed = pd.to_datetime(value, errors="coerce")
        except Exception:
            return None
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp):
            if parsed.tz is not None:
                parsed = parsed.tz_convert(None)
            return parsed
        try:
            ts = pd.Timestamp(parsed)
            if ts.tz is not None:
                ts = ts.tz_convert(None)
            return ts
        except Exception:
            return None

    @staticmethod
    def _detect_date_column_for_sort(rows: List[Dict[str, Any]]) -> Optional[str]:
        if not rows:
            return None

        columns: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            for key in row.keys():
                columns.add(str(key))

        if not columns:
            return None

        preferred: List[str] = []
        for candidate in ("date", "Date"):
            if candidate in columns:
                preferred.append(candidate)
        for column in sorted(columns):
            lowered = column.lower()
            if column in preferred:
                continue
            if "date" in lowered or lowered in {"datetime", "timestamp", "as_of", "asof"}:
                preferred.append(column)

        for column in preferred:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if DataService._coerce_sortable_timestamp(row.get(column)) is not None:
                    return column
        return None

    @staticmethod
    def _finalize_rows(
        rows: List[Dict[str, Any]],
        *,
        limit: Optional[int] = None,
        sort_by_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        out = list(rows or [])
        direction = DataService._normalize_date_sort_direction(sort_by_date)
        if direction and out:
            date_column = DataService._detect_date_column_for_sort(out)
            if date_column:
                valid: List[tuple[pd.Timestamp, Dict[str, Any]]] = []
                missing: List[Dict[str, Any]] = []
                for row in out:
                    ts = DataService._coerce_sortable_timestamp(row.get(date_column))
                    if ts is None:
                        missing.append(row)
                        continue
                    valid.append((ts, row))
                valid.sort(key=lambda item: item[0], reverse=(direction == "desc"))
                out = [item[1] for item in valid] + missing

        if limit is not None:
            out = out[: int(limit)]
        return out

    @staticmethod
    def _first_bronze_blob_path(
        client: Any,
        *,
        container: str,
        prefix: str,
        allowed_suffixes: tuple[str, ...],
    ) -> str:
        normalized = str(prefix or "").strip()
        if not normalized:
            raise ValueError("prefix is required to list Bronze blobs.")
        if not normalized.endswith("/"):
            normalized = normalized + "/"

        list_files = getattr(client, "list_files", None)
        if callable(list_files):
            names = [str(name) for name in list_files(name_starts_with=normalized)]
            candidates = [name for name in names if name.lower().endswith(allowed_suffixes)]
            if not candidates:
                raise FileNotFoundError(f"No Bronze blobs found under {container}/{normalized}")
            return sorted(candidates)[0]

        container_client = getattr(client, "container_client", None)
        if container_client is not None and hasattr(container_client, "list_blobs"):
            for blob in container_client.list_blobs(name_starts_with=normalized):
                name = getattr(blob, "name", None)
                if name and str(name).lower().endswith(allowed_suffixes):
                    return str(name)
            raise FileNotFoundError(f"No Bronze blobs found under {container}/{normalized}")

        raise FileNotFoundError("Storage client does not support listing Bronze blobs.")

    @staticmethod
    def _container_for_layer(layer: str) -> str:
        key = str(layer or "").strip().lower()
        if key == "silver":
            return cfg.AZURE_CONTAINER_SILVER
        if key == "gold":
            return cfg.AZURE_CONTAINER_GOLD
        if key == "bronze":
            return cfg.AZURE_CONTAINER_BRONZE
        raise ValueError(f"Unsupported layer: {layer!r}")

    @staticmethod
    def _read_bronze_alpha26_bucket(
        *,
        container: str,
        client: Any,
        domain: str,
        symbol: Optional[str] = None,
        report_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        resolved_symbol = str(symbol or "").strip().upper()
        resolved_report_type = str(report_type or "").strip().lower()
        if resolved_symbol:
            bucket = bronze_bucketing.bucket_letter(resolved_symbol)
            blob_path = bronze_bucketing.active_bucket_blob_path_for_domain(domain, bucket)
        else:
            blob_infos = bronze_bucketing.list_active_bucket_blob_infos(domain, client)
            if not blob_infos:
                raise FileNotFoundError(f"No Bronze blobs found for domain={domain} in container={container}")
            blob_path = str(sorted(blob_infos, key=lambda item: str(item.get("name", "")))[0].get("name", "")).strip()
            if not blob_path:
                raise FileNotFoundError(f"No Bronze blobs found for domain={domain} in container={container}")

        raw_bytes = mdc.read_raw_bytes(blob_path, client=client)
        if not raw_bytes:
            raise FileNotFoundError(f"Raw blob not found: {container}/{blob_path}")
        df = pd.read_parquet(BytesIO(raw_bytes))

        if resolved_symbol and "symbol" in df.columns:
            df = df[df["symbol"].astype(str).str.upper() == resolved_symbol]
        if resolved_report_type and "report_type" in df.columns:
            df = df[df["report_type"].astype(str).str.lower() == resolved_report_type]

        return DataService._df_to_records_json_safe(df, limit=limit)

    @staticmethod
    def _require_storage_client(container: str) -> Any:
        client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Delta table discovery."
            )
        return client

    @staticmethod
    def _container_for_explorer_layer(layer: str) -> str:
        resolved_layer = str(layer or "").strip().lower()
        if resolved_layer in {"bronze", "silver", "gold"}:
            return DataService._container_for_layer(resolved_layer)
        if resolved_layer == "platinum":
            return cfg.AZURE_CONTAINER_PLATINUM
        raise ValueError("Layer must be 'bronze', 'silver', 'gold', or 'platinum'.")

    @staticmethod
    def _normalize_adls_path(path: Optional[str], *, expect_file: bool) -> str:
        raw = str(path or "").strip().replace("\\", "/")
        raw = raw.lstrip("/")
        parts = [segment for segment in raw.split("/") if segment]
        if any(segment == ".." for segment in parts):
            raise ValueError("Path traversal is not allowed.")
        normalized = "/".join(parts)
        if expect_file:
            if not normalized:
                raise ValueError("file path is required.")
            return normalized
        if not normalized:
            return ""
        return f"{normalized}/"

    @staticmethod
    def _blob_datetime_to_iso(value: Any) -> Optional[str]:
        if value is None:
            return None
        try:
            return value.isoformat()
        except Exception:
            return str(value)

    @staticmethod
    def _coerce_limit(raw: Optional[int], *, default: int, maximum: int) -> int:
        if raw is None:
            return default
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            return default
        if parsed <= 0:
            return default
        return min(parsed, maximum)

    @staticmethod
    def _coerce_non_negative_limit(raw: Optional[int], *, default: int, maximum: int) -> int:
        if raw is None:
            return default
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            return default
        if parsed < 0:
            return default
        return min(parsed, maximum)

    @staticmethod
    def list_adls_tree(
        *,
        layer: str,
        path: Optional[str],
        max_entries: Optional[int] = None,
    ) -> Dict[str, Any]:
        """List one hierarchy level for a layer/path in ADLS."""
        resolved_layer = str(layer or "").strip().lower()
        container = DataService._container_for_explorer_layer(resolved_layer)
        prefix = DataService._normalize_adls_path(path, expect_file=False)
        scan_limit = DataService._coerce_limit(
            max_entries,
            default=_ADLS_TREE_SCAN_LIMIT_DEFAULT,
            maximum=_ADLS_TREE_SCAN_LIMIT_MAX,
        )

        client = DataService._require_storage_client(container)
        entries_by_path: Dict[str, Dict[str, Any]] = {}
        scanned = 0
        truncated = False

        blobs = client.container_client.list_blobs(name_starts_with=prefix or None)
        for blob in blobs:
            scanned += 1
            if scanned > scan_limit:
                truncated = True
                break

            blob_name = str(getattr(blob, "name", "") or "")
            if not blob_name:
                continue
            if prefix and not blob_name.startswith(prefix):
                continue

            relative = blob_name[len(prefix):] if prefix else blob_name
            if not relative:
                continue

            if "/" in relative:
                folder_name = relative.split("/", 1)[0].strip()
                if not folder_name:
                    continue
                folder_path = f"{prefix}{folder_name}/"
                if folder_path not in entries_by_path:
                    entries_by_path[folder_path] = {
                        "type": "folder",
                        "name": folder_name,
                        "path": folder_path,
                        "size": None,
                        "lastModified": None,
                    }
                continue

            content_settings = getattr(blob, "content_settings", None)
            content_type = getattr(content_settings, "content_type", None)
            file_path = f"{prefix}{relative}"
            entries_by_path[file_path] = {
                "type": "file",
                "name": relative,
                "path": file_path,
                "size": getattr(blob, "size", None),
                "lastModified": DataService._blob_datetime_to_iso(getattr(blob, "last_modified", None)),
                "contentType": content_type,
            }

        entries = list(entries_by_path.values())
        entries.sort(key=lambda item: (0 if item.get("type") == "folder" else 1, str(item.get("name", "")).lower()))

        return {
            "layer": resolved_layer,
            "container": container,
            "path": prefix,
            "truncated": truncated,
            "scanLimit": scan_limit,
            "entries": entries,
        }

    @staticmethod
    def _is_probably_plaintext(blob_path: str, content: bytes) -> bool:
        lowered = str(blob_path or "").strip().lower()
        extension = ""
        if "." in lowered.rsplit("/", 1)[-1]:
            extension = "." + lowered.rsplit(".", 1)[-1]

        if extension in _PLAINTEXT_EXTENSIONS:
            return True
        if not content:
            return True
        if b"\x00" in content:
            return False

        decoded = content.decode("utf-8", errors="replace")
        if not decoded:
            return True

        replacement_ratio = decoded.count("\ufffd") / len(decoded)
        control_chars = sum(1 for ch in decoded if ord(ch) < 32 and ch not in {"\n", "\r", "\t"})
        control_ratio = control_chars / len(decoded)
        return replacement_ratio <= 0.02 and control_ratio <= 0.10

    @staticmethod
    def get_adls_file_preview(
        *,
        layer: str,
        path: str,
        max_bytes: Optional[int] = None,
        max_delta_files: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return a bounded plaintext preview for a blob path in ADLS."""
        resolved_layer = str(layer or "").strip().lower()
        container = DataService._container_for_explorer_layer(resolved_layer)
        blob_path = DataService._normalize_adls_path(path, expect_file=True)
        preview_limit = DataService._coerce_limit(
            max_bytes,
            default=_ADLS_PREVIEW_MAX_BYTES_DEFAULT,
            maximum=_ADLS_PREVIEW_MAX_BYTES_MAX,
        )
        delta_file_limit = DataService._coerce_non_negative_limit(
            max_delta_files,
            default=_ADLS_PREVIEW_MAX_DELTA_FILES_DEFAULT,
            maximum=_ADLS_PREVIEW_MAX_DELTA_FILES_MAX,
        )

        client = DataService._require_storage_client(container)
        blob_client = client.container_client.get_blob_client(blob_path)
        if not blob_client.exists():
            raise FileNotFoundError(f"Blob not found: {container}/{blob_path}")

        table_preview = DataService._build_delta_table_preview(
            client=client,
            container=container,
            layer=resolved_layer,
            selected_path=blob_path,
            preview_limit=preview_limit,
            max_delta_files=delta_file_limit,
        )
        if table_preview is not None:
            return table_preview

        parquet_preview = DataService._build_parquet_table_preview(
            blob_client=blob_client,
            container=container,
            layer=resolved_layer,
            selected_path=blob_path,
            preview_limit=preview_limit,
            max_delta_files=delta_file_limit,
        )
        if parquet_preview is not None:
            return parquet_preview

        delta_preview = DataService._build_delta_log_preview(
            client=client,
            container=container,
            layer=resolved_layer,
            selected_path=blob_path,
            preview_limit=preview_limit,
            max_delta_files=delta_file_limit,
        )
        if delta_preview is not None:
            return delta_preview

        payload = blob_client.download_blob(offset=0, length=preview_limit + 1).readall()
        truncated = len(payload) > preview_limit
        if truncated:
            payload = payload[:preview_limit]

        content_type = None
        try:
            props = blob_client.get_blob_properties()
            content_settings = getattr(props, "content_settings", None)
            content_type = getattr(content_settings, "content_type", None)
        except Exception:
            content_type = None

        is_plaintext = DataService._is_probably_plaintext(blob_path, payload)
        if not is_plaintext:
            return {
                "layer": resolved_layer,
                "container": container,
                "path": blob_path,
                "isPlainText": False,
                "encoding": None,
                "truncated": truncated,
                "maxBytes": preview_limit,
                "contentType": content_type,
                "contentPreview": None,
                "previewMode": "blob",
                "processedDeltaFiles": None,
                "maxDeltaFiles": delta_file_limit,
                "deltaLogPath": None,
            }

        decoded = payload.decode("utf-8", errors="replace")
        return {
            "layer": resolved_layer,
            "container": container,
            "path": blob_path,
            "isPlainText": True,
            "encoding": "utf-8",
            "truncated": truncated,
            "maxBytes": preview_limit,
            "contentType": content_type,
            "contentPreview": decoded,
            "previewMode": "blob",
            "processedDeltaFiles": None,
            "maxDeltaFiles": delta_file_limit,
            "deltaLogPath": None,
        }

    @staticmethod
    def _parse_delta_log_version(path: str) -> Optional[int]:
        normalized = str(path or "").strip().strip("/")
        if "/_delta_log/" not in normalized or not normalized.lower().endswith(".json"):
            return None

        stem = normalized.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return int(stem) if stem.isdigit() else None

    @staticmethod
    def _resolve_delta_log_prefix(path: str) -> Optional[str]:
        normalized = str(path or "").strip().strip("/")
        if not normalized:
            return None

        marker = "/_delta_log/"
        if marker in normalized:
            table_root = normalized.split(marker, 1)[0].strip("/")
            return f"{table_root}/_delta_log/" if table_root else None

        lowered = normalized.lower()
        if lowered.endswith(".parquet"):
            parts = normalized.rsplit("/", 1)
            if len(parts) == 2 and parts[0]:
                return f"{parts[0]}/_delta_log/"

        return None

    @staticmethod
    def _list_delta_log_json_paths(client: Any, prefix: str) -> List[str]:
        normalized_prefix = str(prefix or "").strip().strip("/")
        if not normalized_prefix:
            return []
        if not normalized_prefix.endswith("/"):
            normalized_prefix = f"{normalized_prefix}/"

        discovered: List[str] = []
        list_files = getattr(client, "list_files", None)
        if callable(list_files):
            discovered = [str(name) for name in list_files(name_starts_with=normalized_prefix)]
        else:
            container_client = getattr(client, "container_client", None)
            if container_client is not None and hasattr(container_client, "list_blobs"):
                discovered = [
                    str(getattr(blob, "name", ""))
                    for blob in container_client.list_blobs(name_starts_with=normalized_prefix)
                ]

        return sorted(
            name
            for name in discovered
            if name and name.startswith(normalized_prefix) and name.lower().endswith(".json")
        )

    @staticmethod
    def _resolve_delta_table_root(client: Any, path: str) -> Optional[str]:
        normalized = str(path or "").strip().strip("/")
        if not normalized:
            return None

        marker = "/_delta_log/"
        if marker in normalized:
            table_root = normalized.split(marker, 1)[0].strip("/")
            return table_root or None

        if not normalized.lower().endswith(".parquet"):
            return None

        parts = normalized.split("/")
        for depth in range(len(parts) - 1, 0, -1):
            candidate = "/".join(parts[:depth]).strip("/")
            if not candidate:
                continue
            delta_log_prefix = f"{candidate}/_delta_log/"
            if DataService._list_delta_log_json_paths(client, delta_log_prefix):
                return candidate

        return None

    @staticmethod
    def _extract_delta_log_versions(paths: List[str]) -> List[int]:
        versions = {
            version
            for version in (DataService._parse_delta_log_version(path) for path in paths)
            if version is not None
        }
        return sorted(versions)

    @staticmethod
    def _resolve_delta_preview_version(
        available_paths: List[str],
        *,
        selected_path: str,
        max_delta_files: int,
    ) -> Tuple[Optional[int], Optional[int]]:
        versions = DataService._extract_delta_log_versions(available_paths)
        if not versions:
            return DataService._parse_delta_log_version(selected_path), None

        selected_version = DataService._parse_delta_log_version(selected_path)
        upper_bound_version = selected_version if selected_version is not None else versions[-1]
        eligible_versions = [version for version in versions if version <= upper_bound_version]
        if not eligible_versions:
            return selected_version, None

        if max_delta_files <= 0:
            return eligible_versions[-1], len(eligible_versions)

        applied_versions = eligible_versions[:max_delta_files]
        return applied_versions[-1], len(applied_versions)

    @staticmethod
    def _build_tabular_preview_response(
        *,
        container: str,
        layer: str,
        selected_path: str,
        preview_limit: int,
        max_delta_files: int,
        preview_mode: str,
        df: pd.DataFrame,
        content_type: str,
        resolved_table_path: Optional[str],
        table_version: Optional[int],
        delta_log_path: Optional[str],
        processed_delta_files: Optional[int],
    ) -> Dict[str, Any]:
        total_rows = int(len(df))
        preview_rows = DataService._df_to_records_json_safe(df, limit=_ADLS_TABLE_PREVIEW_ROW_LIMIT)

        return {
            "layer": layer,
            "container": container,
            "path": selected_path,
            "isPlainText": False,
            "encoding": None,
            "truncated": False,
            "maxBytes": preview_limit,
            "contentType": content_type,
            "contentPreview": None,
            "previewMode": preview_mode,
            "processedDeltaFiles": processed_delta_files,
            "maxDeltaFiles": max_delta_files,
            "deltaLogPath": delta_log_path,
            "tableColumns": [str(column) for column in df.columns],
            "tableRows": preview_rows,
            "tableRowCount": total_rows,
            "tablePreviewLimit": _ADLS_TABLE_PREVIEW_ROW_LIMIT,
            "tableTruncated": total_rows > _ADLS_TABLE_PREVIEW_ROW_LIMIT,
            "resolvedTablePath": resolved_table_path,
            "tableVersion": table_version,
        }

    @staticmethod
    def _build_delta_table_preview(
        *,
        client: Any,
        container: str,
        layer: str,
        selected_path: str,
        preview_limit: int,
        max_delta_files: int,
    ) -> Optional[Dict[str, Any]]:
        table_root = DataService._resolve_delta_table_root(client, selected_path)
        if not table_root:
            return None

        delta_log_prefix = f"{table_root}/_delta_log/"
        available_delta_paths = DataService._list_delta_log_json_paths(client, delta_log_prefix)
        table_version, processed_delta_files = DataService._resolve_delta_preview_version(
            available_delta_paths,
            selected_path=selected_path,
            max_delta_files=max_delta_files,
        )
        df = delta_core.load_delta(
            container,
            table_root,
            version=table_version,
        )
        if df is None:
            return None

        return DataService._build_tabular_preview_response(
            container=container,
            layer=layer,
            selected_path=selected_path,
            preview_limit=preview_limit,
            max_delta_files=max_delta_files,
            preview_mode="delta-table",
            df=df,
            content_type="application/x-delta-table-preview",
            resolved_table_path=table_root,
            table_version=table_version,
            delta_log_path=delta_log_prefix,
            processed_delta_files=processed_delta_files,
        )

    @staticmethod
    def _build_parquet_table_preview(
        *,
        blob_client: Any,
        container: str,
        layer: str,
        selected_path: str,
        preview_limit: int,
        max_delta_files: int,
    ) -> Optional[Dict[str, Any]]:
        if not str(selected_path or "").strip().lower().endswith(".parquet"):
            return None

        try:
            payload = blob_client.download_blob().readall()
            df = pd.read_parquet(BytesIO(payload))
        except Exception:
            return None

        return DataService._build_tabular_preview_response(
            container=container,
            layer=layer,
            selected_path=selected_path,
            preview_limit=preview_limit,
            max_delta_files=max_delta_files,
            preview_mode="parquet-table",
            df=df,
            content_type="application/x-parquet-preview",
            resolved_table_path=None,
            table_version=None,
            delta_log_path=None,
            processed_delta_files=None,
        )

    @staticmethod
    def _select_delta_log_preview_paths(
        available_paths: List[str],
        *,
        selected_path: str,
        max_delta_files: int,
    ) -> List[str]:
        if not available_paths:
            return []

        selected_version = DataService._parse_delta_log_version(selected_path)
        versions_by_path = [
            (path, DataService._parse_delta_log_version(path))
            for path in available_paths
        ]
        eligible_paths = [
            path
            for path, version in versions_by_path
            if version is not None and (selected_version is None or version <= selected_version)
        ]

        if not eligible_paths:
            return []

        return eligible_paths[:max_delta_files]

    @staticmethod
    def _build_delta_log_preview(
        *,
        client: Any,
        container: str,
        layer: str,
        selected_path: str,
        preview_limit: int,
        max_delta_files: int,
    ) -> Optional[Dict[str, Any]]:
        if max_delta_files <= 0:
            return None

        delta_log_prefix = DataService._resolve_delta_log_prefix(selected_path)
        if not delta_log_prefix:
            return None

        preview_paths = DataService._select_delta_log_preview_paths(
            DataService._list_delta_log_json_paths(client, delta_log_prefix),
            selected_path=selected_path,
            max_delta_files=max_delta_files,
        )
        if not preview_paths:
            return None

        container_client = getattr(client, "container_client", None)
        if container_client is None or not hasattr(container_client, "get_blob_client"):
            return None

        combined = bytearray()
        truncated = False
        processed_paths: List[str] = []

        for candidate_path in preview_paths:
            if combined and not combined.endswith(b"\n"):
                if len(combined) >= preview_limit:
                    truncated = True
                    break
                combined.extend(b"\n")

            remaining = preview_limit - len(combined)
            if remaining <= 0:
                truncated = True
                break

            delta_blob_client = container_client.get_blob_client(candidate_path)
            payload = delta_blob_client.download_blob(offset=0, length=remaining + 1).readall()
            if len(payload) > remaining:
                combined.extend(payload[:remaining])
                truncated = True
                processed_paths.append(candidate_path)
                break

            combined.extend(payload)
            processed_paths.append(candidate_path)

        if not processed_paths:
            return None

        return {
            "layer": layer,
            "container": container,
            "path": selected_path,
            "isPlainText": True,
            "encoding": "utf-8",
            "truncated": truncated,
            "maxBytes": preview_limit,
            "contentType": "application/x-ndjson",
            "contentPreview": combined.decode("utf-8", errors="replace"),
            "previewMode": "delta-log",
            "processedDeltaFiles": len(processed_paths),
            "maxDeltaFiles": max_delta_files,
            "deltaLogPath": delta_log_prefix,
        }

    @staticmethod
    def _discover_delta_table_paths(container: str, prefix: str) -> List[str]:
        normalized = str(prefix or "").strip().strip("/")
        if not normalized:
            raise ValueError("prefix is required to discover Delta tables.")

        client = DataService._require_storage_client(container)
        list_files = getattr(client, "list_files", None)
        if not callable(list_files):
            raise FileNotFoundError("Storage client does not support listing Delta table paths.")

        roots: set[str] = set()
        search_prefix = f"{normalized}/"
        for name in list_files(name_starts_with=search_prefix):
            text = str(name or "")
            marker = "/_delta_log/"
            if marker not in text:
                continue
            root = text.split(marker, 1)[0].strip("/")
            if root and root.startswith(search_prefix.rstrip("/")):
                roots.add(root)
        return sorted(roots)

    @staticmethod
    def _collect_delta_frames(
        container: str,
        paths: List[str],
        *,
        limit: Optional[int] = None,
        enrich: Optional[Any] = None,
    ) -> List[pd.DataFrame]:
        frames: List[pd.DataFrame] = []
        row_budget = int(limit) if limit is not None else None
        rows_collected = 0
        for path in paths:
            try:
                df = delta_core.load_delta(container, path)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            if enrich is not None:
                df = enrich(df, path)
            frames.append(df)
            rows_collected += int(len(df))
            if row_budget is not None and rows_collected >= row_budget:
                break
        return frames

    @staticmethod
    def _frames_to_records(frames: List[pd.DataFrame], *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if not frames:
            return []
        merged = pd.concat(frames, ignore_index=True)
        return DataService._df_to_records_json_safe(merged, limit=limit)

    @staticmethod
    def _read_cross_section_from_prefix(container: str, prefix: str, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        paths = DataService._discover_delta_table_paths(container, prefix)
        frames = DataService._collect_delta_frames(container, paths, limit=limit)
        return DataService._frames_to_records(frames, limit=limit)

    @staticmethod
    def _read_cross_section_from_prefixes(
        container: str,
        prefixes: Sequence[str],
        *,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        for prefix in prefixes:
            paths = DataService._discover_delta_table_paths(container, prefix)
            if not paths:
                continue
            frames = DataService._collect_delta_frames(container, paths, limit=limit)
            return DataService._frames_to_records(frames, limit=limit)
        return []

    @staticmethod
    def _read_delta_from_paths(
        container: str,
        paths: Sequence[str],
        *,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        for path in paths:
            try:
                return DataService._read_delta(container, path, limit=limit)
            except FileNotFoundError:
                continue
        return []

    @staticmethod
    def _read_silver_finance_regular(
        *,
        container: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        symbol = str(ticker or "").strip().upper()
        if symbol:
            bucket = layer_bucketing.bucket_letter(symbol)
            paths = [
                DataPaths.get_silver_finance_bucket_path(sub_domain, bucket)
                for sub_domain in _FINANCE_LAYER_FOLDERS.keys()
            ]
        else:
            paths = DataService._discover_delta_table_paths(container, "finance-data")

        def _enrich(df: pd.DataFrame, path: str) -> pd.DataFrame:
            out = df.copy()
            parts = str(path or "").split("/")
            sub_domain = parts[1] if len(parts) > 2 else ""
            if sub_domain and "sub_domain" not in out.columns:
                out["sub_domain"] = sub_domain
            if symbol and "symbol" in out.columns:
                out = out[out["symbol"].astype(str).str.upper() == symbol]
            return out

        frames = DataService._collect_delta_frames(container, paths, limit=limit, enrich=_enrich)
        return DataService._frames_to_records(frames, limit=limit)

    @staticmethod
    def _read_gold_finance_regular(
        *,
        container: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        symbol = str(ticker or "").strip().upper()
        if symbol:
            bucket = layer_bucketing.bucket_letter(symbol)
            rows = DataService._read_delta(
                container,
                DataPaths.get_gold_finance_alpha26_bucket_path(bucket),
                limit=None,
            )
            return [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]

        return DataService._read_cross_section_from_prefix(
            container,
            "finance/buckets",
            limit=limit,
        )
    
    @staticmethod
    def get_data(
        layer: str, 
        domain: str, 
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        sort_by_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generic data retrieval for market, earnings, and price-target domains.

        Notes
        - Silver/Gold use Delta tables.
        - Bronze stores alpha26 bucket parquet files (`A..Z`) per domain.
        - Cross-sectional requests are assembled from bucketed Delta folders.
        """
        resolved_layer = str(layer or "").strip().lower()
        raw_domain = str(domain or "").strip().lower()
        resolved_domain = "price-target" if raw_domain in {"price-target", "price_target"} else raw_domain
        container = DataService._container_for_layer(resolved_layer)
        resolved_sort = DataService._normalize_date_sort_direction(sort_by_date)
        downstream_limit = None if resolved_sort else limit

        if resolved_layer == "bronze":
            rows = DataService._get_bronze_data(
                container=container,
                domain=resolved_domain,
                ticker=ticker,
                limit=downstream_limit,
            )
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain.startswith("finance/"):
            _, _, sub_domain = resolved_domain.partition("/")
            if not sub_domain:
                raise ValueError("finance domain requires a sub-domain, e.g. finance/balance_sheet")
            return DataService.get_finance_data(
                resolved_layer,
                sub_domain,
                ticker=ticker,
                limit=limit,
                sort_by_date=resolved_sort,
            )

        is_silver = resolved_layer == "silver"
        is_gold = resolved_layer == "gold"
        if is_silver:
            layer_bucketing.silver_layout_mode()
        if is_gold:
            layer_bucketing.gold_layout_mode()
        symbol = str(ticker or "").strip().upper()

        if resolved_domain == "market":
            if symbol:
                path = (
                    DataPaths.get_silver_market_bucket_path(layer_bucketing.bucket_letter(symbol))
                    if is_silver
                    else DataPaths.get_gold_market_bucket_path(layer_bucketing.bucket_letter(symbol))
                )
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            prefix = "market-data/buckets" if is_silver else "market/buckets"
            rows = DataService._read_cross_section_from_prefix(container, prefix, limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain == "finance":
            if is_silver:
                rows = DataService._read_silver_finance_regular(
                    container=container,
                    ticker=symbol or None,
                    limit=downstream_limit,
                )
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            rows = DataService._read_gold_finance_regular(
                container=container,
                ticker=symbol or None,
                limit=downstream_limit,
            )
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain == "earnings":
            if symbol:
                path = (
                    DataPaths.get_silver_earnings_bucket_path(layer_bucketing.bucket_letter(symbol))
                    if is_silver
                    else DataPaths.get_gold_earnings_bucket_path(layer_bucketing.bucket_letter(symbol))
                )
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            prefix = f"{(getattr(cfg, 'EARNINGS_DATA_PREFIX', 'earnings-data') or 'earnings-data')}/buckets" if is_silver else "earnings/buckets"
            rows = DataService._read_cross_section_from_prefix(container, prefix, limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_domain == "price-target":
            if symbol:
                path = (
                    DataPaths.get_silver_price_target_bucket_path(layer_bucketing.bucket_letter(symbol))
                    if is_silver
                    else DataPaths.get_gold_price_targets_bucket_path(layer_bucketing.bucket_letter(symbol))
                )
                rows = DataService._read_delta(container, path, limit=None)
                rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
                return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
            prefix = "price-target-data/buckets" if is_silver else "targets/buckets"
            rows = DataService._read_cross_section_from_prefix(container, prefix, limit=downstream_limit)
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        raise ValueError(f"Domain '{domain}' not supported on generic endpoint")

    @staticmethod
    def get_finance_data(
        layer: str,
        sub_domain: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
        sort_by_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Specialized retrieval for Finance data.
        """
        resolved_layer = str(layer or "").strip().lower()
        resolved_sub = str(sub_domain or "").strip().lower()
        container = DataService._container_for_layer(resolved_layer)
        resolved_sort = DataService._normalize_date_sort_direction(sort_by_date)
        downstream_limit = None if resolved_sort else limit

        if resolved_layer == "bronze":
            client = mdc.get_storage_client(container)
            if client is None:
                raise FileNotFoundError(
                    f"Storage client unavailable for container={container!r}. "
                    "Set Azure storage env vars to enable Bronze exploration."
                )

            if resolved_sub not in _FINANCE_BRONZE_FOLDERS:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")

            report_type = _FINANCE_SUBDOMAIN_TO_REPORT_TYPE.get(resolved_sub)
            if not report_type:
                raise ValueError(f"Unsupported finance sub-domain in alpha26 mode: {sub_domain}")
            rows = DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain="finance",
                symbol=ticker,
                report_type=report_type,
                limit=downstream_limit,
            )
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)
        
        if resolved_layer == "silver":
            if not ticker:
                raise ValueError("ticker is required for Silver finance data.")
            if resolved_sub not in _FINANCE_LAYER_FOLDERS:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")

            layer_bucketing.silver_layout_mode()
            symbol = str(ticker).strip().upper()
            path = DataPaths.get_silver_finance_bucket_path(
                resolved_sub,
                layer_bucketing.bucket_letter(symbol),
            )
            rows = DataService._read_delta(container, path, limit=None)
            rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        if resolved_layer == "gold":
            if not ticker:
                raise ValueError("ticker is required for Gold finance data.")
            if resolved_sub not in _FINANCE_LAYER_FOLDERS:
                raise ValueError(f"Unknown finance sub-domain: {sub_domain}")
            layer_bucketing.gold_layout_mode()
            symbol = str(ticker).strip().upper()
            rows = DataService._read_delta(
                container,
                DataPaths.get_gold_finance_alpha26_bucket_path(layer_bucketing.bucket_letter(symbol)),
                limit=None,
            )
            for row in rows:
                if "sub_domain" not in row:
                    row["sub_domain"] = resolved_sub
            rows = [row for row in rows if str(row.get("symbol", "")).strip().upper() == symbol]
            return DataService._finalize_rows(rows, limit=limit, sort_by_date=resolved_sort)

        raise ValueError("Layer must be 'bronze', 'silver', or 'gold'.")

    @staticmethod
    def _get_bronze_data(
        *,
        container: str,
        domain: str,
        ticker: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Bronze exploration."
            )

        symbol = str(ticker or "").strip().upper() if ticker else ""
        bronze_bucketing.bronze_layout_mode()

        if domain == "market":
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain="market",
                symbol=symbol or None,
                limit=limit,
            )

        if domain == "earnings":
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain="earnings",
                symbol=symbol or None,
                limit=limit,
            )

        if domain == "finance":
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain="finance",
                symbol=symbol or None,
                limit=limit,
            )

        if domain in {"price-target", "price_target"}:
            return DataService._read_bronze_alpha26_bucket(
                container=container,
                client=client,
                domain="price-target",
                symbol=symbol or None,
                limit=limit,
            )

        raise ValueError(f"Domain '{domain}' not supported on Bronze explorer endpoint")

    @staticmethod
    def _read_bronze_raw(
        container: str,
        blob_path: str,
        *,
        kind: str,
        limit: Optional[int] = None,
        client: Any = None,
    ) -> List[Dict[str, Any]]:
        if client is None:
            client = mdc.get_storage_client(container)
        if client is None:
            raise FileNotFoundError(
                f"Storage client unavailable for container={container!r}. "
                "Set Azure storage env vars to enable Bronze exploration."
            )

        raw_bytes = mdc.read_raw_bytes(blob_path, client=client)
        if not raw_bytes:
            raise FileNotFoundError(f"Raw blob not found: {container}/{blob_path}")

        df: pd.DataFrame
        kind_key = str(kind or "").strip().lower()
        if kind_key == "csv":
            df = pd.read_csv(BytesIO(raw_bytes))
        elif kind_key == "json":
            payload = json.loads(raw_bytes.decode("utf-8"))
            if isinstance(payload, list):
                df = pd.DataFrame(payload)
            elif isinstance(payload, dict):
                df = pd.DataFrame([payload])
            else:
                raise ValueError(f"Unsupported JSON payload type: {type(payload).__name__}")
        elif kind_key == "parquet":
            df = pd.read_parquet(BytesIO(raw_bytes))
        else:
            raise ValueError(f"Unsupported bronze kind={kind!r}")

        return DataService._df_to_records_json_safe(df, limit=limit)

    @staticmethod
    def _read_delta(container: str, path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            df = delta_core.load_delta(container, path)
            if df is None:
                raise FileNotFoundError(f"Delta table not found: {container}/{path}")

            return DataService._df_to_records_json_safe(df, limit=limit)
        except Exception as e:
            # Log error
            raise FileNotFoundError(f"Failed to read data at {path}: {str(e)}")

    @staticmethod
    def _extract_finance_domain_rows(
        layer: str,
        domain: str,
        ticker: Optional[str],
        sample_rows: int,
    ) -> List[Dict[str, Any]]:
        normalized_domain = str(domain or "").strip().lower()
        if normalized_domain.startswith("finance/"):
            _, _, remainder = normalized_domain.partition("/")
            sub_domain = remainder.strip()
            if not sub_domain:
                raise ValueError("finance domain requires a sub-domain, e.g. finance/balance_sheet")
            return DataService.get_finance_data(layer, sub_domain, ticker, limit=sample_rows)
        return DataService.get_data(layer, domain, ticker, limit=sample_rows)

    @staticmethod
    def _format_number_bucket_edge(value: float) -> float:
        if value == int(value):
            return float(int(value))
        return float(np.round(value, 6))

    @staticmethod
    def get_column_profile(
        layer: str,
        domain: str,
        column: str,
        *,
        ticker: Optional[str] = None,
        bins: int = 20,
        sample_rows: int = 10000,
        top_values: int = 20,
    ) -> Dict[str, Any]:
        normalized_layer = str(layer or "").strip().lower()
        normalized_domain = str(domain or "").strip().lower()
        normalized_column = str(column or "").strip()

        if not normalized_layer:
            raise ValueError("layer is required.")
        if normalized_layer not in {"bronze", "silver", "gold"}:
            raise ValueError("Layer must be 'bronze', 'silver', or 'gold'.")
        if not normalized_domain:
            raise ValueError("domain is required.")
        if not normalized_column:
            raise ValueError("column is required.")

        resolved_ticker = None if ticker is None else str(ticker).strip().upper() or None
        resolved_bins = max(3, min(int(bins), 200))
        resolved_sample_rows = max(10, min(int(sample_rows), 100000))
        resolved_top_values = max(1, min(int(top_values), 200))

        rows = DataService._extract_finance_domain_rows(
            normalized_layer,
            normalized_domain,
            resolved_ticker,
            sample_rows=resolved_sample_rows,
        )
        if not rows:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": 0,
                "nonNullCount": 0,
                "nullCount": 0,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        df = pd.DataFrame(rows)
        if normalized_column not in df.columns:
            raise ValueError(f"Column '{normalized_column}' not found in sampled data.")

        series = df[normalized_column]
        total_rows = int(len(df))
        series_non_null = series.dropna()
        non_null_count = int(len(series_non_null))
        null_count = total_rows - non_null_count

        if non_null_count == 0:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": total_rows,
                "nonNullCount": 0,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        candidate_str = series_non_null.astype(str).str.strip()
        if candidate_str.empty:
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "string",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        parsed_date = pd.to_datetime(series_non_null, errors="coerce", utc=False)
        date_count = int(parsed_date.notna().sum())
        date_ratio = date_count / non_null_count if non_null_count else 0.0

        if date_ratio >= 0.7:
            date_vals = parsed_date.dropna().dt.to_period("M").astype(str)
            value_counts = date_vals.value_counts().sort_index()
            buckets = []
            for key, count in value_counts.items():
                buckets.append({
                    "label": str(key),
                    "count": int(count),
                })
            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "date",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": buckets,
                "uniqueCount": int(date_vals.nunique()),
                "duplicateCount": int(non_null_count - date_vals.nunique()),
                "topValues": [],
            }

        numeric = pd.to_numeric(series_non_null, errors="coerce")
        numeric_count = int(numeric.notna().sum())
        numeric_ratio = numeric_count / non_null_count if non_null_count else 0.0

        if numeric_ratio >= 0.7 and numeric_count > 0:
            numeric_clean = numeric.replace([np.inf, -np.inf], np.nan).dropna()

            if numeric_clean.empty:
                kind = "string"
                return {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "column": normalized_column,
                    "kind": kind,
                    "totalRows": total_rows,
                    "nonNullCount": non_null_count,
                    "nullCount": null_count,
                    "sampleRows": resolved_sample_rows,
                    "bins": [],
                    "uniqueCount": 0,
                    "duplicateCount": 0,
                    "topValues": [],
                }

            if len(numeric_clean.unique()) == 1:
                value = DataService._format_number_bucket_edge(float(numeric_clean.iloc[0]))
                return {
                    "layer": normalized_layer,
                    "domain": normalized_domain,
                    "column": normalized_column,
                    "kind": "numeric",
                    "totalRows": total_rows,
                    "nonNullCount": non_null_count,
                    "nullCount": null_count,
                    "sampleRows": resolved_sample_rows,
                    "bins": [{"label": str(value), "count": int(len(numeric_clean)), "start": value, "end": value}],
                    "uniqueCount": 1,
                    "duplicateCount": int(non_null_count - 1),
                    "topValues": [],
                }

            try:
                bucketed = pd.cut(numeric_clean, bins=resolved_bins)
            except ValueError:
                unique_values = numeric_clean.drop_duplicates().sort_values()
                min_value = float(unique_values.min())
                max_value = float(unique_values.max())
                if min_value == max_value:
                    return {
                        "layer": normalized_layer,
                        "domain": normalized_domain,
                        "column": normalized_column,
                        "kind": "numeric",
                        "totalRows": total_rows,
                        "nonNullCount": non_null_count,
                        "nullCount": null_count,
                        "sampleRows": resolved_sample_rows,
                        "bins": [{"label": str(DataService._format_number_bucket_edge(min_value)), "count": int(len(numeric_clean)), "start": min_value, "end": max_value}],
                        "uniqueCount": int(unique_values.nunique()),
                        "duplicateCount": int(non_null_count - unique_values.nunique()),
                        "topValues": [],
                    }
                bucketed = pd.qcut(
                    numeric_clean,
                    q=min(20, int(numeric_clean.nunique())),
                    duplicates="drop"
                )
                buckets = bucketed.value_counts().sort_index()
            else:
                buckets = bucketed.value_counts().sort_index()

            payload = []
            for key, count in buckets.items():
                if isinstance(key, pd.Interval):
                    left = DataService._format_number_bucket_edge(float(key.left))
                    right = DataService._format_number_bucket_edge(float(key.right))
                    if isinstance(key.left, (int, float)) and isinstance(key.right, (int, float)) and key.left == key.right:
                        label = str(left)
                    else:
                        label = f"{left} to {right}"
                    payload.append(
                        {
                            "label": label,
                            "count": int(count),
                            "start": left,
                            "end": right,
                        }
                    )
                else:
                    value = DataService._format_number_bucket_edge(float(key))
                    payload.append({"label": str(value), "count": int(count), "start": value, "end": value})

            return {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "column": normalized_column,
                "kind": "numeric",
                "totalRows": total_rows,
                "nonNullCount": non_null_count,
                "nullCount": null_count,
                "sampleRows": resolved_sample_rows,
                "bins": payload,
                "uniqueCount": int(numeric_clean.nunique()),
                "duplicateCount": int(non_null_count - numeric_clean.nunique()),
                "topValues": [],
            }

        value_counts = candidate_str.value_counts()
        unique = int(value_counts.shape[0])
        top_n = value_counts.head(resolved_top_values)

        return {
            "layer": normalized_layer,
            "domain": normalized_domain,
            "column": normalized_column,
            "kind": "string",
            "totalRows": total_rows,
            "nonNullCount": non_null_count,
            "nullCount": null_count,
            "sampleRows": resolved_sample_rows,
            "bins": [],
            "uniqueCount": unique,
            "duplicateCount": int(non_null_count - unique),
            "topValues": [
                {"value": str(value), "count": int(count)} for value, count in top_n.items()
            ],
        }

    @staticmethod
    def build_column_profile_from_rows(
        rows: List[Dict[str, Any]],
        *,
        layer: str,
        domain: str,
        column: str,
        bins: int = 20,
        sample_rows: int = 10000,
        top_values: int = 20,
    ) -> Dict[str, Any]:
        frame = pd.DataFrame(rows or [])
        if frame.empty:
            return {
                "layer": layer,
                "domain": domain,
                "column": column,
                "kind": "string",
                "totalRows": 0,
                "nonNullCount": 0,
                "nullCount": 0,
                "sampleRows": sample_rows,
                "bins": [],
                "uniqueCount": 0,
                "duplicateCount": 0,
                "topValues": [],
            }

        if column not in frame.columns:
            raise ValueError(f"Column '{column}' not found in sampled data.")

        temp_domain = "__profile_tmp__"
        original_get_data = DataService.get_data
        try:
            DataService.get_data = staticmethod(lambda _layer, _domain, _ticker=None, limit=None, sort_by_date=None: frame.to_dict("records"))  # type: ignore[method-assign]
            payload = DataService.get_column_profile(
                layer=layer,
                domain=temp_domain,
                column=column,
                ticker=None,
                bins=bins,
                sample_rows=sample_rows,
                top_values=top_values,
            )
            payload["domain"] = domain
            return payload
        finally:
            DataService.get_data = original_get_data  # type: ignore[method-assign]
