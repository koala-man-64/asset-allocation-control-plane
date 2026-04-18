from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, Iterable, Optional

import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.foundation import run_manifests
ALPHABET_BUCKETS: tuple[str, ...] = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
_DOMAIN_PREFIXES: dict[str, str] = {
    "market": "market-data",
    "finance": "finance-data",
    "earnings": "earnings-data",
    "price-target": "price-target-data",
}


def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def bronze_layout_mode() -> str:
    from asset_allocation_runtime_common.foundation import config as cfg
    mode = (os.environ.get("BRONZE_LAYOUT_MODE") or str(cfg.BRONZE_LAYOUT_MODE)).strip().lower()
    if mode != "alpha26":
        raise ValueError("BRONZE_LAYOUT_MODE must be 'alpha26' when set.")
    return mode


def is_alpha26_mode() -> bool:
    bronze_layout_mode()
    return True


def alpha26_force_rebuild() -> bool:
    raw = os.environ.get("BRONZE_ALPHA26_FORCE_REBUILD")
    if raw is None:
        return True
    return _is_truthy(raw)


def alpha26_codec() -> str:
    raw = (os.environ.get("BRONZE_ALPHA26_CODEC") or "snappy").strip().lower()
    return raw or "snappy"


def bucket_letter(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    for ch in normalized:
        if "A" <= ch <= "Z":
            return ch
    return "X"


def domain_prefix(domain: str) -> str:
    key = str(domain or "").strip().lower()
    if key not in _DOMAIN_PREFIXES:
        raise ValueError(f"Unsupported bronze bucket domain: {domain!r}")
    return _DOMAIN_PREFIXES[key]


def bucket_blob_path_for_domain(domain: str, bucket: str) -> str:
    return bucket_blob_path(domain_prefix(domain), bucket)


def active_domain_prefix(domain: str) -> str:
    resolved = run_manifests.resolve_active_bronze_alpha26_prefix(domain)
    return str(resolved or domain_prefix(domain)).strip().strip("/")


def active_bucket_blob_path_for_domain(domain: str, bucket: str) -> str:
    return bucket_blob_path(active_domain_prefix(domain), bucket)


def canonical_bucket_blob_name(blob_name: str) -> str:
    text = str(blob_name or "").strip().strip("/")
    if not text:
        return ""
    parts = text.split("/")
    if len(parts) < 3 or parts[-2] != "buckets":
        return text
    if "runs" not in parts[:-2]:
        return text
    run_index = parts.index("runs")
    if run_index <= 0 or run_index != len(parts) - 4:
        return text
    return "/".join([*parts[:run_index], "buckets", parts[-1]])


def parse_bucket_from_blob_name(blob_name: str, *, expected_prefix: Optional[str] = None) -> Optional[str]:
    canonical = canonical_bucket_blob_name(blob_name)
    parts = canonical.split("/")
    if len(parts) < 3 or parts[-2] != "buckets":
        return None
    prefix = "/".join(parts[:-2]).strip("/")
    normalized_expected = str(expected_prefix or "").strip().strip("/")
    if normalized_expected and prefix != normalized_expected:
        return None
    filename = parts[-1]
    if "." in filename:
        filename = filename.split(".", 1)[0]
    bucket = filename.strip().upper()
    if bucket not in ALPHABET_BUCKETS:
        return None
    return bucket


def bucket_blob_path(prefix: str, bucket: str) -> str:
    clean_prefix = str(prefix or "").strip().strip("/")
    clean_bucket = str(bucket or "").strip().upper()
    if clean_bucket not in ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket: {bucket!r}")
    return f"{clean_prefix}/buckets/{clean_bucket}.parquet"


def all_bucket_blob_paths(prefix: str) -> list[str]:
    return [bucket_blob_path(prefix, bucket) for bucket in ALPHABET_BUCKETS]


def bucket_blob_paths_for_domain(domain: str) -> list[str]:
    return all_bucket_blob_paths(domain_prefix(domain))


def active_bucket_blob_paths_for_domain(domain: str) -> list[str]:
    return all_bucket_blob_paths(active_domain_prefix(domain))


def list_active_bucket_blob_infos(domain: str, client: Any) -> list[dict[str, Any]]:
    manifest = run_manifests.load_latest_bronze_alpha26_manifest(domain)
    if isinstance(manifest, dict):
        out = run_manifests.manifest_blobs(manifest)
        if out:
            return out

    prefix = f"{domain_prefix(domain).strip('/')}/buckets/"
    list_files = getattr(client, "list_files", None)
    if callable(list_files):
        names = sorted(
            str(name)
            for name in list_files(name_starts_with=prefix)
            if str(name).startswith(prefix) and str(name).endswith(".parquet")
        )
        return [{"name": name} for name in names]

    container_client = getattr(client, "container_client", None)
    if container_client is None or not hasattr(container_client, "list_blobs"):
        return []
    out: list[dict[str, Any]] = []
    for blob in container_client.list_blobs(name_starts_with=prefix):
        name = str(getattr(blob, "name", "") or "")
        if not name or not name.endswith(".parquet"):
            continue
        entry: dict[str, Any] = {"name": name}
        size = getattr(blob, "size", None)
        if isinstance(size, int):
            entry["size"] = size
        last_modified = getattr(blob, "last_modified", None)
        try:
            entry["last_modified"] = last_modified.isoformat() if last_modified is not None else None
        except Exception:
            pass
        out.append(entry)
    out.sort(key=lambda item: str(item.get("name", "")))
    return out


def empty_bucket_frames(schema_columns: Iterable[str]) -> dict[str, pd.DataFrame]:
    cols = [str(c) for c in schema_columns]
    out: dict[str, pd.DataFrame] = {}
    for bucket in ALPHABET_BUCKETS:
        out[bucket] = pd.DataFrame(columns=cols)
    return out


def split_df_by_bucket(df: pd.DataFrame, *, symbol_column: str = "symbol") -> dict[str, pd.DataFrame]:
    if df is None or df.empty:
        return {bucket: pd.DataFrame() for bucket in ALPHABET_BUCKETS}
    if symbol_column not in df.columns:
        raise ValueError(f"Missing symbol column {symbol_column!r}.")

    out: dict[str, pd.DataFrame] = {bucket: pd.DataFrame() for bucket in ALPHABET_BUCKETS}
    with_bucket = df.copy()
    with_bucket[symbol_column] = with_bucket[symbol_column].astype(str).str.upper()
    with_bucket["_bucket"] = with_bucket[symbol_column].map(bucket_letter)
    for bucket, part in with_bucket.groupby("_bucket", sort=True):
        out[str(bucket)] = part.drop(columns=["_bucket"]).reset_index(drop=True)
    return out


def write_bucket_parquet(
    *,
    client: Any,
    prefix: str,
    bucket: str,
    df: pd.DataFrame,
    codec: Optional[str] = None,
) -> str:
    table = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    data = table.to_parquet(index=False, compression=(codec or alpha26_codec()))
    path = bucket_blob_path(prefix, bucket)
    return mdc.store_raw_bytes(data, path, client=client)


def read_bucket_parquet(
    *,
    client: Any,
    prefix: str,
    bucket: str,
    columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    path = bucket_blob_path(prefix, bucket)
    raw = mdc.read_raw_bytes(path, client=client)
    if not raw:
        return pd.DataFrame(columns=columns or [])
    df = pd.read_parquet(BytesIO(raw))
    if columns:
        for col in columns:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[columns]
    return df


def read_active_bucket_parquet(
    *,
    client: Any,
    domain: str,
    bucket: str,
    columns: Optional[list[str]] = None,
) -> pd.DataFrame:
    return read_bucket_parquet(
        client=client,
        prefix=active_domain_prefix(domain),
        bucket=bucket,
        columns=columns,
    )


def write_symbol_index(
    *,
    domain: str,
    symbol_to_bucket: Dict[str, str],
    updated_at: Optional[datetime] = None,
) -> Optional[str]:
    if getattr(mdc, "common_storage_client", None) is None:
        return None

    ts = updated_at or datetime.now(timezone.utc)
    rows: list[dict[str, str]] = []
    for symbol, bucket in sorted(symbol_to_bucket.items()):
        rows.append(
            {
                "symbol": str(symbol).upper(),
                "bucket": str(bucket).upper(),
                "updated_at": ts.isoformat(),
            }
        )
    df = pd.DataFrame(rows, columns=["symbol", "bucket", "updated_at"])
    data = df.to_parquet(index=False, compression=alpha26_codec())
    path = f"system/bronze-index/{domain}/latest.parquet"
    mdc.store_raw_bytes(data, path, client=mdc.common_storage_client)
    return path


def load_symbol_index(domain: str) -> pd.DataFrame:
    if getattr(mdc, "common_storage_client", None) is None:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at"])
    path = f"system/bronze-index/{domain}/latest.parquet"
    raw = mdc.read_raw_bytes(path, client=mdc.common_storage_client)
    if not raw:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at"])
    try:
        df = pd.read_parquet(BytesIO(raw))
    except Exception:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at"])
    expected = ["symbol", "bucket", "updated_at"]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA
    return df[expected]


def load_symbol_set(domain: str) -> set[str]:
    df = load_symbol_index(domain)
    if df.empty:
        return set()
    return {str(sym).strip() for sym in df["symbol"].dropna().astype(str).tolist() if str(sym).strip()}
