from __future__ import annotations

import os
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from core import core as mdc
from core import bronze_bucketing


ALPHABET_BUCKETS: tuple[str, ...] = bronze_bucketing.ALPHABET_BUCKETS


def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def normalize_sub_domain(value: Optional[str]) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def silver_layout_mode() -> str:
    from core import config as cfg

    mode = (os.environ.get("SILVER_LAYOUT_MODE") or str(cfg.SILVER_LAYOUT_MODE)).strip().lower()
    if mode != "alpha26":
        raise ValueError("SILVER_LAYOUT_MODE must be 'alpha26' when set.")
    return mode


def is_silver_alpha26_mode() -> bool:
    silver_layout_mode()
    return True


def silver_alpha26_force_rebuild() -> bool:
    raw = os.environ.get("SILVER_ALPHA26_FORCE_REBUILD")
    if raw is None:
        return True
    return _is_truthy(raw)


def gold_layout_mode() -> str:
    from core import config as cfg

    mode = (os.environ.get("GOLD_LAYOUT_MODE") or str(cfg.GOLD_LAYOUT_MODE)).strip().lower()
    if mode != "alpha26":
        raise ValueError("GOLD_LAYOUT_MODE must be 'alpha26' when set.")
    return mode


def is_gold_alpha26_mode() -> bool:
    gold_layout_mode()
    return True


def bucket_letter(symbol: str) -> str:
    return bronze_bucketing.bucket_letter(symbol)


def silver_bucket_path(*, domain: str, bucket: str, finance_sub_domain: Optional[str] = None) -> str:
    b = str(bucket or "").strip().upper()
    d = str(domain or "").strip().lower().replace("_", "-")
    if b not in ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket {bucket!r}.")
    if d == "market":
        return f"market-data/buckets/{b}"
    if d == "earnings":
        return "earnings-data/buckets/{bucket}".format(bucket=b)
    if d == "price-target":
        return f"price-target-data/buckets/{b}"
    if d == "finance":
        sub = str(finance_sub_domain or "").strip().lower().replace("-", "_")
        if not sub:
            raise ValueError("finance_sub_domain is required for silver finance buckets.")
        return f"finance-data/{sub}/buckets/{b}"
    raise ValueError(f"Unsupported silver bucket domain={domain!r}")


def gold_bucket_path(*, domain: str, bucket: str, finance_sub_domain: Optional[str] = None) -> str:
    b = str(bucket or "").strip().upper()
    d = str(domain or "").strip().lower().replace("_", "-")
    if b not in ALPHABET_BUCKETS:
        raise ValueError(f"Invalid bucket {bucket!r}.")
    if d == "market":
        return f"market/buckets/{b}"
    if d == "earnings":
        return f"earnings/buckets/{b}"
    if d == "price-target":
        return f"targets/buckets/{b}"
    if d == "finance":
        sub = str(finance_sub_domain or "").strip().lower().replace("-", "_")
        if not sub:
            raise ValueError("finance_sub_domain is required for gold finance buckets.")
        return f"finance/{sub}/buckets/{b}"
    raise ValueError(f"Unsupported gold bucket domain={domain!r}")


def all_silver_bucket_paths(*, domain: str, finance_sub_domain: Optional[str] = None) -> list[str]:
    return [silver_bucket_path(domain=domain, bucket=b, finance_sub_domain=finance_sub_domain) for b in ALPHABET_BUCKETS]


def all_gold_bucket_paths(*, domain: str, finance_sub_domain: Optional[str] = None) -> list[str]:
    return [
        gold_bucket_path(domain=domain, bucket=b, finance_sub_domain=finance_sub_domain)
        for b in ALPHABET_BUCKETS
    ]


def _index_path(*, layer: str, domain: str) -> str:
    clean_layer = str(layer or "").strip().lower()
    clean_domain = str(domain or "").strip().lower().replace("_", "-")
    return f"system/{clean_layer}-index/{clean_domain}/latest.parquet"


def write_layer_symbol_index(
    *,
    layer: str,
    domain: str,
    symbol_to_bucket: dict[str, str],
    sub_domain: Optional[str] = None,
    updated_at: Optional[datetime] = None,
) -> Optional[str]:
    if getattr(mdc, "common_storage_client", None) is None:
        return None
    ts = updated_at or datetime.now(timezone.utc)
    rows: list[dict[str, str]] = []
    clean_sub_domain = normalize_sub_domain(sub_domain)
    for symbol, bucket in sorted(symbol_to_bucket.items()):
        row = {
            "symbol": str(symbol).strip().upper(),
            "bucket": str(bucket).strip().upper(),
            "updated_at": ts.isoformat(),
        }
        if clean_sub_domain:
            row["sub_domain"] = clean_sub_domain
        rows.append(row)
    cols = ["symbol", "bucket", "updated_at", "sub_domain"]
    df = pd.DataFrame(rows, columns=cols)
    existing = load_layer_symbol_index(layer=layer, domain=domain)
    if not existing.empty:
        existing_sub = (
            existing["sub_domain"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("-", "_", regex=False)
        )
        if clean_sub_domain:
            preserved = existing[existing_sub != clean_sub_domain].copy()
            df = pd.concat([preserved[cols], df], ignore_index=True)
        else:
            # Preserve sub-domain rows when refreshing the root index so finance can
            # safely rewrite aggregate rows without clobbering untouched sub-domains.
            preserved = existing[existing_sub != ""].copy()
            if not preserved.empty:
                df = pd.concat([preserved[cols], df], ignore_index=True)

    if not df.empty:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
        df["bucket"] = df["bucket"].astype(str).str.strip().str.upper()
        df["sub_domain"] = (
            df["sub_domain"].fillna("").astype(str).str.strip().str.lower().str.replace("-", "_", regex=False)
        )
        df.loc[df["sub_domain"] == "", "sub_domain"] = pd.NA
        df = df.drop_duplicates(subset=["symbol", "sub_domain"], keep="last").reset_index(drop=True)

    payload = df.to_parquet(index=False, compression=bronze_bucketing.alpha26_codec())
    path = _index_path(layer=layer, domain=domain)
    mdc.store_raw_bytes(payload, path, client=mdc.common_storage_client)
    return path


def load_layer_symbol_index(*, layer: str, domain: str) -> pd.DataFrame:
    path = _index_path(layer=layer, domain=domain)
    if getattr(mdc, "common_storage_client", None) is None:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at", "sub_domain"])
    raw = mdc.read_raw_bytes(path, client=mdc.common_storage_client)
    if not raw:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at", "sub_domain"])
    try:
        df = pd.read_parquet(BytesIO(raw))
    except Exception:
        return pd.DataFrame(columns=["symbol", "bucket", "updated_at", "sub_domain"])
    expected = ["symbol", "bucket", "updated_at", "sub_domain"]
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA
    return df[expected]


def load_layer_symbol_set(*, layer: str, domain: str, sub_domain: Optional[str] = None) -> set[str]:
    df = load_layer_symbol_index(layer=layer, domain=domain)
    if df.empty:
        return set()
    clean_sub_domain = normalize_sub_domain(sub_domain)
    if clean_sub_domain and "sub_domain" in df.columns:
        normalized = (
            df["sub_domain"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("-", "_", regex=False)
        )
        df = df[normalized == clean_sub_domain]
    elif "sub_domain" in df.columns:
        normalized = (
            df["sub_domain"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace("-", "_", regex=False)
        )
        df = df[normalized == ""]
    return {
        str(value).strip().upper()
        for value in df["symbol"].dropna().astype(str).tolist()
        if str(value).strip()
    }


def load_layer_symbol_to_bucket_map(
    *,
    layer: str,
    domain: str,
    sub_domain: Optional[str] = None,
) -> dict[str, str]:
    out: dict[str, str] = {}
    existing = load_layer_symbol_index(layer=layer, domain=domain)
    if existing is None or existing.empty:
        return out
    if "symbol" not in existing.columns or "bucket" not in existing.columns:
        return out
    if "sub_domain" not in existing.columns:
        existing = existing.copy()
        existing["sub_domain"] = pd.NA

    normalized_sub = (
        existing["sub_domain"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace("-", "_", regex=False)
    )
    target_sub_domain = normalize_sub_domain(sub_domain)
    if target_sub_domain:
        existing = existing[normalized_sub == target_sub_domain]
    else:
        existing = existing[normalized_sub == ""]

    valid_buckets = set(ALPHABET_BUCKETS)
    for _, row in existing.iterrows():
        symbol = str(row.get("symbol") or "").strip().upper()
        bucket = str(row.get("bucket") or "").strip().upper()
        if not symbol or bucket not in valid_buckets:
            continue
        out[symbol] = bucket
    return out


def merge_symbol_to_bucket_map(
    existing: dict[str, str],
    *,
    touched_buckets: set[str],
    touched_symbol_to_bucket: dict[str, str],
) -> dict[str, str]:
    out = {
        symbol: bucket
        for symbol, bucket in existing.items()
        if bucket not in touched_buckets
    }
    out.update(touched_symbol_to_bucket)
    return out


def count_staged_frame_rows(bucket_frames: Mapping[Any, Sequence[pd.DataFrame]]) -> int:
    total_rows = 0
    for parts in bucket_frames.values():
        for frame in parts:
            if frame is not None:
                total_rows += int(len(frame))
    return total_rows
