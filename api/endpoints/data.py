from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, Optional, Sequence

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request
from asset_allocation_runtime_common.foundation.blob_storage import BlobStorageClient
from api.service.dependencies import get_settings, validate_auth
from asset_allocation_runtime_common.market_data import layer_bucketing
from asset_allocation_runtime_common.market_data.delta_core import load_delta
from asset_allocation_runtime_common.market_data.pipeline import DataPaths
from asset_allocation_runtime_common.foundation.postgres import PostgresError, connect
from asset_allocation_runtime_common.domain.regime import DEFAULT_REGIME_MODEL_NAME
from core.regime_repository import RegimeRepository

from ..data_service import DataService
from api.service.validation_service import ValidationService

router = APIRouter()
logger = logging.getLogger("asset-allocation.api.data")
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
_STORAGE_USAGE_LIMIT_DEFAULT = 200_000
_STORAGE_USAGE_LIMIT_MAX = 2_000_000

_STORAGE_USAGE_CATALOG = (
    (
        "bronze",
        "AZURE_CONTAINER_BRONZE",
        "Bronze",
        (
            "market-data",
            "finance-data",
            "earnings-data",
            "price-target-data",
            "quiver-data",
        ),
    ),
    (
        "silver",
        "AZURE_CONTAINER_SILVER",
        "Silver",
        (
            "market-data",
            "finance-data",
            "earnings-data",
            "price-target-data",
            "quiver-data",
        ),
    ),
    (
        "gold",
        "AZURE_CONTAINER_GOLD",
        "Gold",
        (
            "market",
            "finance",
            "earnings",
            "targets",
            "quiver",
        ),
    ),
    (
        "platinum",
        "AZURE_CONTAINER_PLATINUM",
        "Platinum",
        ("platinum",),
    ),
)


def _storage_usage_scan_limit(default: int = _STORAGE_USAGE_LIMIT_DEFAULT) -> int:
    raw = (os.environ.get("DATA_USAGE_SCAN_LIMIT") or "").strip() or os.environ.get(
        "DOMAIN_METADATA_MAX_SCANNED_BLOBS",
        str(default),
    ).strip()

    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return default
    if value > _STORAGE_USAGE_LIMIT_MAX:
        return _STORAGE_USAGE_LIMIT_MAX
    return value


def _ensure_folder_prefix(prefix: str) -> str:
    normalized = str(prefix or "").strip().strip("/")
    if not normalized:
        return ""
    return f"{normalized}/"


def _summarize_container_prefix(
    *,
    client: BlobStorageClient,
    prefix: Optional[str],
    scan_limit: int,
) -> Dict[str, Optional[int] | bool | str]:
    file_count = 0
    total_bytes = 0
    scanned = 0
    truncated = False
    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            scanned += 1
            if scanned > scan_limit:
                truncated = True
                break
            file_count += 1
            blob_size = getattr(blob, "size", None)
            if isinstance(blob_size, int):
                total_bytes += blob_size
        return {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "truncated": truncated,
            "error": None,
        }
    except Exception as exc:
        return {
            "file_count": None,
            "total_bytes": None,
            "truncated": False,
            "error": str(exc),
        }


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _resolve_postgres_dsn(request: Request) -> Optional[str]:
    """
    Prefer POSTGRES_DSN when present (core scripts), otherwise fall back to POSTGRES_DSN.

    Normalizes SQLAlchemy-style DSNs (postgresql+asyncpg://...) to psycopg-friendly (postgresql://...).
    """
    raw = os.environ.get("POSTGRES_DSN")
    dsn = _strip_or_none(raw) or _strip_or_none(get_settings(request).postgres_dsn)
    if not dsn:
        return None
    if dsn.startswith("postgresql+asyncpg://"):
        return "postgresql://" + dsn.removeprefix("postgresql+asyncpg://")
    return dsn


def _parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date={value!r} (expected YYYY-MM-DD).") from exc


def _first_present(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    existing = {str(c): True for c in columns}
    for name in candidates:
        if name in existing:
            return name
    return None


def _safe_numeric(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return float(int(value))
        return float(value)
    except (TypeError, ValueError):
        return None


def _validate_ticker(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().upper()
    if not normalized:
        return None
    if not _TICKER_RE.fullmatch(normalized):
        raise HTTPException(
            status_code=400,
            detail="Invalid ticker format. Expected pattern: ^[A-Z][A-Z0-9.-]{0,9}$",
        )
    return normalized


@router.get("/storage-usage")
def get_storage_usage(
    request: Request,
    scan_limit: Optional[int] = Query(
        default=None,
        ge=1,
        le=_STORAGE_USAGE_LIMIT_MAX,
        description="Limit blobs scanned per container/prefix to avoid runaway reads.",
    ),
) -> Dict[str, Any]:
    validate_auth(request)
    resolved_scan_limit = scan_limit if scan_limit is not None else _storage_usage_scan_limit()

    containers = []
    for layer_name, container_env, layer_label, folder_paths in _STORAGE_USAGE_CATALOG:
        container = (os.environ.get(container_env) or "").strip()
        if not container:
            containers.append(
                {
                    "layer": layer_name,
                    "layerLabel": layer_label,
                    "container": "",
                    "totalFiles": None,
                    "totalBytes": None,
                    "truncated": False,
                    "error": f"Missing container env var: {container_env}",
                    "folders": [],
                }
            )
            continue

        try:
            client = BlobStorageClient(container_name=container, ensure_container_exists=False)
        except Exception as exc:
            containers.append(
                {
                    "layer": layer_name,
                    "layerLabel": layer_label,
                    "container": container,
                    "totalFiles": None,
                    "totalBytes": None,
                    "truncated": False,
                    "error": f"Storage client init failed: {exc}",
                    "folders": [],
                }
            )
            continue

        container_summary = _summarize_container_prefix(
            client=client,
            prefix=None,
            scan_limit=resolved_scan_limit,
        )
        folder_payloads = []
        for folder_path in folder_paths:
            normalized_prefix = _ensure_folder_prefix(folder_path)
            folder_summary = _summarize_container_prefix(
                client=client,
                prefix=normalized_prefix,
                scan_limit=resolved_scan_limit,
            )
            folder_payloads.append(
                {
                    "path": normalized_prefix,
                    "fileCount": folder_summary["file_count"],
                    "totalBytes": folder_summary["total_bytes"],
                    "truncated": folder_summary["truncated"],
                    "error": folder_summary["error"],
                }
            )

        containers.append(
            {
                "layer": layer_name,
                "layerLabel": layer_label,
                "container": container,
                "totalFiles": container_summary["file_count"],
                "totalBytes": container_summary["total_bytes"],
                "truncated": container_summary["truncated"],
                "error": container_summary["error"],
                "folders": folder_payloads,
            }
        )

    return {
        "generatedAt": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "scanLimit": resolved_scan_limit,
        "containers": containers,
    }


def _find_latest_market_date(
    *,
    gold_container: str,
    symbols: Sequence[str],
    max_symbols: int = 300,
) -> Optional[date]:
    latest: Optional[date] = None
    layer_bucketing.gold_layout_mode()
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        path = DataPaths.get_gold_market_bucket_path(bucket)
        try:
            df = load_delta(gold_container, path, columns=["date"])
        except Exception:
            continue
        if df is None or df.empty:
            continue
        date_col = _first_present(df.columns.tolist(), ["date", "Date"])
        if not date_col:
            continue
        values = pd.to_datetime(df[date_col], errors="coerce").dropna()
        if values.empty:
            continue
        candidate = values.max().date()
        if latest is None or candidate > latest:
            latest = candidate
    return latest


def _query_symbols(conn, *, q: Optional[str] = None) -> pd.DataFrame:  # type: ignore[no-untyped-def]
    query = """
        SELECT
            symbol,
            name,
            sector,
            industry,
            country,
            COALESCE(
                is_optionable,
                CASE
                    WHEN upper(trim(COALESCE(optionable, ''))) IN ('Y', 'YES', 'TRUE', 'T', '1') THEN TRUE
                    WHEN upper(trim(COALESCE(optionable, ''))) IN ('N', 'NO', 'FALSE', 'F', '0') THEN FALSE
                    ELSE NULL
                END
            ) AS is_optionable
        FROM core.symbols
        ORDER BY symbol
    """
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df = df[~df["symbol"].str.contains(r"\.", na=False)]
    if q:
        needle = str(q).strip().upper()
        if needle:
            name_col = "name" if "name" in df.columns else None
            if name_col:
                mask = df["symbol"].str.contains(needle, na=False) | df[name_col].astype(str).str.upper().str.contains(needle, na=False)
            else:
                mask = df["symbol"].str.contains(needle, na=False)
            df = df[mask]
    return df.reset_index(drop=True)


@router.get("/symbols")
def list_symbols(
    request: Request,
    q: Optional[str] = Query(default=None, description="Search string (symbol/name)"),
    limit: int = Query(default=5000, ge=1, le=20000),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """
    Returns the symbol universe from Postgres (`core.symbols`).
    """
    validate_auth(request)
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN or POSTGRES_DSN).")

    try:
        with connect(dsn) as conn:
            df = _query_symbols(conn, q=q)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Symbols unavailable: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Symbols query failed: {exc}") from exc

    total = int(len(df))
    page = df.iloc[int(offset) : int(offset) + int(limit)].copy()
    page = page.rename(columns={"is_optionable": "isOptionable"})
    page = page.where(pd.notnull(page), None)

    return {
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "symbols": page.to_dict(orient="records"),
    }


@router.get("/screener")
def get_stock_screener(
    request: Request,
    q: Optional[str] = Query(default=None, description="Search string (symbol/name)"),
    limit: int = Query(default=250, ge=1, le=2000),
    offset: int = Query(default=0, ge=0),
    as_of: Optional[str] = Query(default=None, description="As-of date (YYYY-MM-DD). Defaults to latest available."),
    sort: str = Query(
        default="volume",
        description="Sort key: symbol|close|volume|return_1d|return_5d|vol_20d|drawdown_1y|atr_14d|compression_score",
    ),
    direction: str = Query(default="desc", description="Sort direction: asc|desc"),
) -> Dict[str, Any]:
    """
    Daily stock screener snapshot combining:
      - Silver: latest OHLCV for the as-of date.
      - Gold: engineered features for the as-of date.
      - Postgres: symbol metadata (core.symbols).
    """
    validate_auth(request)

    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN or POSTGRES_DSN).")

    gold_container = os.environ.get("AZURE_CONTAINER_GOLD") or os.environ.get("AZURE_FOLDER_MARKET") or ""
    silver_container = os.environ.get("AZURE_CONTAINER_SILVER") or ""
    gold_container = gold_container.strip()
    silver_container = silver_container.strip()
    if not (gold_container and silver_container):
        raise HTTPException(status_code=503, detail="Storage containers are not configured (AZURE_CONTAINER_SILVER/AZURE_CONTAINER_GOLD).")

    try:
        with connect(dsn) as conn:
            symbols_df = _query_symbols(conn, q=q)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Symbols unavailable: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Symbols query failed: {exc}") from exc

    symbol_list = symbols_df["symbol"].astype(str).str.upper().tolist() if not symbols_df.empty else []
    requested = _parse_iso_date(as_of)
    resolved_date = requested or _find_latest_market_date(gold_container=gold_container, symbols=symbol_list)
    if resolved_date is None:
        raise HTTPException(status_code=503, detail="No Gold market feature data found for recent dates.")

    resolved_dt = datetime(resolved_date.year, resolved_date.month, resolved_date.day)

    gold_cols = [
        "return_1d",
        "return_5d",
        "vol_20d",
        "drawdown_1y",
        "atr_14d",
        "gap_atr",
        "sma_50d",
        "sma_200d",
        "trend_50_200",
        "above_sma_50",
        "bb_width_20d",
        "compression_score",
        "volume_z_20d",
        "volume_pct_rank_252d",
    ]
    layer_bucketing.gold_layout_mode()
    layer_bucketing.silver_layout_mode()

    gold_frames: list[pd.DataFrame] = []
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        gold_path = DataPaths.get_gold_market_bucket_path(bucket)
        try:
            gold_src = load_delta(gold_container, gold_path, columns=["date", "symbol", *gold_cols])
        except Exception:
            continue
        if gold_src is None or gold_src.empty:
            continue
        gdf = gold_src.copy()
        g_date_col = _first_present(gdf.columns.tolist(), ["date", "Date"])
        g_symbol_col = _first_present(gdf.columns.tolist(), ["symbol", "Symbol"])
        if not g_date_col or not g_symbol_col:
            continue
        gdf[g_date_col] = pd.to_datetime(gdf[g_date_col], errors="coerce").dt.normalize()
        gdf = gdf[gdf[g_date_col] == resolved_dt]
        if gdf.empty:
            continue
        if g_symbol_col != "symbol":
            gdf = gdf.rename(columns={g_symbol_col: "symbol"})
        gdf["symbol"] = gdf["symbol"].astype(str).str.upper()
        gold_frames.append(gdf)

    silver_frames: list[pd.DataFrame] = []
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        silver_path = DataPaths.get_silver_market_bucket_path(bucket)
        try:
            silver_src = load_delta(
                silver_container,
                silver_path,
                columns=["Date", "date", "Symbol", "symbol", "Open", "High", "Low", "Close", "Volume"],
            )
        except Exception:
            continue
        if silver_src is None or silver_src.empty:
            continue
        sdf = silver_src.copy()
        s_date_col = _first_present(sdf.columns.tolist(), ["Date", "date"])
        s_symbol_col = _first_present(sdf.columns.tolist(), ["symbol", "Symbol"])
        if not s_date_col or not s_symbol_col:
            continue
        sdf[s_date_col] = pd.to_datetime(sdf[s_date_col], errors="coerce").dt.normalize()
        sdf = sdf[sdf[s_date_col] == resolved_dt]
        if sdf.empty:
            continue
        if s_symbol_col != "symbol":
            sdf = sdf.rename(columns={s_symbol_col: "symbol"})
        sdf["symbol"] = sdf["symbol"].astype(str).str.upper()
        silver_frames.append(sdf)

    gold_df = pd.concat(gold_frames, ignore_index=True) if gold_frames else pd.DataFrame()
    silver_df = pd.concat(silver_frames, ignore_index=True) if silver_frames else pd.DataFrame()

    if gold_df.empty:
        raise HTTPException(status_code=503, detail=f"Gold market features unavailable for {resolved_date.isoformat()}.")
    if silver_df.empty:
        raise HTTPException(status_code=503, detail=f"Silver market data unavailable for {resolved_date.isoformat()}.")

    gold_df["symbol"] = gold_df["symbol"].astype(str).str.upper()
    gold_df = gold_df.sort_values(["symbol"]).drop_duplicates(subset=["symbol"], keep="last")
    silver_symbol_col = _first_present(silver_df.columns.tolist(), ["symbol", "Symbol"])
    if not silver_symbol_col:
        raise HTTPException(status_code=500, detail="Silver market schema missing Symbol/date columns.")
    silver_df = silver_df.rename(
        columns={
            silver_symbol_col: "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    silver_df["symbol"] = silver_df["symbol"].astype(str).str.upper()
    silver_df = silver_df.sort_values(["symbol"]).drop_duplicates(subset=["symbol"], keep="last")

    merged = symbols_df.merge(silver_df[["symbol", "open", "high", "low", "close", "volume"]], on="symbol", how="left")
    merged = merged.merge(gold_df[["symbol", *gold_cols]], on="symbol", how="left", suffixes=("", "_gold"))

    merged["has_silver"] = merged["close"].notna().astype(int)
    merged["has_gold"] = merged["return_1d"].notna().astype(int)

    sort_key = str(sort or "").strip()
    allowed_sorts = {
        "symbol": "symbol",
        "close": "close",
        "volume": "volume",
        "return_1d": "return_1d",
        "return_5d": "return_5d",
        "vol_20d": "vol_20d",
        "drawdown_1y": "drawdown_1y",
        "atr_14d": "atr_14d",
        "compression_score": "compression_score",
    }
    col = allowed_sorts.get(sort_key, "volume")
    ascending = str(direction or "").strip().lower() == "asc"

    merged = merged.sort_values(by=[col, "symbol"], ascending=[ascending, True], na_position="last")

    total = int(len(merged))
    page = merged.iloc[int(offset) : int(offset) + int(limit)].copy()

    # JSON-safe conversion.
    page = page.rename(
        columns={
            "is_optionable": "isOptionable",
            "return_1d": "return1d",
            "return_5d": "return5d",
            "vol_20d": "vol20d",
            "drawdown_1y": "drawdown1y",
            "atr_14d": "atr14d",
            "gap_atr": "gapAtr",
            "sma_50d": "sma50d",
            "sma_200d": "sma200d",
            "trend_50_200": "trend50_200",
            "above_sma_50": "aboveSma50",
            "bb_width_20d": "bbWidth20d",
            "compression_score": "compressionScore",
            "volume_z_20d": "volumeZ20d",
            "volume_pct_rank_252d": "volumePctRank252d",
            "has_silver": "hasSilver",
            "has_gold": "hasGold",
        }
    )
    page = page.where(pd.notnull(page), None)

    return {
        "asOf": resolved_date.isoformat(),
        "total": total,
        "limit": int(limit),
        "offset": int(offset),
        "rows": page.to_dict(orient="records"),
    }


@router.get("/adls/tree")
def get_adls_tree(
    request: Request,
    layer: str = Query(default="gold", description="Storage layer: bronze|silver|gold|platinum"),
    path: Optional[str] = Query(default=None, description="Folder path prefix to list."),
    max_entries: int = Query(default=5000, ge=1, le=100000, description="Max blobs scanned while listing."),
) -> Dict[str, Any]:
    """List one ADLS hierarchy level (folders + files) for the given layer/path."""
    validate_auth(request)
    try:
        return DataService.list_adls_tree(
            layer=layer,
            path=path,
            max_entries=max_entries,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/adls/file-preview")
def get_adls_file_preview(
    request: Request,
    layer: str = Query(default="gold", description="Storage layer: bronze|silver|gold|platinum"),
    path: str = Query(..., description="Blob path to preview."),
    max_bytes: int = Query(default=262144, ge=1024, le=1048576, description="Max bytes returned in preview."),
    max_delta_files: int = Query(default=0, ge=0, le=99, description="Max Delta log files processed when deriving a Delta preview."),
) -> Dict[str, Any]:
    """Return plaintext preview for an ADLS blob when content is text-like."""
    validate_auth(request)
    try:
        return DataService.get_adls_file_preview(
            layer=layer,
            path=path,
            max_bytes=max_bytes,
            max_delta_files=max_delta_files,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{layer}/profile")
def get_column_profile(
    layer: str,
    request: Request,
    domain: str = Query(default="", description="Domain name (market, finance, earnings, price-target, finance/balance_sheet, ...)"),
    column: str = Query(default="", description="Column to profile"),
    bins: int = Query(default=20, ge=3, le=200, description="Histogram bucket count"),
    sample_rows: int = Query(default=10000, ge=10, le=100000, description="Rows to sample"),
    top_values: int = Query(default=20, ge=1, le=200, description="Top string values to return"),
    ticker: Optional[str] = Query(default=None, description="Ticker filter when supported"),
):
    """
    Compute column profile for a selected dataset:
    - numeric: buckets for distribution (histogram-like)
    - date: monthly buckets
    - string: unique/total/duplicate counts plus top values
    """
    request_id = request.headers.get("x-request-id", "")
    ticker_normalized = _validate_ticker(ticker)
    logger.info(
        "Column profile request: layer=%s domain=%s column=%s bins=%s sample_rows=%s request_id=%s",
        layer,
        domain,
        column,
        bins,
        sample_rows,
        request_id or "-",
    )
    validate_auth(request)

    if layer not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be 'bronze', 'silver', or 'gold'.")
    if not str(domain or "").strip():
        raise HTTPException(status_code=400, detail="Missing required domain.")
    if not str(column or "").strip():
        raise HTTPException(status_code=400, detail="Missing required column.")

    try:
        return DataService.get_column_profile(
            layer=layer,
            domain=domain,
            column=column,
            ticker=ticker_normalized,
            bins=bins,
            sample_rows=sample_rows,
            top_values=top_values,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{layer}/{domain}")
def get_data_generic(
    layer: str,
    domain: str,
    request: Request,
    ticker: Optional[str] = None,
    limit: Optional[int] = Query(default=None, ge=1, le=10000, description="Max rows to return"),
    date_sort: Optional[str] = Query(
        default=None,
        description="Optional date sort direction: asc|desc",
    ),
):
    """
    Generic endpoint for retrieving data from Bronze/Silver/Gold layers.
    Delegates to DataService for logic.
    """
    # Validation
    request_id = request.headers.get("x-request-id", "")
    ticker_normalized = _validate_ticker(ticker)
    logger.info(
        "Data generic request: layer=%s domain=%s ticker=%s date_sort=%s request_id=%s",
        layer,
        domain,
        ticker_normalized or "-",
        (str(date_sort).strip().lower() if date_sort is not None else "-"),
        request_id or "-",
    )
    validate_auth(request)
    if layer not in ["bronze", "silver", "gold"]:
        raise HTTPException(
            status_code=400,
            detail="Layer must be 'bronze', 'silver', or 'gold'.",
        )

    normalized_date_sort: Optional[str] = None
    if date_sort is not None:
        normalized_date_sort = str(date_sort).strip().lower()
        if normalized_date_sort not in {"asc", "desc"}:
            raise HTTPException(status_code=400, detail="date_sort must be 'asc' or 'desc'.")

    try:
        return DataService.get_data(
            layer,
            domain,
            ticker_normalized,
            limit=limit,
            sort_by_date=normalized_date_sort,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _load_regime_dataset_rows(
    *,
    request: Request,
    dataset: str,
    model_name: str,
    model_version: int | None,
    start_date: date | None,
    end_date: date | None,
    limit: int,
) -> list[dict[str, Any]]:
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for regime datasets.")
    repo = RegimeRepository(dsn)
    dataset_key = str(dataset or "").strip().lower()
    if dataset_key == "inputs":
        return repo.list_regime_inputs(start_date=start_date, end_date=end_date, limit=limit)
    if dataset_key == "history":
        return repo.list_regime_history(
            model_name=model_name,
            model_version=model_version,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    if dataset_key == "latest":
        latest = repo.get_regime_latest(model_name=model_name, model_version=model_version)
        return [latest] if latest else []
    if dataset_key == "transitions":
        return repo.list_regime_transitions(
            model_name=model_name,
            model_version=model_version,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    raise HTTPException(status_code=400, detail="dataset must be one of: inputs, history, latest, transitions")


@router.get("/gold/regime/{dataset}")
def get_gold_regime_dataset(
    dataset: str,
    request: Request,
    model_name: str = Query(default=DEFAULT_REGIME_MODEL_NAME, alias="modelName"),
    model_version: int | None = Query(default=None, alias="modelVersion", ge=1),
    start_date: date | None = Query(default=None, alias="startDate"),
    end_date: date | None = Query(default=None, alias="endDate"),
    limit: int = Query(default=500, ge=1, le=5000),
) -> list[dict[str, Any]]:
    validate_auth(request)
    return _load_regime_dataset_rows(
        request=request,
        dataset=dataset,
        model_name=model_name,
        model_version=model_version,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )


@router.get("/gold/regime/{dataset}/profile")
def get_gold_regime_dataset_profile(
    dataset: str,
    request: Request,
    column: str = Query(..., min_length=1),
    model_name: str = Query(default=DEFAULT_REGIME_MODEL_NAME, alias="modelName"),
    model_version: int | None = Query(default=None, alias="modelVersion", ge=1),
    start_date: date | None = Query(default=None, alias="startDate"),
    end_date: date | None = Query(default=None, alias="endDate"),
    bins: int = Query(default=20, ge=3, le=200),
    sample_rows: int = Query(default=10000, alias="sampleRows", ge=10, le=100000),
    top_values: int = Query(default=20, alias="topValues", ge=1, le=200),
) -> dict[str, Any]:
    validate_auth(request)
    rows = _load_regime_dataset_rows(
        request=request,
        dataset=dataset,
        model_name=model_name,
        model_version=model_version,
        start_date=start_date,
        end_date=end_date,
        limit=sample_rows,
    )
    return DataService.build_column_profile_from_rows(
        rows,
        layer="gold",
        domain=f"regime/{dataset}",
        column=column,
        bins=bins,
        sample_rows=sample_rows,
        top_values=top_values,
    )


@router.get("/{layer}/finance/{sub_domain}")
def get_finance_data(
    layer: str,
    sub_domain: str,
    request: Request,
    ticker: Optional[str] = Query(default=None, description="Ticker (required for Silver/Gold; optional for Bronze)"),
    limit: Optional[int] = Query(default=None, ge=1, le=10000, description="Max rows to return"),
):
    """
    Specialized endpoint for Finance data.
    """
    request_id = request.headers.get("x-request-id", "")
    ticker_normalized = _validate_ticker(ticker)
    logger.info(
        "Finance data request: layer=%s sub_domain=%s ticker=%s request_id=%s",
        layer,
        sub_domain,
        ticker_normalized or "-",
        request_id or "-",
    )
    validate_auth(request)
    if layer not in ["bronze", "silver", "gold"]:
        raise HTTPException(status_code=400, detail="Layer must be 'bronze', 'silver', or 'gold'")

    try:
        if limit is None:
            return DataService.get_finance_data(layer, sub_domain, ticker_normalized)
        return DataService.get_finance_data(layer, sub_domain, ticker_normalized, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quality/{layer}/{domain}/validation")
def get_validation_report(
    layer: str,
    domain: str,
    request: Request,
    ticker: Optional[str] = Query(default=None),
):
    """
    Returns a data quality validation report for the specified layer and domain.
    Computed on-demand by ValidationService.
    """
    request_id = request.headers.get("x-request-id", "")
    ticker_normalized = _validate_ticker(ticker)
    logger.info(
        "Validation report request: layer=%s domain=%s ticker=%s request_id=%s",
        layer,
        domain,
        ticker_normalized or "-",
        request_id or "-"
    )
    validate_auth(request)
    
    # Basic validation of inputs
    if layer not in ["bronze", "silver", "gold"]:
         raise HTTPException(status_code=400, detail="Layer must be 'bronze', 'silver', or 'gold'")

    try:
        report = ValidationService.get_validation_report(layer, domain, ticker_normalized)
        return report
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Validation report failed")
        raise HTTPException(status_code=500, detail=str(e))
