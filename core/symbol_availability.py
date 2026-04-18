from __future__ import annotations

"""Provider-scoped symbol availability used by Bronze scheduling.

Bronze jobs sync provider listings into Postgres, then schedule only the domain
subset returned by ``get_domain_symbols(domain)`` instead of the raw merged
symbol universe.
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Literal

import nasdaqdatalink
import pandas as pd

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.providers.alpha_vantage_gateway_client import AlphaVantageGatewayClient
from asset_allocation_runtime_common.providers.massive_gateway_client import MassiveGatewayClient
from asset_allocation_runtime_common.foundation.postgres import connect
DomainName = Literal["market", "finance", "earnings", "price-target"]
ProviderName = Literal["massive", "alpha_vantage", "nasdaq"]

DOMAIN_PROVIDER_MAP: dict[DomainName, ProviderName] = {
    "market": "massive",
    "finance": "massive",
    "earnings": "alpha_vantage",
    "price-target": "nasdaq",
}
DOMAIN_SOURCE_COLUMN_MAP: dict[DomainName, str] = {
    "market": "source_massive",
    "finance": "source_massive",
    "earnings": "source_alpha_vantage",
    "price-target": "source_nasdaq",
}
PROVIDER_SOURCE_COLUMN_MAP: dict[ProviderName, str] = {
    "massive": "source_massive",
    "alpha_vantage": "source_alpha_vantage",
    "nasdaq": "source_nasdaq",
}
_MASSIVE_PROVIDER_ALIASES = {
    "I:VIX": "^VIX",
    "I:VIX3M": "^VIX3M",
}
_MARKET_ALLOWED_ASSET_TYPES = frozenset({"STOCK", "ETF", "FUND", "CS", "ETS", "ETV", "ETN"})
_MARKET_REQUIRED_SYMBOLS = frozenset({"SPY", "^VIX", "^VIX3M"})
_ADVISORY_LOCK_KEYS: dict[str, tuple[int, int]] = {
    "source_massive": (11873, 42021),
    "source_alpha_vantage": (11873, 42022),
    "source_nasdaq": (11873, 42023),
}


@dataclass(frozen=True)
class SyncResult:
    provider: str
    source_column: str
    listed_count: int
    inserted_count: int
    disabled_count: int
    duration_ms: int
    lock_wait_ms: int


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_massive_symbol(value: object) -> str:
    normalized = _normalize_symbol(value)
    return _MASSIVE_PROVIDER_ALIASES.get(normalized, normalized)


def _normalize_asset_type(value: object) -> str:
    return str(value or "").strip().upper()


def _normalize_bool_series(series: pd.Series | object, *, index: pd.Index) -> pd.Series:
    if not isinstance(series, pd.Series):
        return pd.Series(False, index=index, dtype=bool)
    lowered = series.astype(str).str.strip().str.lower()
    mask = lowered.isin({"1", "true", "t", "yes", "y", "on"})
    return mask.fillna(False).astype(bool)


def get_symbol_availability_mask(
    df: pd.DataFrame,
    provider: Literal["massive", "alpha_vantage", "nasdaq"],
) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    index = df.index
    if provider == "alpha_vantage":
        return _normalize_bool_series(df.get("source_alpha_vantage", False), index=index)

    column = PROVIDER_SOURCE_COLUMN_MAP[provider]
    return _normalize_bool_series(df.get(column, False), index=index)


def _normalize_massive_records(records: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        symbol = _normalize_massive_symbol(
            record.get("Symbol") or record.get("symbol") or record.get("ticker")
        )
        if not symbol:
            continue
        rows.append(
            {
                "Symbol": symbol,
                "Name": record.get("Name") or record.get("name"),
                "Exchange": record.get("Exchange") or record.get("exchange") or record.get("primary_exchange"),
                "AssetType": record.get("AssetType") or record.get("asset_type") or record.get("type"),
                "source_massive": True,
            }
        )
    if not rows:
        return pd.DataFrame(columns=["Symbol", "Name", "Exchange", "AssetType", "source_massive"])
    out = pd.DataFrame(rows)
    out["Symbol"] = out["Symbol"].astype(str).str.strip().str.upper()
    out = out[out["Symbol"].ne("")]
    return out.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)


def _market_domain_eligibility_mask(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    index = df.index
    asset_type_series = df.get("AssetType")
    if isinstance(asset_type_series, pd.Series):
        asset_type_mask = asset_type_series.apply(_normalize_asset_type).isin(_MARKET_ALLOWED_ASSET_TYPES)
    else:
        asset_type_mask = pd.Series(False, index=index, dtype=bool)
    symbol_series = df.get("Symbol")
    if isinstance(symbol_series, pd.Series):
        required_symbol_mask = symbol_series.apply(_normalize_symbol).isin(_MARKET_REQUIRED_SYMBOLS)
    else:
        required_symbol_mask = pd.Series(False, index=index, dtype=bool)
    return (asset_type_mask | required_symbol_mask).fillna(False).astype(bool)


def _market_excluded_asset_type_breakdown(df: pd.DataFrame, *, eligible_mask: pd.Series) -> dict[str, int]:
    if df is None or df.empty:
        return {}
    excluded = df[~eligible_mask].copy()
    if excluded.empty:
        return {}
    symbol_series = excluded.get("Symbol")
    if isinstance(symbol_series, pd.Series):
        excluded = excluded[~symbol_series.apply(_normalize_symbol).isin(_MARKET_REQUIRED_SYMBOLS)].copy()
    if excluded.empty:
        return {}
    asset_type_series = excluded.get("AssetType")
    if isinstance(asset_type_series, pd.Series):
        normalized = asset_type_series.apply(_normalize_asset_type).replace("", "UNKNOWN").fillna("UNKNOWN")
    else:
        normalized = pd.Series("UNKNOWN", index=excluded.index, dtype="object")
    counts = normalized.value_counts()
    return {str(name): int(count) for name, count in counts.items()}


def _fetch_massive_symbols_df() -> pd.DataFrame:
    with MassiveGatewayClient.from_env() as client:
        records = client.get_tickers(market="stocks", locale="us", active=True)
    df = _normalize_massive_records(records)
    if df.empty:
        raise RuntimeError("Massive ticker sync returned no symbols.")
    return df


def _fetch_alpha_vantage_symbols_df() -> pd.DataFrame:
    with AlphaVantageGatewayClient.from_env() as client:
        csv_text = client.get_listing_status_csv(state="active")
    df = mdc._parse_alpha_vantage_listing_status_csv(str(csv_text))
    if df.empty:
        raise RuntimeError("Alpha Vantage listing-status sync returned no symbols.")
    df["source_alpha_vantage"] = True
    return df


def _fetch_price_target_symbols_df() -> pd.DataFrame:
    api_key = str(os.environ.get("NASDAQ_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("NASDAQ_API_KEY is required for Nasdaq price-target symbol sync.")
    nasdaqdatalink.ApiConfig.api_key = api_key
    df = nasdaqdatalink.get_table("ZACKS/TP", paginate=True, qopts={"columns": ["ticker"]})
    if df is None or df.empty or "ticker" not in df.columns:
        raise RuntimeError("Nasdaq price-target symbol sync returned no symbols.")
    out = pd.DataFrame({"Symbol": df["ticker"].astype(str).str.strip().str.upper()})
    out = out[out["Symbol"].ne("")]
    out = out.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    out["source_nasdaq"] = True
    if out.empty:
        raise RuntimeError("Nasdaq price-target symbol sync returned no symbols.")
    return out


def _fetch_provider_symbols_df(provider: ProviderName) -> pd.DataFrame:
    if provider == "massive":
        return _fetch_massive_symbols_df()
    if provider == "alpha_vantage":
        return _fetch_alpha_vantage_symbols_df()
    if provider == "nasdaq":
        return _fetch_price_target_symbols_df()
    raise ValueError(f"Unsupported provider={provider!r}")


def _provider_sync_payload(*, domain: DomainName, result: SyncResult) -> dict[str, object]:
    return {
        "symbol_availability": {
            "domain": domain,
            "provider": result.provider,
            "source_column": result.source_column,
            "listed_count": int(result.listed_count),
            "inserted_count": int(result.inserted_count),
            "disabled_count": int(result.disabled_count),
            "duration_ms": int(result.duration_ms),
            "lock_wait_ms": int(result.lock_wait_ms),
            "refreshed_at": pd.Timestamp.utcnow().isoformat(),
        }
    }


def _apply_availability_sync(cur, *, df_symbols: pd.DataFrame, source_column: str) -> tuple[int, int]:
    mdc._ensure_symbols_tables(cur)
    symbols = [
        _normalize_symbol(symbol)
        for symbol in df_symbols.get("Symbol", pd.Series(dtype="object")).tolist()
        if _normalize_symbol(symbol)
    ]
    symbols = list(dict.fromkeys(symbols))
    if not symbols:
        raise RuntimeError(f"Refusing to sync empty availability set for {source_column}.")

    cur.execute("CREATE TEMP TABLE tmp_symbol_availability (symbol TEXT PRIMARY KEY) ON COMMIT DROP;")
    cur.executemany(
        "INSERT INTO tmp_symbol_availability(symbol) VALUES (%s) ON CONFLICT DO NOTHING;",
        [(symbol,) for symbol in symbols],
    )
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM {mdc._SYMBOLS_TABLE} AS s
        INNER JOIN tmp_symbol_availability AS t
          ON t.symbol = s.symbol;
        """
    )
    row = cur.fetchone()
    existing_count = int(row[0] or 0) if row else 0

    mdc.upsert_symbols_to_db(df_symbols, cur=cur)

    set_false = f"{source_column} = FALSE, updated_at = now()"
    current_true = f"COALESCE(s.{source_column}, FALSE)"

    cur.execute(
        f"""
        UPDATE {mdc._SYMBOLS_TABLE} AS s
        SET {set_false}
        WHERE {current_true}
          AND NOT EXISTS (
              SELECT 1
              FROM tmp_symbol_availability AS t
              WHERE t.symbol = s.symbol
          );
        """
    )
    disabled_count = int(getattr(cur, "rowcount", 0) or 0)
    inserted_count = max(0, len(symbols) - existing_count)
    return inserted_count, disabled_count


def get_domain_symbols(domain: DomainName) -> pd.DataFrame:
    """Return the Postgres-backed symbol subset available for a Bronze domain."""
    provider = DOMAIN_PROVIDER_MAP[domain]
    df = mdc.get_symbols_from_db()
    if df is None:
        raise RuntimeError("Postgres symbol availability state is unavailable.")
    if df.empty:
        return df
    mask = get_symbol_availability_mask(df, provider)
    domain_df = df[mask].copy().reset_index(drop=True)
    if domain != "market" or domain_df.empty:
        return domain_df

    eligible_mask = _market_domain_eligibility_mask(domain_df)
    excluded_count = int((~eligible_mask).sum())
    if excluded_count:
        breakdown = _market_excluded_asset_type_breakdown(domain_df, eligible_mask=eligible_mask)
        breakdown_text = ", ".join(f"{asset_type}={count}" for asset_type, count in breakdown.items()) or "UNKNOWN=0"
        mdc.write_line(
            "Market domain eligibility filter excluded "
            f"{excluded_count} symbols by asset_type: {breakdown_text}"
        )
    return domain_df[eligible_mask].copy().reset_index(drop=True)


def sync_domain_availability(domain: DomainName) -> SyncResult:
    """Refresh the provider-scoped availability set for a Bronze domain."""
    provider = DOMAIN_PROVIDER_MAP[domain]
    source_column = DOMAIN_SOURCE_COLUMN_MAP[domain]
    dsn = str(os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise RuntimeError("POSTGRES_DSN is required for symbol availability sync.")

    started = time.perf_counter()
    df_symbols = _fetch_provider_symbols_df(provider)
    df_symbols = df_symbols.copy()
    listed_count = int(len(df_symbols))

    lock_key = _ADVISORY_LOCK_KEYS[source_column]
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            lock_started = time.perf_counter()
            cur.execute("SELECT pg_advisory_lock(%s, %s);", lock_key)
            lock_wait_ms = int((time.perf_counter() - lock_started) * 1000)
            try:
                inserted_count, disabled_count = _apply_availability_sync(
                    cur,
                    df_symbols=df_symbols,
                    source_column=source_column,
                )
                result = SyncResult(
                    provider=provider,
                    source_column=source_column,
                    listed_count=listed_count,
                    inserted_count=inserted_count,
                    disabled_count=disabled_count,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    lock_wait_ms=lock_wait_ms,
                )
                cur.execute(
                    f"""
                    INSERT INTO {mdc._SYMBOL_SYNC_STATE_TABLE}(id, last_refreshed_at, last_refreshed_sources, last_refresh_error)
                    VALUES (1, now(), %s, NULL)
                    ON CONFLICT (id) DO UPDATE
                    SET last_refreshed_at = EXCLUDED.last_refreshed_at,
                        last_refreshed_sources = EXCLUDED.last_refreshed_sources,
                        last_refresh_error = NULL;
                    """,
                    (json.dumps(_provider_sync_payload(domain=domain, result=result)),),
                )
                return result
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s, %s);", lock_key)
