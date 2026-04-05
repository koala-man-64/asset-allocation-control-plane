from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.config import parse_debug_symbols
from core.postgres import PostgresError
from core.runtime_config import (
    delete_runtime_config,
    default_scopes_by_precedence,
    get_effective_runtime_config,
    list_runtime_config,
    upsert_runtime_config,
)

logger = logging.getLogger(__name__)

_DEBUG_SYMBOLS_SCOPE = "global"
_DEBUG_SYMBOLS_KEY = "DEBUG_SYMBOLS"
_DEBUG_SYMBOLS_DESCRIPTION = (
    "Comma-separated allowlist of symbols applied when debug filtering is configured."
)


@dataclass(frozen=True)
class DebugSymbolsState:
    symbols_raw: str
    symbols: list[str]
    updated_at: Optional[datetime]
    updated_by: Optional[str]


def _normalize_symbols_text(value: object) -> str:
    symbols = parse_debug_symbols(value)
    return ",".join(symbols)


def _resolve_dsn(dsn: Optional[str]) -> Optional[str]:
    raw = dsn or os.environ.get("POSTGRES_DSN")
    value = (raw or "").strip()
    return value or None


def read_debug_symbols_state(dsn: Optional[str] = None) -> Optional[DebugSymbolsState]:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    rows = list_runtime_config(resolved, scopes=[_DEBUG_SYMBOLS_SCOPE], keys=[_DEBUG_SYMBOLS_KEY])
    if not rows:
        return None

    row = rows[0]
    symbols_raw = str(row.value or "")
    symbols = parse_debug_symbols(symbols_raw)
    return DebugSymbolsState(
        symbols_raw=symbols_raw,
        symbols=symbols,
        updated_at=row.updated_at,
        updated_by=row.updated_by,
    )


def replace_debug_symbols_state(
    *,
    dsn: Optional[str],
    symbols: object,
    actor: Optional[str] = None,
) -> DebugSymbolsState:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    normalized = _normalize_symbols_text(symbols)
    if not normalized:
        raise ValueError("DEBUG_SYMBOLS cannot be empty.")
    upsert_runtime_config(
        dsn=resolved,
        scope=_DEBUG_SYMBOLS_SCOPE,
        key=_DEBUG_SYMBOLS_KEY,
        value=normalized,
        description=_DEBUG_SYMBOLS_DESCRIPTION,
        actor=actor,
    )

    state = read_debug_symbols_state(resolved)
    if state is None:
        raise RuntimeError("Failed to persist DEBUG_SYMBOLS runtime config.")
    return state


def delete_debug_symbols_state(*, dsn: Optional[str]) -> bool:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")
    return delete_runtime_config(dsn=resolved, scope=_DEBUG_SYMBOLS_SCOPE, key=_DEBUG_SYMBOLS_KEY)


def refresh_debug_symbols_from_db(dsn: Optional[str] = None) -> list[str]:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        logger.warning("POSTGRES_DSN not set; using DEBUG_SYMBOLS from environment.")
        return _apply_debug_symbols_from_env()

    try:
        effective = get_effective_runtime_config(
            resolved,
            scopes_by_precedence=default_scopes_by_precedence(),
            keys=[_DEBUG_SYMBOLS_KEY],
        )
    except Exception as exc:
        logger.warning("Failed to load debug symbols from runtime config; using env fallback. (%s)", exc)
        return _apply_debug_symbols_from_env()

    item = effective.get(_DEBUG_SYMBOLS_KEY)
    symbols = parse_debug_symbols(item.value) if item else []
    _apply_debug_symbols_to_config(symbols)
    return symbols


def _apply_debug_symbols_from_env() -> list[str]:
    env_value = os.environ.get("DEBUG_SYMBOLS")
    symbols = parse_debug_symbols(env_value or "")
    _apply_debug_symbols_to_config(symbols)
    return symbols


def _apply_debug_symbols_to_config(symbols: list[str]) -> None:
    try:
        from core import config as cfg

        cfg.settings.DEBUG_SYMBOLS = list(symbols)
        cfg.DEBUG_SYMBOLS = list(symbols)
    except Exception as exc:
        logger.warning("Failed to update runtime DEBUG_SYMBOLS config: %s", exc)
