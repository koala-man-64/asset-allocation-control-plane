from __future__ import annotations

import os
from typing import Any, Iterable, Optional, Sequence

import pandas as pd


class PostgresError(RuntimeError):
    pass


def get_dsn(env_var: str) -> Optional[str]:
    raw = os.environ.get(env_var)
    if not raw:
        return None
    value = str(raw).strip()
    return value or None


def _import_psycopg():
    try:
        import psycopg  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise PostgresError(
            "psycopg is required for Postgres features. Install dependencies from requirements.txt."
        ) from exc
    return psycopg


def connect(dsn: str):
    psycopg = _import_psycopg()
    return psycopg.connect(dsn)


def require_columns(df: pd.DataFrame, required: Sequence[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def copy_rows(
    cursor: Any,
    *,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> None:
    cols = ", ".join(columns)
    statement = f"COPY {table} ({cols}) FROM STDIN"
    with cursor.copy(statement) as copy:
        for row in rows:
            copy.write_row(row)

