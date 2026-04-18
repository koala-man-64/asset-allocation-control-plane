from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def _escape_kql_literal(value: str) -> str:
    return str(value or "").replace("'", "''")


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None
