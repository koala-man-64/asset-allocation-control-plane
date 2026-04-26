from __future__ import annotations

# Transitional compatibility wrapper; remove after call-site migration.
from asset_allocation_runtime_common.market_data import market_symbols as _owner


def __getattr__(name: str):
    return getattr(_owner, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_owner)))
