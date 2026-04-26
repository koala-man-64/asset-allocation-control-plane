from __future__ import annotations

# Transitional compatibility wrapper; remove after call-site migration.
from asset_allocation_contracts import finance as _owner


def __getattr__(name: str):
    return getattr(_owner, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_owner)))
