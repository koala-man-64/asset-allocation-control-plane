from __future__ import annotations

import json
from typing import Callable, Optional


def parse_last_checkpoint_version(raw: str) -> Optional[int]:
    try:
        payload = json.loads(raw or "")
    except json.JSONDecodeError:
        return None
    version = payload.get("version")
    return int(version) if isinstance(version, int) else None


def find_latest_delta_version(
    exists: Callable[[int], bool],
    *,
    start_version: int = 0,
    max_probe_version: int = 10_000_000,
) -> Optional[int]:
    """
    Finds the latest contiguous Delta commit version by probing for the existence of
    commit JSON files (e.g., 00000000000000000010.json) via an `exists(version)` callback.

    Uses exponential search + binary search: O(log N) probes.
    """
    start = max(int(start_version), 0)
    if start > max_probe_version:
        start = 0

    # If the starting version doesn't exist, fall back to 0.
    if not exists(start):
        start = 0
        if not exists(start):
            return None

    step = 1
    while True:
        candidate = start + step
        if candidate > max_probe_version:
            break
        if exists(candidate):
            step *= 2
            continue
        break

    low = start
    high = min(start + step, max_probe_version + 1)  # high is exclusive (first missing or probe ceiling)
    while low + 1 < high:
        mid = (low + high) // 2
        if exists(mid):
            low = mid
        else:
            high = mid

    return low

