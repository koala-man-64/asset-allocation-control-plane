#!/usr/bin/env python3
"""One-time remediation for Delta index artifact columns in Silver/Gold tables.

Scans configured Silver/Gold domain prefixes, detects tables that contain known
index artifact columns, and rewrites those tables through core.delta_core.store_delta
to remove artifact columns from data and schema.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from typing import Iterable, List

from asset_allocation_runtime_common.market_data import core as mdc
from asset_allocation_runtime_common.market_data import delta_core
_NON_ALNUM_RE = re.compile(r"[^0-9a-z]+")
_INDEX_ARTIFACT_EXACT_NAMES = {
    "index",
    "level_0",
    "index_level_0",
}


@dataclass(frozen=True)
class ScanTarget:
    layer: str
    container: str
    prefixes: List[str]


def _normalize_name(name: str) -> str:
    normalized = _NON_ALNUM_RE.sub("_", str(name).strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _is_index_artifact_column(name: str) -> bool:
    normalized = _normalize_name(name)
    if not normalized:
        return False
    if normalized in _INDEX_ARTIFACT_EXACT_NAMES:
        return True
    if normalized.startswith("unnamed_"):
        suffix = normalized[len("unnamed_") :]
        if suffix.replace("_", "").isdigit():
            return True
    if normalized.startswith("index_level_"):
        suffix = normalized[len("index_level_") :]
        if suffix.replace("_", "").isdigit():
            return True
    return False


def _discover_delta_table_paths(*, container: str, prefixes: Iterable[str]) -> list[str]:
    client = mdc.get_storage_client(container)
    if client is None:
        raise RuntimeError(f"Storage client unavailable for container={container!r}.")

    marker = "/_delta_log/"
    roots: set[str] = set()
    for prefix in prefixes:
        search_prefix = f"{str(prefix).strip('/')}/"
        for name in client.list_files(name_starts_with=search_prefix):
            text = str(name or "")
            if marker not in text:
                continue
            root = text.split(marker, 1)[0].strip("/")
            if not root.startswith(search_prefix.rstrip("/")):
                continue
            roots.add(root)
    return sorted(roots)


def _resolve_container(explicit: str | None, env_key: str, fallback: str) -> str:
    if explicit:
        return explicit.strip()
    raw = os.environ.get(env_key)
    if raw and raw.strip():
        return raw.strip()
    return fallback


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Remediate Delta index artifact columns for Silver and Gold tables.")
    parser.add_argument(
        "--silver-container",
        default=None,
        help="Override Silver container name (default: AZURE_CONTAINER_SILVER or 'silver').",
    )
    parser.add_argument(
        "--gold-container",
        default=None,
        help="Override Gold container name (default: AZURE_CONTAINER_GOLD or 'gold').",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report contaminated tables; do not rewrite.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit final summary as JSON.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    silver_container = _resolve_container(args.silver_container, "AZURE_CONTAINER_SILVER", "silver")
    gold_container = _resolve_container(args.gold_container, "AZURE_CONTAINER_GOLD", "gold")

    targets = [
        ScanTarget(
            layer="silver",
            container=silver_container,
            prefixes=["market-data", "finance-data", "earnings-data", "price-target-data"],
        ),
        ScanTarget(
            layer="gold",
            container=gold_container,
            prefixes=["market", "finance", "earnings", "targets"],
        ),
    ]

    summary = {
        "tables_scanned": 0,
        "tables_contaminated": 0,
        "tables_rewritten": 0,
        "tables_failed": 0,
        "tables_skipped_clean": 0,
        "tables_dry_run": 0,
    }

    mdc.write_line(
        "Starting Delta index artifact remediation scan "
        f"(dry_run={args.dry_run}, silver_container={silver_container}, gold_container={gold_container})."
    )

    for target in targets:
        try:
            table_paths = _discover_delta_table_paths(container=target.container, prefixes=target.prefixes)
        except Exception as exc:
            summary["tables_failed"] += 1
            mdc.write_error(f"Failed to discover {target.layer} Delta tables: {exc}")
            continue

        mdc.write_line(
            f"Discovered {len(table_paths)} {target.layer} Delta table(s) under prefixes={target.prefixes}."
        )
        for table_path in table_paths:
            summary["tables_scanned"] += 1
            schema_cols = delta_core.get_delta_schema_columns(target.container, table_path)
            if not schema_cols:
                summary["tables_skipped_clean"] += 1
                continue

            artifact_cols = [str(col) for col in schema_cols if _is_index_artifact_column(str(col))]
            if not artifact_cols:
                summary["tables_skipped_clean"] += 1
                continue

            summary["tables_contaminated"] += 1
            mdc.write_line(
                f"Detected index artifacts in {target.layer} table {table_path}: artifact_cols={artifact_cols}"
            )
            if args.dry_run:
                summary["tables_dry_run"] += 1
                continue

            try:
                df = delta_core.load_delta(target.container, table_path)
                if df is None:
                    raise RuntimeError("Unable to load table for remediation (load_delta returned None).")
                delta_core.store_delta(df, target.container, table_path, mode="overwrite")
                summary["tables_rewritten"] += 1
            except Exception as exc:
                summary["tables_failed"] += 1
                mdc.write_error(f"Failed to remediate {target.layer} table {table_path}: {exc}")

    if args.json:
        print(json.dumps(summary, sort_keys=True))
    else:
        mdc.write_line(
            "Delta index artifact remediation summary: "
            f"tables_scanned={summary['tables_scanned']} "
            f"tables_contaminated={summary['tables_contaminated']} "
            f"tables_rewritten={summary['tables_rewritten']} "
            f"tables_failed={summary['tables_failed']} "
            f"tables_skipped_clean={summary['tables_skipped_clean']} "
            f"tables_dry_run={summary['tables_dry_run']}"
        )

    return 0 if summary["tables_failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
