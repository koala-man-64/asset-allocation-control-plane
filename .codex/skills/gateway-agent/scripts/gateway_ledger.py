#!/usr/bin/env python3
"""Append-only gateway ledger helper."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

SCHEMA_VERSION = "1"
DEFAULT_LEASE_MINUTES = 15
LEDGER_FILENAME = "gateway_agent_ledger.csv"
FIELDNAMES = [
    "schema_version",
    "event_id",
    "event_time_utc",
    "project_id",
    "folder_relpath",
    "task_id",
    "run_id",
    "agent_id",
    "access_mode",
    "status",
    "summary",
    "started_at_utc",
    "heartbeat_at_utc",
    "eta_utc",
    "lease_expires_at_utc",
    "completed_at_utc",
    "message_ref",
    "depends_on",
    "handoff_to",
    "eta_confidence",
    "notes",
]
ACTIVE_STATUSES = {"claim", "heartbeat", "progress"}
CLOSED_STATUSES = {"done", "abandon"}
STATUS_CHOICES = sorted(ACTIVE_STATUSES | CLOSED_STATUSES | {"blocked", "handoff", "stale_reclaim", "conflict"})
ACCESS_CHOICES = {"read", "shared", "exclusive"}


@dataclass
class AppendResult:
    event: dict[str, str]
    auto_events: list[dict[str, str]]
    outcome: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def format_utc(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc(value: str | None, field_name: str) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise SystemExit(f"{field_name} must be ISO 8601 UTC, for example 2026-04-19T20:00:00Z") from exc
    if parsed.tzinfo is None:
        raise SystemExit(f"{field_name} must include a timezone and end with Z")
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def sanitize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    while "  " in text:
        text = text.replace("  ", " ")
    return text


def make_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


def ensure_ledger(ledger_path: Path) -> None:
    if ledger_path.exists():
        return
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()


def load_rows(ledger_path: Path) -> list[dict[str, str]]:
    if not ledger_path.exists():
        return []
    with ledger_path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def append_rows(ledger_path: Path, rows: list[dict[str, str]]) -> None:
    with ledger_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        for row in rows:
            writer.writerow({field: sanitize_text(row.get(field, "")) for field in FIELDNAMES})


def latest_by_task(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    latest: dict[str, dict[str, str]] = {}
    for row in rows:
        task_id = sanitize_text(row.get("task_id"))
        if task_id:
            latest[task_id] = row
    return latest


def normalize_folder(project_root: Path, folder: str | None) -> str:
    if not folder:
        return "."
    candidate = Path(folder)
    resolved = (project_root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        relative = resolved.relative_to(project_root)
    except ValueError as exc:
        raise SystemExit(f"folder must stay within {project_root}") from exc
    return "." if str(relative) == "." else relative.as_posix()


def normalize_message_ref(project_root: Path, message_ref: str | None) -> str:
    if not message_ref:
        return ""
    candidate = Path(message_ref)
    resolved = (project_root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    try:
        relative = resolved.relative_to(project_root)
    except ValueError as exc:
        raise SystemExit(f"message_ref must be relative to {project_root} or inside it") from exc
    return relative.as_posix()


def parse_float_string(value: float | None, previous: str = "") -> str:
    if value is None:
        return previous
    if value < 0.0 or value > 1.0:
        raise SystemExit("eta_confidence must be between 0.0 and 1.0")
    normalized = f"{value:.3f}".rstrip("0").rstrip(".")
    return normalized or "0"


def parse_existing_time(row: dict[str, str] | None, field: str) -> datetime | None:
    if not row:
        return None
    return parse_utc(row.get(field), field)


def split_folder(folder_relpath: str) -> list[str]:
    return [] if folder_relpath == "." else [part for part in folder_relpath.split("/") if part]


def folders_overlap(left: str, right: str) -> bool:
    left_parts = split_folder(left)
    right_parts = split_folder(right)
    if not left_parts or not right_parts:
        return True
    short, long = (left_parts, right_parts) if len(left_parts) <= len(right_parts) else (right_parts, left_parts)
    return long[: len(short)] == short


def access_modes_compatible(left: str, right: str) -> bool:
    if left == "read" and right == "read":
        return True
    if left == "shared" and right == "shared":
        return True
    return False


def is_nonstale_active(row: dict[str, str], reference_time: datetime) -> bool:
    if row.get("status") not in ACTIVE_STATUSES:
        return False
    lease_expires_at = parse_utc(row.get("lease_expires_at_utc"), "lease_expires_at_utc")
    return bool(lease_expires_at and lease_expires_at > reference_time)


def is_stale_active(row: dict[str, str], reference_time: datetime) -> bool:
    if row.get("status") not in ACTIVE_STATUSES:
        return False
    lease_expires_at = parse_utc(row.get("lease_expires_at_utc"), "lease_expires_at_utc")
    return bool(lease_expires_at and lease_expires_at <= reference_time)


def task_state(row: dict[str, str], reference_time: datetime) -> str:
    status = sanitize_text(row.get("status"))
    if status in ACTIVE_STATUSES:
        return "active" if is_nonstale_active(row, reference_time) else "stale"
    return status


def new_row() -> dict[str, str]:
    return {field: "" for field in FIELDNAMES}


def build_row(
    *,
    event_time: datetime,
    project_id: str,
    folder_relpath: str,
    task_id: str,
    run_id: str,
    agent_id: str,
    access_mode: str,
    status: str,
    summary: str,
    started_at: datetime | None,
    heartbeat_at: datetime | None,
    eta_at: datetime | None,
    lease_expires_at: datetime | None,
    completed_at: datetime | None,
    message_ref: str,
    depends_on: str,
    handoff_to: str,
    eta_confidence: str,
    notes: str,
) -> dict[str, str]:
    row = new_row()
    row.update(
        {
            "schema_version": SCHEMA_VERSION,
            "event_id": make_id("evt"),
            "event_time_utc": format_utc(event_time),
            "project_id": sanitize_text(project_id),
            "folder_relpath": sanitize_text(folder_relpath),
            "task_id": sanitize_text(task_id),
            "run_id": sanitize_text(run_id),
            "agent_id": sanitize_text(agent_id),
            "access_mode": sanitize_text(access_mode),
            "status": sanitize_text(status),
            "summary": sanitize_text(summary),
            "started_at_utc": format_utc(started_at),
            "heartbeat_at_utc": format_utc(heartbeat_at),
            "eta_utc": format_utc(eta_at),
            "lease_expires_at_utc": format_utc(lease_expires_at),
            "completed_at_utc": format_utc(completed_at),
            "message_ref": sanitize_text(message_ref),
            "depends_on": sanitize_text(depends_on),
            "handoff_to": sanitize_text(handoff_to),
            "eta_confidence": sanitize_text(eta_confidence),
            "notes": sanitize_text(notes),
        }
    )
    return row


def conflict_notes(conflicts: list[dict[str, str]]) -> str:
    parts = []
    for row in conflicts:
        parts.append(
            f"{row['agent_id']}:{row['task_id']}:{row['folder_relpath']}:{row['access_mode']} until {row['lease_expires_at_utc']}"
        )
    return "; ".join(parts)


def validate_access_mode(access_mode: str, status: str) -> str:
    normalized = sanitize_text(access_mode)
    if normalized not in ACCESS_CHOICES:
        raise SystemExit(f"{status} requires access_mode to be one of: read, shared, exclusive")
    return normalized


def derive_project_id(project_root: Path, explicit_project_id: str | None) -> str:
    return sanitize_text(explicit_project_id) or project_root.name


def append_event(args: argparse.Namespace) -> AppendResult:
    project_root = Path(args.project_root).resolve()
    ledger_path = project_root / LEDGER_FILENAME
    ensure_ledger(ledger_path)
    existing_rows = load_rows(ledger_path)
    latest = latest_by_task(existing_rows)
    event_time = parse_utc(args.event_time, "event_time") or utc_now()
    previous = latest.get(args.task_id) if args.task_id else None

    if args.status != "claim" and not args.task_id:
        raise SystemExit(f"{args.status} requires --task-id")
    if args.status == "claim" and args.task_id and args.task_id in latest:
        raise SystemExit("claim must use a new task_id; omit --task-id to generate one")
    if args.status != "claim" and not previous:
        raise SystemExit(f"task_id {args.task_id} was not found in the ledger")
    if previous and previous.get("status") in CLOSED_STATUSES and args.status not in {"conflict", "stale_reclaim"}:
        raise SystemExit(f"task_id {args.task_id} is already closed with status {previous['status']}")

    task_id = sanitize_text(args.task_id) or make_id("task")
    run_id = sanitize_text(args.run_id) or sanitize_text(previous.get("run_id") if previous else "") or make_id("run")
    project_id = derive_project_id(project_root, args.project_id)
    folder_relpath = normalize_folder(project_root, args.folder or (previous.get("folder_relpath") if previous else "."))
    access_mode = validate_access_mode(args.access_mode or (previous.get("access_mode") if previous else ""), args.status)
    summary = sanitize_text(args.summary or (previous.get("summary") if previous else ""))
    if args.status == "claim" and not summary:
        raise SystemExit("claim requires --summary")
    if len(summary) > 160:
        raise SystemExit("summary must be 160 characters or fewer")

    started_at = parse_utc(args.started_at, "started_at") or parse_existing_time(previous, "started_at_utc")
    if args.status == "claim" and not started_at:
        started_at = event_time
    heartbeat_at = event_time
    eta_at = parse_utc(args.eta, "eta") or parse_existing_time(previous, "eta_utc")
    message_ref = normalize_message_ref(project_root, args.message_ref) if args.message_ref else sanitize_text(previous.get("message_ref") if previous else "")
    depends_on = sanitize_text(args.depends_on) or sanitize_text(previous.get("depends_on") if previous else "")
    handoff_to = sanitize_text(args.handoff_to) or sanitize_text(previous.get("handoff_to") if previous else "")
    eta_confidence = parse_float_string(args.eta_confidence, sanitize_text(previous.get("eta_confidence") if previous else ""))
    notes = sanitize_text(args.notes)

    if args.status in {"heartbeat", "progress"}:
        previous_lease = parse_existing_time(previous, "lease_expires_at_utc")
        if previous.get("status") not in ACTIVE_STATUSES or not previous_lease or previous_lease <= event_time:
            raise SystemExit("heartbeat and progress require a non-stale active task; create a new claim instead")

    overlapping = [
        row
        for row in latest.values()
        if row.get("task_id") != task_id and folders_overlap(folder_relpath, sanitize_text(row.get("folder_relpath", ".")))
    ]

    auto_events: list[dict[str, str]] = []
    if args.status == "claim":
        for row in overlapping:
            if is_stale_active(row, event_time):
                auto_events.append(
                    build_row(
                        event_time=event_time,
                        project_id=project_id,
                        folder_relpath=sanitize_text(row.get("folder_relpath", ".")),
                        task_id=sanitize_text(row.get("task_id")),
                        run_id=run_id,
                        agent_id=sanitize_text(args.agent_id),
                        access_mode=sanitize_text(row.get("access_mode")),
                        status="stale_reclaim",
                        summary=sanitize_text(row.get("summary")),
                        started_at=parse_utc(row.get("started_at_utc"), "started_at_utc"),
                        heartbeat_at=event_time,
                        eta_at=parse_utc(row.get("eta_utc"), "eta_utc"),
                        lease_expires_at=None,
                        completed_at=None,
                        message_ref=sanitize_text(row.get("message_ref")),
                        depends_on=sanitize_text(row.get("depends_on")),
                        handoff_to="",
                        eta_confidence=sanitize_text(row.get("eta_confidence")),
                        notes=f"Expired lease reclaimed by {sanitize_text(args.agent_id)} for {task_id}",
                    )
                )

    active_conflicts = [
        row
        for row in overlapping
        if is_nonstale_active(row, event_time) and not access_modes_compatible(access_mode, sanitize_text(row.get("access_mode")))
    ]

    if active_conflicts:
        conflict_event = build_row(
            event_time=event_time,
            project_id=project_id,
            folder_relpath=folder_relpath,
            task_id=task_id,
            run_id=run_id,
            agent_id=sanitize_text(args.agent_id),
            access_mode=access_mode,
            status="conflict",
            summary=summary or sanitize_text(previous.get("summary") if previous else ""),
            started_at=started_at or event_time,
            heartbeat_at=event_time,
            eta_at=eta_at,
            lease_expires_at=None,
            completed_at=None,
            message_ref=message_ref,
            depends_on=depends_on,
            handoff_to=handoff_to,
            eta_confidence=eta_confidence,
            notes=notes or conflict_notes(active_conflicts),
        )
        rows_to_write = [*auto_events, conflict_event]
        append_rows(ledger_path, rows_to_write)
        return AppendResult(event=conflict_event, auto_events=auto_events, outcome="conflict")

    lease_minutes = args.lease_minutes if args.lease_minutes is not None else DEFAULT_LEASE_MINUTES
    lease_expires_at = None
    completed_at = None

    if args.status in ACTIVE_STATUSES:
        lease_expires_at = event_time + timedelta(minutes=lease_minutes)
    elif args.status == "done":
        completed_at = event_time
    elif args.status == "blocked" and args.lease_minutes is not None:
        lease_expires_at = event_time + timedelta(minutes=lease_minutes)
    elif args.status == "handoff" and args.lease_minutes is not None:
        lease_expires_at = event_time + timedelta(minutes=lease_minutes)

    event = build_row(
        event_time=event_time,
        project_id=project_id,
        folder_relpath=folder_relpath,
        task_id=task_id,
        run_id=run_id,
        agent_id=sanitize_text(args.agent_id),
        access_mode=access_mode,
        status=args.status,
        summary=summary,
        started_at=started_at,
        heartbeat_at=heartbeat_at,
        eta_at=eta_at,
        lease_expires_at=lease_expires_at,
        completed_at=completed_at,
        message_ref=message_ref,
        depends_on=depends_on,
        handoff_to=handoff_to,
        eta_confidence=eta_confidence,
        notes=notes,
    )
    append_rows(ledger_path, [*auto_events, event])
    return AppendResult(event=event, auto_events=auto_events, outcome=args.status)


def snapshot(args: argparse.Namespace) -> dict[str, Any]:
    project_root = Path(args.project_root).resolve()
    ledger_path = project_root / LEDGER_FILENAME
    rows = load_rows(ledger_path)
    reference_time = parse_utc(args.reference_time, "reference_time") or utc_now()
    latest = latest_by_task(rows)

    tasks = []
    for row in latest.values():
        view = dict(row)
        view["derived_state"] = task_state(row, reference_time)
        tasks.append(view)
    tasks.sort(key=lambda item: item.get("event_time_utc", ""), reverse=True)

    active_tasks = [task for task in tasks if task["derived_state"] == "active"]
    blocked_tasks = [task for task in tasks if task["derived_state"] == "blocked"]
    stale_tasks = [task for task in tasks if task["derived_state"] == "stale"]
    handoffs = [task for task in tasks if task["derived_state"] == "handoff"]
    closed_tasks = [task for task in tasks if task["derived_state"] in {"done", "abandon"}]
    conflicts = [task for task in tasks if task["derived_state"] == "conflict"]

    folder_owners: dict[str, list[dict[str, str]]] = {}
    for task in active_tasks:
        folder_owners.setdefault(task["folder_relpath"], []).append(
            {
                "task_id": task["task_id"],
                "agent_id": task["agent_id"],
                "access_mode": task["access_mode"],
                "summary": task["summary"],
                "lease_expires_at_utc": task["lease_expires_at_utc"],
            }
        )

    return {
        "project_root": str(project_root),
        "ledger_path": str(ledger_path),
        "ledger_exists": ledger_path.exists(),
        "event_count": len(rows),
        "active_tasks": active_tasks,
        "blocked_tasks": blocked_tasks,
        "stale_tasks": stale_tasks,
        "handoffs": handoffs,
        "closed_tasks": closed_tasks[:10],
        "conflicts": conflicts,
        "folder_owners": folder_owners,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage the append-only gateway agent ledger.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    append_parser = subparsers.add_parser("append", help="Append a ledger event.")
    append_parser.add_argument("--project-root", required=True)
    append_parser.add_argument("--project-id")
    append_parser.add_argument("--agent-id", required=True)
    append_parser.add_argument("--task-id")
    append_parser.add_argument("--run-id")
    append_parser.add_argument("--folder")
    append_parser.add_argument("--access-mode")
    append_parser.add_argument("--status", required=True, choices=STATUS_CHOICES)
    append_parser.add_argument("--summary")
    append_parser.add_argument("--started-at")
    append_parser.add_argument("--event-time")
    append_parser.add_argument("--eta")
    append_parser.add_argument("--lease-minutes", type=int)
    append_parser.add_argument("--message-ref")
    append_parser.add_argument("--depends-on")
    append_parser.add_argument("--handoff-to")
    append_parser.add_argument("--eta-confidence", type=float)
    append_parser.add_argument("--notes")

    snapshot_parser = subparsers.add_parser("snapshot", help="Show current derived task state.")
    snapshot_parser.add_argument("--project-root", required=True)
    snapshot_parser.add_argument("--reference-time")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "append":
        result = append_event(args)
        payload = {
            "ledger_path": str(Path(args.project_root).resolve() / LEDGER_FILENAME),
            "outcome": result.outcome,
            "auto_events": result.auto_events,
            "event": result.event,
        }
        print(json.dumps(payload, indent=2))
        return

    if args.command == "snapshot":
        print(json.dumps(snapshot(args), indent=2))
        return

    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
