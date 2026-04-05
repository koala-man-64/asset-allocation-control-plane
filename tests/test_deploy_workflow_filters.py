from __future__ import annotations

from pathlib import Path

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _iter_filtered_repo_paths(workflow_doc: dict) -> list[str]:
    out: list[str] = []
    jobs = workflow_doc.get("jobs") or {}
    for job in jobs.values():
        if not isinstance(job, dict):
            continue
        for step in job.get("steps") or []:
            if not isinstance(step, dict):
                continue
            filters = ((step.get("with") or {}).get("filters"))
            if not isinstance(filters, str):
                continue
            for raw_line in filters.splitlines():
                stripped = raw_line.strip()
                if not stripped.startswith("- "):
                    continue
                candidate = stripped[2:].strip().strip('"').strip("'")
                if "*" in candidate:
                    continue
                if candidate.startswith("tasks/") or candidate.startswith("deploy/"):
                    out.append(candidate)
    return out


def test_workflow_path_filters_reference_existing_repo_files() -> None:
    repo_root = _repo_root()
    workflow_root = repo_root / ".github" / "workflows"
    missing: list[str] = []

    for workflow_path in sorted(workflow_root.glob("*.yml")):
        doc = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        for candidate in _iter_filtered_repo_paths(doc):
            if not (repo_root / candidate).exists():
                missing.append(f"{workflow_path.name}: {candidate}")

    assert missing == [], "workflow path filters reference missing files:\n" + "\n".join(missing)
