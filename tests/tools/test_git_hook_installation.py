from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_install_git_hooks_script_sets_repo_managed_hooks_path() -> None:
    text = (REPO_ROOT / "scripts" / "dev" / "install_git_hooks.py").read_text(encoding="utf-8")

    assert '["git", "config", "core.hooksPath", hooks_path]' in text
    assert 'hooks_path = ".githooks"' in text


def test_pre_commit_hook_runs_contract_artifact_gate_on_main_with_remediation_message() -> None:
    text = (REPO_ROOT / ".githooks" / "pre-commit").read_text(encoding="utf-8")

    branch_guard = 'current_branch="$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)"'
    gate_call = "python scripts/automation/run_quality_gate.py contract-artifacts"

    assert branch_guard in text
    assert 'if [ "${current_branch}" != "main" ]; then' in text
    assert text.index(branch_guard) < text.index(gate_call)
    assert gate_call in text
    assert "python scripts/automation/export_contract_artifacts.py" in text
    assert "api/contracts/" in text
