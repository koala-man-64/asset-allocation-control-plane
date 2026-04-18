from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_run_quality_gate_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "automation" / "run_quality_gate.py"
    spec = importlib.util.spec_from_file_location("run_quality_gate_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_command_includes_contract_artifact_check_gate() -> None:
    run_quality_gate = _load_run_quality_gate_module()

    command, cwd = run_quality_gate.build_command("contract-artifacts")

    assert cwd == run_quality_gate.REPO_ROOT
    assert command[1:] == ["scripts/automation/export_contract_artifacts.py", "--check"]


def test_main_returns_contract_artifact_gate_exit_code(monkeypatch) -> None:
    run_quality_gate = _load_run_quality_gate_module()
    sentinel_command = ["python", "scripts/automation/export_contract_artifacts.py", "--check"]
    sentinel_cwd = run_quality_gate.REPO_ROOT
    monkeypatch.setattr(run_quality_gate, "build_command", lambda gate: (sentinel_command, sentinel_cwd))
    monkeypatch.setattr(run_quality_gate, "run", lambda argv, cwd: 7)

    assert run_quality_gate.main(["run_quality_gate.py", "contract-artifacts"]) == 7
