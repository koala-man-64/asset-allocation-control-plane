from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_export_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "automation" / "export_contract_artifacts.py"
    spec = importlib.util.spec_from_file_location("export_contract_artifacts_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_check_mode_passes_when_artifacts_are_current(tmp_path: Path, monkeypatch) -> None:
    export = _load_export_module()
    artifact_path = tmp_path / "control-plane.openapi.json"
    artifact_path.write_text("expected\n", encoding="utf-8")
    monkeypatch.setattr(export, "_render_artifact_texts", lambda: {artifact_path: "expected\n"})

    assert export.main(["--check"]) == 0
    assert artifact_path.read_text(encoding="utf-8") == "expected\n"


def test_check_mode_reports_drift_without_rewriting_files(tmp_path: Path, monkeypatch) -> None:
    export = _load_export_module()
    artifact_path = tmp_path / "control-plane.openapi.json"
    artifact_path.write_text("tracked\n", encoding="utf-8")
    monkeypatch.setattr(export, "_render_artifact_texts", lambda: {artifact_path: "generated\n"})

    assert export.main(["--check"]) == 1
    assert artifact_path.read_text(encoding="utf-8") == "tracked\n"
