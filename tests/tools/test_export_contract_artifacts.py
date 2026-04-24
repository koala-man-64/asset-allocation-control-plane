from __future__ import annotations

import importlib.util
import json
import os
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


def test_check_mode_reports_openapi_path_summary(tmp_path: Path, monkeypatch, capsys) -> None:
    export = _load_export_module()
    artifact_path = tmp_path / "control-plane.openapi.json"
    artifact_path.write_text(
        json.dumps(
            {
                "paths": {
                    "/api/changed": {"get": {"responses": {"200": {"description": "old"}}}},
                    "/api/removed": {"get": {"responses": {"200": {"description": "ok"}}}},
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        export,
        "_render_artifact_texts",
        lambda: {
            artifact_path: json.dumps(
                {
                    "paths": {
                        "/api/added": {"post": {"responses": {"200": {"description": "created"}}}},
                        "/api/changed": {"get": {"responses": {"200": {"description": "new"}}}},
                    }
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        },
    )

    assert export.main(["--check"]) == 1

    captured = capsys.readouterr()
    assert "OpenAPI paths: tracked=2 generated=2." in captured.err
    assert "Paths only in tracked (1):" in captured.err
    assert "/api/removed" in captured.err
    assert "Paths only in generated (1):" in captured.err
    assert "/api/added" in captured.err
    assert "Changed shared paths: 1." in captured.err
    assert "/api/changed" in captured.err
    assert "Diff preview:" in captured.err


def test_check_mode_reports_format_only_json_drift(tmp_path: Path, monkeypatch, capsys) -> None:
    export = _load_export_module()
    artifact_path = tmp_path / "ui-runtime-config.schema.json"
    artifact_path.write_text("{\"alpha\":1,\"beta\":2}\n", encoding="utf-8")
    monkeypatch.setattr(
        export,
        "_render_artifact_texts",
        lambda: {
            artifact_path: json.dumps({"alpha": 1, "beta": 2}, indent=2, sort_keys=True) + "\n",
        },
    )

    assert export.main(["--check"]) == 1

    captured = capsys.readouterr()
    assert "Semantic JSON matches; drift is formatting-only." in captured.err


def test_openapi_export_uses_canonical_unprefixed_api_surface(monkeypatch) -> None:
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")
    export = _load_export_module()

    artifacts = export._render_artifact_texts()
    openapi = json.loads(artifacts[export.OUTPUT_DIR / "control-plane.openapi.json"])
    paths = openapi["paths"]

    assert "/api/ai/chat/stream" in paths
    assert "/asset-allocation/api/ai/chat/stream" not in paths
    assert os.environ["API_ROOT_PREFIX"] == "asset-allocation"
