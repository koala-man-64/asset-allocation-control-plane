from __future__ import annotations

import builtins
import importlib.util
import json
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest


_SHARED_PINS = {
    "asset-allocation-contracts": "3.14.0",
    "asset-allocation-runtime-common": "3.5.3",
}


def _load_export_module():
    module_path = Path(__file__).resolve().parents[2] / "scripts" / "automation" / "export_contract_artifacts.py"
    spec = importlib.util.spec_from_file_location("export_contract_artifacts_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _patch_shared_versions(export, monkeypatch: pytest.MonkeyPatch, versions: dict[str, str]) -> None:
    original_version = export.importlib.metadata.version

    def version(name: str) -> str:
        if name in versions:
            return versions[name]
        return original_version(name)

    monkeypatch.setattr(export, "_shared_dependency_pins", lambda: dict(_SHARED_PINS))
    monkeypatch.setattr(export.importlib.metadata, "version", version)


def _install_fake_ui_config(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeUiRuntimeConfig:
        @staticmethod
        def model_json_schema() -> dict[str, object]:
            return {"type": "object"}

    fake_ui_config_module = ModuleType("asset_allocation_contracts.ui_config")
    fake_ui_config_module.UiRuntimeConfig = FakeUiRuntimeConfig  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "asset_allocation_contracts.ui_config", fake_ui_config_module)


def _install_fake_export_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_app_module = ModuleType("api.service.app")
    fake_app_module.create_app = lambda: SimpleNamespace(openapi=lambda: {"paths": {"/api/fake": {}}})  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "api.service.app", fake_app_module)
    _install_fake_ui_config(monkeypatch)


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


def test_shared_dependency_guard_allows_matching_versions(tmp_path: Path, monkeypatch) -> None:
    export = _load_export_module()
    _patch_shared_versions(export, monkeypatch, dict(_SHARED_PINS))
    _install_fake_export_dependencies(monkeypatch)
    monkeypatch.setattr(export, "ENV_WEB_PATH", tmp_path / ".env.web")

    artifacts = export._render_artifact_texts()

    openapi = json.loads(artifacts[export.OUTPUT_DIR / "control-plane.openapi.json"])
    ui_schema = json.loads(artifacts[export.OUTPUT_DIR / "ui-runtime-config.schema.json"])
    assert openapi["paths"] == {"/api/fake": {}}
    assert ui_schema == {"type": "object"}


def test_shared_dependency_guard_rejects_mismatch_before_app_import(monkeypatch) -> None:
    export = _load_export_module()
    _patch_shared_versions(
        export,
        monkeypatch,
        {
            "asset-allocation-contracts": "3.11.0",
            "asset-allocation-runtime-common": "3.5.3",
        },
    )
    imported_app = False
    original_import = builtins.__import__

    def tracking_import(name: str, *args, **kwargs):
        nonlocal imported_app
        if name == "api.service.app":
            imported_app = True
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", tracking_import)

    with pytest.raises(SystemExit) as exc_info:
        export._render_artifact_texts()

    message = str(exc_info.value)
    assert "asset-allocation-contracts: required 3.14.0, installed 3.11.0" in message
    assert "python -m pip install asset-allocation-contracts==3.14.0" in message
    assert "asset-allocation-runtime-common==3.5.3 --no-deps" in message
    assert imported_app is False


def test_shared_dependency_guard_rejects_missing_package(monkeypatch) -> None:
    export = _load_export_module()
    monkeypatch.setattr(export, "_shared_dependency_pins", lambda: dict(_SHARED_PINS))

    def version(name: str) -> str:
        if name == "asset-allocation-contracts":
            raise export.importlib.metadata.PackageNotFoundError(name)
        return _SHARED_PINS[name]

    monkeypatch.setattr(export.importlib.metadata, "version", version)

    with pytest.raises(SystemExit) as exc_info:
        export._assert_shared_dependency_versions_current()

    message = str(exc_info.value)
    assert "asset-allocation-contracts: required 3.14.0, installed not installed" in message
    assert "python -m pip install asset-allocation-contracts==3.14.0" in message


def test_openapi_export_uses_canonical_unprefixed_api_surface(monkeypatch) -> None:
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")
    export = _load_export_module()
    _patch_shared_versions(export, monkeypatch, dict(_SHARED_PINS))

    artifacts = export._render_artifact_texts()
    openapi = json.loads(artifacts[export.OUTPUT_DIR / "control-plane.openapi.json"])
    paths = openapi["paths"]

    assert "/api/ai/chat/stream" in paths
    assert "/asset-allocation/api/ai/chat/stream" not in paths
    assert os.environ["API_ROOT_PREFIX"] == "asset-allocation"


def test_openapi_export_uses_env_web_over_process_environment(tmp_path: Path, monkeypatch) -> None:
    env_web_path = tmp_path / ".env.web"
    env_web_path.write_text(
        "\n".join(
            [
                "API_AUTH_SESSION_MODE=cookie",
                "UI_AUTH_PROVIDER=password",
                "API_ROOT_PREFIX=from-env-web",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("API_AUTH_SESSION_MODE", "bearer")
    monkeypatch.delenv("DISABLE_DOTENV", raising=False)
    monkeypatch.delenv("UI_AUTH_PROVIDER", raising=False)
    monkeypatch.setenv("API_ROOT_PREFIX", "from-shell")

    captured_env: dict[str, str | None] = {}

    def create_app() -> SimpleNamespace:
        captured_env["API_AUTH_SESSION_MODE"] = os.environ.get("API_AUTH_SESSION_MODE")
        captured_env["UI_AUTH_PROVIDER"] = os.environ.get("UI_AUTH_PROVIDER")
        captured_env["API_ROOT_PREFIX"] = os.environ.get("API_ROOT_PREFIX")
        return SimpleNamespace(openapi=lambda: {"paths": {"/api/fake": {}}})

    fake_app_module = ModuleType("api.service.app")
    fake_app_module.create_app = create_app  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "api.service.app", fake_app_module)
    _install_fake_ui_config(monkeypatch)

    export = _load_export_module()
    monkeypatch.setattr(export, "_assert_shared_dependency_versions_current", lambda: None)
    monkeypatch.setattr(export, "ENV_WEB_PATH", env_web_path)

    export._render_artifact_texts()

    assert captured_env == {
        "API_AUTH_SESSION_MODE": "cookie",
        "UI_AUTH_PROVIDER": "password",
        "API_ROOT_PREFIX": "",
    }
    assert os.environ["API_AUTH_SESSION_MODE"] == "bearer"
    assert "UI_AUTH_PROVIDER" not in os.environ
    assert os.environ["API_ROOT_PREFIX"] == "from-shell"
