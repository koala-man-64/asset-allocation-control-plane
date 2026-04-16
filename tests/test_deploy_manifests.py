from __future__ import annotations

from pathlib import Path

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> dict[str, object]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict), f"{path} did not parse into a YAML mapping"
    return loaded


def _scale_block(path: Path) -> dict[str, object]:
    doc = _load_yaml(path)
    properties = doc.get("properties")
    assert isinstance(properties, dict), f"{path} is missing properties"
    template = properties.get("template")
    assert isinstance(template, dict), f"{path} is missing template"
    scale = template.get("scale")
    assert isinstance(scale, dict), f"{path} is missing scale"
    return scale


def test_api_manifests_pin_single_always_on_replica() -> None:
    repo_root = _repo_root()
    public_scale = _scale_block(repo_root / "deploy" / "app_api_public.yaml")
    private_scale = _scale_block(repo_root / "deploy" / "app_api.yaml")

    assert public_scale["minReplicas"] == 1
    assert public_scale["maxReplicas"] == 1
    assert private_scale["minReplicas"] == 1
    assert private_scale["maxReplicas"] == 1
    assert public_scale == private_scale
