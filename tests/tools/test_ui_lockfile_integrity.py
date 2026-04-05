from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Tuple

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
UI_DIR = REPO_ROOT / "ui"
LOCKFILE_PATH = UI_DIR / "pnpm-lock.yaml"
PACKAGE_JSON_PATH = UI_DIR / "package.json"


def _load_yaml_mapping(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise AssertionError(f"{path} did not parse into a YAML mapping.")
    return data


def _collect_duplicate_yaml_mapping_keys(path: Path) -> list[Tuple[str, str, list[int]]]:
    with path.open("r", encoding="utf-8") as handle:
        root = yaml.compose(handle)

    duplicates: list[Tuple[str, str, list[int]]] = []

    def walk(node: yaml.Node, node_path: str = "") -> None:
        if isinstance(node, yaml.MappingNode):
            seen: dict[str, list[int]] = defaultdict(list)
            for key_node, _value_node in node.value:
                key = key_node.value if isinstance(key_node, yaml.ScalarNode) else str(key_node)
                seen[key].append(key_node.start_mark.line + 1)

            for key, lines in seen.items():
                if len(lines) > 1:
                    duplicates.append((node_path or "<root>", key, lines))

            for key_node, value_node in node.value:
                key = key_node.value if isinstance(key_node, yaml.ScalarNode) else str(key_node)
                child_path = f"{node_path}.{key}" if node_path else key
                walk(value_node, child_path)
            return

        if isinstance(node, yaml.SequenceNode):
            for index, item in enumerate(node.value):
                walk(item, f"{node_path}[{index}]")

    if root is not None:
        walk(root)
    return duplicates


def _split_lock_key(key: str) -> Tuple[str, str]:
    depth = 0
    separator = -1
    for idx, char in enumerate(key):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "@" and depth == 0:
            separator = idx

    if separator <= 0:
        raise AssertionError(f"Unable to split lockfile key '{key}' into package/version.")
    return key[:separator], key[separator + 1 :]


def _iter_dependency_sections(mapping: dict) -> Iterable[Tuple[str, dict]]:
    for section in ("dependencies", "devDependencies", "optionalDependencies"):
        values = mapping.get(section)
        if isinstance(values, dict):
            yield section, values


def _is_external_ref(version: str) -> bool:
    return version.startswith(("link:", "workspace:", "file:", "portal:"))


def test_ui_lockfile_has_no_duplicate_yaml_keys() -> None:
    duplicates = _collect_duplicate_yaml_mapping_keys(LOCKFILE_PATH)
    assert not duplicates, f"Duplicate YAML mapping keys found in {LOCKFILE_PATH}: {duplicates[:10]}"


def test_ui_lockfile_matches_package_json_specifiers() -> None:
    package_json = json.loads(PACKAGE_JSON_PATH.read_text(encoding="utf-8"))
    lockfile = _load_yaml_mapping(LOCKFILE_PATH)

    importer = lockfile.get("importers", {}).get(".", {})
    assert isinstance(importer, dict), "Missing '.' importer in pnpm-lock.yaml"

    for section in ("dependencies", "devDependencies", "optionalDependencies"):
        manifest_deps = package_json.get(section, {}) or {}
        lock_deps = importer.get(section, {}) or {}

        assert isinstance(lock_deps, dict), f"Lockfile importer section '{section}' is not a mapping."

        for name, specifier in manifest_deps.items():
            entry = lock_deps.get(name)
            assert isinstance(entry, dict), f"Missing '{name}' in lockfile importer '{section}'."
            assert (
                entry.get("specifier") == specifier
            ), f"Specifier mismatch for {name}: lockfile={entry.get('specifier')} manifest={specifier}"


def test_ui_lockfile_dependency_graph_references_exist() -> None:
    lockfile = _load_yaml_mapping(LOCKFILE_PATH)
    importers = lockfile.get("importers", {})
    snapshots = lockfile.get("snapshots", {})
    packages = lockfile.get("packages", {})

    assert isinstance(importers, dict), "Lockfile 'importers' section missing or invalid."
    assert isinstance(snapshots, dict), "Lockfile 'snapshots' section missing or invalid."
    assert isinstance(packages, dict), "Lockfile 'packages' section missing or invalid."

    # Importer-resolved versions must point to snapshot keys.
    for importer_name, importer in importers.items():
        if not isinstance(importer, dict):
            continue
        for section, deps in _iter_dependency_sections(importer):
            for dep_name, dep_meta in deps.items():
                if not isinstance(dep_meta, dict):
                    continue
                resolved = dep_meta.get("version")
                if not isinstance(resolved, str) or _is_external_ref(resolved):
                    continue
                snapshot_key = f"{dep_name}@{resolved}"
                assert snapshot_key in snapshots, (
                    f"Importer '{importer_name}' {section} dependency '{dep_name}' "
                    f"points to missing snapshot '{snapshot_key}'."
                )

    # Snapshot dependency edges must point to other snapshot keys.
    for snapshot_key, snapshot_meta in snapshots.items():
        if not isinstance(snapshot_meta, dict):
            continue
        for section in ("dependencies", "optionalDependencies"):
            deps = snapshot_meta.get(section, {})
            if not isinstance(deps, dict):
                continue
            for dep_name, resolved in deps.items():
                if not isinstance(resolved, str) or _is_external_ref(resolved):
                    continue
                target_snapshot_key = f"{dep_name}@{resolved}"
                assert target_snapshot_key in snapshots, (
                    f"Snapshot '{snapshot_key}' {section} dependency '{dep_name}' "
                    f"points to missing snapshot '{target_snapshot_key}'."
                )

    # Each snapshot entry should map to package metadata.
    for snapshot_key in snapshots:
        dep_name, resolved = _split_lock_key(snapshot_key)
        base_version = resolved.split("(", 1)[0]
        candidates = (f"{dep_name}@{resolved}", f"{dep_name}@{base_version}")
        assert any(candidate in packages for candidate in candidates), (
            f"Snapshot '{snapshot_key}' has no corresponding package entry in 'packages'."
        )
