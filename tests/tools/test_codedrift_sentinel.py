from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


def _load_sentinel_module():
    module_path = (
        Path(__file__).resolve().parents[2]
        / ".codex/skills/code-drift-sentinel/scripts/codedrift_sentinel.py"
    )
    spec = importlib.util.spec_from_file_location("codedrift_sentinel_module", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_iter_removed_lines_by_file_respects_filters() -> None:
    sentinel = _load_sentinel_module()
    diff_text = "\n".join(
        [
            "diff --git a/artifacts/drift_report.md b/artifacts/drift_report.md",
            "--- a/artifacts/drift_report.md",
            "+++ b/artifacts/drift_report.md",
            "-def test_from_report_text():",
            "diff --git a/tests/unit/test_example.py b/tests/unit/test_example.py",
            "--- a/tests/unit/test_example.py",
            "+++ b/tests/unit/test_example.py",
            "-def test_real_case():",
        ]
    )

    removed = sentinel.iter_removed_lines_by_file(
        diff_text,
        include_patterns=sentinel.TEST_PATH_PATTERNS,
        exclude_patterns=sentinel.DEFAULT_EXCLUDED_PATH_PATTERNS,
    )

    assert removed == [("tests/unit/test_example.py", "-def test_real_case():")]


def test_detect_behavioral_and_test_drift_ignores_artifact_noise() -> None:
    sentinel = _load_sentinel_module()
    diff_text = "\n".join(
        [
            "diff --git a/artifacts/drift_report.md b/artifacts/drift_report.md",
            "--- a/artifacts/drift_report.md",
            "+++ b/artifacts/drift_report.md",
            "-def test_from_report_text():",
        ]
    )

    findings = sentinel.detect_behavioral_and_test_drift(
        changed_files=["artifacts/drift_report.md"],
        quality_results=[],
        compare_diff=diff_text,
    )

    assert not any(item.title == "Test cases removed" for item in findings)


def test_detect_config_infra_drift_uses_config_file_removed_lines_only() -> None:
    sentinel = _load_sentinel_module()

    diff_non_config = "\n".join(
        [
            "diff --git a/tasks/example.py b/tasks/example.py",
            "--- a/tasks/example.py",
            "+++ b/tasks/example.py",
            "-lint should not escalate config severity here",
            "diff --git a/deploy/job_x.yaml b/deploy/job_x.yaml",
            "--- a/deploy/job_x.yaml",
            "+++ b/deploy/job_x.yaml",
            "-name: SOME_ENV",
        ]
    )
    findings_non_config = sentinel.detect_config_infra_drift(
        changed_files=["tasks/example.py", "deploy/job_x.yaml"],
        compare_diff=diff_non_config,
        recent_log="",
    )
    config_change = next(item for item in findings_non_config if item.title == "Configuration/infra files changed")
    assert config_change.severity == "medium"

    diff_config_gate = "\n".join(
        [
            "diff --git a/.github/workflows/checks.yml b/.github/workflows/checks.yml",
            "--- a/.github/workflows/checks.yml",
            "+++ b/.github/workflows/checks.yml",
            "-      - name: lint",
        ]
    )
    findings_config_gate = sentinel.detect_config_infra_drift(
        changed_files=[".github/workflows/checks.yml"],
        compare_diff=diff_config_gate,
        recent_log="",
    )
    config_change_gate = next(item for item in findings_config_gate if item.title == "Configuration/infra files changed")
    assert config_change_gate.severity == "high"


def test_detect_behavioral_and_test_drift_flags_speculative_null_guard() -> None:
    sentinel = _load_sentinel_module()
    diff_text = "\n".join(
        [
            "diff --git a/ui/src/example.tsx b/ui/src/example.tsx",
            "--- a/ui/src/example.tsx",
            "+++ b/ui/src/example.tsx",
            "+const metadataUpdatedAt = metadata?.metadataSource === 'artifact' ? metadata.computedAt || null : null;",
        ]
    )

    findings = sentinel.detect_behavioral_and_test_drift(
        changed_files=["ui/src/example.tsx"],
        quality_results=[],
        compare_diff=diff_text,
        detection_config=sentinel.DEFAULT_CONFIG.get("detection", {}),
    )

    safeguard_finding = next(
        (item for item in findings if item.title == "Speculative safeguard or placeholder fallback introduced"),
        None,
    )
    assert safeguard_finding is not None
    assert "ui/src/example.tsx" in safeguard_finding.files


def test_detect_behavioral_and_test_drift_flags_placeholder_fallback_literal() -> None:
    sentinel = _load_sentinel_module()
    diff_text = "\n".join(
        [
            "diff --git a/ui/src/example.tsx b/ui/src/example.tsx",
            "--- a/ui/src/example.tsx",
            "+++ b/ui/src/example.tsx",
            "+const metadataUpdatedDisplay = metadataUpdatedAt ? formatMetadataTimestamp(metadataUpdatedAt) : 'N/A';",
        ]
    )

    findings = sentinel.detect_behavioral_and_test_drift(
        changed_files=["ui/src/example.tsx"],
        quality_results=[],
        compare_diff=diff_text,
        detection_config=sentinel.DEFAULT_CONFIG.get("detection", {}),
    )

    safeguard_finding = next(
        (item for item in findings if item.title == "Speculative safeguard or placeholder fallback introduced"),
        None,
    )
    assert safeguard_finding is not None
    assert any("N/A" in evidence for evidence in safeguard_finding.evidence)
