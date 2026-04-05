#!/usr/bin/env python3
"""Code Drift Sentinel.

Detect, explain, and optionally remediate code drift caused by multi-agent and
human collaboration.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import fnmatch
import json
import os
import pathlib
import re
import subprocess
import sys
import textwrap
import time
from collections import Counter, defaultdict
from typing import Any

import yaml

DEFAULT_CATEGORY_WEIGHTS = {
    "security": 40,
    "api": 35,
    "architecture": 25,
    "behavioral": 25,
    "dependency": 20,
    "test": 25,
    "performance": 15,
    "style": 5,
    "docs": 3,
    "config_infra": 15,
}

SEVERITY_MULTIPLIER = {
    "low": 0.5,
    "medium": 1.0,
    "high": 1.5,
    "critical": 2.0,
}

REMEDIATION_PRIORITY = {
    "security": 1,
    "behavioral": 1,
    "api": 2,
    "architecture": 3,
    "dependency": 4,
    "test": 5,
    "performance": 5,
    "style": 6,
    "docs": 6,
    "config_infra": 4,
}

DEFAULT_CONFIG: dict[str, Any] = {
    "baseline": {"branch": None, "tag": None, "commit": None},
    "standards": {
        "format": [],
        "lint": [],
        "typecheck": [],
        "test_fast": [],
        "test_full": [],
        "security": [],
        "benchmark": [],
        "format_fix": [],
        "lint_fix": [],
    },
    "architecture": {
        "layers": [],
        "module_boundaries": [],
        "blessed_patterns": [],
    },
    "dependencies": {
        "allowlist": [],
        "denylist": [],
        "lockfile_required": False,
        "license_policy": {},
    },
    "api": {
        "public_globs": [],
        "breaking_change_policy": "warn",
    },
    "thresholds": {
        "drift_score_fail": 35,
        "category_severity_fail": {},
        "category_weights": {},
    },
    "auto_remediate": {
        "enabled": False,
        "max_files_changed": 50,
        "safe_directories": [],
        "commands": [],
    },
    "risk_controls": {
        "protected_globs": [],
        "require_tests_passing_before_automerge": True,
    },
    "reporting": {
        "markdown_path": "artifacts/drift_report.md",
        "json_path": "artifacts/drift_report.json",
        "patch_path": "artifacts/drift_remediation.patch",
    },
    "detection": {
        "lookback_days": 14,
        "exclude_globs": [],
        "speculative_safeguards": {
            "enabled": True,
            "patterns": [
                r"\?.*:\s*(?:None|null|False)\b",
                r"\|\|\s*(?:null|undefined)\b",
                r"\?\?\s*(?:null|undefined)\b",
                r"\breturn\s+(?:None|null|False)\b",
                r"\?.*:\s*['\"](?:N/?A|unknown|no data|no-data|unavailable|not available)['\"]",
                r"\|\|\s*['\"](?:N/?A|unknown|no data|no-data|unavailable|not available)['\"]",
                r"\?\?\s*['\"](?:N/?A|unknown|no data|no-data|unavailable|not available)['\"]",
                r"\breturn\s+['\"](?:N/?A|unknown|no data|no-data|unavailable|not available)['\"]",
            ],
        },
    },
}

LOCKFILE_PATTERNS = [
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "poetry.lock",
    "requirements.lock.txt",
    "Pipfile.lock",
    "uv.lock",
]

MANIFEST_PATTERNS = [
    "package.json",
    "pyproject.toml",
    "requirements.txt",
    "requirements-dev.txt",
    "Pipfile",
]

TEST_PATH_PATTERNS = [
    "tests/**",
    "**/test_*.py",
    "**/*_test.py",
    "**/*.spec.ts",
    "**/*.test.ts",
    "**/*.spec.tsx",
    "**/*.test.tsx",
    "**/*.spec.js",
    "**/*.test.js",
]

DOC_PATH_PATTERNS = [
    "docs/**",
    "README.md",
    "CHANGELOG.md",
    "**/*.md",
]

CONFIG_PATH_PATTERNS = [
    ".github/workflows/**",
    "deploy/**",
    "infra/**",
    "docker-compose*.yml",
    "docker-compose*.yaml",
    "Dockerfile*",
    "**/*.tf",
    "**/*.tfvars",
    "**/*.yaml",
    "**/*.yml",
]

CODE_PATH_PATTERNS = [
    "**/*.py",
    "**/*.js",
    "**/*.jsx",
    "**/*.ts",
    "**/*.tsx",
    "**/*.go",
    "**/*.java",
    "**/*.cs",
    "**/*.rs",
]

DEFAULT_EXCLUDED_PATH_PATTERNS = [
    "artifacts/**",
]

SIGNATURE_PATTERNS = [
    re.compile(r"\bdef\s+[A-Za-z_][A-Za-z0-9_]*\s*\("),
    re.compile(r"\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*[:(]"),
    re.compile(r"\bexport\s+(async\s+)?function\s+[A-Za-z_][A-Za-z0-9_]*\s*\("),
    re.compile(r"\bfunction\s+[A-Za-z_][A-Za-z0-9_]*\s*\("),
    re.compile(r"\binterface\s+[A-Za-z_][A-Za-z0-9_]*\b"),
    re.compile(r"\btype\s+[A-Za-z_][A-Za-z0-9_]*\s*="),
    re.compile(r"\bpublic\s+[A-Za-z_][A-Za-z0-9_<>\[\]]*\s+[A-Za-z_][A-Za-z0-9_]*\s*\("),
]

IMPORT_PATTERNS = [
    re.compile(r"^\s*from\s+([A-Za-z0-9_./@-]+)\s+import\s+"),
    re.compile(r"^\s*import\s+([A-Za-z0-9_./@,-]+)"),
    re.compile(r"^\s*import\s+.*\s+from\s+['\"]([^'\"]+)['\"]"),
    re.compile(r"^\s*const\s+.*=\s*require\(['\"]([^'\"]+)['\"]\)"),
]

SECRETS_PATTERNS = {
    "AWS access key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "Hardcoded password": re.compile(r"(?i)password\s*[:=]\s*['\"][^'\"]+['\"]"),
    "Hardcoded API key": re.compile(r"(?i)api[_-]?key\s*[:=]\s*['\"][^'\"]+['\"]"),
    "Private key material": re.compile(r"-----BEGIN (RSA|EC|OPENSSH|DSA) PRIVATE KEY-----"),
}

INSECURE_PATTERNS = {
    "Weak hash usage": re.compile(r"\b(md5|sha1)\s*\("),
    "Weak random for security context": re.compile(r"\brandom\.random\s*\("),
    "SSL verification disabled": re.compile(r"verify\s*=\s*False"),
}

MICRO_ARCH_PATTERNS = {
    "exceptions": re.compile(r"\b(raise|throw\s+new|throw\s+)\b"),
    "result_objects": re.compile(r"\b(Result|Either|is_ok\(|is_err\(|Ok\(|Err\()"),
    "sentinel_returns": re.compile(r"\breturn\s+(None|null|False)\b"),
}


@dataclasses.dataclass
class CommandResult:
    name: str
    command: str
    status: str
    exit_code: int | None
    duration_sec: float
    output_excerpt: str


@dataclasses.dataclass
class Finding:
    category: str
    severity: str
    confidence: float
    title: str
    expected: str
    observed: str
    files: list[str]
    evidence: list[str]
    remediation: str
    risk: str
    verification: list[str]
    attribution: dict[str, list[str]] = dataclasses.field(default_factory=dict)
    score: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "severity": self.severity,
            "confidence": round(self.confidence, 2),
            "title": self.title,
            "expected": self.expected,
            "observed": self.observed,
            "files": sorted(set(self.files)),
            "evidence": self.evidence,
            "remediation": self.remediation,
            "risk": self.risk,
            "verification": self.verification,
            "attribution": self.attribution,
            "score": round(self.score, 2),
        }


def trim_output(text: str, max_lines: int = 60, max_chars: int = 8000) -> str:
    if not text:
        return ""
    text = text.strip("\n")
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[:max_lines] + ["... (trimmed)"]
    clipped = "\n".join(lines)
    if len(clipped) > max_chars:
        clipped = clipped[:max_chars] + "\n... (trimmed)"
    return clipped


def run_list_command(cmd: list[str], cwd: pathlib.Path, timeout: int = 180) -> tuple[int, str, str]:
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def run_shell_command(command: str, cwd: pathlib.Path, timeout: int = 1800) -> CommandResult:
    started = time.perf_counter()
    if os.name == "nt":
        shell_command = ["powershell.exe", "-NoProfile", "-Command", command]
    else:
        shell_command = [os.environ.get("SHELL", "/bin/bash"), "-lc", command]

    proc = subprocess.run(
        shell_command,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout,
    )
    elapsed = time.perf_counter() - started
    combined = "\n".join(part for part in [proc.stdout, proc.stderr] if part)

    status = "passed"
    if proc.returncode != 0:
        if proc.returncode == 127 or "not found" in combined.lower():
            status = "unavailable"
        else:
            status = "failed"

    return CommandResult(
        name="",
        command=command,
        status=status,
        exit_code=proc.returncode,
        duration_sec=round(elapsed, 2),
        output_excerpt=trim_output(combined),
    )


def git_output(repo: pathlib.Path, args: list[str], timeout: int = 120) -> str:
    code, out, err = run_list_command(["git", *args], cwd=repo, timeout=timeout)
    if code != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {err.strip() or out.strip()}")
    return out.strip()


def git_maybe_output(repo: pathlib.Path, args: list[str], timeout: int = 120) -> str:
    code, out, _ = run_list_command(["git", *args], cwd=repo, timeout=timeout)
    if code != 0:
        return ""
    return out.strip()


def git_ref_exists(repo: pathlib.Path, ref: str) -> bool:
    code, _, _ = run_list_command(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd=repo)
    return code == 0


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(repo: pathlib.Path, config_path: str) -> tuple[dict[str, Any], list[str]]:
    issues: list[str] = []
    resolved = repo / config_path
    if not resolved.exists():
        issues.append(f"Configuration file missing: {config_path}. Using built-in defaults.")
        return dict(DEFAULT_CONFIG), issues

    try:
        loaded = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # pylint: disable=broad-except
        issues.append(f"Failed to parse {config_path}: {exc}. Using defaults.")
        return dict(DEFAULT_CONFIG), issues

    if not isinstance(loaded, dict):
        issues.append(f"Configuration at {config_path} is not a YAML mapping. Using defaults.")
        return dict(DEFAULT_CONFIG), issues

    merged = deep_merge(DEFAULT_CONFIG, loaded)
    return merged, issues


def path_matches(path: str, patterns: list[str]) -> bool:
    posix_path = pathlib.PurePosixPath(path)
    for pattern in patterns:
        if posix_path.match(pattern):
            return True
        if fnmatch.fnmatch(path, pattern):
            return True
    return False


def is_excluded_path(path: str, exclude_globs: list[str] | None) -> bool:
    patterns = list(DEFAULT_EXCLUDED_PATH_PATTERNS)
    if exclude_globs:
        patterns.extend(str(item) for item in exclude_globs if str(item).strip())
    return path_matches(path, patterns)


def normalize_import_target(value: str) -> str:
    value = value.strip().strip("\"'")
    if not value or value.startswith("."):
        return ""
    if value.startswith("@/"):
        return f"src/{value[2:]}"
    if value.startswith("/"):
        return value[1:]
    if "/" in value:
        return value
    if "." in value and value.count(".") >= 1:
        return value.replace(".", "/")
    return value


def read_file_if_exists(path: pathlib.Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except Exception:  # pylint: disable=broad-except
        return ""


def safe_json_load(text: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        return {}
    return {}


def parse_requirement_deps(text: str) -> set[str]:
    deps: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        if ";" in line:
            line = line.split(";", 1)[0].strip()
        name = re.split(r"[<>=~!\[]", line, maxsplit=1)[0].strip()
        if name:
            deps.add(name.lower())
    return deps


def parse_pyproject_deps(text: str) -> set[str]:
    deps: set[str] = set()
    in_dep_block = False
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("dependencies") and "[" in line:
            in_dep_block = True
            continue
        if in_dep_block:
            if line.startswith("]"):
                in_dep_block = False
                continue
            match = re.match(r'"([^\"\s]+)', line)
            if match:
                name = re.split(r"[<>=~!\[]", match.group(1), maxsplit=1)[0].strip()
                if name:
                    deps.add(name.lower())

        ptool = re.match(r"^([A-Za-z0-9_.-]+)\s*=\s*\"[^\"]+\"", line)
        if ptool and "tool.poetry.dependencies" in text:
            candidate = ptool.group(1)
            if candidate not in {"python"}:
                deps.add(candidate.lower())
    return deps


def parse_dependencies_for_file(path: str, content: str) -> set[str]:
    if path.endswith("package.json"):
        data = safe_json_load(content)
        deps = set()
        for key in ["dependencies", "devDependencies", "peerDependencies", "optionalDependencies"]:
            section = data.get(key, {})
            if isinstance(section, dict):
                deps.update({str(dep).lower() for dep in section.keys()})
        return deps
    if path.endswith("pyproject.toml"):
        return parse_pyproject_deps(content)
    if "requirements" in path and path.endswith(".txt"):
        return parse_requirement_deps(content)
    return set()


def parse_imports(content: str) -> set[str]:
    imports: set[str] = set()
    for line in content.splitlines():
        for pattern in IMPORT_PATTERNS:
            match = pattern.search(line)
            if not match:
                continue
            raw_target = match.group(1).split(",", 1)[0].strip()
            target = normalize_import_target(raw_target)
            if target:
                imports.add(target)
    return imports


def get_diff(repo: pathlib.Path, compare_from: str, compare_to: str, file_path: str | None = None) -> str:
    args = ["diff", "--unified=3", f"{compare_from}..{compare_to}"]
    if file_path:
        args.extend(["--", file_path])
    return git_maybe_output(repo, args, timeout=240)


def get_working_tree_diff(repo: pathlib.Path) -> str:
    unstaged = git_maybe_output(repo, ["diff", "--unified=3"], timeout=240)
    staged = git_maybe_output(repo, ["diff", "--cached", "--unified=3"], timeout=240)
    parts = [part for part in [unstaged, staged] if part]
    return "\n\n".join(parts)


def extract_hunks(diff_text: str, max_hunks: int = 2, max_lines_per_hunk: int = 18) -> list[str]:
    if not diff_text:
        return []
    lines = diff_text.splitlines()
    hunks: list[str] = []
    idx = 0
    while idx < len(lines) and len(hunks) < max_hunks:
        if not lines[idx].startswith("@@"):
            idx += 1
            continue
        start = idx
        idx += 1
        captured = [lines[start]]
        line_count = 0
        while idx < len(lines) and not lines[idx].startswith("@@"):
            captured.append(lines[idx])
            idx += 1
            line_count += 1
            if line_count >= max_lines_per_hunk:
                captured.append("... (hunk trimmed)")
                while idx < len(lines) and not lines[idx].startswith("@@"):
                    idx += 1
                break
        hunks.append("\n".join(captured))
    return hunks


def read_file_from_ref(repo: pathlib.Path, ref: str, file_path: str) -> str:
    return git_maybe_output(repo, ["show", f"{ref}:{file_path}"], timeout=120)


def git_porcelain(repo: pathlib.Path) -> list[str]:
    out = git_maybe_output(repo, ["status", "--porcelain"], timeout=60)
    return [line for line in out.splitlines() if line.strip()]


def parse_changed_files_from_porcelain(lines: list[str]) -> tuple[list[str], list[str]]:
    tracked: list[str] = []
    untracked: list[str] = []
    for line in lines:
        if len(line) < 4:
            continue
        status = line[:2]
        path = line[3:]
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        if status == "??":
            untracked.append(path)
        else:
            tracked.append(path)
    return sorted(set(tracked)), sorted(set(untracked))


def resolve_baseline(repo: pathlib.Path, config: dict[str, Any], cli_override: str | None) -> tuple[str, str]:
    baseline_cfg = config.get("baseline", {}) or {}
    candidates: list[tuple[str, str]] = []

    if cli_override:
        candidates.append((cli_override, "CLI override"))

    if baseline_cfg.get("commit"):
        candidates.append((str(baseline_cfg["commit"]), "configured baseline.commit"))
    if baseline_cfg.get("tag"):
        candidates.append((str(baseline_cfg["tag"]), "configured baseline.tag"))
    if baseline_cfg.get("branch"):
        branch = str(baseline_cfg["branch"])
        candidates.append((branch, "configured baseline.branch"))
        candidates.append((f"origin/{branch}", "configured baseline.branch (origin)"))

    candidates.extend(
        [
            ("main", "default main"),
            ("origin/main", "default origin/main"),
            ("master", "fallback master"),
            ("origin/master", "fallback origin/master"),
        ]
    )

    for ref, reason in candidates:
        if ref and git_ref_exists(repo, ref):
            return ref, reason

    latest_tag = git_maybe_output(repo, ["describe", "--tags", "--abbrev=0"], timeout=30)
    if latest_tag and git_ref_exists(repo, latest_tag):
        return latest_tag, "latest release tag"

    first_commit = git_output(repo, ["rev-list", "--max-parents=0", "HEAD"], timeout=30).splitlines()[0]
    return first_commit, "repository root commit fallback"


def resolve_compare_refs(
    repo: pathlib.Path,
    baseline_ref: str,
    force_ci: bool,
    pr_head: str | None,
) -> tuple[str, str, bool]:
    head_ref = pr_head or os.getenv("CODEDRIFT_PR_HEAD") or os.getenv("PR_HEAD") or "HEAD"
    if not git_ref_exists(repo, head_ref):
        head_ref = "HEAD"

    ci_detected = force_ci or (os.getenv("CI", "").lower() in {"1", "true", "yes"})
    compare_from = baseline_ref
    compare_to = head_ref

    if ci_detected:
        merge_base = git_maybe_output(repo, ["merge-base", head_ref, baseline_ref], timeout=30)
        if merge_base:
            compare_from = merge_base
        compare_to = head_ref

    return compare_from, compare_to, ci_detected


def list_changed_files(
    repo: pathlib.Path,
    compare_from: str,
    compare_to: str,
    include_worktree: bool,
    exclude_globs: list[str] | None = None,
) -> list[str]:
    changed = set(
        line.strip()
        for line in git_maybe_output(repo, ["diff", "--name-only", f"{compare_from}..{compare_to}"], timeout=120).splitlines()
        if line.strip()
    )

    if include_worktree:
        changed.update(
            line.strip()
            for line in git_maybe_output(repo, ["diff", "--name-only"], timeout=60).splitlines()
            if line.strip()
        )
        changed.update(
            line.strip()
            for line in git_maybe_output(repo, ["diff", "--cached", "--name-only"], timeout=60).splitlines()
            if line.strip()
        )

    kept = [path for path in sorted(changed) if not is_excluded_path(path, exclude_globs)]
    return kept


def iter_removed_lines_by_file(
    diff_text: str,
    *,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> list[tuple[str, str]]:
    removed: list[tuple[str, str]] = []
    current_file: str | None = None

    for raw_line in diff_text.splitlines():
        line = raw_line.rstrip("\n")
        if line.startswith("diff --git "):
            current_file = None
            continue
        if line.startswith("+++ "):
            candidate = line[4:].strip()
            if candidate.startswith("b/"):
                candidate = candidate[2:]
            if not candidate or candidate == "/dev/null":
                current_file = None
                continue
            current_file = candidate
            continue
        if not line.startswith("-") or line.startswith("---"):
            continue
        if not current_file:
            continue
        if include_patterns and not path_matches(current_file, include_patterns):
            continue
        if exclude_patterns and path_matches(current_file, exclude_patterns):
            continue
        removed.append((current_file, line))

    return removed


def iter_added_lines_by_file(
    diff_text: str,
    *,
    include_patterns: list[str] | None = None,
    exclude_patterns: list[str] | None = None,
) -> list[tuple[str, str]]:
    added: list[tuple[str, str]] = []
    current_file: str | None = None

    for raw_line in diff_text.splitlines():
        line = raw_line.rstrip("\n")
        if line.startswith("diff --git "):
            current_file = None
            continue
        if line.startswith("+++ "):
            candidate = line[4:].strip()
            if candidate.startswith("b/"):
                candidate = candidate[2:]
            if not candidate or candidate == "/dev/null":
                current_file = None
                continue
            current_file = candidate
            continue
        if not line.startswith("+") or line.startswith("+++"):
            continue
        if not current_file:
            continue
        if include_patterns and not path_matches(current_file, include_patterns):
            continue
        if exclude_patterns and path_matches(current_file, exclude_patterns):
            continue
        added.append((current_file, line))

    return added


def collect_recent_log(repo: pathlib.Path, lookback_days: int) -> str:
    return git_maybe_output(
        repo,
        [
            "log",
            f"--since={lookback_days} days ago",
            "--pretty=format:%h|%an|%ad|%s",
            "--date=short",
            "--name-only",
        ],
        timeout=120,
    )


def run_quality_gates(
    repo: pathlib.Path,
    config: dict[str, Any],
    mode: str,
    skip: bool,
    include_full_tests: bool,
) -> list[CommandResult]:
    if skip:
        return [
            CommandResult(
                name="quality-gates",
                command="<skipped>",
                status="skipped",
                exit_code=None,
                duration_sec=0.0,
                output_excerpt="Skipped by --skip-quality-gates",
            )
        ]

    standards = config.get("standards", {}) or {}
    queue: list[tuple[str, str]] = []
    for key in ["format", "lint", "typecheck", "test_fast", "security", "benchmark"]:
        for command in standards.get(key, []) or []:
            queue.append((key, command))

    if mode == "auto-remediate" or include_full_tests:
        for command in standards.get("test_full", []) or []:
            queue.append(("test_full", command))

    if not queue:
        return [
            CommandResult(
                name="quality-gates",
                command="<none>",
                status="unavailable",
                exit_code=None,
                duration_sec=0.0,
                output_excerpt="No standards commands configured.",
            )
        ]

    results: list[CommandResult] = []
    for name, command in queue:
        result = run_shell_command(command=command, cwd=repo)
        result.name = name
        results.append(result)
    return results


def detect_style_drift(
    repo: pathlib.Path,
    compare_from: str,
    compare_to: str,
    quality_results: list[CommandResult],
) -> list[Finding]:
    findings: list[Finding] = []

    diff_check = git_maybe_output(repo, ["diff", "--check", f"{compare_from}..{compare_to}"], timeout=120)
    if diff_check:
        findings.append(
            Finding(
                category="style",
                severity="medium",
                confidence=0.9,
                title="Diff introduces formatting or whitespace violations",
                expected="Formatting and whitespace should remain consistent with project standards.",
                observed="`git diff --check` reported whitespace/style violations.",
                files=sorted({line.split(":", 1)[0] for line in diff_check.splitlines() if ":" in line}),
                evidence=diff_check.splitlines()[:8],
                remediation="Run formatter and lint auto-fix commands, then re-check with `git diff --check`.",
                risk="low",
                verification=["git diff --check"],
            )
        )

    lint_failures = [res for res in quality_results if res.name in {"format", "lint"} and res.status == "failed"]
    if lint_failures:
        evidence = []
        for failure in lint_failures[:3]:
            evidence.append(f"{failure.name}: exit {failure.exit_code}")
            if failure.output_excerpt:
                evidence.extend(failure.output_excerpt.splitlines()[:3])
        findings.append(
            Finding(
                category="style",
                severity="medium",
                confidence=0.85,
                title="Configured style gates failed",
                expected="Formatter/linter checks should pass relative to baseline standards.",
                observed="One or more configured style commands failed.",
                files=[],
                evidence=evidence[:12],
                remediation="Address formatter/linter violations and keep commands in `.codedrift.yml` deterministic.",
                risk="low",
                verification=[res.command for res in lint_failures[:3]],
            )
        )

    return findings


def detect_architecture_drift(
    repo: pathlib.Path,
    changed_files: list[str],
    architecture_cfg: dict[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []
    layers = architecture_cfg.get("layers", []) or []
    if not layers:
        return findings

    violations: list[tuple[str, str, str]] = []
    for layer in layers:
        path_glob = str(layer.get("path_glob", "")).strip()
        if not path_glob:
            continue

        forbid = [str(p) for p in (layer.get("forbid_imports_from", []) or [])]
        allow = [str(p) for p in (layer.get("allow_imports_from", []) or [])]

        for file_path in changed_files:
            if not path_matches(file_path, [path_glob]):
                continue
            content = read_file_if_exists(repo / file_path)
            if not content:
                continue

            imports = parse_imports(content)
            for imported in imports:
                if forbid and path_matches(imported, forbid):
                    violations.append((file_path, imported, "forbidden"))
                if allow and imported.startswith("src/") and not path_matches(imported, allow):
                    violations.append((file_path, imported, "outside_allowlist"))

    if violations:
        evidence = [
            f"{file_path}: imports `{imported}` ({reason})"
            for file_path, imported, reason in violations[:20]
        ]
        findings.append(
            Finding(
                category="architecture",
                severity="high",
                confidence=0.82,
                title="Layering rule violations detected",
                expected="Changed modules should respect configured architecture layer import rules.",
                observed=f"Found {len(violations)} import(s) violating layer constraints.",
                files=sorted({item[0] for item in violations}),
                evidence=evidence,
                remediation="Move cross-layer logic to approved boundaries or introduce shared abstractions allowed by the layer contract.",
                risk="medium",
                verification=["Re-run code drift audit after refactor", "Run lint/typecheck/tests"],
            )
        )

    return findings


def detect_api_drift(
    repo: pathlib.Path,
    changed_files: list[str],
    compare_from: str,
    compare_to: str,
    api_cfg: dict[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []
    public_globs = [str(item) for item in (api_cfg.get("public_globs", []) or [])]
    if not public_globs:
        return findings

    breaking_policy = str(api_cfg.get("breaking_change_policy", "warn")).lower()
    impacted: list[str] = []
    removed_signatures: list[str] = []
    added_signatures: list[str] = []

    for file_path in changed_files:
        if not path_matches(file_path, public_globs):
            continue
        diff = get_diff(repo, compare_from, compare_to, file_path)
        if not diff:
            continue

        impacted.append(file_path)
        for line in diff.splitlines():
            if not line or line[0] not in {"+", "-"}:
                continue
            text = line[1:].strip()
            if text.startswith("+++") or text.startswith("---"):
                continue
            if any(pattern.search(text) for pattern in SIGNATURE_PATTERNS):
                if line.startswith("-"):
                    removed_signatures.append(f"{file_path}: {text}")
                elif line.startswith("+"):
                    added_signatures.append(f"{file_path}: {text}")

    if impacted and (removed_signatures or added_signatures):
        severity = "high"
        if removed_signatures and breaking_policy == "strict":
            severity = "critical"
        elif not removed_signatures:
            severity = "medium"

        evidence = removed_signatures[:10] + added_signatures[:10]
        findings.append(
            Finding(
                category="api",
                severity=severity,
                confidence=0.78,
                title="Public API surface changed",
                expected="Public interfaces should remain stable unless explicit breaking change policy allows a change.",
                observed=(
                    f"Detected {len(removed_signatures)} removed and {len(added_signatures)} added signature-like changes "
                    "in public API files."
                ),
                files=sorted(set(impacted)),
                evidence=evidence,
                remediation="Version or adapt API changes, add compatibility shims, and document contract updates.",
                risk="high" if severity in {"high", "critical"} else "medium",
                verification=["Run contract/unit tests", "Review public API docs and changelog"],
            )
        )

    return findings


def detect_dependency_drift(
    repo: pathlib.Path,
    changed_files: list[str],
    compare_from: str,
    dependencies_cfg: dict[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []
    allowlist = {str(item).lower() for item in (dependencies_cfg.get("allowlist", []) or [])}
    denylist = {str(item).lower() for item in (dependencies_cfg.get("denylist", []) or [])}
    lockfile_required = bool(dependencies_cfg.get("lockfile_required", False))

    manifest_files = [path for path in changed_files if any(path.endswith(name) for name in MANIFEST_PATTERNS)]
    lockfile_files = [path for path in changed_files if any(path.endswith(name) for name in LOCKFILE_PATTERNS)]

    added_deps: dict[str, list[str]] = defaultdict(list)
    denied: dict[str, list[str]] = defaultdict(list)
    outside_allowlist: dict[str, list[str]] = defaultdict(list)

    for manifest in manifest_files:
        before_content = read_file_from_ref(repo, compare_from, manifest)
        after_content = read_file_if_exists(repo / manifest)
        before_deps = parse_dependencies_for_file(manifest, before_content)
        after_deps = parse_dependencies_for_file(manifest, after_content)
        for dep in sorted(after_deps - before_deps):
            added_deps[manifest].append(dep)
            if dep in denylist:
                denied[manifest].append(dep)
            if allowlist and dep not in allowlist:
                outside_allowlist[manifest].append(dep)

    if denied:
        evidence = [
            f"{manifest}: {', '.join(sorted(set(deps)))}"
            for manifest, deps in sorted(denied.items())
        ]
        findings.append(
            Finding(
                category="dependency",
                severity="critical",
                confidence=0.95,
                title="Denied dependencies introduced",
                expected="Dependencies on denylisted packages must not be introduced.",
                observed="One or more denylisted dependencies were added.",
                files=sorted(denied.keys()),
                evidence=evidence,
                remediation="Remove denylisted dependencies and replace with approved alternatives.",
                risk="high",
                verification=["Re-run dependency policy checks", "Run full test suite"],
            )
        )

    if outside_allowlist:
        evidence = [
            f"{manifest}: {', '.join(sorted(set(deps)))}"
            for manifest, deps in sorted(outside_allowlist.items())
        ]
        findings.append(
            Finding(
                category="dependency",
                severity="high",
                confidence=0.88,
                title="Dependencies outside allowlist introduced",
                expected="New dependencies should come from the configured allowlist or explicit approval flow.",
                observed="Found dependencies not present in allowlist.",
                files=sorted(outside_allowlist.keys()),
                evidence=evidence,
                remediation="Drop unapproved dependencies or explicitly update allowlist with review sign-off.",
                risk="medium",
                verification=["Review dependency approval records", "Re-run code drift audit"],
            )
        )

    if lockfile_required and manifest_files and not lockfile_files:
        findings.append(
            Finding(
                category="dependency",
                severity="medium",
                confidence=0.92,
                title="Dependency manifests changed without lockfile updates",
                expected="Lockfile should change with dependency manifest updates when lockfile policy is enabled.",
                observed="Manifest files changed but no known lockfile changed.",
                files=sorted(manifest_files),
                evidence=["Manifest files: " + ", ".join(sorted(manifest_files))],
                remediation="Regenerate lockfile and commit it with dependency changes.",
                risk="low",
                verification=["Run package manager install/lock", "Re-run dependency check"],
            )
        )

    if added_deps and not denied and not outside_allowlist:
        evidence = [f"{manifest}: {', '.join(deps)}" for manifest, deps in sorted(added_deps.items())]
        findings.append(
            Finding(
                category="dependency",
                severity="low",
                confidence=0.7,
                title="Dependencies changed",
                expected="Dependency changes should be intentional, minimal, and policy-aligned.",
                observed="New dependencies were detected compared to baseline.",
                files=sorted(added_deps.keys()),
                evidence=evidence[:8],
                remediation="Confirm necessity, ownership, and long-term maintenance burden for new dependencies.",
                risk="low",
                verification=["Run tests", "Check security/license policy"],
            )
        )

    return findings


def detect_behavioral_and_test_drift(
    changed_files: list[str],
    quality_results: list[CommandResult],
    compare_diff: str,
    detection_config: dict[str, Any] | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    test_definition_pattern = re.compile(r"\b(def\s+test_|(?:it|test)(?:\.each)?\()")

    test_failures = [result for result in quality_results if result.name in {"test_fast", "test_full"} and result.status == "failed"]
    if test_failures:
        evidence: list[str] = []
        for failure in test_failures[:2]:
            evidence.append(f"{failure.name}: exit {failure.exit_code}")
            evidence.extend(failure.output_excerpt.splitlines()[:8])
        findings.append(
            Finding(
                category="behavioral",
                severity="high",
                confidence=0.93,
                title="Regression signal from test failures",
                expected="Tests should pass against the updated codebase and baseline behavior.",
                observed="Configured test commands reported failures.",
                files=[],
                evidence=evidence[:16],
                remediation="Fix failing behavior and add or update tests to lock expected semantics.",
                risk="high",
                verification=[failure.command for failure in test_failures],
            )
        )

    added_test_files: set[str] = set()
    added_test_lines = iter_added_lines_by_file(
        compare_diff,
        include_patterns=TEST_PATH_PATTERNS,
        exclude_patterns=DEFAULT_EXCLUDED_PATH_PATTERNS,
    )
    for file_path, added_line in added_test_lines:
        if test_definition_pattern.search(added_line):
            added_test_files.add(file_path)

    removed_test_markers: list[str] = []
    removed_test_files: set[str] = set()
    removed_test_lines = iter_removed_lines_by_file(
        compare_diff,
        include_patterns=TEST_PATH_PATTERNS,
        exclude_patterns=DEFAULT_EXCLUDED_PATH_PATTERNS,
    )
    for file_path, removed_line in removed_test_lines:
        if test_definition_pattern.search(removed_line) and file_path not in added_test_files:
            removed_test_files.add(file_path)
            removed_test_markers.append(f"{file_path}: {removed_line}")

    if removed_test_markers:
        findings.append(
            Finding(
                category="test",
                severity="high",
                confidence=0.8,
                title="Test cases removed",
                expected="Coverage should not regress for changed behavior.",
                observed="Detected removed test definitions/assertions in diff.",
                files=sorted(removed_test_files),
                evidence=removed_test_markers[:12],
                remediation="Restore removed tests or add equivalent coverage for modified behavior.",
                risk="medium",
                verification=["Run fast and full tests", "Check coverage trend"],
            )
        )

    code_changed = [f for f in changed_files if path_matches(f, CODE_PATH_PATTERNS) and not path_matches(f, TEST_PATH_PATTERNS)]
    tests_changed = [f for f in changed_files if path_matches(f, TEST_PATH_PATTERNS)]
    if code_changed and not tests_changed:
        findings.append(
            Finding(
                category="test",
                severity="medium",
                confidence=0.72,
                title="Code changed without nearby test updates",
                expected="Behavioral code changes should include corresponding test updates where relevant.",
                observed="Detected source changes without matching test file changes.",
                files=code_changed[:20],
                evidence=["Changed non-test files: " + ", ".join(code_changed[:15])],
                remediation="Add targeted unit tests for modified modules and critical edge cases.",
                risk="medium",
                verification=["Run fast tests", "Review changed modules for edge cases"],
            )
        )

    speculative_cfg = (detection_config or {}).get("speculative_safeguards", {}) or {}
    speculative_enabled = speculative_cfg.get("enabled", True)
    if speculative_enabled:
        pattern_values = speculative_cfg.get("patterns") or DEFAULT_CONFIG["detection"]["speculative_safeguards"]["patterns"]
        compiled_patterns: list[re.Pattern[str]] = []
        for value in pattern_values:
            pattern_text = str(value or "").strip()
            if not pattern_text:
                continue
            try:
                compiled_patterns.append(re.compile(pattern_text, re.IGNORECASE))
            except re.error:
                continue

        suspicious_hits: list[str] = []
        suspicious_files: set[str] = set()
        for file_path, added_line in iter_added_lines_by_file(
            compare_diff,
            include_patterns=CODE_PATH_PATTERNS,
            exclude_patterns=[*TEST_PATH_PATTERNS, *DOC_PATH_PATTERNS, *DEFAULT_EXCLUDED_PATH_PATTERNS],
        ):
            payload = added_line[1:].strip()
            if not payload or re.match(r"^(#|//|/\*|\*)", payload):
                continue
            if any(pattern.search(payload) for pattern in compiled_patterns):
                suspicious_files.add(file_path)
                suspicious_hits.append(f"{file_path}: {payload[:180]}")

        if suspicious_hits:
            findings.append(
                Finding(
                    category="behavioral",
                    severity="medium",
                    confidence=0.78,
                    title="Speculative safeguard or placeholder fallback introduced",
                    expected="Behavior changes should be requirement-driven; avoid precautionary guards or placeholder fallbacks that alter semantics without explicit product intent.",
                    observed="Added code introduces sentinel/null branches or placeholder fallback copy that can hide data or change behavior defensively.",
                    files=sorted(suspicious_files),
                    evidence=suspicious_hits[:12],
                    remediation="Remove speculative safeguards unless the requirement explicitly calls for them. If missing-data behavior is required, use approved copy and cover it with tests.",
                    risk="medium",
                    verification=[
                        "Review the requirement for missing-data behavior",
                        "Add or update tests for the intended behavior",
                        "Re-run the drift audit",
                    ],
                )
            )

    return findings


def detect_security_drift(
    changed_files: list[str],
    combined_diff: str,
    quality_results: list[CommandResult],
    risk_controls: dict[str, Any],
) -> list[Finding]:
    findings: list[Finding] = []

    secret_hits: list[str] = []
    weak_crypto_hits: list[str] = []

    for line in combined_diff.splitlines():
        if not line.startswith("+") or line.startswith("+++"):
            continue
        payload = line[1:]
        for label, pattern in SECRETS_PATTERNS.items():
            if pattern.search(payload):
                secret_hits.append(f"{label}: {payload[:140]}")
        for label, pattern in INSECURE_PATTERNS.items():
            if pattern.search(payload):
                weak_crypto_hits.append(f"{label}: {payload[:140]}")

    if secret_hits:
        findings.append(
            Finding(
                category="security",
                severity="critical",
                confidence=0.97,
                title="Potential secret leakage detected",
                expected="Credentials/secrets should never be committed in plaintext.",
                observed="Added lines match secret-like patterns.",
                files=[],
                evidence=secret_hits[:12],
                remediation="Remove leaked secrets immediately, rotate credentials, and use secret manager references.",
                risk="high",
                verification=["Run secret scanner", "Rotate impacted credentials"],
            )
        )

    if weak_crypto_hits:
        findings.append(
            Finding(
                category="security",
                severity="high",
                confidence=0.88,
                title="Unsafe security patterns introduced",
                expected="Use approved cryptographic and transport security defaults.",
                observed="Added code includes weak hashing/random or disabled SSL verification patterns.",
                files=[],
                evidence=weak_crypto_hits[:10],
                remediation="Replace insecure primitives with approved secure equivalents and enforce defaults.",
                risk="high",
                verification=["Run security scan", "Run relevant unit tests"],
            )
        )

    protected_globs = [str(item) for item in (risk_controls.get("protected_globs", []) or [])]
    if protected_globs:
        touched = [f for f in changed_files if path_matches(f, protected_globs)]
        if touched:
            findings.append(
                Finding(
                    category="security",
                    severity="high",
                    confidence=0.93,
                    title="Protected paths changed",
                    expected="Protected areas require explicit opt-in review before modification.",
                    observed="Files matching protected globs were modified.",
                    files=touched[:20],
                    evidence=["Protected files touched: " + ", ".join(touched[:15])],
                    remediation="Require explicit approver sign-off and run focused regression/security tests.",
                    risk="high",
                    verification=["Manual security review", "Auth/infrastructure regression tests"],
                )
            )

    security_gate_failures = [res for res in quality_results if res.name == "security" and res.status == "failed"]
    if security_gate_failures:
        evidence = []
        for failure in security_gate_failures[:2]:
            evidence.append(f"{failure.command} failed (exit {failure.exit_code})")
            evidence.extend(failure.output_excerpt.splitlines()[:8])
        findings.append(
            Finding(
                category="security",
                severity="critical",
                confidence=0.9,
                title="Security quality gate failed",
                expected="Configured security scanning commands should pass.",
                observed="A configured security command failed.",
                files=[],
                evidence=evidence,
                remediation="Investigate scan findings and remediate before merging.",
                risk="high",
                verification=[failure.command for failure in security_gate_failures],
            )
        )

    return findings


def detect_performance_drift(
    changed_files: list[str],
    compare_diff: str,
    quality_results: list[CommandResult],
) -> list[Finding]:
    findings: list[Finding] = []

    suspicious: list[str] = []
    diff_lines = compare_diff.splitlines()
    for index, line in enumerate(diff_lines):
        if not line.startswith("+") or line.startswith("+++"):
            continue
        payload = line[1:]
        if re.search(r"\bfor\b.+\bin\b", payload):
            window = "\n".join(diff_lines[index : index + 5])
            if re.search(r"(select\s+|\.query\(|requests\.|httpx\.|fetch\(|\.execute\()", window, re.IGNORECASE):
                suspicious.append(payload[:140])

    benchmark_failures = [res for res in quality_results if res.name == "benchmark" and res.status == "failed"]

    if suspicious or benchmark_failures:
        evidence = suspicious[:8]
        for failure in benchmark_failures[:2]:
            evidence.append(f"Benchmark command failed: {failure.command} (exit {failure.exit_code})")
            evidence.extend(failure.output_excerpt.splitlines()[:6])

        findings.append(
            Finding(
                category="performance",
                severity="medium" if benchmark_failures else "low",
                confidence=0.62,
                title="Potential performance drift signals detected",
                expected="Performance-sensitive code paths should avoid repeated query/network calls in loops and benchmark regressions.",
                observed="Detected suspicious loop + I/O patterns and/or benchmark command failures.",
                files=changed_files[:10],
                evidence=evidence[:14],
                remediation="Inspect hotspots for N+1 patterns, cache repeated calls, and add focused micro-benchmarks.",
                risk="medium",
                verification=["Run benchmark command(s)", "Profile impacted endpoints/functions"],
            )
        )

    return findings


def detect_docs_drift(
    changed_files: list[str],
    has_api_finding: bool,
) -> list[Finding]:
    findings: list[Finding] = []

    code_changed = any(path_matches(path, CODE_PATH_PATTERNS) for path in changed_files)
    docs_changed = any(path_matches(path, DOC_PATH_PATTERNS) for path in changed_files)

    if code_changed and not docs_changed:
        severity = "medium" if has_api_finding else "low"
        findings.append(
            Finding(
                category="docs",
                severity=severity,
                confidence=0.68,
                title="Code changed without documentation updates",
                expected="Docs/examples/changelog should stay aligned with behavior and API changes.",
                observed="No documentation files changed alongside code updates.",
                files=[path for path in changed_files if path_matches(path, CODE_PATH_PATTERNS)][:20],
                evidence=["No files matched docs patterns in the change set."],
                remediation="Update README/docs/changelog to reflect behavior, API, and configuration changes.",
                risk="low",
                verification=["Review docs for changed modules", "Run docs lint/checks if available"],
            )
        )

    return findings


def detect_config_infra_drift(
    changed_files: list[str],
    compare_diff: str,
    recent_log: str,
) -> list[Finding]:
    findings: list[Finding] = []

    touched_config = [
        path
        for path in changed_files
        if path_matches(path, CONFIG_PATH_PATTERNS) and not is_excluded_path(path, DEFAULT_EXCLUDED_PATH_PATTERNS)
    ]
    if touched_config:
        touched_config_set = set(touched_config)
        weakened_gate_lines: list[str] = []
        for file_path, removed_line in iter_removed_lines_by_file(
            compare_diff,
            include_patterns=CONFIG_PATH_PATTERNS,
            exclude_patterns=DEFAULT_EXCLUDED_PATH_PATTERNS,
        ):
            if file_path not in touched_config_set:
                continue
            if re.search(r"\b(lint|typecheck|test|security)\b", removed_line, re.IGNORECASE):
                weakened_gate_lines.append(f"{file_path}: {removed_line}")
        severity = "high" if weakened_gate_lines else "medium"
        findings.append(
            Finding(
                category="config_infra",
                severity=severity,
                confidence=0.8,
                title="Configuration/infra files changed",
                expected="Pipeline and infra changes should preserve or strengthen quality/safety gates.",
                observed="CI/deploy/configuration files were modified.",
                files=touched_config[:20],
                evidence=(weakened_gate_lines[:10] if weakened_gate_lines else ["Changed config files: " + ", ".join(touched_config[:12])]),
                remediation="Review config deltas with release/security owners and validate gates remain enforced.",
                risk="medium",
                verification=["Run CI pipeline in branch", "Validate deploy plans and policy checks"],
            )
        )

    churn_counter = 0
    for chunk in recent_log.split("\n\n"):
        lines = [line for line in chunk.splitlines() if line.strip()]
        for line in lines[1:]:
            if path_matches(line, CONFIG_PATH_PATTERNS):
                churn_counter += 1
                break
    if churn_counter >= 6:
        findings.append(
            Finding(
                category="config_infra",
                severity="medium",
                confidence=0.6,
                title="Recent config churn detected",
                expected="Configuration should remain stable and coordinated across contributors.",
                observed=f"{churn_counter} recent commits touched config/infra files in lookback window.",
                files=[],
                evidence=[f"Lookback config-touching commits: {churn_counter}"],
                remediation="Consolidate config ownership, batch related changes, and document rationale in PRs.",
                risk="medium",
                verification=["Inspect recent config PRs", "Audit gate consistency across workflows"],
            )
        )

    return findings


def detect_multi_agent_patterns(
    repo: pathlib.Path,
    changed_files: list[str],
    compare_from: str,
    compare_to: str,
    recent_log: str,
) -> list[Finding]:
    findings: list[Finding] = []
    code_files = [path for path in changed_files if path_matches(path, CODE_PATH_PATTERNS)]
    if not code_files:
        return findings

    non_test_code_files = [path for path in code_files if not path_matches(path, TEST_PATH_PATTERNS)]
    test_code_files = [path for path in code_files if path_matches(path, TEST_PATH_PATTERNS)]

    strategy_hits: Counter[str] = Counter()
    baseline_hits: Counter[str] = Counter()

    helper_defs: dict[str, list[str]] = defaultdict(list)
    abstraction_defs: dict[str, list[str]] = defaultdict(list)

    test_style_hits: Counter[str] = Counter()

    for file_path in non_test_code_files:
        content = read_file_if_exists(repo / file_path)
        baseline_content = read_file_from_ref(repo, compare_from, file_path)

        for strategy, pattern in MICRO_ARCH_PATTERNS.items():
            if pattern.search(content):
                strategy_hits[strategy] += 1
            if baseline_content and pattern.search(baseline_content):
                baseline_hits[strategy] += 1

        if re.search(r"(helper|util)", file_path, re.IGNORECASE):
            for match in re.finditer(r"^\s*(?:def|function|const)\s+([A-Za-z_][A-Za-z0-9_]*)", content, re.MULTILINE):
                helper_defs[match.group(1).lower()].append(file_path)

        for match in re.finditer(r"^\s*class\s+([A-Za-z_][A-Za-z0-9_]*(Service|Manager|Client|Wrapper))", content, re.MULTILINE):
            abstraction_defs[match.group(1)].append(file_path)

    for file_path in test_code_files:
        content = read_file_if_exists(repo / file_path)
        if re.search(r"toMatchSnapshot|snapshot", content):
            test_style_hits["snapshot"] += 1
        if re.search(r"jest\.mock|mock\.patch|sinon", content):
            test_style_hits["mock-heavy"] += 1
        if re.search(r"integration|@pytest\.mark\.integration|describe\(['\"]integration", content, re.IGNORECASE):
            test_style_hits["integration-heavy"] += 1

    observed_strategies = [name for name, count in strategy_hits.items() if count > 0]
    if len(observed_strategies) >= 2 and baseline_hits:
        baseline_winner = baseline_hits.most_common(1)[0][0]
        observed_winner = strategy_hits.most_common(1)[0][0]
        if baseline_winner != observed_winner:
            findings.append(
                Finding(
                    category="architecture",
                    severity="medium",
                    confidence=0.7,
                    title="Inconsistent micro-architecture patterns",
                    expected=f"Use a consistent error-handling pattern. Baseline prevalence favors `{baseline_winner}`.",
                    observed=f"Dominant strategy shifted from `{baseline_winner}` to `{observed_winner}`.",
                    files=non_test_code_files[:20],
                    evidence=[f"Baseline strategy counts: {dict(baseline_hits)}", f"Observed strategy counts: {dict(strategy_hits)}"],
                    remediation=f"Standardize error handling around `{baseline_winner}` and migrate divergent paths incrementally.",
                    risk="medium",
                    verification=["Run targeted tests for standardized paths", "Re-run drift audit"],
                )
            )

    duplicate_helpers = {name: paths for name, paths in helper_defs.items() if len(set(paths)) > 1}
    if duplicate_helpers:
        evidence = [f"{name}: {', '.join(sorted(set(paths)))}" for name, paths in sorted(duplicate_helpers.items())]
        files = sorted({path for paths in duplicate_helpers.values() for path in paths})
        findings.append(
            Finding(
                category="architecture",
                severity="medium",
                confidence=0.74,
                title="Duplicated utilities detected",
                expected="Utility behavior should be centralized to one shared implementation per concern.",
                observed="Multiple helper implementations with matching names were found.",
                files=files,
                evidence=evidence[:10],
                remediation="Select one canonical helper, migrate call sites, and delete duplicates.",
                risk="medium",
                verification=["Run unit tests for consolidated helper", "Search for duplicate helper usage"],
            )
        )

    competing_abstractions = {name: paths for name, paths in abstraction_defs.items() if len(set(paths)) > 1}
    if competing_abstractions:
        evidence = [f"{name}: {', '.join(sorted(set(paths)))}" for name, paths in sorted(competing_abstractions.items())]
        files = sorted({path for paths in competing_abstractions.values() for path in paths})
        findings.append(
            Finding(
                category="architecture",
                severity="medium",
                confidence=0.66,
                title="Competing abstractions detected",
                expected="Service/wrapper abstractions should converge on one adopted architecture pattern.",
                observed="Parallel abstractions with overlapping responsibilities were introduced.",
                files=files,
                evidence=evidence[:10],
                remediation="Choose the baseline-consistent abstraction and deprecate alternatives with migration notes.",
                risk="medium",
                verification=["Run integration tests around affected modules"],
            )
        )

    if len([key for key, count in test_style_hits.items() if count > 0]) >= 2:
        findings.append(
            Finding(
                category="test",
                severity="medium",
                confidence=0.63,
                title="Test philosophy drift",
                expected="A single dominant testing style should be applied within a module area.",
                observed=f"Competing test styles detected: {dict(test_style_hits)}.",
                files=test_code_files[:20],
                evidence=[f"Test style counts: {dict(test_style_hits)}"],
                remediation="Define module-level test style guidance (snapshot vs mocks vs integration) and align suites.",
                risk="low",
                verification=["Run full test suite", "Review flaky test rates"],
            )
        )

    return findings


def add_attribution(
    repo: pathlib.Path,
    findings: list[Finding],
    compare_from: str,
    compare_to: str,
) -> None:
    for finding in findings:
        attribution: dict[str, list[str]] = {}
        for file_path in sorted(set(finding.files))[:12]:
            log = git_maybe_output(
                repo,
                [
                    "log",
                    "--pretty=format:%h|%an|%ad|%s",
                    "--date=short",
                    f"{compare_from}..{compare_to}",
                    "--",
                    file_path,
                ],
                timeout=40,
            )
            entries = [line for line in log.splitlines() if line.strip()]
            if not entries:
                fallback = git_maybe_output(
                    repo,
                    ["log", "-n", "1", "--pretty=format:%h|%an|%ad|%s", "--date=short", "--", file_path],
                    timeout=20,
                )
                entries = [line for line in fallback.splitlines() if line.strip()]
            if entries:
                attribution[file_path] = entries[:3]
        finding.attribution = attribution


def compute_scores(
    findings: list[Finding],
    config: dict[str, Any],
) -> tuple[float, dict[str, float], dict[str, int]]:
    configured_weights = {
        str(key): float(value)
        for key, value in ((config.get("thresholds", {}) or {}).get("category_weights", {}) or {}).items()
        if isinstance(value, (int, float))
    }
    weights = dict(DEFAULT_CATEGORY_WEIGHTS)
    weights.update(configured_weights)

    category_scores: dict[str, float] = defaultdict(float)
    category_counts: dict[str, int] = defaultdict(int)

    total = 0.0
    for finding in findings:
        weight = weights.get(finding.category, 5)
        multiplier = SEVERITY_MULTIPLIER.get(finding.severity, 1.0)
        finding.score = weight * multiplier
        total += finding.score
        category_scores[finding.category] += finding.score
        category_counts[finding.category] += 1

    return round(total, 2), {k: round(v, 2) for k, v in category_scores.items()}, dict(category_counts)


def build_hotspots(findings: list[Finding]) -> list[dict[str, Any]]:
    counts: dict[str, int] = defaultdict(int)
    scores: dict[str, float] = defaultdict(float)
    for finding in findings:
        unique_files = set(finding.files) or {"(command-output)"}
        per_file = finding.score / max(1, len(unique_files))
        for file_path in unique_files:
            counts[file_path] += 1
            scores[file_path] += per_file

    rows = [
        {"path": path, "findings": counts[path], "score": round(scores[path], 2)}
        for path in counts
    ]
    rows.sort(key=lambda item: (item["score"], item["findings"]), reverse=True)
    return rows[:10]


def category_heading(category: str) -> str:
    return {
        "style": "Style Drift",
        "architecture": "Architecture Drift",
        "api": "API Drift",
        "dependency": "Dependency Drift",
        "behavioral": "Behavioral Drift",
        "performance": "Performance Drift",
        "security": "Security Drift",
        "test": "Test Drift",
        "docs": "Docs Drift",
        "config_infra": "Config/Infra Drift",
    }.get(category, category)


def generate_remediation_actions(findings: list[Finding]) -> list[dict[str, Any]]:
    ordered = sorted(
        findings,
        key=lambda finding: (
            REMEDIATION_PRIORITY.get(finding.category, 99),
            -SEVERITY_MULTIPLIER.get(finding.severity, 1.0),
            -finding.score,
        ),
    )

    actions: list[dict[str, Any]] = []
    for finding in ordered:
        actions.append(
            {
                "category": finding.category,
                "severity": finding.severity,
                "title": finding.title,
                "what_to_change": finding.remediation,
                "why_it_is_drift": f"Expected: {finding.expected} Observed: {finding.observed}",
                "patch_approach": "Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.",
                "risk_level": finding.risk,
                "verification_steps": finding.verification,
                "files": finding.files,
            }
        )
    return actions


def build_patch_preview(
    repo: pathlib.Path,
    mode: str,
    hotspots: list[dict[str, Any]],
    compare_from: str,
    compare_to: str,
) -> list[dict[str, Any]]:
    if mode != "recommend":
        return []

    preview: list[dict[str, Any]] = []
    for hotspot in hotspots[:5]:
        path = hotspot["path"]
        if path == "(command-output)":
            continue
        diff = get_diff(repo, compare_from, compare_to, path)
        if not diff:
            diff = git_maybe_output(repo, ["diff", "--unified=3", "--", path], timeout=120)
        if not diff:
            diff = git_maybe_output(repo, ["diff", "--cached", "--unified=3", "--", path], timeout=120)
        hunks = extract_hunks(diff, max_hunks=2, max_lines_per_hunk=16)
        if hunks:
            preview.append({"file": path, "hunks": hunks})
    return preview


def safe_remove_untracked(repo: pathlib.Path, files: list[str]) -> None:
    for rel in files:
        target = repo / rel
        if target.exists() and target.is_file():
            target.unlink(missing_ok=True)


def revert_files(repo: pathlib.Path, tracked: list[str], untracked: list[str]) -> None:
    if tracked:
        subprocess.run(["git", "restore", "--staged", "--worktree", "--", *tracked], cwd=str(repo), check=False)
    if untracked:
        safe_remove_untracked(repo, untracked)


def run_auto_remediation(
    repo: pathlib.Path,
    config: dict[str, Any],
    skip_quality_gates: bool,
) -> dict[str, Any]:
    auto_cfg = config.get("auto_remediate", {}) or {}
    risk_cfg = config.get("risk_controls", {}) or {}
    standards = config.get("standards", {}) or {}

    if not bool(auto_cfg.get("enabled", False)):
        return {"status": "skipped", "reason": "auto_remediate.enabled is false"}

    pre_status = git_porcelain(repo)
    if pre_status:
        return {
            "status": "blocked",
            "reason": "Auto-remediate requires a clean working tree to guarantee safe rollback.",
            "details": pre_status[:20],
        }

    commands = [str(cmd) for cmd in (auto_cfg.get("commands", []) or []) if str(cmd).strip()]
    if not commands:
        commands = [
            *[str(cmd) for cmd in (standards.get("format_fix", []) or [])],
            *[str(cmd) for cmd in (standards.get("lint_fix", []) or [])],
        ]
    if not commands:
        return {"status": "skipped", "reason": "No auto-remediation commands configured"}

    command_results: list[CommandResult] = []
    for command in commands:
        result = run_shell_command(command, repo)
        result.name = "auto_fix"
        command_results.append(result)
        if result.status == "failed":
            tracked, untracked = parse_changed_files_from_porcelain(git_porcelain(repo))
            revert_files(repo, tracked, untracked)
            return {
                "status": "failed",
                "reason": f"Auto-remediation command failed: {command}",
                "command_results": [dataclasses.asdict(item) for item in command_results],
            }

    status_lines = git_porcelain(repo)
    tracked_changed, untracked_changed = parse_changed_files_from_porcelain(status_lines)
    changed_files = sorted(set(tracked_changed + untracked_changed))

    if not changed_files:
        return {
            "status": "no_changes",
            "reason": "Auto-remediation commands made no file changes",
            "command_results": [dataclasses.asdict(item) for item in command_results],
        }

    max_files = int(auto_cfg.get("max_files_changed", 50) or 50)
    if len(changed_files) > max_files:
        revert_files(repo, tracked_changed, untracked_changed)
        return {
            "status": "failed_reverted",
            "reason": f"Changed files ({len(changed_files)}) exceeded max_files_changed ({max_files})",
            "changed_files": changed_files,
            "command_results": [dataclasses.asdict(item) for item in command_results],
        }

    safe_dirs = [str(item) for item in (auto_cfg.get("safe_directories", []) or [])]
    if safe_dirs:
        outside = [path for path in changed_files if not path_matches(path, safe_dirs)]
        if outside:
            revert_files(repo, tracked_changed, untracked_changed)
            return {
                "status": "failed_reverted",
                "reason": "Changes occurred outside auto_remediate.safe_directories",
                "changed_files": changed_files,
                "outside_safe_directories": outside,
                "command_results": [dataclasses.asdict(item) for item in command_results],
            }

    protected = [str(item) for item in (risk_cfg.get("protected_globs", []) or [])]
    protected_hits = [path for path in changed_files if path_matches(path, protected)]
    if protected_hits:
        revert_files(repo, tracked_changed, untracked_changed)
        return {
            "status": "failed_reverted",
            "reason": "Auto-remediation touched protected paths",
            "changed_files": changed_files,
            "protected_hits": protected_hits,
            "command_results": [dataclasses.asdict(item) for item in command_results],
        }

    gate_results = run_quality_gates(repo, config, mode="auto-remediate", skip=skip_quality_gates, include_full_tests=True)
    gate_failures = [item for item in gate_results if item.status == "failed"]
    if gate_failures:
        revert_files(repo, tracked_changed, untracked_changed)
        return {
            "status": "failed_reverted",
            "reason": "Post-remediation checks failed",
            "changed_files": changed_files,
            "command_results": [dataclasses.asdict(item) for item in command_results],
            "quality_gate_results": [dataclasses.asdict(item) for item in gate_results],
        }

    patch_path = pathlib.Path(config.get("reporting", {}).get("patch_path", "artifacts/drift_remediation.patch"))
    patch_abs = repo / patch_path
    patch_abs.parent.mkdir(parents=True, exist_ok=True)
    patch = git_maybe_output(repo, ["diff", "--binary"], timeout=240)
    patch_abs.write_text(patch + "\n", encoding="utf-8")

    return {
        "status": "applied",
        "changed_files": changed_files,
        "command_results": [dataclasses.asdict(item) for item in command_results],
        "quality_gate_results": [dataclasses.asdict(item) for item in gate_results],
        "patch_path": str(patch_path),
        "suggested_commit_message": "chore(codedrift): auto-remediate deterministic drift",
    }


def render_markdown_report(data: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Drift Report")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Mode: `{data['mode']}`")
    lines.append(f"- Generated at: `{data['generated_at']}`")
    lines.append(
        f"- Baseline: `{data['baseline']['resolved']}` ({data['baseline']['reason']})"
    )
    lines.append(
        f"- Compare: `{data['baseline']['compare_from']}` -> `{data['baseline']['compare_to']}`"
    )
    lines.append(
        f"- Drift score: **{data['drift_score']}** (threshold fail: `{data['thresholds']['drift_score_fail']}`)"
    )
    lines.append(f"- Result: **{data['gate_result'].upper()}**")
    lines.append("")

    lines.append("## Top Drift Hotspots")
    if data["hotspots"]:
        lines.append("| File/Module | Findings | Score |")
        lines.append("|---|---:|---:|")
        for hotspot in data["hotspots"]:
            lines.append(f"| `{hotspot['path']}` | {hotspot['findings']} | {hotspot['score']} |")
    else:
        lines.append("No hotspots detected.")
    lines.append("")

    lines.append("## Category Findings")
    if not data["findings"]:
        lines.append("No drift findings detected.")
    else:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for finding in data["findings"]:
            grouped[finding["category"]].append(finding)

        for category in sorted(grouped.keys(), key=lambda key: REMEDIATION_PRIORITY.get(key, 99)):
            lines.append(f"### {category_heading(category)}")
            for finding in grouped[category]:
                lines.append(f"- **[{finding['severity'].upper()}] {finding['title']}** (confidence {finding['confidence']})")
                lines.append(f"  - Expected vs Observed: {finding['expected']} | {finding['observed']}")
                if finding["files"]:
                    lines.append(f"  - Files: {', '.join(f'`{path}`' for path in finding['files'][:10])}")
                if finding["evidence"]:
                    lines.append("  - Evidence:")
                    for item in finding["evidence"][:6]:
                        lines.append(f"    - {item}")
                if finding.get("attribution"):
                    lines.append("  - Attribution:")
                    for path, commits in sorted(finding["attribution"].items())[:4]:
                        lines.append(f"    - `{path}`")
                        for commit in commits[:2]:
                            lines.append(f"      - {commit}")
                lines.append(f"  - Recommendation: {finding['remediation']}")
                if finding["verification"]:
                    lines.append("  - Verification:")
                    for command in finding["verification"][:5]:
                        lines.append(f"    - `{command}`")
            lines.append("")

    lines.append("## Suggested Remediation Plan")
    if data["suggested_actions"]:
        for idx, action in enumerate(data["suggested_actions"], start=1):
            lines.append(
                f"{idx}. **[{action['severity'].upper()}] {action['title']}** ({category_heading(action['category'])})"
            )
            lines.append(f"   - What to change: {action['what_to_change']}")
            lines.append(f"   - Why: {action['why_it_is_drift']}")
            lines.append(f"   - Patch approach: {action['patch_approach']}")
            lines.append(f"   - Risk: {action['risk_level']}")
            if action["verification_steps"]:
                lines.append("   - Verification:")
                for step in action["verification_steps"][:4]:
                    lines.append(f"     - `{step}`")
    else:
        lines.append("No remediation actions required.")
    lines.append("")

    if data.get("patch_preview"):
        lines.append("## Patch Preview (Recommend Mode)")
        for item in data["patch_preview"]:
            lines.append(f"### `{item['file']}`")
            for hunk in item["hunks"]:
                lines.append("```diff")
                lines.append(hunk)
                lines.append("```")
        lines.append("")

    if data["baseline"].get("ci_context"):
        lines.append("## Merge Risk Assessment")
        high_risk = [f for f in data["findings"] if f["severity"] in {"high", "critical"}]
        lines.append(
            f"- High/Critical findings: {len(high_risk)}"
        )
        lines.append(
            f"- Drift gate: {'FAIL' if data['gate_result'] == 'fail' else 'PASS'}"
        )
        if high_risk:
            lines.append("- Merge recommendation: Block until top findings are remediated.")
        else:
            lines.append("- Merge recommendation: Acceptable with follow-up action items.")
        lines.append("")

    lines.append("## Appendix")
    lines.append("### Tool Run Status")
    for status in data.get("tool_run_status", []):
        lines.append(
            f"- `{status['name']}` `{status['command']}` -> **{status['status']}**"
            + (f" (exit {status['exit_code']})" if status.get("exit_code") is not None else "")
        )
        if status.get("output_excerpt"):
            lines.append("```text")
            lines.append(trim_output(status["output_excerpt"], max_lines=12, max_chars=2000))
            lines.append("```")

    if data.get("auto_remediation"):
        lines.append("### Auto-Remediation Result")
        lines.append("```json")
        lines.append(json.dumps(data["auto_remediation"], indent=2))
        lines.append("```")

    return "\n".join(lines).rstrip() + "\n"


def write_reports(repo: pathlib.Path, config: dict[str, Any], report_data: dict[str, Any]) -> tuple[pathlib.Path, pathlib.Path]:
    reporting = config.get("reporting", {}) or {}
    markdown_path = repo / str(reporting.get("markdown_path", "artifacts/drift_report.md"))
    json_path = repo / str(reporting.get("json_path", "artifacts/drift_report.json"))

    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    markdown_path.write_text(render_markdown_report(report_data), encoding="utf-8")
    json_path.write_text(json.dumps(report_data, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return markdown_path, json_path


def print_ci_summary(report_data: dict[str, Any], markdown_path: pathlib.Path) -> None:
    findings = report_data.get("findings", [])
    print("CODE DRIFT GATE FAILED")
    print(f"drift_score={report_data['drift_score']} threshold={report_data['thresholds']['drift_score_fail']}")
    top = sorted(findings, key=lambda item: item.get("score", 0.0), reverse=True)[:5]
    for finding in top:
        files = ", ".join(finding.get("files", [])[:2]) if finding.get("files") else "(no-file)"
        print(f"- [{finding['severity']}] {finding['category']}: {finding['title']} :: {files}")
    print(f"Report: {markdown_path}")


def classify_all_drift(
    repo: pathlib.Path,
    config: dict[str, Any],
    changed_files: list[str],
    compare_from: str,
    compare_to: str,
    combined_diff: str,
    quality_results: list[CommandResult],
    recent_log: str,
) -> list[Finding]:
    findings: list[Finding] = []

    findings.extend(detect_style_drift(repo, compare_from, compare_to, quality_results))
    findings.extend(detect_architecture_drift(repo, changed_files, config.get("architecture", {}) or {}))
    findings.extend(detect_api_drift(repo, changed_files, compare_from, compare_to, config.get("api", {}) or {}))
    findings.extend(detect_dependency_drift(repo, changed_files, compare_from, config.get("dependencies", {}) or {}))
    findings.extend(
        detect_behavioral_and_test_drift(
            changed_files,
            quality_results,
            combined_diff,
            config.get("detection", {}) or {},
        )
    )
    findings.extend(detect_performance_drift(changed_files, combined_diff, quality_results))
    findings.extend(detect_security_drift(changed_files, combined_diff, quality_results, config.get("risk_controls", {}) or {}))

    has_api_finding = any(item.category == "api" for item in findings)
    findings.extend(detect_docs_drift(changed_files, has_api_finding))
    findings.extend(detect_config_infra_drift(changed_files, combined_diff, recent_log))
    findings.extend(detect_multi_agent_patterns(repo, changed_files, compare_from, compare_to, recent_log))

    add_attribution(repo, findings, compare_from, compare_to)
    return findings


def normalize_mode(value: str) -> str:
    value = value.strip().lower()
    if value not in {"audit", "recommend", "auto-remediate"}:
        raise ValueError(f"Unsupported mode: {value}")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detect and remediate multi-agent code drift.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Modes:
              audit           Detect drift and produce reports only.
              recommend       Detect drift and produce reports + patch preview.
              auto-remediate  Detect drift, apply safe deterministic fixes, and validate.
            """
        ),
    )
    parser.add_argument("--repo", default=".", help="Repository root (default: current directory)")
    parser.add_argument("--config", default=".codedrift.yml", help="Path to .codedrift.yml relative to repo root")
    parser.add_argument("--mode", default="audit", choices=["audit", "recommend", "auto-remediate"], help="Operating mode")
    parser.add_argument("--baseline-ref", default=None, help="Override baseline ref (commit/tag/branch)")
    parser.add_argument("--ci", action="store_true", help="Force CI PR comparison behavior")
    parser.add_argument("--pr-head", default=None, help="Optional PR head ref/SHA")
    parser.add_argument("--skip-quality-gates", action="store_true", help="Skip running configured formatter/lint/typecheck/test commands")
    parser.add_argument("--include-full-tests", action="store_true", help="Run standards.test_full in non-auto modes")
    parser.add_argument("--quiet", action="store_true", help="Reduce console output")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    repo = pathlib.Path(args.repo).resolve()
    if not repo.exists() or not (repo / ".git").exists():
        print(f"[ERROR] repo must exist and contain .git: {repo}", file=sys.stderr)
        return 1

    mode = normalize_mode(args.mode)

    config, config_issues = load_config(repo, args.config)

    baseline_ref, baseline_reason = resolve_baseline(repo, config, args.baseline_ref)
    compare_from, compare_to, ci_context = resolve_compare_refs(repo, baseline_ref, args.ci, args.pr_head)

    include_worktree = not ci_context
    detection_cfg = config.get("detection", {}) or {}
    configured_excludes = [str(item) for item in (detection_cfg.get("exclude_globs", []) or []) if str(item).strip()]
    changed_files = list_changed_files(
        repo,
        compare_from,
        compare_to,
        include_worktree,
        exclude_globs=configured_excludes,
    )

    range_diff = get_diff(repo, compare_from, compare_to)
    working_diff = get_working_tree_diff(repo) if include_worktree else ""
    combined_diff = "\n\n".join(part for part in [range_diff, working_diff] if part)

    lookback_days = int((config.get("detection", {}) or {}).get("lookback_days", 14) or 14)
    recent_log = collect_recent_log(repo, lookback_days)

    tool_run_status: list[CommandResult] = []
    quality_results = run_quality_gates(
        repo=repo,
        config=config,
        mode=mode,
        skip=args.skip_quality_gates,
        include_full_tests=args.include_full_tests,
    )
    tool_run_status.extend(quality_results)

    auto_remediation: dict[str, Any] | None = None
    if mode == "auto-remediate":
        auto_remediation = run_auto_remediation(repo, config, skip_quality_gates=args.skip_quality_gates)

    findings = classify_all_drift(
        repo=repo,
        config=config,
        changed_files=changed_files,
        compare_from=compare_from,
        compare_to=compare_to,
        combined_diff=combined_diff,
        quality_results=quality_results,
        recent_log=recent_log,
    )

    if config_issues:
        findings.append(
            Finding(
                category="config_infra",
                severity="low",
                confidence=1.0,
                title="Configuration loading issues",
                expected="A valid `.codedrift.yml` should be available and parse cleanly.",
                observed="Configuration was missing or partially invalid.",
                files=[args.config],
                evidence=config_issues,
                remediation="Create or fix `.codedrift.yml` using the sample in this skill.",
                risk="low",
                verification=["Validate YAML syntax", "Re-run drift audit"],
            )
        )

    drift_score, category_scores, category_counts = compute_scores(findings, config)
    hotspots = build_hotspots(findings)
    patch_preview = build_patch_preview(repo, mode, hotspots, compare_from, compare_to)
    suggested_actions = generate_remediation_actions(findings)

    drift_score_fail = float((config.get("thresholds", {}) or {}).get("drift_score_fail", 35) or 35)
    gate_result = "fail" if drift_score >= drift_score_fail else "pass"

    report_data: dict[str, Any] = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": mode,
        "baseline": {
            "requested": args.baseline_ref,
            "resolved": baseline_ref,
            "reason": baseline_reason,
            "compare_from": compare_from,
            "compare_to": compare_to,
            "ci_context": ci_context,
        },
        "drift_score": drift_score,
        "category_scores": category_scores,
        "category_counts": category_counts,
        "hotspots": hotspots,
        "findings": [item.as_dict() for item in findings],
        "suggested_actions": suggested_actions,
        "patch_preview": patch_preview,
        "tool_run_status": [dataclasses.asdict(item) for item in tool_run_status],
        "thresholds": {
            "drift_score_fail": drift_score_fail,
        },
        "gate_result": gate_result,
        "auto_remediation": auto_remediation,
        "changed_files": changed_files,
    }

    markdown_path, json_path = write_reports(repo, config, report_data)

    if not args.quiet:
        print(f"Drift report markdown: {markdown_path}")
        print(f"Drift report json: {json_path}")
        print(f"drift_score={drift_score} threshold={drift_score_fail} gate={gate_result}")
        print(f"findings={len(findings)} changed_files={len(changed_files)}")

    if gate_result == "fail":
        print_ci_summary(report_data, markdown_path)
        return 2

    if auto_remediation and auto_remediation.get("status", "").startswith("failed"):
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
