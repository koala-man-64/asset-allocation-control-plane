from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REQUIREMENTS = REPO_ROOT / "requirements.txt"
REQUIREMENTS_LOCK = REPO_ROOT / "requirements.lock.txt"
PYPROJECT = REPO_ROOT / "pyproject.toml"


def _get_pinned_version(path: Path, package_name: str) -> str | None:
    prefix = f"{package_name.lower()}=="
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith(prefix):
            return line.split("==", 1)[1].strip()
    return None


def test_sqlalchemy_present_and_consistent_across_runtime_manifests() -> None:
    req_version = _get_pinned_version(REQUIREMENTS, "sqlalchemy")
    lock_version = _get_pinned_version(REQUIREMENTS_LOCK, "sqlalchemy")

    assert req_version is not None, "sqlalchemy must be pinned in requirements.txt"
    assert lock_version is not None, "sqlalchemy must be pinned in requirements.lock.txt"
    assert req_version == lock_version, (
        f"sqlalchemy version mismatch: requirements.txt={req_version} requirements.lock.txt={lock_version}"
    )


def test_sqlalchemy_declared_in_pyproject_runtime_dependencies() -> None:
    text = PYPROJECT.read_text(encoding="utf-8").lower()
    assert '"sqlalchemy==' in text, "pyproject runtime dependencies must include pinned sqlalchemy"
