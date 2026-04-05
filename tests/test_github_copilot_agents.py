from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _skill_names() -> list[str]:
    skills_dir = _repo_root() / ".codex" / "skills"
    return sorted(
        path.name
        for path in skills_dir.iterdir()
        if path.is_dir() and (path / "SKILL.md").exists()
    )


def _agent_path(skill_name: str) -> Path:
    return _repo_root() / ".github" / "agents" / f"{skill_name}.agent.md"


def test_every_repo_local_skill_has_a_matching_github_copilot_agent() -> None:
    missing = [skill_name for skill_name in _skill_names() if not _agent_path(skill_name).exists()]
    assert missing == [], f"Missing GitHub Copilot agent files: {missing}"


def test_github_copilot_agent_frontmatter_declares_expected_target() -> None:
    for skill_name in _skill_names():
        text = _agent_path(skill_name).read_text(encoding="utf-8")
        assert f"name: {skill_name}" in text
        assert "target: github-copilot" in text
