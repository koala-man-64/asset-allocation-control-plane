from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_kalshi_smoke_script_loads_env_files_and_exposes_operator_flags() -> None:
    repo_root = _repo_root()
    script = repo_root / "scripts" / "ops" / "kalshi_nyc_weather_smoke.py"
    text = script.read_text(encoding="utf-8")

    assert "DEFAULT_ENV_PATHS" in text
    assert "--env-file" in text
    assert "--environment" in text
    assert "--market-ticker" in text
    assert "Loaded env files:" in text
    assert "Using the current process environment only." in text
    assert "KALSHI_PRIVATE_KEY_PATH" in text


def test_ops_readme_documents_kalshi_env_loading_and_market_override() -> None:
    repo_root = _repo_root()
    readme = repo_root / "scripts" / "ops" / "README.md"
    text = readme.read_text(encoding="utf-8")

    assert "mirrors the env-file loading pattern used by the Schwab" in text
    assert "KALSHI_DEMO_API_KEY_ID" not in text
    assert "KALSHI_LIVE_API_KEY_ID" in text
    assert "--market-ticker" in text
    assert "--env-file" in text
