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


def test_deploy_root_contains_only_api_yaml_manifests() -> None:
    repo_root = _repo_root()
    manifest_names = {path.name for path in (repo_root / "deploy").glob("*.yaml")}
    assert manifest_names == {"app_api.yaml", "app_api_public.yaml"}


def test_api_manifests_include_ai_relay_secret_and_env_surface() -> None:
    repo_root = _repo_root()
    public_doc = _load_yaml(repo_root / "deploy" / "app_api_public.yaml")
    private_doc = _load_yaml(repo_root / "deploy" / "app_api.yaml")

    def _secret_names(doc: dict[str, object]) -> set[str]:
        secrets = doc["properties"]["configuration"]["secrets"]
        return {entry["name"] for entry in secrets}

    def _env_names(doc: dict[str, object]) -> set[str]:
        env = doc["properties"]["template"]["containers"][0]["env"]
        return {entry["name"] for entry in env}

    expected_envs = {
        "ALPACA_TIMEOUT_SECONDS",
        "ALPACA_MAX_RETRIES",
        "ALPACA_BACKOFF_BASE_SECONDS",
        "ALPACA_REQUIRED_ROLES",
        "ALPACA_TRADING_REQUIRED_ROLES",
        "ALPACA_PAPER_TRADING_BASE_URL",
        "ALPACA_PAPER_API_KEY_ID",
        "ALPACA_PAPER_SECRET_KEY",
        "ALPACA_LIVE_TRADING_BASE_URL",
        "ALPACA_LIVE_API_KEY_ID",
        "ALPACA_LIVE_SECRET_KEY",
        "AI_RELAY_ENABLED",
        "AI_RELAY_MODEL",
        "AI_RELAY_REASONING_EFFORT",
        "AI_RELAY_TIMEOUT_SECONDS",
        "AI_RELAY_MAX_PROMPT_CHARS",
        "AI_RELAY_MAX_FILES",
        "AI_RELAY_MAX_FILE_BYTES",
        "AI_RELAY_MAX_TOTAL_FILE_BYTES",
        "AI_RELAY_MAX_OUTPUT_TOKENS",
        "AI_RELAY_REQUIRED_ROLES",
        "AI_RELAY_API_KEY",
        "SYMBOL_ENRICHMENT_ENABLED",
        "SYMBOL_ENRICHMENT_MODEL",
        "SYMBOL_ENRICHMENT_CONFIDENCE_MIN",
        "SYMBOL_ENRICHMENT_MAX_SYMBOLS_PER_RUN",
        "SYMBOL_ENRICHMENT_ALLOWED_JOBS",
    }

    assert "alpaca-paper-api-key-id" in _secret_names(public_doc)
    assert "alpaca-paper-secret-key" in _secret_names(public_doc)
    assert "alpaca-live-api-key-id" in _secret_names(public_doc)
    assert "alpaca-live-secret-key" in _secret_names(public_doc)
    assert "alpaca-paper-api-key-id" in _secret_names(private_doc)
    assert "alpaca-paper-secret-key" in _secret_names(private_doc)
    assert "alpaca-live-api-key-id" in _secret_names(private_doc)
    assert "alpaca-live-secret-key" in _secret_names(private_doc)
    assert "ai-relay-api-key" in _secret_names(public_doc)
    assert "ai-relay-api-key" in _secret_names(private_doc)
    assert expected_envs <= _env_names(public_doc)
    assert expected_envs <= _env_names(private_doc)
