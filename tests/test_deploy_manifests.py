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
        "KALSHI_ENABLED",
        "KALSHI_TRADING_ENABLED",
        "KALSHI_TIMEOUT_SECONDS",
        "KALSHI_READ_RETRY_ATTEMPTS",
        "KALSHI_READ_RETRY_BASE_DELAY_SECONDS",
        "KALSHI_REQUIRED_ROLES",
        "KALSHI_TRADING_REQUIRED_ROLES",
        "KALSHI_LIVE_BASE_URL",
        "KALSHI_LIVE_API_KEY_ID",
        "KALSHI_LIVE_PRIVATE_KEY_PEM",
        "ETRADE_ENABLED",
        "ETRADE_TRADING_ENABLED",
        "ETRADE_CALLBACK_URL",
        "ETRADE_TIMEOUT_SECONDS",
        "ETRADE_READ_RETRY_ATTEMPTS",
        "ETRADE_READ_RETRY_BASE_DELAY_SECONDS",
        "ETRADE_PENDING_AUTH_TTL_SECONDS",
        "ETRADE_PREVIEW_TTL_SECONDS",
        "ETRADE_IDLE_RENEW_SECONDS",
        "ETRADE_SESSION_EXPIRY_GUARD_SECONDS",
        "ETRADE_REQUIRED_ROLES",
        "ETRADE_TRADING_REQUIRED_ROLES",
        "ETRADE_SANDBOX_CONSUMER_KEY",
        "ETRADE_SANDBOX_CONSUMER_SECRET",
        "ETRADE_LIVE_CONSUMER_KEY",
        "ETRADE_LIVE_CONSUMER_SECRET",
        "SCHWAB_ENABLED",
        "SCHWAB_TRADING_ENABLED",
        "SCHWAB_APP_CALLBACK_URL",
        "SCHWAB_TIMEOUT_SECONDS",
        "SCHWAB_REQUIRED_ROLES",
        "SCHWAB_TRADING_REQUIRED_ROLES",
        "SCHWAB_CLIENT_ID",
        "SCHWAB_CLIENT_SECRET",
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
        "UI_SHARED_PASSWORD_HASH",
        "SYMBOL_ENRICHMENT_ENABLED",
        "SYMBOL_ENRICHMENT_MODEL",
        "SYMBOL_ENRICHMENT_CONFIDENCE_MIN",
        "SYMBOL_ENRICHMENT_MAX_SYMBOLS_PER_RUN",
        "SYMBOL_ENRICHMENT_ALLOWED_JOBS",
        "UI_AUTH_PROVIDER",
    }

    assert "alpaca-paper-api-key-id" in _secret_names(public_doc)
    assert "alpaca-paper-secret-key" in _secret_names(public_doc)
    assert "alpaca-live-api-key-id" in _secret_names(public_doc)
    assert "alpaca-live-secret-key" in _secret_names(public_doc)
    assert "alpaca-paper-api-key-id" in _secret_names(private_doc)
    assert "alpaca-paper-secret-key" in _secret_names(private_doc)
    assert "alpaca-live-api-key-id" in _secret_names(private_doc)
    assert "alpaca-live-secret-key" in _secret_names(private_doc)
    assert "kalshi-live-api-key-id" in _secret_names(public_doc)
    assert "kalshi-live-private-key-pem" in _secret_names(public_doc)
    assert "kalshi-live-api-key-id" in _secret_names(private_doc)
    assert "kalshi-live-private-key-pem" in _secret_names(private_doc)
    assert "etrade-sandbox-consumer-key" in _secret_names(public_doc)
    assert "etrade-sandbox-consumer-secret" in _secret_names(public_doc)
    assert "etrade-live-consumer-key" in _secret_names(public_doc)
    assert "etrade-live-consumer-secret" in _secret_names(public_doc)
    assert "etrade-sandbox-consumer-key" in _secret_names(private_doc)
    assert "etrade-sandbox-consumer-secret" in _secret_names(private_doc)
    assert "etrade-live-consumer-key" in _secret_names(private_doc)
    assert "etrade-live-consumer-secret" in _secret_names(private_doc)
    assert "schwab-client-id" in _secret_names(public_doc)
    assert "schwab-client-secret" in _secret_names(public_doc)
    assert "schwab-client-id" in _secret_names(private_doc)
    assert "schwab-client-secret" in _secret_names(private_doc)
    assert "schwab-access-token" not in _secret_names(public_doc)
    assert "schwab-refresh-token" not in _secret_names(public_doc)
    assert "schwab-access-token" not in _secret_names(private_doc)
    assert "schwab-refresh-token" not in _secret_names(private_doc)
    assert "ai-relay-api-key" in _secret_names(public_doc)
    assert "ai-relay-api-key" in _secret_names(private_doc)
    assert "ui-shared-password-hash" in _secret_names(public_doc)
    assert "ui-shared-password-hash" in _secret_names(private_doc)
    assert "SCHWAB_ACCESS_TOKEN" not in _env_names(public_doc)
    assert "SCHWAB_REFRESH_TOKEN" not in _env_names(public_doc)
    assert "SCHWAB_ACCESS_TOKEN" not in _env_names(private_doc)
    assert "SCHWAB_REFRESH_TOKEN" not in _env_names(private_doc)
    assert expected_envs <= _env_names(public_doc)
    assert expected_envs <= _env_names(private_doc)


def test_api_manifests_default_to_internal_parameterized_split_identity_runtime() -> None:
    repo_root = _repo_root()
    public_doc = _load_yaml(repo_root / "deploy" / "app_api_public.yaml")
    private_doc = _load_yaml(repo_root / "deploy" / "app_api.yaml")

    for doc in (public_doc, private_doc):
        assert doc["name"] == "${API_APP_NAME}"
        user_assigned = doc["identity"]["userAssignedIdentities"]
        assert "${ACR_PULL_IDENTITY_RESOURCE_ID}" in user_assigned
        assert "${API_RUNTIME_IDENTITY_RESOURCE_ID}" in user_assigned

        registries = doc["properties"]["configuration"]["registries"]
        assert registries[0]["identity"] == "${ACR_PULL_IDENTITY_RESOURCE_ID}"

        env = {
            entry["name"]: entry
            for entry in doc["properties"]["template"]["containers"][0]["env"]
        }
        assert env["AZURE_CLIENT_ID"]["value"] == "${API_RUNTIME_IDENTITY_CLIENT_ID}"
        assert "${ACR_PULL_IDENTITY_CLIENT_ID}" not in str(doc)

        for name in {
            "SYSTEM_READ_REQUIRED_ROLES",
            "SYSTEM_LOGS_READ_REQUIRED_ROLES",
            "SYSTEM_OPERATE_REQUIRED_ROLES",
            "RUNTIME_CONFIG_WRITE_REQUIRED_ROLES",
            "JOB_OPERATE_REQUIRED_ROLES",
            "PURGE_WRITE_REQUIRED_ROLES",
        }:
            assert name in env

    assert private_doc["properties"]["configuration"]["ingress"]["external"] is False
    assert public_doc["properties"]["configuration"]["ingress"]["external"] is True
