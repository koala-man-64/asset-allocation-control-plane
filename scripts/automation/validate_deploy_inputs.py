from __future__ import annotations

import json
import os
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlparse


REQUIRED_ENV_NAMES = (
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_SUBSCRIPTION_ID",
    "AZURE_STORAGE_CONNECTION_STRING",
    "ALPHA_VANTAGE_API_KEY",
    "NASDAQ_API_KEY",
    "POSTGRES_DSN",
    "SERVICE_ACCOUNT_NAME",
    "ASSET_ALLOCATION_API_BASE_URL",
)
REMOVED_DEPLOY_ENV_KEYS = (
    "API_KEY",
    "ASSET_ALLOCATION_API_KEY",
    "VITE_BACKTEST_API_BASE_URL",
)


def _repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


REPO_ROOT = _repo_root()
_WORKLOAD_PROFILE_PATTERN = re.compile(r"^\s*workloadProfileName:\s*(.+?)\s*$")
_CPU_PATTERN = re.compile(r"^\s*cpu:\s*(.+?)\s*$")
_MEMORY_PATTERN = re.compile(r"^\s*memory:\s*(.+?)\s*$")
_CONSUMPTION_CPU_STEP = Decimal("0.25")
_CONSUMPTION_MAX_CPU = Decimal("4.0")
_CONSUMPTION_MAX_MEMORY_GI = Decimal("8.0")


def fail(message: str) -> None:
    print(f"::error::{message}")
    raise SystemExit(1)


def require_value(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        fail(f"Missing required GitHub Actions value: {name}")
    return value


def optional_value(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def has_env_key(name: str) -> bool:
    return name in os.environ


def parse_bool(name: str, *, default: bool = False) -> bool:
    raw = optional_value(name)
    if not raw:
        return default
    lowered = raw.lower()
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    fail(f"{name} must be a boolean value.")


def _strip_yaml_scalar(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if "#" in value:
        value = value.split("#", 1)[0].rstrip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1].strip()
    return value


def _parse_decimal_value(raw: str, *, field_name: str, manifest_path: Path) -> Decimal:
    value = _strip_yaml_scalar(raw)
    try:
        return Decimal(value)
    except InvalidOperation:
        fail(f"{manifest_path}: {field_name} must be numeric, got {value!r}.")


def _parse_memory_gib(raw: str, *, manifest_path: Path) -> Decimal:
    value = _strip_yaml_scalar(raw)
    if not value.endswith("Gi"):
        fail(f"{manifest_path}: Consumption job memory must use Gi units, got {value!r}.")
    try:
        return Decimal(value[:-2])
    except InvalidOperation:
        fail(f"{manifest_path}: memory must be numeric Gi units, got {value!r}.")


def _is_multiple_of(value: Decimal, step: Decimal) -> bool:
    if step <= 0:
        return False
    steps = value / step
    return steps == steps.to_integral_value()


def _format_decimal(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    if "." not in normalized:
        return normalized
    normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def validate_job_manifest_resources(manifest_root: Path | None = None) -> None:
    root = Path(manifest_root) if manifest_root is not None else (REPO_ROOT / "deploy")
    manifests = sorted(root.glob("job_*.yaml"))
    if not manifests:
        return

    for manifest_path in manifests:
        workload_profile = ""
        cpu_values: list[Decimal] = []
        memory_values: list[Decimal] = []

        for line in manifest_path.read_text(encoding="utf-8").splitlines():
            workload_match = _WORKLOAD_PROFILE_PATTERN.match(line)
            if workload_match:
                workload_profile = _strip_yaml_scalar(workload_match.group(1))
                continue

            cpu_match = _CPU_PATTERN.match(line)
            if cpu_match:
                cpu_values.append(
                    _parse_decimal_value(cpu_match.group(1), field_name="cpu", manifest_path=manifest_path)
                )
                continue

            memory_match = _MEMORY_PATTERN.match(line)
            if memory_match:
                memory_values.append(_parse_memory_gib(memory_match.group(1), manifest_path=manifest_path))

        if workload_profile != "Consumption":
            continue

        if not cpu_values or not memory_values or len(cpu_values) != len(memory_values):
            fail(
                f"{manifest_path}: Consumption job manifests must define matching cpu/memory entries for each "
                "container resource block."
            )

        total_cpu = sum(cpu_values, Decimal("0"))
        total_memory_gi = sum(memory_values, Decimal("0"))
        expected_memory_gi = total_cpu * Decimal("2")

        if total_cpu < _CONSUMPTION_CPU_STEP or total_cpu > _CONSUMPTION_MAX_CPU:
            fail(
                f"{manifest_path}: Consumption total CPU must be between "
                f"{_format_decimal(_CONSUMPTION_CPU_STEP)} and {_format_decimal(_CONSUMPTION_MAX_CPU)}, "
                f"got cpu={_format_decimal(total_cpu)}."
            )
        if total_memory_gi <= 0 or total_memory_gi > _CONSUMPTION_MAX_MEMORY_GI:
            fail(
                f"{manifest_path}: Consumption total memory must be between 0.5Gi and "
                f"{_format_decimal(_CONSUMPTION_MAX_MEMORY_GI)}Gi, got memory={_format_decimal(total_memory_gi)}Gi."
            )
        if not _is_multiple_of(total_cpu, _CONSUMPTION_CPU_STEP):
            fail(
                f"{manifest_path}: Consumption total CPU must use 0.25-vCPU increments, "
                f"got cpu={_format_decimal(total_cpu)}."
            )
        if total_memory_gi != expected_memory_gi:
            fail(
                f"{manifest_path}: Consumption workloads require memory to equal 2x total CPU. "
                f"Got cpu={_format_decimal(total_cpu)} memory={_format_decimal(total_memory_gi)}Gi; "
                f"expected {_format_decimal(expected_memory_gi)}Gi."
            )


def parse_float(name: str, *, default: float, min_value: float = 0.0, max_value: float = 86400.0) -> float:
    raw = optional_value(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        fail(f"{name} must be a number.")
    if not (min_value <= value <= max_value):
        fail(f"{name} must be between {min_value} and {max_value}.")
    return value


def parse_int(name: str, *, default: int, min_value: int = 0, max_value: int = 86400) -> int:
    raw = optional_value(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        fail(f"{name} must be an integer.")
    if not (min_value <= value <= max_value):
        fail(f"{name} must be between {min_value} and {max_value}.")
    return value


def parse_json_array(name: str) -> list[object]:
    raw = optional_value(name)
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        fail(f"{name} must be valid JSON.")
    if not isinstance(payload, list):
        fail(f"{name} must be a JSON array.")
    return payload


def parse_postgres_url(name: str) -> tuple[str, int, str]:
    value = require_value(name)
    parsed = urlparse(value)
    if parsed.scheme not in {"postgresql", "postgres"}:
        fail(f"{name} must be a postgresql:// URL")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        fail(f"{name} is missing host")
    port = parsed.port or 5432
    database = (parsed.path or "").lstrip("/").strip()
    if not database:
        fail(f"{name} is missing database name")
    return host, int(port), database


def validate_api_base_url() -> None:
    parsed = urlparse(require_value("ASSET_ALLOCATION_API_BASE_URL"))
    host = (parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"http", "https"} or not host:
        fail("ASSET_ALLOCATION_API_BASE_URL must be an http(s) URL (e.g., http://asset-allocation-api)")
    if host in {"localhost", "127.0.0.1", "::1"}:
        fail(
            "ASSET_ALLOCATION_API_BASE_URL must not point to localhost in production. "
            "For Azure Container Apps Jobs, use http://asset-allocation-api (no port) or the API app FQDN."
        )
    if parsed.port == 8000:
        fail(
            "ASSET_ALLOCATION_API_BASE_URL must not include :8000 in production. "
            "Container Apps ingress listens on 80/443; use http://asset-allocation-api "
            "(no port) or the API app FQDN."
        )


def validate_log_level() -> None:
    log_level = optional_value("LOG_LEVEL").upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        fail("LOG_LEVEL must be one of: DEBUG|INFO|WARNING|ERROR|CRITICAL")


def validate_log_analytics() -> None:
    workspace_id = optional_value("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
    parse_float("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", default=5.0, min_value=0.5, max_value=30.0)
    parse_int("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", default=3, min_value=1, max_value=25)
    parse_float("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS", default=5.0, min_value=0.1, max_value=300.0)
    parse_int("SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES", default=15, min_value=1, max_value=1440)
    parse_json_array("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON")

    if workspace_id:
        parse_float("REALTIME_LOG_STREAM_POLL_SECONDS", default=5.0, min_value=1.0, max_value=300.0)
        parse_int("REALTIME_LOG_STREAM_LOOKBACK_SECONDS", default=30, min_value=10, max_value=86400)
        parse_int("REALTIME_LOG_STREAM_BATCH_SIZE", default=200, min_value=10, max_value=500)


def validate_auth_configuration() -> None:
    for deprecated_name in REMOVED_DEPLOY_ENV_KEYS:
        if has_env_key(deprecated_name):
            fail(
                f"{deprecated_name} is no longer supported. Remove the stale API-key/backtest compatibility setting."
            )

    api_oidc_issuer = optional_value("API_OIDC_ISSUER")
    api_oidc_audience = optional_value("API_OIDC_AUDIENCE")
    api_oidc_jwks_url = optional_value("API_OIDC_JWKS_URL")
    api_oidc_required_scopes = optional_value("API_OIDC_REQUIRED_SCOPES")
    api_oidc_required_roles = optional_value("API_OIDC_REQUIRED_ROLES")
    api_oidc_inputs_present = any(
        (
            api_oidc_issuer,
            api_oidc_audience,
            api_oidc_jwks_url,
            api_oidc_required_scopes,
            api_oidc_required_roles,
        )
    )

    if not api_oidc_inputs_present:
        fail("Production deploy requires API OIDC configuration.")
    if not api_oidc_issuer:
        fail("API_OIDC_ISSUER is required for the production deploy workflow.")
    if not api_oidc_audience:
        fail("API_OIDC_AUDIENCE is required for the production deploy workflow.")

    ui_oidc_values = {
        "UI_OIDC_CLIENT_ID": optional_value("UI_OIDC_CLIENT_ID"),
        "UI_OIDC_AUTHORITY": optional_value("UI_OIDC_AUTHORITY"),
        "UI_OIDC_SCOPES": optional_value("UI_OIDC_SCOPES"),
        "UI_OIDC_REDIRECT_URI": optional_value("UI_OIDC_REDIRECT_URI"),
    }

    if not any(ui_oidc_values.values()):
        fail(
            "Production deploy requires browser OIDC configuration for the UI. "
            "Set UI_OIDC_CLIENT_ID, UI_OIDC_AUTHORITY, UI_OIDC_SCOPES, and "
            "UI_OIDC_REDIRECT_URI. The deployed UI only supports OIDC."
        )

    missing_ui_oidc = [name for name, value in ui_oidc_values.items() if not value]
    if missing_ui_oidc:
        fail(
            "Production deploy requires complete browser OIDC configuration for the UI. "
            f"Missing: {', '.join(missing_ui_oidc)}. The deployed UI only supports OIDC."
        )

    parsed = urlparse(ui_oidc_values["UI_OIDC_REDIRECT_URI"])
    if parsed.scheme != "https" or not (parsed.hostname or "").strip():
        fail("UI_OIDC_REDIRECT_URI must be an absolute https:// URL.")

    if not optional_value("ASSET_ALLOCATION_API_SCOPE"):
        fail("ASSET_ALLOCATION_API_SCOPE is required for bronze job managed-identity callers.")


def validate_ai_relay_configuration() -> None:
    if not parse_bool("AI_RELAY_ENABLED", default=False):
        return

    require_value("AI_RELAY_API_KEY")
    if not optional_value("AI_RELAY_REQUIRED_ROLES"):
        fail("AI_RELAY_REQUIRED_ROLES is required when AI_RELAY_ENABLED=true.")

    model = optional_value("AI_RELAY_MODEL") or "gpt-5.4-mini"
    reasoning_effort = (optional_value("AI_RELAY_REASONING_EFFORT") or "low").lower()
    supported_efforts = {"none", "minimal", "low", "medium", "high", "xhigh"}
    if reasoning_effort not in supported_efforts:
        fail(
            "AI_RELAY_REASONING_EFFORT must be one of: "
            + ", ".join(sorted(supported_efforts))
        )

    parse_float("AI_RELAY_TIMEOUT_SECONDS", default=120.0, min_value=1.0, max_value=900.0)
    parse_int("AI_RELAY_MAX_PROMPT_CHARS", default=40_000, min_value=1, max_value=200_000)
    parse_int("AI_RELAY_MAX_FILES", default=4, min_value=0, max_value=16)
    max_file_bytes = parse_int("AI_RELAY_MAX_FILE_BYTES", default=5 * 1024 * 1024, min_value=1, max_value=50 * 1024 * 1024)
    max_total_bytes = parse_int(
        "AI_RELAY_MAX_TOTAL_FILE_BYTES",
        default=20 * 1024 * 1024,
        min_value=1,
        max_value=50 * 1024 * 1024,
    )
    parse_int("AI_RELAY_MAX_OUTPUT_TOKENS", default=4_000, min_value=1, max_value=32_000)

    if max_total_bytes < max_file_bytes:
        fail("AI_RELAY_MAX_TOTAL_FILE_BYTES must be greater than or equal to AI_RELAY_MAX_FILE_BYTES.")

    if model == "gpt-5-pro" and reasoning_effort != "high":
        fail("AI_RELAY_MODEL=gpt-5-pro only supports AI_RELAY_REASONING_EFFORT=high.")
    if model.startswith("gpt-5.1") and reasoning_effort not in {"none", "low", "medium", "high"}:
        fail(
            "gpt-5.1 models only support reasoning efforts none, low, medium, and high."
        )


def main() -> int:
    for name in REQUIRED_ENV_NAMES:
        require_value(name)

    parse_postgres_url("POSTGRES_DSN")
    validate_job_manifest_resources()
    validate_api_base_url()
    validate_log_level()
    validate_log_analytics()
    validate_auth_configuration()
    validate_ai_relay_configuration()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
