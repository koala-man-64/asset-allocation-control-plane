from __future__ import annotations

import os

import pytest

from tasks.common import job_trigger


def test_ensure_api_awake_cloud_runtime_probes_health(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "exec-1")

    probe_calls: list[str] = []

    def _probe_once(**kwargs):
        probe_calls.append(kwargs["health_url"])
        return True, "status=200"

    monkeypatch.setattr(job_trigger, "_probe_health", _probe_once)

    job_trigger.ensure_api_awake_from_env(required=True)

    assert probe_calls == ["http://asset-allocation-api/healthz"]


def test_ensure_api_awake_raises_when_required_and_base_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ASSET_ALLOCATION_API_BASE_URL", raising=False)
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "exec-1")

    with pytest.raises(RuntimeError, match="ASSET_ALLOCATION_API_BASE_URL"):
        job_trigger.ensure_api_awake_from_env(required=True)


def test_ensure_api_awake_starts_container_app_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "exec-1")

    probes = iter(
        [
            (False, "status=503"),
            (True, "status=200"),
        ]
    )

    monkeypatch.setattr(
        job_trigger,
        "_probe_health",
        lambda **_kwargs: next(probes),
    )
    monkeypatch.setattr(job_trigger.time, "sleep", lambda _seconds: None)

    start_calls: list[tuple[str, bool]] = []

    def _fake_start(*, app_name: str, cfg: job_trigger.ArmConfig, required: bool = True) -> bool:
        assert cfg.subscription_id == "sub"
        assert cfg.resource_group == "rg"
        start_calls.append((app_name, required))
        return True

    monkeypatch.setattr(job_trigger, "_start_container_app", _fake_start)

    job_trigger.ensure_api_awake_from_env(required=True)

    assert start_calls == [("asset-allocation-api", True)]


def test_ensure_api_awake_local_waits_for_local_health(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CONTAINER_APP_JOB_EXECUTION_NAME", raising=False)
    monkeypatch.delenv("CONTAINER_APP_REPLICA_NAME", raising=False)
    monkeypatch.delenv("CONTAINER_APP_ENV_DNS_SUFFIX", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")

    probe_calls: list[str] = []
    probes = iter(
        [
            (False, "status=503"),
            (False, "status=503"),
            (True, "status=200"),
        ]
    )

    def _fake_probe(*, health_url: str, timeout_seconds: float) -> tuple[bool, str]:
        assert timeout_seconds > 0
        probe_calls.append(health_url)
        return next(probes)

    monkeypatch.setattr(job_trigger, "_probe_health", _fake_probe)
    monkeypatch.setattr(job_trigger.time, "sleep", lambda _seconds: None)

    def _unexpected_start(**_kwargs):
        raise AssertionError("local runtime should not call ARM startup")

    monkeypatch.setattr(job_trigger, "_start_container_app", _unexpected_start)

    job_trigger.ensure_api_awake_from_env(required=True)

    assert probe_calls == []
    assert (os.environ.get("ASSET_ALLOCATION_API_BASE_URL") or "").strip() == "http://asset-allocation-api"


def test_resolve_startup_container_apps_matches_allowlist_from_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOB_STARTUP_API_CONTAINER_APPS", raising=False)
    monkeypatch.delenv("API_CONTAINER_APP_NAME", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "asset-allocation-api,asset-allocation-ui")

    resolved = job_trigger._resolve_startup_container_apps("https://asset-allocation-api.internal.azurecontainerapps.io")
    assert resolved == ["asset-allocation-api"]


def test_trigger_next_job_from_env_logs_multi_job_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIGGER_NEXT_JOB_NAME", "silver-market-job, gold-market-job")

    messages: list[str] = []
    dispatched: list[tuple[str, bool]] = []

    monkeypatch.setattr(job_trigger.mdc, "write_line", messages.append)
    monkeypatch.setattr(
        job_trigger,
        "trigger_containerapp_job_start",
        lambda *, job_name, required=True: dispatched.append((job_name, required)),
    )

    job_trigger.trigger_next_job_from_env()

    assert dispatched == [
        ("silver-market-job", True),
        ("gold-market-job", True),
    ]
    assert any(
        "Downstream trigger plan: jobs=silver-market-job,gold-market-job required=true count=2"
        in message
        for message in messages
    )
