from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx

from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.control_plane import _map_job_execution_status

logger = logging.getLogger(__name__)

_RESOURCE_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?")


def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _extract_arm_error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except Exception:
        return (response.text or "").strip()
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            detail = error.get("message") or error.get("detail")
            if isinstance(detail, str) and detail.strip():
                return detail.strip()
        if isinstance(error, str) and error.strip():
            return error.strip()
        detail = payload.get("message")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
    if isinstance(payload, str):
        return payload.strip()
    return (response.text or "").strip()


def resolve_backtest_job_name() -> str:
    job_name = str(os.environ.get("BACKTEST_ACA_JOB_NAME") or "backtests-job").strip()
    if not _RESOURCE_NAME_PATTERN.fullmatch(job_name):
        raise ValueError("BACKTEST_ACA_JOB_NAME is invalid.")
    return job_name


def _arm_config_from_env() -> ArmConfig:
    subscription_id = str(os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID") or "").strip()
    resource_group = str(os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP") or "").strip()
    if not subscription_id or not resource_group:
        raise ValueError("Azure job triggering is not configured.")
    api_version = str(os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION") or "").strip() or ArmConfig.api_version
    timeout_raw = str(os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS") or "").strip()
    try:
        timeout_seconds = float(timeout_raw) if timeout_raw else 5.0
    except ValueError:
        timeout_seconds = 5.0
    return ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )


def trigger_backtest_job(job_name: str) -> dict[str, Any]:
    try:
        cfg = _arm_config_from_env()
    except ValueError:
        if _is_truthy(os.environ.get("TEST_MODE")):
            return {"status": "queued", "executionName": None, "jobName": job_name}
        raise
    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=job_name)
            payload = arm.post_json(f"{job_url}/start")
    except httpx.HTTPStatusError as exc:
        message = _extract_arm_error_message(exc.response)
        raise ValueError(message or str(exc)) from exc
    execution_name = None
    if isinstance(payload, dict):
        execution_name = str(payload.get("name") or "").strip() or None
    return {"status": "queued", "executionName": execution_name, "jobName": job_name}


def get_job_execution(job_name: str, execution_name: str) -> dict[str, Any] | None:
    resolved_execution_name = str(execution_name or "").strip()
    if not resolved_execution_name:
        return None

    try:
        cfg = _arm_config_from_env()
    except ValueError:
        if _is_truthy(os.environ.get("TEST_MODE")):
            return None
        raise
    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=job_name)
            payload = arm.get_json(f"{job_url}/executions/{resolved_execution_name}")
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None
        message = _extract_arm_error_message(exc.response)
        raise ValueError(message or str(exc)) from exc

    props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    raw_status = str(props.get("status") or "")
    end_time = str(props.get("endTime") or "")
    return {
        "executionName": str(payload.get("name") or resolved_execution_name) or resolved_execution_name,
        "executionId": str(payload.get("id") or "") or None,
        "status": _map_job_execution_status(raw_status, end_time=end_time),
        "statusCode": raw_status or None,
        "startTime": str(props.get("startTime") or "") or None,
        "endTime": end_time or None,
    }
