import sys
from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from monitoring.log_analytics import extract_first_table_rows
from core.redaction import redact_sensitive_text


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def _system_attr(name: str, default: Any) -> Any:
    system_module = sys.modules.get("api.endpoints.system")
    if system_module is None:
        return default
    return getattr(system_module, name, default)


def _compat_export(name: str, target: Any) -> Any:
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        resolved = _system_attr(name, target)
        if resolved is _wrapper:
            resolved = target
        return resolved(*args, **kwargs)

    _wrapper.__name__ = getattr(target, "__name__", name)
    _wrapper.__doc__ = getattr(target, "__doc__", None)
    _wrapper.__module__ = __name__
    return _wrapper


_ACTIVE_JOB_EXECUTION_STATUS_TOKENS = frozenset(
    {"running", "processing", "inprogress", "starting", "queued", "waiting", "scheduling"}
)


def _configured_job_allowlist(os_module: Any) -> List[str]:
    raw = os_module.environ.get("SYSTEM_HEALTH_ARM_JOBS")
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _job_name_allowed(job_name: str, allowlist: Sequence[str]) -> bool:
    if not allowlist or "*" in {item.strip() for item in allowlist}:
        return True
    return job_name in allowlist


def _normalize_job_execution_status_token(value: Optional[str]) -> str:
    return "".join(ch for ch in str(value or "").strip().lower() if ch.isalnum())


def _is_active_job_execution_status(value: Optional[str]) -> bool:
    return _normalize_job_execution_status_token(value) in _ACTIVE_JOB_EXECUTION_STATUS_TOKENS


def _is_active_job_execution(execution: Dict[str, Any]) -> bool:
    return _is_active_job_execution_status(execution.get("status")) and not str(
        execution.get("endTime") or ""
    ).strip()


def _select_anchored_job_executions(
    executions: Sequence[Dict[str, Any]], *, limit: int
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []

    selected = list(executions[:limit])
    if not selected:
        return selected

    active_execution = next(
        (execution for execution in executions if _is_active_job_execution(execution)),
        None,
    )
    if active_execution is None or active_execution in selected:
        return selected

    return [active_execution, *selected[: max(0, limit - 1)]]


def _coalesce_log_row_string(row: Dict[str, Any], *keys: str) -> str:
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_console_log_entries(payload: Dict[str, Any]) -> List[Dict[str, Optional[str]]]:
    entries: List[Dict[str, Optional[str]]] = []
    for row in extract_first_table_rows(payload):
        if not isinstance(row, dict):
            continue
        message = redact_sensitive_text(
            _coalesce_log_row_string(row, "msg", "Log_s", "Log", "LogMessage_s", "Message", "message")
        )
        if not message:
            continue
        entries.append(
            {
                "timestamp": _coalesce_log_row_string(row, "TimeGenerated", "timegenerated") or None,
                "stream_s": _coalesce_log_row_string(row, "stream_s", "Stream_s", "stream", "Stream") or None,
                "executionName": _coalesce_log_row_string(
                    row,
                    "executionName",
                    "ExecutionName",
                    "exec",
                    "Exec",
                    "execution_name",
                    "Execution_Name",
                )
                or None,
                "message": message,
            }
        )
    return entries


def _extract_log_lines(payload: Dict[str, Any]) -> List[str]:
    return [str(item.get("message") or "") for item in _extract_console_log_entries(payload) if item.get("message")]


_normalize_job_execution_status_token_impl = _normalize_job_execution_status_token
_normalize_job_execution_status_token = _compat_export(
    "_normalize_job_execution_status_token",
    _normalize_job_execution_status_token_impl,
)
_is_active_job_execution_status_impl = _is_active_job_execution_status
_is_active_job_execution_status = _compat_export(
    "_is_active_job_execution_status",
    _is_active_job_execution_status_impl,
)
_is_active_job_execution_impl = _is_active_job_execution
_is_active_job_execution = _compat_export("_is_active_job_execution", _is_active_job_execution_impl)
_select_anchored_job_executions_impl = _select_anchored_job_executions
_select_anchored_job_executions = _compat_export(
    "_select_anchored_job_executions",
    _select_anchored_job_executions_impl,
)
_coalesce_log_row_string_impl = _coalesce_log_row_string
_coalesce_log_row_string = _compat_export("_coalesce_log_row_string", _coalesce_log_row_string_impl)
_extract_console_log_entries_impl = _extract_console_log_entries
_extract_console_log_entries = _compat_export(
    "_extract_console_log_entries",
    _extract_console_log_entries_impl,
)
_extract_log_lines_impl = _extract_log_lines
_extract_log_lines = _compat_export("_extract_log_lines", _extract_log_lines_impl)


def build_router(*, runtime: ModuleType) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.post("/jobs/{job_name}/run")
    def trigger_job_run(job_name: str, request: Request) -> JSONResponse:
        require_job_operate_access = _runtime_attr(runtime, "require_job_operate_access")
        job_control_context = _runtime_attr(runtime, "_job_control_context")
        logger = _runtime_attr(runtime, "logger")
        os_module = _runtime_attr(runtime, "os")
        re_module = _runtime_attr(runtime, "re")
        arm_config_cls = _runtime_attr(runtime, "ArmConfig")
        azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
        httpx_module = _runtime_attr(runtime, "httpx")
        extract_arm_error_message = _runtime_attr(runtime, "_extract_arm_error_message")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        jobs_topic = _runtime_attr(runtime, "REALTIME_TOPIC_JOBS")
        system_health_topic = _runtime_attr(runtime, "REALTIME_TOPIC_SYSTEM_HEALTH")

        require_job_operate_access(request)
        control_context = job_control_context(request)
        logger.info(
            "Trigger job run requested: job=%s actor=%s requestId=%s",
            job_name,
            control_context.get("actor") or "-",
            control_context.get("requestId") or "-",
        )

        subscription_id_raw = os_module.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
        subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
        resource_group_raw = os_module.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
        resource_group = resource_group_raw.strip() if resource_group_raw else ""
        job_allowlist = _configured_job_allowlist(os_module)

        if not (subscription_id and resource_group):
            raise HTTPException(status_code=503, detail="Azure job triggering is not configured.")

        resolved = (job_name or "").strip()
        if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
            raise HTTPException(status_code=400, detail="Invalid job name.")
        if not _job_name_allowed(resolved, job_allowlist):
            raise HTTPException(status_code=404, detail="Job not found.")

        api_version_env = os_module.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
        api_version = api_version_env.strip() if api_version_env else ""
        if not api_version:
            api_version = arm_config_cls.api_version

        timeout_env = os_module.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
        try:
            timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
        except ValueError:
            timeout_seconds = 5.0

        cfg = arm_config_cls(
            subscription_id=subscription_id,
            resource_group=resource_group,
            api_version=api_version,
            timeout_seconds=timeout_seconds,
        )

        try:
            with azure_arm_client(cfg) as arm:
                job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
                payload = arm.post_json(f"{job_url}/start")
        except httpx_module.HTTPStatusError as exc:
            message = extract_arm_error_message(exc.response)
            logger.warning(
                "Azure job start failed: job=%s status=%s message=%s",
                resolved,
                exc.response.status_code,
                message or "?",
            )
            if "suspended" in (message or "").lower():
                raise HTTPException(
                    status_code=409,
                    detail=f"Job is suspended. Resume it, then trigger again. ({message})",
                ) from exc
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Failed to trigger job: {message or str(exc)}",
            ) from exc
        except Exception as exc:
            logger.exception("Failed to trigger Azure job run: job=%s", resolved)
            raise HTTPException(status_code=502, detail=f"Failed to trigger job: {exc}") from exc

        execution_id: Optional[str] = None
        execution_name: Optional[str] = None
        if isinstance(payload, dict):
            execution_id = str(payload.get("id") or "") or None
            execution_name = str(payload.get("name") or "") or None

        logger.info("Triggered Azure job run: job=%s execution=%s", resolved, execution_name or execution_id or "?")
        response_payload = {
            "jobName": resolved,
            "status": "queued",
            "executionId": execution_id,
            "executionName": execution_name,
            "command": "run",
            **control_context,
        }
        emit_realtime(
            jobs_topic,
            "JOB_STATE_CHANGED",
            {
                "jobName": resolved,
                "action": "run",
                "command": "run",
                "status": "queued",
                "executionId": execution_id,
                "executionName": execution_name,
                **control_context,
            },
        )
        emit_realtime(
            system_health_topic,
            "SYSTEM_HEALTH_UPDATE",
            {
                "source": "job-control",
                "jobName": resolved,
                "action": "run",
                "command": "run",
                **control_context,
            },
        )
        return JSONResponse(response_payload, status_code=202)

    @router.post("/jobs/{job_name}/suspend")
    def suspend_job(job_name: str, request: Request) -> JSONResponse:
        return _job_state_command(runtime=runtime, request=request, job_name=job_name, action="suspend")

    @router.post("/jobs/{job_name}/stop")
    def stop_job(job_name: str, request: Request) -> JSONResponse:
        return _job_state_command(runtime=runtime, request=request, job_name=job_name, action="stop")

    @router.post("/jobs/{job_name}/resume")
    def resume_job(job_name: str, request: Request) -> JSONResponse:
        return _job_state_command(runtime=runtime, request=request, job_name=job_name, action="resume")

    @router.get("/jobs/{job_name}/logs")
    def get_job_logs(
        job_name: str,
        request: Request,
        runs: int = Query(1, ge=1, le=10),
    ) -> JSONResponse:
        require_system_logs_read_access = _runtime_attr(runtime, "require_system_logs_read_access")
        os_module = _runtime_attr(runtime, "os")
        re_module = _runtime_attr(runtime, "re")
        arm_config_cls = _runtime_attr(runtime, "ArmConfig")
        azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
        azure_log_analytics_client = _runtime_attr(runtime, "AzureLogAnalyticsClient")
        logger = _runtime_attr(runtime, "logger")
        datetime_cls = _runtime_attr(runtime, "datetime")
        timezone_obj = _runtime_attr(runtime, "timezone")
        timedelta_cls = _runtime_attr(runtime, "timedelta")
        parse_dt = _runtime_attr(runtime, "_parse_dt")
        select_anchored_job_executions = _runtime_attr(runtime, "_select_anchored_job_executions")
        escape_kql_literal = _runtime_attr(runtime, "_escape_kql_literal")
        extract_console_log_entries = _runtime_attr(runtime, "_extract_console_log_entries")

        require_system_logs_read_access(request)

        subscription_id_raw = os_module.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
        subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
        resource_group_raw = os_module.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
        resource_group = resource_group_raw.strip() if resource_group_raw else ""
        job_allowlist = _configured_job_allowlist(os_module)

        if not (subscription_id and resource_group):
            raise HTTPException(status_code=503, detail="Azure job log retrieval is not configured.")

        resolved = (job_name or "").strip()
        if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
            raise HTTPException(status_code=400, detail="Invalid job name.")
        if not _job_name_allowed(resolved, job_allowlist):
            raise HTTPException(status_code=404, detail="Job not found.")

        workspace_id_raw = os_module.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
        workspace_id = workspace_id_raw.strip() if workspace_id_raw else ""
        if not workspace_id:
            raise HTTPException(status_code=503, detail="Log Analytics is not configured for job log retrieval.")

        log_timeout_raw = os_module.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS")
        try:
            log_timeout_seconds = float(log_timeout_raw.strip()) if log_timeout_raw else 5.0
        except ValueError:
            log_timeout_seconds = 5.0

        api_version_env = os_module.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
        api_version = api_version_env.strip() if api_version_env else ""
        if not api_version:
            api_version = arm_config_cls.api_version

        timeout_env = os_module.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
        try:
            timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
        except ValueError:
            timeout_seconds = 5.0

        cfg = arm_config_cls(
            subscription_id=subscription_id,
            resource_group=resource_group,
            api_version=api_version,
            timeout_seconds=timeout_seconds,
        )

        try:
            with azure_arm_client(cfg) as arm:
                job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
                exec_payload = arm.get_json(f"{job_url}/executions")
        except Exception as exc:
            logger.exception("Failed to list Azure job executions: job=%s", resolved)
            raise HTTPException(status_code=502, detail=f"Failed to list job executions: {exc}") from exc

        now = datetime_cls.now(timezone_obj.utc)
        values = exec_payload.get("value") if isinstance(exec_payload.get("value"), list) else []
        executions: List[Dict[str, Any]] = []
        for item in values:
            if not isinstance(item, dict):
                continue
            props = item.get("properties") if isinstance(item.get("properties"), dict) else {}
            start_time = str(props.get("startTime") or "")
            end_time = str(props.get("endTime") or "")
            executions.append(
                {
                    "executionName": str(item.get("name") or "") or None,
                    "executionId": str(item.get("id") or "") or None,
                    "status": str(props.get("status") or "") or None,
                    "startTime": start_time or None,
                    "endTime": end_time or None,
                    "_start_ts": (parse_dt(start_time) or now).timestamp(),
                }
            )

        executions.sort(key=lambda e: float(e.get("_start_ts") or 0.0), reverse=True)
        selected = select_anchored_job_executions(executions, limit=max(0, int(runs)))
        tail_lines = 10

        out_runs: List[Dict[str, Any]] = []
        with azure_log_analytics_client(timeout_seconds=log_timeout_seconds) as log_client:
            for run in selected:
                exec_name = str(run.get("executionName") or "").strip()
                start_dt = parse_dt(str(run.get("startTime") or "")) or now
                end_dt = parse_dt(str(run.get("endTime") or "")) or now
                if end_dt < start_dt:
                    end_dt = now

                start = start_dt - timedelta_cls(minutes=5)
                end = end_dt + timedelta_cls(minutes=10)
                if end - start > timedelta_cls(hours=24):
                    start = end - timedelta_cls(hours=24)

                timespan = f"{start.isoformat()}/{end.isoformat()}"
                job_kql = escape_kql_literal(resolved)
                exec_kql = escape_kql_literal(exec_name)
                query = f"""
let jobName = '{job_kql}';
let execName = '{exec_kql}';
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppConsoleLogs
| extend job = tostring(
    column_ifexists('ContainerJobName_s',
        column_ifexists('ContainerName_s',
            column_ifexists('ContainerAppJobName_s',
                column_ifexists('JobName_s',
                    column_ifexists('JobName',
                        column_ifexists('ContainerAppName_s', '')
                    )
                )
            )
        )
    )
)
| extend exec = tostring(
    column_ifexists('ContainerGroupName_s',
        column_ifexists('ContainerGroupName',
            column_ifexists('ContainerAppJobExecutionName_s',
                column_ifexists('ExecutionName_s',
                    column_ifexists('ExecutionName',
                        column_ifexists('ContainerGroupId_g',
                            column_ifexists('ContainerAppJobExecutionId_g',
                                column_ifexists('ContainerAppJobExecutionId_s', '')
                            )
                        )
                    )
                )
            )
        )
    )
)
| extend resource = tostring(column_ifexists('_ResourceId', column_ifexists('ResourceId', '')))
| extend msg = tostring(
    column_ifexists('Log_s',
        column_ifexists('Log',
            column_ifexists('LogMessage_s',
                column_ifexists('Message',
                    column_ifexists('message', '')
                )
            )
        )
    )
)
| extend stream_s = tostring(
    column_ifexists('Stream_s',
        column_ifexists('stream_s',
            column_ifexists('Stream',
                column_ifexists('stream', '')
            )
        )
    )
)
| extend jobMatch = (job != '' and job contains jobName) or (resource contains jobName)
| extend execMatch = execName != '' and ((exec != '' and exec contains execName) or (resource contains execName))
| where jobMatch or execMatch
| order by execMatch desc, jobMatch desc, TimeGenerated desc
| take {tail_lines}
| project TimeGenerated, executionName=exec, stream_s, msg
| order by TimeGenerated asc
""".strip()

                try:
                    payload = log_client.query(workspace_id=workspace_id, query=query, timespan=timespan)
                    console_logs = extract_console_log_entries(payload)
                    lines = [str(item.get("message") or "") for item in console_logs if item.get("message")]
                    err = None
                except Exception as exc:
                    console_logs = []
                    lines = []
                    err = str(exc)

                out_runs.append(
                    {
                        "executionName": run.get("executionName"),
                        "executionId": run.get("executionId"),
                        "status": run.get("status"),
                        "startTime": run.get("startTime"),
                        "endTime": run.get("endTime"),
                        "tail": lines,
                        "consoleLogs": console_logs,
                        "error": err,
                    }
                )

        for item in selected:
            item.pop("_start_ts", None)

        return JSONResponse(
            {
                "jobName": resolved,
                "runsRequested": int(runs),
                "runsReturned": len(out_runs),
                "tailLines": tail_lines,
                "runs": out_runs,
            },
            headers={"Cache-Control": "no-store"},
        )

    return router, {
        "trigger_job_run": trigger_job_run,
        "suspend_job": suspend_job,
        "stop_job": stop_job,
        "resume_job": resume_job,
        "get_job_logs": get_job_logs,
    }


def _job_state_command(*, runtime: ModuleType, request: Request, job_name: str, action: str) -> JSONResponse:
    require_job_operate_access = _runtime_attr(runtime, "require_job_operate_access")
    job_control_context = _runtime_attr(runtime, "_job_control_context")
    logger = _runtime_attr(runtime, "logger")
    os_module = _runtime_attr(runtime, "os")
    re_module = _runtime_attr(runtime, "re")
    arm_config_cls = _runtime_attr(runtime, "ArmConfig")
    azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
    httpx_module = _runtime_attr(runtime, "httpx")
    extract_arm_error_message = _runtime_attr(runtime, "_extract_arm_error_message")
    emit_realtime = _runtime_attr(runtime, "_emit_realtime")
    jobs_topic = _runtime_attr(runtime, "REALTIME_TOPIC_JOBS")
    system_health_topic = _runtime_attr(runtime, "REALTIME_TOPIC_SYSTEM_HEALTH")
    action_label = {"suspend": "Suspended", "stop": "Stopped", "resume": "Resumed"}.get(
        action,
        f"{action.capitalize()}ed",
    )

    require_job_operate_access(request)
    control_context = job_control_context(request)
    logger.info(
        "%s job requested: job=%s actor=%s requestId=%s",
        action.capitalize(),
        job_name,
        control_context.get("actor") or "-",
        control_context.get("requestId") or "-",
    )

    subscription_id_raw = os_module.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os_module.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""
    job_allowlist = _configured_job_allowlist(os_module)

    if not (subscription_id and resource_group):
        raise HTTPException(status_code=503, detail="Azure job control is not configured.")

    resolved = (job_name or "").strip()
    if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid job name.")
    if not _job_name_allowed(resolved, job_allowlist):
        raise HTTPException(status_code=404, detail="Job not found.")

    api_version_env = os_module.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = arm_config_cls.api_version

    timeout_env = os_module.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = arm_config_cls(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with azure_arm_client(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            target_url = f"{job_url}/{action}"
            try:
                payload = arm.post_json(target_url)
            except httpx_module.HTTPStatusError as exc:
                if action == "stop" and exc.response.status_code in {404, 405}:
                    logger.warning(
                        "Stop job endpoint unavailable, falling back to suspend for job=%s status=%s",
                        resolved,
                        exc.response.status_code,
                    )
                    payload = arm.post_json(f"{job_url}/suspend")
                else:
                    message = extract_arm_error_message(exc.response)
                    raise HTTPException(
                        status_code=exc.response.status_code,
                        detail=f"Failed to {action} job: {message or str(exc)}",
                    ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to %s Azure job: job=%s", action, resolved)
        raise HTTPException(status_code=502, detail=f"Failed to {action} job: {exc}") from exc

    running_state: Optional[str] = None
    if isinstance(payload, dict):
        props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
        running_state = str(props.get("runningState") or "") or None

    logger.info("%s Azure job: job=%s running_state=%s", action_label, resolved, running_state or "?")
    response_payload = {
        "jobName": resolved,
        "action": action,
        "runningState": running_state,
        "command": action,
        **control_context,
    }
    emit_realtime(
        jobs_topic,
        "JOB_STATE_CHANGED",
        {
            "jobName": resolved,
            "action": action,
            "command": action,
            "runningState": running_state,
            **control_context,
        },
    )
    emit_realtime(
        system_health_topic,
        "SYSTEM_HEALTH_UPDATE",
        {
            "source": "job-control",
            "jobName": resolved,
            "action": action,
            "command": action,
            **control_context,
        },
    )
    return JSONResponse(response_payload, status_code=202)
