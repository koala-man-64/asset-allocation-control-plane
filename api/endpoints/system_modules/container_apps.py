import json
import logging
import os
import sys
from datetime import datetime, timezone
from types import ModuleType
from typing import Any, Dict, List, Optional

import httpx

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("asset-allocation.api.system.container_apps")


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


def _normalize_container_app_name(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _container_app_allowlist() -> tuple[str, str, List[str]]:
    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""
    app_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS")
    app_allowlist = [item.strip() for item in (app_names_raw or "").split(",") if item.strip()]
    return subscription_id, resource_group, app_allowlist


def _container_app_health_url_overrides() -> Dict[str, str]:
    raw = (os.environ.get("SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        logger.warning("Invalid SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON; expected JSON object.")
        return {}
    if not isinstance(payload, dict):
        logger.warning("Invalid SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON type; expected JSON object.")
        return {}

    out: Dict[str, str] = {}
    for key, value in payload.items():
        name = _normalize_container_app_name(str(key or ""))
        url = str(value or "").strip()
        if not name or not url:
            continue
        out[name] = url
    return out


def _container_app_default_health_path(app_name: str) -> str:
    lowered = _normalize_container_app_name(app_name)
    if "api" in lowered:
        return "/healthz"
    return "/"


def _resolve_container_app_health_url(
    app_name: str,
    *,
    ingress_fqdn: Optional[str],
    overrides: Dict[str, str],
) -> Optional[str]:
    override = overrides.get(_normalize_container_app_name(app_name))
    if override:
        if override.startswith(("http://", "https://")):
            return override
        if override.startswith("/") and ingress_fqdn:
            path = override if override.startswith("/") else f"/{override}"
            return f"https://{ingress_fqdn}{path}"
        return override

    if not ingress_fqdn:
        return None
    path = _container_app_default_health_path(app_name)
    if not path.startswith("/"):
        path = f"/{path}"
    return f"https://{ingress_fqdn}{path}"


def _probe_container_app_health(url: str, *, timeout_seconds: float) -> Dict[str, Any]:
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        with httpx.Client(timeout=max(0.5, float(timeout_seconds)), follow_redirects=True, trust_env=False) as client:
            response = client.get(url)
        status_code = int(response.status_code)
        if 200 <= status_code < 400:
            status = "healthy"
        elif 400 <= status_code < 500:
            status = "warning"
        else:
            status = "error"
        return {
            "status": status,
            "url": url,
            "httpStatus": status_code,
            "checkedAt": checked_at,
            "error": None,
        }
    except Exception as exc:
        return {
            "status": "error",
            "url": url,
            "httpStatus": None,
            "checkedAt": checked_at,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _resource_status_from_provisioning_state(value: str) -> str:
    state = str(value or "").strip().lower()
    if state == "succeeded":
        return "healthy"
    if state in {"failed", "canceled", "cancelled"}:
        return "error"
    if state in {"creating", "updating", "deleting", "inprogress"}:
        return "warning"
    if not state:
        return "unknown"
    return "warning"


def _worse_status(a: str, b: str) -> str:
    order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return b if order.get(b, 0) > order.get(a, 0) else a


def _extract_container_app_properties(payload: Dict[str, Any]) -> Dict[str, Any]:
    props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    configuration = props.get("configuration") if isinstance(props.get("configuration"), dict) else {}
    ingress = configuration.get("ingress") if isinstance(configuration.get("ingress"), dict) else {}

    provisioning_state = str(props.get("provisioningState") or "").strip() or None
    running_state = (
        str(props.get("runningStatus") or "").strip()
        or str(props.get("runningState") or "").strip()
        or None
    )
    latest_ready_revision = str(props.get("latestReadyRevisionName") or "").strip() or None
    ingress_fqdn = str(ingress.get("fqdn") or "").strip() or None
    resource_id = str(payload.get("id") or "").strip() or None

    return {
        "provisioningState": provisioning_state,
        "runningState": running_state,
        "latestReadyRevisionName": latest_ready_revision,
        "ingressFqdn": ingress_fqdn,
        "azureId": resource_id,
    }


_normalize_container_app_name_impl = _normalize_container_app_name
_normalize_container_app_name = _compat_export("_normalize_container_app_name", _normalize_container_app_name_impl)
_container_app_allowlist_impl = _container_app_allowlist
_container_app_allowlist = _compat_export("_container_app_allowlist", _container_app_allowlist_impl)
_container_app_health_url_overrides_impl = _container_app_health_url_overrides
_container_app_health_url_overrides = _compat_export(
    "_container_app_health_url_overrides",
    _container_app_health_url_overrides_impl,
)
_container_app_default_health_path_impl = _container_app_default_health_path
_container_app_default_health_path = _compat_export(
    "_container_app_default_health_path",
    _container_app_default_health_path_impl,
)
_resolve_container_app_health_url_impl = _resolve_container_app_health_url
_resolve_container_app_health_url = _compat_export(
    "_resolve_container_app_health_url",
    _resolve_container_app_health_url_impl,
)
_probe_container_app_health_impl = _probe_container_app_health
_probe_container_app_health = _compat_export("_probe_container_app_health", _probe_container_app_health_impl)
_resource_status_from_provisioning_state_impl = _resource_status_from_provisioning_state
_resource_status_from_provisioning_state = _compat_export(
    "_resource_status_from_provisioning_state",
    _resource_status_from_provisioning_state_impl,
)
_worse_status_impl = _worse_status
_worse_status = _compat_export("_worse_status", _worse_status_impl)
_extract_container_app_properties_impl = _extract_container_app_properties
_extract_container_app_properties = _compat_export(
    "_extract_container_app_properties",
    _extract_container_app_properties_impl,
)


def build_router(*, runtime: ModuleType) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/container-apps")
    def list_container_apps(
        request: Request,
        probe: bool = Query(True, description="When true, perform live health pings for each app."),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        container_app_allowlist = _runtime_attr(runtime, "_container_app_allowlist")
        os_module = _runtime_attr(runtime, "os")
        arm_config_cls = _runtime_attr(runtime, "ArmConfig")
        azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
        extract_container_app_properties = _runtime_attr(runtime, "_extract_container_app_properties")
        resource_status_from_provisioning_state = _runtime_attr(runtime, "_resource_status_from_provisioning_state")
        resolve_container_app_health_url = _runtime_attr(runtime, "_resolve_container_app_health_url")
        probe_container_app_health = _runtime_attr(runtime, "_probe_container_app_health")
        worse_status = _runtime_attr(runtime, "_worse_status")
        container_app_health_url_overrides = _runtime_attr(runtime, "_container_app_health_url_overrides")
        datetime_cls = _runtime_attr(runtime, "datetime")
        timezone_obj = _runtime_attr(runtime, "timezone")
        re_module = _runtime_attr(runtime, "re")

        validate_auth(request)

        subscription_id, resource_group, app_allowlist = container_app_allowlist()
        if not (subscription_id and resource_group and app_allowlist):
            raise HTTPException(status_code=503, detail="Container app monitoring is not configured.")

        api_version_env = os_module.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
        api_version = api_version_env.strip() if api_version_env else ""
        if not api_version:
            api_version = arm_config_cls.api_version

        timeout_env = os_module.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
        try:
            timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
        except ValueError:
            timeout_seconds = 5.0

        probe_timeout_env = os_module.environ.get("SYSTEM_HEALTH_CONTAINERAPP_PING_TIMEOUT_SECONDS")
        try:
            probe_timeout_seconds = float(probe_timeout_env.strip()) if probe_timeout_env else 5.0
        except ValueError:
            probe_timeout_seconds = 5.0

        health_url_overrides = container_app_health_url_overrides()
        cfg = arm_config_cls(
            subscription_id=subscription_id,
            resource_group=resource_group,
            api_version=api_version,
            timeout_seconds=timeout_seconds,
        )

        items: List[Dict[str, Any]] = []
        checked_at = datetime_cls.now(timezone_obj.utc).isoformat()

        with azure_arm_client(cfg) as arm:
            for app_name in app_allowlist:
                resolved = (app_name or "").strip()
                if not resolved:
                    continue
                if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved):
                    items.append(
                        {
                            "name": resolved,
                            "status": "error",
                            "error": "Invalid container app name in allowlist.",
                            "health": None,
                            "checkedAt": checked_at,
                        }
                    )
                    continue

                app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=resolved)
                try:
                    payload = arm.get_json(app_url)
                except Exception as exc:
                    items.append(
                        {
                            "name": resolved,
                            "status": "error",
                            "error": f"Failed to read ARM state: {type(exc).__name__}: {exc}",
                            "health": None,
                            "checkedAt": checked_at,
                        }
                    )
                    continue

                props = extract_container_app_properties(payload if isinstance(payload, dict) else {})
                status = resource_status_from_provisioning_state(str(props.get("provisioningState") or ""))
                health_url = resolve_container_app_health_url(
                    resolved,
                    ingress_fqdn=props.get("ingressFqdn"),
                    overrides=health_url_overrides,
                )
                health: Optional[Dict[str, Any]]
                if probe and health_url:
                    health = probe_container_app_health(health_url, timeout_seconds=probe_timeout_seconds)
                    status = worse_status(status, str(health.get("status") or "unknown"))
                elif probe and not health_url:
                    health = {
                        "status": "unknown",
                        "url": None,
                        "httpStatus": None,
                        "checkedAt": checked_at,
                        "error": "No health URL is configured and no ingress FQDN was found.",
                    }
                else:
                    health = None

                details = f"provisioningState={props.get('provisioningState') or 'Unknown'}"
                if props.get("runningState"):
                    details += f", runningState={props.get('runningState')}"
                if props.get("latestReadyRevisionName"):
                    details += f", latestReadyRevision={props.get('latestReadyRevisionName')}"

                items.append(
                    {
                        "name": resolved,
                        "resourceType": "Microsoft.App/containerApps",
                        "status": status,
                        "details": details,
                        "provisioningState": props.get("provisioningState"),
                        "runningState": props.get("runningState"),
                        "latestReadyRevisionName": props.get("latestReadyRevisionName"),
                        "ingressFqdn": props.get("ingressFqdn"),
                        "azureId": props.get("azureId"),
                        "health": health,
                        "checkedAt": checked_at,
                        "error": None,
                    }
                )

        return JSONResponse(
            {
                "probed": bool(probe),
                "apps": items,
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.get("/container-apps/{app_name}/logs")
    def get_container_app_logs(
        app_name: str,
        request: Request,
        minutes: int = Query(60, ge=1, le=1440),
        tail: int = Query(50, ge=1, le=200),
    ) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        container_app_allowlist = _runtime_attr(runtime, "_container_app_allowlist")
        os_module = _runtime_attr(runtime, "os")
        re_module = _runtime_attr(runtime, "re")
        datetime_cls = _runtime_attr(runtime, "datetime")
        timezone_obj = _runtime_attr(runtime, "timezone")
        timedelta_cls = _runtime_attr(runtime, "timedelta")
        escape_kql_literal = _runtime_attr(runtime, "_escape_kql_literal")
        azure_log_analytics_client = _runtime_attr(runtime, "AzureLogAnalyticsClient")
        extract_log_lines = _runtime_attr(runtime, "_extract_log_lines")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)

        subscription_id, resource_group, app_allowlist = container_app_allowlist()
        if not (subscription_id and resource_group and app_allowlist):
            raise HTTPException(status_code=503, detail="Container app log retrieval is not configured.")

        resolved = (app_name or "").strip()
        if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
            raise HTTPException(status_code=400, detail="Invalid container app name.")
        if resolved not in app_allowlist:
            raise HTTPException(status_code=404, detail="Container app not found.")

        workspace_id_raw = os_module.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
        workspace_id = workspace_id_raw.strip() if workspace_id_raw else ""
        if not workspace_id:
            raise HTTPException(status_code=503, detail="Log Analytics is not configured for container app log retrieval.")

        log_timeout_raw = os_module.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS")
        try:
            log_timeout_seconds = float(log_timeout_raw.strip()) if log_timeout_raw else 5.0
        except ValueError:
            log_timeout_seconds = 5.0

        now = datetime_cls.now(timezone_obj.utc)
        start = now - timedelta_cls(minutes=max(1, int(minutes)))
        timespan = f"{start.isoformat()}/{now.isoformat()}"

        app_kql = escape_kql_literal(resolved)
        tail_lines = max(1, int(tail))
        query = f"""
let appName = '{app_kql}';
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppConsoleLogs
| extend app = tostring(
    column_ifexists('ContainerAppName_s',
        column_ifexists('ContainerName_s',
            column_ifexists('ContainerName',
                column_ifexists('AppName_s', '')
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
| where (app != '' and app contains appName)
    or (resource contains strcat('/containerApps/', appName))
| where msg != ''
| order by TimeGenerated desc
| take {tail_lines}
| project TimeGenerated, msg
| order by TimeGenerated asc
""".strip()

        try:
            with azure_log_analytics_client(timeout_seconds=log_timeout_seconds) as log_client:
                payload = log_client.query(workspace_id=workspace_id, query=query, timespan=timespan)
                lines = extract_log_lines(payload)
        except Exception as exc:
            logger.exception("Failed to query container app logs: app=%s", resolved)
            raise HTTPException(status_code=502, detail=f"Failed to query container app logs: {exc}") from exc

        return JSONResponse(
            {
                "appName": resolved,
                "lookbackMinutes": int(minutes),
                "tailLines": tail_lines,
                "logs": lines[-tail_lines:],
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.post("/container-apps/{app_name}/start")
    def start_container_app(app_name: str, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        container_app_allowlist = _runtime_attr(runtime, "_container_app_allowlist")
        os_module = _runtime_attr(runtime, "os")
        re_module = _runtime_attr(runtime, "re")
        arm_config_cls = _runtime_attr(runtime, "ArmConfig")
        azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
        httpx_module = _runtime_attr(runtime, "httpx")
        extract_arm_error_message = _runtime_attr(runtime, "_extract_arm_error_message")
        extract_container_app_properties = _runtime_attr(runtime, "_extract_container_app_properties")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        container_apps_topic = _runtime_attr(runtime, "REALTIME_TOPIC_CONTAINER_APPS")
        system_health_topic = _runtime_attr(runtime, "REALTIME_TOPIC_SYSTEM_HEALTH")

        validate_auth(request)

        subscription_id, resource_group, app_allowlist = container_app_allowlist()
        if not (subscription_id and resource_group and app_allowlist):
            raise HTTPException(status_code=503, detail="Container app control is not configured.")

        resolved = (app_name or "").strip()
        if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
            raise HTTPException(status_code=400, detail="Invalid container app name.")
        if resolved not in app_allowlist:
            raise HTTPException(status_code=404, detail="Container app not found.")

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
                app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=resolved)
                payload = arm.post_json(f"{app_url}/start")
        except httpx_module.HTTPStatusError as exc:
            message = extract_arm_error_message(exc.response)
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Failed to start container app: {message or str(exc)}",
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Failed to start container app: {exc}") from exc

        props = extract_container_app_properties(payload if isinstance(payload, dict) else {})
        response_payload = {
            "appName": resolved,
            "action": "start",
            "provisioningState": props.get("provisioningState"),
            "runningState": props.get("runningState"),
        }
        emit_realtime(
            container_apps_topic,
            "CONTAINER_APP_STATE_CHANGED",
            response_payload,
        )
        emit_realtime(
            system_health_topic,
            "SYSTEM_HEALTH_UPDATE",
            {"source": "container-app-control", "appName": resolved, "action": "start"},
        )
        return JSONResponse(response_payload, status_code=202)

    @router.post("/container-apps/{app_name}/stop")
    def stop_container_app(app_name: str, request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        container_app_allowlist = _runtime_attr(runtime, "_container_app_allowlist")
        os_module = _runtime_attr(runtime, "os")
        re_module = _runtime_attr(runtime, "re")
        arm_config_cls = _runtime_attr(runtime, "ArmConfig")
        azure_arm_client = _runtime_attr(runtime, "AzureArmClient")
        httpx_module = _runtime_attr(runtime, "httpx")
        extract_arm_error_message = _runtime_attr(runtime, "_extract_arm_error_message")
        extract_container_app_properties = _runtime_attr(runtime, "_extract_container_app_properties")
        emit_realtime = _runtime_attr(runtime, "_emit_realtime")
        container_apps_topic = _runtime_attr(runtime, "REALTIME_TOPIC_CONTAINER_APPS")
        system_health_topic = _runtime_attr(runtime, "REALTIME_TOPIC_SYSTEM_HEALTH")

        validate_auth(request)

        subscription_id, resource_group, app_allowlist = container_app_allowlist()
        if not (subscription_id and resource_group and app_allowlist):
            raise HTTPException(status_code=503, detail="Container app control is not configured.")

        resolved = (app_name or "").strip()
        if not re_module.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
            raise HTTPException(status_code=400, detail="Invalid container app name.")
        if resolved not in app_allowlist:
            raise HTTPException(status_code=404, detail="Container app not found.")

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
                app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=resolved)
                payload = arm.post_json(f"{app_url}/stop")
        except httpx_module.HTTPStatusError as exc:
            message = extract_arm_error_message(exc.response)
            raise HTTPException(
                status_code=exc.response.status_code,
                detail=f"Failed to stop container app: {message or str(exc)}",
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Failed to stop container app: {exc}") from exc

        props = extract_container_app_properties(payload if isinstance(payload, dict) else {})
        response_payload = {
            "appName": resolved,
            "action": "stop",
            "provisioningState": props.get("provisioningState"),
            "runningState": props.get("runningState"),
        }
        emit_realtime(
            container_apps_topic,
            "CONTAINER_APP_STATE_CHANGED",
            response_payload,
        )
        emit_realtime(
            system_health_topic,
            "SYSTEM_HEALTH_UPDATE",
            {"source": "container-app-control", "appName": resolved, "action": "stop"},
        )
        return JSONResponse(response_payload, status_code=202)

    return router, {
        "list_container_apps": list_container_apps,
        "get_container_app_logs": get_container_app_logs,
        "start_container_app": start_container_app,
        "stop_container_app": stop_container_app,
    }
