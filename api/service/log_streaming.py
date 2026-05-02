from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional, Sequence

from api.service.realtime import RealtimeManager
from core.redaction import redact_sensitive_text
from monitoring.log_analytics import AzureLogAnalyticsClient, extract_first_table_rows

logger = logging.getLogger("asset-allocation.api.log_streaming")

JOB_LOG_TOPIC_PREFIX = "job-logs:"
CONTAINER_APP_LOG_TOPIC_PREFIX = "container-app-logs:"
CONSOLE_LOG_STREAM_EVENT = "CONSOLE_LOG_STREAM"
_RESOURCE_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?")


def build_job_log_topic(job_name: str, execution_name: Optional[str] = None) -> str:
    job = str(job_name or "").strip()
    execution = str(execution_name or "").strip()
    if not execution:
        return f"{JOB_LOG_TOPIC_PREFIX}{job}"
    return f"{JOB_LOG_TOPIC_PREFIX}{job}/executions/{execution}"


def build_container_app_log_topic(app_name: str) -> str:
    return f"{CONTAINER_APP_LOG_TOPIC_PREFIX}{str(app_name or '').strip()}"


@dataclass(frozen=True)
class LogStreamSpec:
    topic: str
    resource_type: Literal["job", "container-app"]
    resource_name: str
    execution_name: Optional[str] = None


@dataclass(frozen=True)
class LogStreamingConfig:
    workspace_id: str
    timeout_seconds: float
    poll_seconds: float
    lookback_seconds: int
    batch_size: int


@dataclass
class _TopicHistory:
    max_ids: int = 500
    ordered_ids: deque[str] = field(default_factory=deque)
    seen_ids: set[str] = field(default_factory=set)

    def remember(self, entry_id: str) -> bool:
        if not entry_id or entry_id in self.seen_ids:
            return False
        self.ordered_ids.append(entry_id)
        self.seen_ids.add(entry_id)
        while len(self.ordered_ids) > self.max_ids:
            removed = self.ordered_ids.popleft()
            self.seen_ids.discard(removed)
        return True


def parse_log_stream_topic(topic: str) -> Optional[LogStreamSpec]:
    raw = str(topic or "").strip()
    if raw.startswith(JOB_LOG_TOPIC_PREFIX):
        remainder = raw[len(JOB_LOG_TOPIC_PREFIX) :].strip()
        execution_name: Optional[str] = None
        if "/executions/" in remainder:
            name, execution_name = remainder.split("/executions/", 1)
            name = name.strip()
            execution_name = execution_name.strip() or None
        else:
            name = remainder
        kind: Literal["job", "container-app"] = "job"
    elif raw.startswith(CONTAINER_APP_LOG_TOPIC_PREFIX):
        name = raw[len(CONTAINER_APP_LOG_TOPIC_PREFIX) :].strip()
        execution_name = None
        kind = "container-app"
    else:
        return None

    if not name or not _RESOURCE_NAME_PATTERN.fullmatch(name):
        return None
    if execution_name is not None and not _RESOURCE_NAME_PATTERN.fullmatch(execution_name):
        return None
    return LogStreamSpec(
        topic=raw,
        resource_type=kind,
        resource_name=name,
        execution_name=execution_name,
    )


def _split_csv(raw: Optional[str]) -> list[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _escape_kql_literal(value: str) -> str:
    return str(value or "").replace("'", "''")


def _load_streaming_config() -> Optional[LogStreamingConfig]:
    workspace_id_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
    workspace_id = workspace_id_raw.strip() if workspace_id_raw else ""
    if not workspace_id:
        return None

    timeout_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS")
    poll_raw = os.environ.get("REALTIME_LOG_STREAM_POLL_SECONDS")
    lookback_raw = os.environ.get("REALTIME_LOG_STREAM_LOOKBACK_SECONDS")
    batch_raw = os.environ.get("REALTIME_LOG_STREAM_BATCH_SIZE")

    try:
        timeout_seconds = float(timeout_raw.strip()) if timeout_raw else 5.0
    except ValueError:
        timeout_seconds = 5.0
    try:
        poll_seconds = float(poll_raw.strip()) if poll_raw else 5.0
    except ValueError:
        poll_seconds = 5.0
    try:
        lookback_seconds = int(lookback_raw.strip()) if lookback_raw else max(30, int(poll_seconds * 3))
    except ValueError:
        lookback_seconds = max(30, int(poll_seconds * 3))
    try:
        batch_size = int(batch_raw.strip()) if batch_raw else 200
    except ValueError:
        batch_size = 200

    return LogStreamingConfig(
        workspace_id=workspace_id,
        timeout_seconds=max(1.0, timeout_seconds),
        poll_seconds=max(1.0, poll_seconds),
        lookback_seconds=max(10, lookback_seconds),
        batch_size=max(10, min(batch_size, 500)),
    )


def _resource_is_allowlisted(spec: LogStreamSpec) -> bool:
    if spec.resource_type == "job":
        job_names = _split_csv(os.environ.get("SYSTEM_HEALTH_ARM_JOBS"))
        return not job_names or "*" in job_names or spec.resource_name in job_names
    return spec.resource_name in _split_csv(os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS"))


def _build_job_query(job_name: str, *, execution_name: Optional[str] = None, limit: int) -> str:
    job_kql = _escape_kql_literal(job_name)
    execution_kql = _escape_kql_literal(execution_name or "")
    return f"""
let jobName = '{job_kql}';
let execFilter = '{execution_kql}';
let nonempty = (value:dynamic) {{ iff(isnotempty(tostring(value)), tostring(value), dynamic(null)) }};
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppConsoleLogs
| extend job = tostring(coalesce(
    nonempty(column_ifexists('ContainerJobName_s', '')),
    nonempty(column_ifexists('ContainerJobName', '')),
    nonempty(column_ifexists('ContainerAppJobName_s', '')),
    nonempty(column_ifexists('ContainerAppJobName', '')),
    nonempty(column_ifexists('JobName_s', '')),
    nonempty(column_ifexists('JobName', '')),
    nonempty(column_ifexists('ContainerAppName_s', '')),
    nonempty(column_ifexists('ContainerAppName', '')),
    nonempty(column_ifexists('ContainerName_s', '')),
    nonempty(column_ifexists('ContainerName', '')),
    ''
))
| extend executionName = tostring(coalesce(
    nonempty(column_ifexists('ContainerGroupName_s', '')),
    nonempty(column_ifexists('ContainerGroupName_g', '')),
    nonempty(column_ifexists('ContainerGroupName', '')),
    nonempty(column_ifexists('ContainerAppJobExecutionName_s', '')),
    nonempty(column_ifexists('ContainerAppJobExecutionName', '')),
    nonempty(column_ifexists('ExecutionName_s', '')),
    nonempty(column_ifexists('ExecutionName', '')),
    nonempty(column_ifexists('ContainerGroupId_s', '')),
    nonempty(column_ifexists('ContainerGroupId_g', '')),
    nonempty(column_ifexists('ContainerGroupId', '')),
    nonempty(column_ifexists('ContainerAppJobExecutionId_s', '')),
    nonempty(column_ifexists('ContainerAppJobExecutionId_g', '')),
    nonempty(column_ifexists('ContainerAppJobExecutionId', '')),
    ''
))
| extend resource = tostring(column_ifexists('_ResourceId', column_ifexists('ResourceId', '')))
| extend msg = tostring(coalesce(
    nonempty(column_ifexists('Log_s', '')),
    nonempty(column_ifexists('Log', '')),
    nonempty(column_ifexists('LogMessage_s', '')),
    nonempty(column_ifexists('LogMessage', '')),
    nonempty(column_ifexists('Message_s', '')),
    nonempty(column_ifexists('Message', '')),
    nonempty(column_ifexists('message', '')),
    ''
))
| extend stream_s = tostring(coalesce(
    nonempty(column_ifexists('Stream_s', '')),
    nonempty(column_ifexists('stream_s', '')),
    nonempty(column_ifexists('Stream', '')),
    nonempty(column_ifexists('stream', '')),
    ''
))
| extend matchesJob = (
    (job != '' and job contains jobName)
    or (resource contains strcat('/jobs/', jobName))
    or (resource contains jobName)
)
| extend matchesExecution = (
    execFilter == ''
    or (execFilter != '' and executionName == execFilter)
    or (execFilter != '' and executionName startswith strcat(execFilter, '-'))
    or (execFilter != '' and resource contains strcat('/executions/', execFilter))
    or (execFilter != '' and resource contains execFilter)
)
| where matchesJob and matchesExecution
| where msg != ''
| order by TimeGenerated desc
| take {max(1, int(limit))}
| project TimeGenerated, executionName, stream_s, msg
| order by TimeGenerated asc
""".strip()


def _build_container_app_query(app_name: str, *, limit: int) -> str:
    app_kql = _escape_kql_literal(app_name)
    return f"""
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
| where ((app != '' and app contains appName) or (resource contains strcat('/containerApps/', appName)))
| where msg != ''
| order by TimeGenerated desc
| take {max(1, int(limit))}
| project TimeGenerated, msg
| order by TimeGenerated asc
""".strip()


def _coalesce_string(row: dict[str, Any], *keys: str) -> str:
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_stream_lines(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = extract_first_table_rows(payload)
    entries: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        message = redact_sensitive_text(
            _coalesce_string(
                row,
                "msg",
                "Log_s",
                "Log",
                "LogMessage_s",
                "LogMessage",
                "Message_s",
                "Message",
                "message",
                "log",
            )
        )
        if not message:
            continue
        timestamp = _coalesce_string(row, "TimeGenerated", "timegenerated") or datetime.now(timezone.utc).isoformat()
        execution_name = _coalesce_string(
            row,
            "executionName",
            "ExecutionName",
            "exec",
            "Exec",
            "ContainerGroupName_s",
            "ContainerGroupName_g",
            "ContainerGroupName",
            "ContainerGroupId_s",
            "ContainerGroupId_g",
            "ContainerGroupId",
            "ContainerAppJobExecutionName_s",
            "ContainerAppJobExecutionName",
            "ContainerAppJobExecutionId_s",
            "ContainerAppJobExecutionId_g",
            "ContainerAppJobExecutionId",
        )
        stream_name = _coalesce_string(row, "stream_s", "Stream_s", "stream", "Stream")
        digest_source = "|".join([timestamp, execution_name, stream_name, message])
        entry_id = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()
        entries.append(
            {
                "id": entry_id,
                "timestamp": timestamp,
                "stream_s": stream_name or None,
                "message": message,
                "executionName": execution_name or None,
            }
        )
    return entries


class LogStreamManager:
    def __init__(self, realtime: RealtimeManager) -> None:
        self._realtime = realtime
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()
        self._shutdown = asyncio.Event()

    async def ensure_streams(self, topics: Sequence[str]) -> None:
        config = _load_streaming_config()
        if config is None:
            return

        async with self._lock:
            for topic in topics:
                spec = parse_log_stream_topic(topic)
                if spec is None or not _resource_is_allowlisted(spec):
                    continue
                task = self._tasks.get(spec.topic)
                if task is not None and not task.done():
                    continue
                self._tasks[spec.topic] = asyncio.create_task(
                    self._run_stream(spec, config),
                    name=f"log-stream:{spec.topic}",
                )

    async def prune_unused_streams(self, topics: Optional[Sequence[str]] = None) -> None:
        targets = {str(topic).strip() for topic in topics or self._tasks.keys()}
        to_stop: list[tuple[str, asyncio.Task[None]]] = []
        async with self._lock:
            for topic in list(targets):
                task = self._tasks.get(topic)
                if task is None:
                    continue
                if task.done() or not self._realtime.has_subscribers(topic):
                    self._tasks.pop(topic, None)
                    to_stop.append((topic, task))

        for topic, task in to_stop:
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.debug("Log stream cancelled: topic=%s", topic)
            except Exception:
                logger.exception("Log stream failed while stopping: topic=%s", topic)

    async def shutdown(self) -> None:
        self._shutdown.set()
        async with self._lock:
            tasks = list(self._tasks.items())
            self._tasks.clear()

        for topic, task in tasks:
            if not task.done():
                task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.debug("Log stream shutdown cancelled topic=%s", topic)
            except Exception:
                logger.exception("Log stream shutdown failed: topic=%s", topic)

    async def _run_stream(self, spec: LogStreamSpec, config: LogStreamingConfig) -> None:
        logger.info("Starting console log stream: topic=%s", spec.topic)
        history = _TopicHistory(max_ids=max(500, config.batch_size * 4))
        try:
            with AzureLogAnalyticsClient(timeout_seconds=config.timeout_seconds) as client:
                while not self._shutdown.is_set():
                    if not self._realtime.has_subscribers(spec.topic):
                        return

                    now = datetime.now(timezone.utc)
                    start = now - timedelta(seconds=config.lookback_seconds)
                    timespan = f"{start.isoformat()}/{now.isoformat()}"
                    query = (
                        _build_job_query(
                            spec.resource_name,
                            execution_name=spec.execution_name,
                            limit=config.batch_size,
                        )
                        if spec.resource_type == "job"
                        else _build_container_app_query(spec.resource_name, limit=config.batch_size)
                    )

                    try:
                        payload = await asyncio.to_thread(
                            client.query,
                            workspace_id=config.workspace_id,
                            query=query,
                            timespan=timespan,
                        )
                        entries = _extract_stream_lines(payload)
                        row_count = len(extract_first_table_rows(payload))
                        if row_count == 0:
                            logger.info(
                                "Console log stream query returned no rows: "
                                "topic=%s resource_type=%s resource_name=%s execution=%s "
                                "timespan=%s row_count=%d",
                                spec.topic,
                                spec.resource_type,
                                spec.resource_name,
                                spec.execution_name or "-",
                                timespan,
                                row_count,
                            )
                        new_entries = [
                            entry for entry in entries if history.remember(str(entry.get("id") or ""))
                        ]
                        if new_entries:
                            await self._realtime.broadcast(
                                spec.topic,
                                {
                                    "type": CONSOLE_LOG_STREAM_EVENT,
                                    "payload": {
                                        "resourceType": spec.resource_type,
                                        "resourceName": spec.resource_name,
                                        "lines": new_entries,
                                        "polledAt": now.isoformat(),
                                    },
                                },
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning("Console log stream poll failed: topic=%s err=%s", spec.topic, exc)

                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=config.poll_seconds)
                    except asyncio.TimeoutError:
                        continue
        except asyncio.CancelledError:
            raise
        finally:
            async with self._lock:
                current = self._tasks.get(spec.topic)
                if current is asyncio.current_task():
                    self._tasks.pop(spec.topic, None)
            logger.info("Stopped console log stream: topic=%s", spec.topic)
