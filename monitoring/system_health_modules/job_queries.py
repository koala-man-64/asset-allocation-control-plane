from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence

from monitoring.log_analytics import AzureLogAnalyticsClient, extract_first_table_rows
from monitoring.system_health_modules.signals import _newer_execution, _parse_iso_start_time, _utc_now

logger = logging.getLogger("asset_allocation.monitoring.system_health")

RETRY_SYMBOL_METADATA_JOB_NAMES = frozenset({"bronze-finance-job", "bronze-market-job"})
RETRY_SYMBOL_LOG_MESSAGE = "Retry-on-next-run candidates (not promoted):"


def _escape_kql_literal(value: str) -> str:
    return str(value or "").replace("'", "''")


def _query_job_system_log_messages(
    client: AzureLogAnalyticsClient,
    *,
    workspace_id: str,
    job_name: str,
    execution_name: Optional[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> List[str]:
    job_kql = _escape_kql_literal(job_name)
    exec_kql = _escape_kql_literal(str(execution_name or ""))
    end = end_time or _utc_now()
    start = start_time or (end - timedelta(minutes=30))
    if end < start:
        end = _utc_now()
    start = start - timedelta(minutes=5)
    end = end + timedelta(minutes=15)
    if end - start > timedelta(hours=24):
        start = end - timedelta(hours=24)
    timespan = f"{start.isoformat()}/{end.isoformat()}"

    query = f"""
let jobName = '{job_kql}';
let execName = '{exec_kql}';
union isfuzzy=true ContainerAppSystemLogs_CL, ContainerAppSystemLogs
| extend job = tostring(
    column_ifexists('ContainerJobName_s',
        column_ifexists('JobName_s',
            column_ifexists('JobName',
                column_ifexists('ContainerAppJobName_s', '')
            )
        )
    )
)
| extend exec = tostring(
    column_ifexists('ContainerAppJobExecutionName_s',
        column_ifexists('ExecutionName_s',
            column_ifexists('ExecutionName',
                column_ifexists('ContainerGroupName_s',
                    column_ifexists('ContainerGroupName', '')
                )
            )
        )
    )
)
| extend resource = tostring(column_ifexists('_ResourceId', column_ifexists('ResourceId', '')))
| extend reason = tostring(column_ifexists('Reason_s', column_ifexists('Reason', '')))
| extend msg_raw = tostring(
    column_ifexists('Log_s',
        column_ifexists('Log',
            column_ifexists('Message',
                column_ifexists('message', '')
            )
        )
    )
)
| extend msg = trim(@" ", strcat(reason, ' ', msg_raw))
| extend jobMatch = (job != '' and job contains jobName) or (resource contains jobName)
| extend execMatch = execName != '' and ((exec != '' and exec contains execName) or (resource contains execName))
| where jobMatch or execMatch
| order by execMatch desc, jobMatch desc, TimeGenerated desc
| take 200
| project msg
""".strip()
    payload = client.query(workspace_id=workspace_id, query=query, timespan=timespan)
    rows = extract_first_table_rows(payload)
    return [str(row.get("msg") or "").strip() for row in rows if str(row.get("msg") or "").strip()]


def _query_recent_bronze_symbol_counts(
    client: AzureLogAnalyticsClient,
    *,
    workspace_id: str,
    job_name: str,
    lookback_hours: int,
) -> List[Dict[str, Any]]:
    job_kql = _escape_kql_literal(job_name)
    end = _utc_now()
    start = end - timedelta(hours=max(lookback_hours, 1))
    timespan = f"{start.isoformat()}/{end.isoformat()}"
    query = f"""
let jobName = '{job_kql}';
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
| where ((job != '' and job contains jobName) or (resource contains jobName))
| where msg has 'alpha26 buckets written:'
| extend symbol_count = tolong(extract(@"symbols=([0-9]+)", 1, msg))
| where isnotnull(symbol_count)
| order by TimeGenerated desc
| take 5
| project TimeGenerated, symbol_count, msg
""".strip()
    payload = client.query(workspace_id=workspace_id, query=query, timespan=timespan)
    rows = extract_first_table_rows(payload)
    out: List[Dict[str, Any]] = []
    seen_timestamps: set[str] = set()
    for row in rows:
        timestamp = str(row.get("TimeGenerated") or "").strip()
        if not timestamp or timestamp in seen_timestamps:
            continue
        seen_timestamps.add(timestamp)
        try:
            symbol_count = int(row.get("symbol_count") or 0)
        except Exception:
            continue
        out.append(
            {
                "timeGenerated": timestamp,
                "symbolCount": symbol_count,
                "message": str(row.get("msg") or "").strip(),
            }
        )
    return out


def _query_recent_bronze_finance_ingest_summaries(
    client: AzureLogAnalyticsClient,
    *,
    workspace_id: str,
    job_name: str,
    lookback_hours: int,
) -> List[Dict[str, Any]]:
    job_kql = _escape_kql_literal(job_name)
    end = _utc_now()
    start = end - timedelta(hours=max(lookback_hours, 1))
    timespan = f"{start.isoformat()}/{end.isoformat()}"
    query = f"""
let jobName = '{job_kql}';
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
| where ((job != '' and job contains jobName) or (resource contains jobName))
| where msg has 'Bronze Massive finance ingest complete:'
| extend processed = tolong(extract(@"processed=([0-9]+)", 1, msg))
| extend written = tolong(extract(@"written=([0-9]+)", 1, msg))
| where isnotnull(processed) and isnotnull(written)
| order by TimeGenerated desc
| take 5
| project TimeGenerated, processed, written, msg
""".strip()
    payload = client.query(workspace_id=workspace_id, query=query, timespan=timespan)
    rows = extract_first_table_rows(payload)
    out: List[Dict[str, Any]] = []
    seen_timestamps: set[str] = set()
    for row in rows:
        timestamp = str(row.get("TimeGenerated") or "").strip()
        if not timestamp or timestamp in seen_timestamps:
            continue
        seen_timestamps.add(timestamp)
        try:
            processed = int(row.get("processed") or 0)
            written = int(row.get("written") or 0)
        except Exception:
            continue
        out.append(
            {
                "timeGenerated": timestamp,
                "processed": processed,
                "written": written,
                "message": str(row.get("msg") or "").strip(),
            }
        )
    return out


def _supports_retry_symbol_metadata(job_name: str) -> bool:
    return str(job_name or "").strip().lower() in RETRY_SYMBOL_METADATA_JOB_NAMES


def _parse_retry_symbol_metadata(message: str) -> Optional[Dict[str, Any]]:
    text = str(message or "").strip()
    if not text or RETRY_SYMBOL_LOG_MESSAGE not in text:
        return None

    count_match = re.search(r"\bcount=(\d+)\b", text)
    symbols_match = re.search(r"\bsymbols=(.*)$", text)
    if count_match is None or symbols_match is None:
        return None

    try:
        count = int(count_match.group(1))
    except Exception:
        return None

    raw_symbols = symbols_match.group(1).strip()
    truncated = False
    if raw_symbols.endswith(" ..."):
        raw_symbols = raw_symbols[:-4].rstrip()
        truncated = True

    symbols = [item.strip() for item in raw_symbols.split(",") if item.strip()]
    if count < len(symbols):
        count = len(symbols)
    if count <= 0 and not symbols:
        return None

    return {
        "retrySymbols": symbols,
        "retrySymbolCount": count,
        "retrySymbolsTruncated": truncated,
    }


def _query_job_retry_symbol_metadata(
    client: AzureLogAnalyticsClient,
    *,
    workspace_id: str,
    job_name: str,
    execution_name: Optional[str],
    start_time: Optional[datetime],
    end_time: Optional[datetime],
) -> Optional[Dict[str, Any]]:
    job_kql = _escape_kql_literal(job_name)
    exec_kql = _escape_kql_literal(str(execution_name or ""))
    message_kql = _escape_kql_literal(RETRY_SYMBOL_LOG_MESSAGE)
    end = end_time or _utc_now()
    start = start_time or (end - timedelta(minutes=30))
    if end < start:
        end = _utc_now()
    start = start - timedelta(minutes=5)
    end = end + timedelta(minutes=15)
    if end - start > timedelta(hours=24):
        start = end - timedelta(hours=24)
    timespan = f"{start.isoformat()}/{end.isoformat()}"

    query = f"""
let jobName = '{job_kql}';
let execName = '{exec_kql}';
let retryMessage = '{message_kql}';
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
| extend jobMatch = (job != '' and job contains jobName) or (resource contains jobName)
| extend execMatch = execName != '' and ((exec != '' and exec contains execName) or (resource contains execName))
| where jobMatch or execMatch
| where msg has retryMessage
| order by execMatch desc, jobMatch desc, TimeGenerated desc
| take 10
| project TimeGenerated, executionName=exec, msg
""".strip()
    payload = client.query(workspace_id=workspace_id, query=query, timespan=timespan)
    rows = extract_first_table_rows(payload)
    for row in rows:
        parsed = _parse_retry_symbol_metadata(str(row.get("msg") or ""))
        if parsed is None:
            continue
        timestamp = str(row.get("TimeGenerated") or "").strip()
        if timestamp:
            parsed["retrySymbolsUpdatedAt"] = timestamp
        return parsed
    return None


def _enrich_recent_job_retry_symbol_metadata(
    *,
    runs: Sequence[Dict[str, Any]],
    log_client: Optional[AzureLogAnalyticsClient],
    workspace_id: str,
) -> None:
    if log_client is None or not workspace_id:
        return

    latest_supported_runs: Dict[str, Dict[str, Any]] = {}
    for run in runs:
        job_name = str(run.get("jobName") or "").strip()
        if not _supports_retry_symbol_metadata(job_name):
            continue
        if str(run.get("status") or "").strip().lower() in {"running", "pending"}:
            continue
        existing = latest_supported_runs.get(job_name)
        if _newer_execution(run, existing):
            latest_supported_runs[job_name] = run

    for job_name, run in latest_supported_runs.items():
        try:
            metadata = _query_job_retry_symbol_metadata(
                log_client,
                workspace_id=workspace_id,
                job_name=job_name,
                execution_name=str(run.get("executionName") or "").strip() or None,
                start_time=_parse_iso_start_time(str(run.get("startTime") or "")),
                end_time=_parse_iso_start_time(str(run.get("endTime") or "")),
            )
        except Exception as exc:
            logger.info(
                "Skipping retry symbol metadata probe for job=%s: %s",
                job_name,
                exc,
                exc_info=True,
            )
            continue
        if not metadata:
            continue
        existing_metadata = run.get("metadata") if isinstance(run.get("metadata"), dict) else {}
        run["metadata"] = {**existing_metadata, **metadata}
