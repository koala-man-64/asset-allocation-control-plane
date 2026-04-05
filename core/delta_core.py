import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
import re

import pandas as pd
from azure.core.exceptions import AzureError, ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerSasPermissions, generate_container_sas
from deltalake import DeltaTable, write_deltalake

# Configure logger
logger = logging.getLogger(__name__)
_checked_containers = set()
_INDEX_ARTIFACT_EXACT_NAMES = {
    "index",
    "level_0",
    "index_level_0",
}
_NON_ALNUM_RE = re.compile(r"[^0-9a-z]+")


def _normalize_index_artifact_name(name: Any) -> str:
    normalized = _NON_ALNUM_RE.sub("_", str(name).strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized


def _is_index_artifact_column(name: Any) -> bool:
    normalized = _normalize_index_artifact_name(name)
    if not normalized:
        return False
    if normalized in _INDEX_ARTIFACT_EXACT_NAMES:
        return True
    if normalized.startswith("unnamed_"):
        suffix = normalized[len("unnamed_") :]
        if suffix.replace("_", "").isdigit():
            return True
    if normalized.startswith("index_level_"):
        suffix = normalized[len("index_level_") :]
        if suffix.replace("_", "").isdigit():
            return True
    return False


def _has_canonical_range_index(df: pd.DataFrame) -> bool:
    if not isinstance(df.index, pd.RangeIndex):
        return False
    return bool(df.index.start == 0 and df.index.step == 1 and df.index.name is None)


def _sanitize_df_for_delta_write(df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
    out = df
    index_was_reset = False

    if not _has_canonical_range_index(out):
        out = out.reset_index(drop=True)
        index_was_reset = True

    dropped_artifact_columns = [str(col) for col in out.columns if _is_index_artifact_column(col)]
    if dropped_artifact_columns:
        out = out.drop(columns=dropped_artifact_columns)

    return out, {
        "index_was_reset": index_was_reset,
        "dropped_artifact_columns": dropped_artifact_columns,
    }


def _log_all_null_column_profiles(df: pd.DataFrame, *, path: str) -> None:
    if df is None or df.empty:
        return

    all_null_columns = [
        f"{column}(dtype={df[column].dtype})"
        for column in df.columns
        if df[column].isna().all()
    ]
    if not all_null_columns:
        return

    logger.warning(
        "Pre-write Delta all-null columns for %s: rows=%d columns=%s",
        path,
        int(len(df)),
        all_null_columns,
    )


def _split_artifact_and_non_artifact_columns(columns: List[str]) -> tuple[List[str], List[str]]:
    artifact_columns: List[str] = []
    non_artifact_columns: List[str] = []
    for col in columns:
        if _is_index_artifact_column(col):
            artifact_columns.append(col)
        else:
            non_artifact_columns.append(col)
    return artifact_columns, non_artifact_columns

def _looks_float_type(schema_type: str) -> bool:
    """
    Heuristic for identifying numeric schema columns likely to fail on bad string values.
    """
    normalized = (schema_type or "").lower()
    numeric_markers = [
        "float",
        "double",
        "decimal",
        "int",
        "bigint",
        "smallint",
        "integer",
        "long",
    ]
    return any(marker in normalized for marker in numeric_markers)


def _log_delta_cast_candidates(df: pd.DataFrame, container: str, path: str, error_text: str) -> None:
    """
    Best-effort column-level diagnostics for delta write cast failures.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)
        dt = DeltaTable(uri, storage_options=opts)

        table_types = {}
        for field in dt.schema().fields:
            table_types[field.name] = str(getattr(field, "data_type", ""))

        suspect_columns = []
        for column in df.columns:
            col_name = str(column)
            field_type = table_types.get(col_name, "")
            if field_type and not _looks_float_type(field_type):
                continue

            series = df[column]
            if pd.api.types.is_numeric_dtype(series.dtype):
                continue

            non_null = series.dropna()
            if non_null.empty:
                continue

            parsed = pd.to_numeric(non_null, errors="coerce")
            invalid = non_null[pd.isna(parsed)]
            if invalid.empty:
                continue

            samples = [str(v) for v in invalid.astype(str).head(5).tolist()]
            has_none = any(v.lower() == "none" for v in invalid.astype(str).tolist())
            suspect_columns.append(
                f"{col_name}(invalid={len(invalid)}, type={field_type or 'unknown'}, "
                f"samples={samples}, has_none_literal={has_none})"
            )

        if suspect_columns:
            logger.error(
                "Potential cast failure columns for %s (error=%s): %s",
                path,
                error_text,
                suspect_columns,
            )
        else:
            string_invalid = [
                str(column)
                for column in df.columns
                if not pd.api.types.is_numeric_dtype(df[column].dtype)
                and pd.Series(df[column].astype(str), dtype=str).eq("None").any()
            ]

            if string_invalid:
                logger.error(
                    "Potential cast failure columns for %s (error=%s): string columns with 'None' literal=%s",
                    path,
                    error_text,
                    string_invalid,
                )
    except Exception as exc:
        logger.warning(f"Failed to compute cast candidate diagnostics for {path}: {exc}")


def _get_existing_delta_schema_columns(uri: str, storage_options: Dict[str, str]) -> Optional[List[str]]:
    try:
        dt = DeltaTable(uri, storage_options=storage_options)
        return [field.name for field in dt.schema().fields]
    except Exception as exc:
        if _is_missing_delta_table_error(exc):
            return None
        logger.warning(f"Failed to read Delta schema for {uri}: {exc}")
        return None


def _log_delta_schema_mismatch(df: pd.DataFrame, container: str, path: str) -> None:
    """
    Best-effort diagnostic logging for schema mismatches between an existing Delta table and a DataFrame.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)
        table_cols = _get_existing_delta_schema_columns(uri, opts)
        if not table_cols:
            logger.error(
                "Delta schema mismatch diagnostics unavailable for %s (no existing schema found).",
                path,
            )
            return

        df_cols = [str(c) for c in df.columns.tolist()]
        missing_in_df = [c for c in table_cols if c not in df_cols]
        extra_in_df = [c for c in df_cols if c not in table_cols]
        order_matches = df_cols == table_cols

        logger.error(
            "Delta schema mismatch for %s: df_cols=%d table_cols=%d missing_in_df=%s extra_in_df=%s order_matches=%s",
            path,
            len(df_cols),
            len(table_cols),
            missing_in_df,
            extra_in_df,
            order_matches,
        )

        # Helpful hint for the known rename from the previous column name.
        if "drawdown_1y" in df_cols and "drawdown" in table_cols and "drawdown" not in df_cols:
            logger.error(
                "Delta schema hint for %s: existing table has 'drawdown' but DataFrame has 'drawdown_1y'.",
                path,
            )
    except Exception as exc:
        logger.warning(f"Failed to compute schema mismatch diagnostics for {path}: {exc}")


def _compare_columns(df_columns: List[str], table_columns: List[str]) -> Dict[str, Any]:
    missing_in_df = [c for c in table_columns if c not in df_columns]
    extra_in_df = [c for c in df_columns if c not in table_columns]
    order_matches = df_columns == table_columns
    same_set = not missing_in_df and not extra_in_df
    return {
        "missing_in_df": missing_in_df,
        "extra_in_df": extra_in_df,
        "order_matches": order_matches,
        "same_set": same_set,
    }


def _log_store_delta_column_comparison(
    *,
    path: str,
    df_columns: List[str],
    table_columns: List[str],
) -> None:
    comparison = _compare_columns(df_columns, table_columns)
    if comparison["same_set"] and comparison["order_matches"]:
        return

    logger.warning(
        "Pre-write Delta column check for %s: df_cols=%d table_cols=%d missing_in_df=%s extra_in_df=%s order_matches=%s",
        path,
        len(df_columns),
        len(table_columns),
        comparison["missing_in_df"],
        comparison["extra_in_df"],
        comparison["order_matches"],
    )
    if "drawdown_1y" in df_columns and "drawdown" in table_columns and "drawdown" not in df_columns:
        logger.warning(
            "Pre-write Delta schema hint for %s: existing table has 'drawdown' but DataFrame has 'drawdown_1y'.",
            path,
        )


def _parse_connection_string(conn_str: str) -> Dict[str, str]:
    """Parses Azure Storage Connection String into a dictionary."""
    return dict(item.split('=', 1) for item in conn_str.split(';') if '=' in item)


def _infer_storage_auth_mode(storage_options: Dict[str, str]) -> str:
    if storage_options.get("account_key"):
        return "account_key"
    if storage_options.get("sas_token"):
        return "sas_token"
    if (
        storage_options.get("client_id")
        and storage_options.get("client_secret")
        and storage_options.get("tenant_id")
    ):
        return "service_principal"
    if storage_options.get("identity_endpoint"):
        return "managed_identity"
    if str(storage_options.get("use_azure_cli", "")).strip().lower() == "true":
        return "azure_cli"
    return "unknown"


def get_delta_storage_auth_diagnostics(container: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns a non-secret summary of how Delta storage auth is currently resolved.
    Intended for startup diagnostics and incident triage.
    """
    conn_str_raw = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    conn_str = conn_str_raw.strip() if conn_str_raw else ""
    cs_map = _parse_connection_string(conn_str) if conn_str else {}

    account_key_raw = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
    account_key = account_key_raw.strip() if account_key_raw else ""
    access_key_raw = os.environ.get("AZURE_STORAGE_ACCESS_KEY")
    access_key = access_key_raw.strip() if access_key_raw else ""
    sas_token_raw = os.environ.get("AZURE_STORAGE_SAS_TOKEN")
    sas_token = sas_token_raw.strip() if sas_token_raw else ""
    client_secret_raw = os.environ.get("AZURE_CLIENT_SECRET")
    client_secret = client_secret_raw.strip() if client_secret_raw else ""
    identity_endpoint_raw = os.environ.get("IDENTITY_ENDPOINT") or os.environ.get("MSI_ENDPOINT")
    identity_endpoint = identity_endpoint_raw.strip() if identity_endpoint_raw else ""

    storage_options = get_delta_storage_options(container=container)
    mode = _infer_storage_auth_mode(storage_options)

    key_source = None
    if storage_options.get("account_key"):
        if account_key:
            key_source = "AZURE_STORAGE_ACCOUNT_KEY"
        elif access_key:
            key_source = "AZURE_STORAGE_ACCESS_KEY"
        elif cs_map.get("AccountKey"):
            key_source = "AZURE_STORAGE_CONNECTION_STRING"
        else:
            key_source = "unknown"

    return {
        "mode": mode,
        "container": container,
        "accountName": storage_options.get("account_name") or cs_map.get("AccountName"),
        "optionKeys": sorted(storage_options.keys()),
        "hasConnectionString": bool(conn_str),
        "hasAccountKeyEnv": bool(account_key),
        "hasAccessKeyEnv": bool(access_key),
        "hasSasTokenEnv": bool(sas_token),
        "hasClientSecretEnv": bool(client_secret),
        "hasIdentityEndpoint": bool(identity_endpoint),
        "accountKeySource": key_source,
    }

def _get_user_delegation_sas(
    container: Optional[str],
    account_name: Optional[str],
    ttl_minutes: int = 60,
) -> Optional[str]:
    if not container or not account_name:
        return None

    try:
        credential = DefaultAzureCredential()
        account_url = f"https://{account_name}.blob.core.windows.net"
        service_client = BlobServiceClient(account_url=account_url, credential=credential)
        start = datetime.now(timezone.utc) - timedelta(minutes=5)
        expiry = start + timedelta(minutes=ttl_minutes)
        delegation_key = service_client.get_user_delegation_key(start, expiry)
        permissions = ContainerSasPermissions(
            read=True,
            write=True,
            delete=True,
            list=True,
            add=True,
            create=True,
        )
        return generate_container_sas(
            account_name=account_name,
            container_name=container,
            user_delegation_key=delegation_key,
            permission=permissions,
            expiry=expiry,
            start=start,
        )
    except Exception as exc:
        logger.warning(f"Failed to generate user delegation SAS for {container}: {exc}")
        return None

def _ensure_container_exists(container: Optional[str]) -> None:
    if not container or container in _checked_containers:
        return

    cs_map = {}
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if conn_str:
        cs_map = _parse_connection_string(conn_str)

    account_name = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME') or cs_map.get('AccountName')
    account_key = (
        os.environ.get('AZURE_STORAGE_ACCOUNT_KEY')
        or os.environ.get('AZURE_STORAGE_ACCESS_KEY')
        or cs_map.get('AccountKey')
    )
    sas_token = os.environ.get('AZURE_STORAGE_SAS_TOKEN')

    try:
        if conn_str:
            service_client = BlobServiceClient.from_connection_string(conn_str)
        elif account_name:
            account_url = f"https://{account_name}.blob.core.windows.net"
            credential = account_key or sas_token or DefaultAzureCredential()
            service_client = BlobServiceClient(account_url=account_url, credential=credential)
        else:
            logger.warning(f"Container creation skipped; missing account name for {container}.")
            return

        container_client = service_client.get_container_client(container)
        if not container_client.exists():
            container_client.create_container()
            logger.info(f"Created container: {container}")
    except ResourceExistsError:
        pass
    except AzureError as exc:
        logger.warning(f"Failed to ensure container exists for {container}: {exc}")
    finally:
        _checked_containers.add(container)

def get_delta_storage_options(container: Optional[str] = None) -> Dict[str, str]:
    """
    Constructs the storage_options dictionary required by deltalake (delta-rs)
    for Azure Blob Storage authentication.
    
    Prioritizes Account Key if available, otherwise attempts to configure for
    Managed Identity (via different provider configs if supported) or SAS.
    
    Note: delta-rs support for Azure Managed Identity can be complex.
    For now, we support:
    1. Account Key (AZURE_STORAGE_ACCOUNT_KEY or parsed from Connection String)
    2. Connection String (not directly supported by simple options, usually parsed)
    3. SAS Token (AZURE_STORAGE_SAS_TOKEN)
    4. Azure CLI/Identity fallback (azure_use_azure_cli='true')
    """
    options = {}
    
    # 0. Helper: Parse Connection String if present
    cs_map = {}
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if conn_str:
        cs_map = _parse_connection_string(conn_str)
    
    # Account Name is mandatory
    account_name = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
    if not account_name:
        account_name = cs_map.get('AccountName')
        
    if account_name:
        options['account_name'] = account_name

    # 1. Account Key
    account_key = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY') or os.environ.get('AZURE_STORAGE_ACCESS_KEY')
    if not account_key:
        account_key = cs_map.get('AccountKey')

    if account_key:
        options['account_key'] = account_key
        return options

    # 2. SAS Token
    sas_token = os.environ.get('AZURE_STORAGE_SAS_TOKEN')
    if sas_token:
        options['sas_token'] = sas_token
        return options

    # 3. Client Secret (Service Principal)
    client_id = os.environ.get('AZURE_CLIENT_ID')
    client_secret = os.environ.get('AZURE_CLIENT_SECRET')
    tenant_id = os.environ.get('AZURE_TENANT_ID')
    
    if client_id and client_secret and tenant_id:
        options['client_id'] = client_id
        options['client_secret'] = client_secret
        options['tenant_id'] = tenant_id
        return options

    # 4. Managed Identity / Azure CLI (Fallback)
    # If we are in an Azure environment (Container Apps, App Service, VM), IDENTITY_ENDPOINT is usually set.
    # In that case, we should NOT force use_azure_cli, as the underlying library (object_store/azure-identity)
    # should automatically detect Managed Identity.
    # We only default to Azure CLI if we are NOT in a known MSI environment.
    if os.environ.get('IDENTITY_ENDPOINT') or os.environ.get('MSI_ENDPOINT'):
        identity_endpoint = os.environ.get('IDENTITY_ENDPOINT') or os.environ.get('MSI_ENDPOINT')
        if identity_endpoint:
            options['identity_endpoint'] = identity_endpoint
        sas_token = _get_user_delegation_sas(container, account_name)
        if sas_token:
            options['sas_token'] = sas_token
        else:
            logger.info("Detected Managed Identity environment; user delegation SAS unavailable.")
        # Do not set 'use_azure_cli' to true, relying on default chain/MSI if needed.
    else:
        # Local development fallback: try Azure CLI
        options['use_azure_cli'] = 'true'
    
    return options

def get_delta_table_uri(container: str, path: str, account_name: Optional[str] = None) -> str:
    """
    Returns the full abfss:// URI for a Delta table.
    """
    acc = account_name or os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
    if not acc:
         # Try logic from parsing connection string if environment variable is missing
         # Re-use parsing logic (inefficient to do twice but safe)
         conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
         if conn_str:
             cs_map = dict(item.split('=', 1) for item in conn_str.split(';') if '=' in item)
             acc = cs_map.get('AccountName')

    if not acc:
         raise ValueError("AZURE_STORAGE_ACCOUNT_NAME must be set (or parseable from AZURE_STORAGE_CONNECTION_STRING) to construct Delta URI.")
         
    # Clean path
    path = path.strip('/')
    
    # Format: abfss://<container>@<account>.dfs.core.windows.net/<path>
    # Note: simple w/o dfs sometimes works for blob, but abfss is standard for Data Lake / Delta
    # using object_store.
    return f"abfss://{container}@{acc}.dfs.core.windows.net/{path}"

def get_delta_schema_columns(container: str, path: str) -> Optional[List[str]]:
    """
    Returns the column names for an existing Delta table, or None if unavailable.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)
        return _get_existing_delta_schema_columns(uri, opts)
    except Exception as exc:
        logger.warning(f"Failed to resolve Delta schema for {path}: {exc}")
        return None


def _is_missing_delta_table_error(exc: Exception) -> bool:
    text = str(exc).strip().lower()
    markers = (
        "no files in log segment",
        "not a delta table",
        "table not found",
        "path not found",
        "resource not found",
        "blob not found",
        "404",
    )
    return any(marker in text for marker in markers)

def store_delta(
    df: pd.DataFrame, 
    container: str, 
    path: str, 
    mode: str = 'overwrite', 
    partition_by: list = None,
    predicate: Optional[str] = None,
    schema_mode: Optional[str] = None,
) -> None:
    """
    Writes a pandas DataFrame to a Delta table in Azure.
    """
    df_to_write = df
    try:
        _ensure_container_exists(container)
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)

        df_to_write, sanitize_meta = _sanitize_df_for_delta_write(df)
        index_was_reset = bool(sanitize_meta.get("index_was_reset", False))
        dropped_artifact_columns = [str(col) for col in (sanitize_meta.get("dropped_artifact_columns") or [])]

        table_cols = _get_existing_delta_schema_columns(uri, opts)
        if table_cols:
            sanitized_df_cols = [str(c) for c in df_to_write.columns.tolist()]
            _log_store_delta_column_comparison(
                path=path,
                df_columns=sanitized_df_cols,
                table_columns=table_cols,
            )

        logger.info(
            "Delta write prep for %s: rows=%d index_was_reset=%s dropped_artifact_columns=%s",
            path,
            int(len(df_to_write)),
            index_was_reset,
            dropped_artifact_columns,
        )
        _log_all_null_column_profiles(df_to_write, path=path)

        write_deltalake(
            uri,
            df_to_write,
            mode=mode,
            partition_by=partition_by,
            predicate=predicate,
            schema_mode=schema_mode,
            storage_options=opts
        )
        logger.info(f"Successfully wrote Delta table to {path}")
    except Exception as e:
        logger.error(f"Failed to write Delta table {path}: {e}")
        error_text = str(e)
        if "Cannot cast" in error_text:
            _log_delta_cast_candidates(df_to_write, container, path, error_text)
        if "Cannot cast schema" in error_text or "number of fields does not match" in error_text:
            _log_delta_schema_mismatch(df_to_write, container, path)
        raise

def load_delta(
    container: str,
    path: str,
    version: int = None,
    columns: Optional[List[str]] = None,
    filters: Any = None,
    log_buffer_size: Optional[int] = None,
) -> Optional[pd.DataFrame]:
    """
    Reads a Delta table from Azure into a pandas DataFrame.
    Returns None if table does not exist or access fails.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)
        
        dt = DeltaTable(uri, version=version, storage_options=opts, log_buffer_size=log_buffer_size)
        return dt.to_pandas(columns=columns, filters=filters)
    except Exception as e:
        if _is_missing_delta_table_error(e):
            logger.info(f"Delta table not found for {path}; returning empty.")
        else:
            logger.warning(f"Failed to load Delta table {path}: {e}")
        return None

def get_delta_last_commit(container: str, path: str) -> Optional[float]:
    """
    Returns the timestamp of the last commit to the Delta table.
    Useful for freshness checks.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)
        
        dt = DeltaTable(uri, storage_options=opts)
        # history() returns a list of dictionaries. 0-th index is usually latest? 
        # Actually dt.history() in python returns a list of dicts, most recent first usually?
        # Let's check metadata directly or history.
        # dt.metadata().created_time is creation.
        # let's use history(1)
        hist = dt.history(1)
        if hist:
            # timestamp is usually in milliseconds or microseconds?
            # dictionary keys: timestamp, operation, etc.
            # timestamp is int (ms since epoch)
            ts = hist[0].get('timestamp')
            if ts:
                return ts / 1000.0 # Convert to seconds for standard unix time
        return None
    except Exception as e:
        if _is_missing_delta_table_error(e):
            logger.info(f"Delta history not found for {path}; treating as missing source table.")
        else:
            logger.warning(f"Failed to get Delta history for {path}: {e}")
        return None


def vacuum_delta_table(
    container: str,
    path: str,
    *,
    retention_hours: int = 0,
    dry_run: bool = False,
    enforce_retention_duration: bool = False,
    full: bool = False,
) -> int:
    """
    Best-effort Delta VACUUM to physically remove unreferenced files from storage.

    Returns number of deleted file paths reported by delta-rs.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options(container)
        dt = DeltaTable(uri, storage_options=opts)
        removed = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
            full=full,
        )
        removed_count = len(removed or [])
        logger.info(
            "Vacuumed Delta table %s (container=%s): removed_files=%d dry_run=%s retention_hours=%s full=%s",
            path,
            container,
            removed_count,
            dry_run,
            retention_hours,
            full,
        )
        return removed_count
    except Exception as exc:
        logger.warning(f"Failed to vacuum Delta table {path}: {exc}")
        return 0
