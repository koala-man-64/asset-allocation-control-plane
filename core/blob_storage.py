import os
import io
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import logging
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from azure.core.pipeline.transport import RequestsTransport

logger = logging.getLogger(__name__)

# Suppress verbose Azure logs (HTTP headers, etc)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)

class BlobStorageClient:
    def __init__(self, account_name=None, connection_string=None, container_name='market-data', ensure_container_exists: bool = True):
        # 1. Try config/env for Account Name (Preferred)
        self.account_name = account_name or os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
        # 2. Try config/env for Connection String.
        self.connection_string = connection_string or os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        
        self.container_name = container_name
        
        # Configure transport with larger connection pool
        # Default is 10, which causes "Connection pool is full" with many threads
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        session.mount('https://', adapter)
        transport = RequestsTransport(session=session)

        if self.connection_string:
            # KEY/STRING PATH (Prioritized for robustness/CI)
            logger.info("Initializing BlobStorageClient with Connection String")
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string, transport=transport)
        elif self.account_name:
            # IDENTITY PATH (Fallback if CS missing)
            logger.info(f"Initializing BlobStorageClient with Managed Identity for account: {self.account_name}")
            account_url = f"https://{self.account_name}.blob.core.windows.net"
            credential = DefaultAzureCredential()
            self.blob_service_client = BlobServiceClient(account_url, credential=credential, transport=transport)
        else:
            raise ValueError("Authentication failed: Set AZURE_STORAGE_CONNECTION_STRING (Preferred) or AZURE_STORAGE_ACCOUNT_NAME (Identity).")
            
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        
        # Ensure container exists
        if ensure_container_exists:
            try:
                if not self.container_client.exists():
                    self.container_client.create_container()
                    logger.info(f"Created container: {self.container_name}")
            except Exception as e:
                logger.warning(f"Container creation/check might have failed (permissions/race): {e}")

    def file_exists(self, remote_path: str) -> bool:
        blob_client = self.container_client.get_blob_client(remote_path)
        return blob_client.exists()

    def list_files(self, name_starts_with=None) -> list:
        """
        Lists files in the container.
        """
        try:
            blobs = self.container_client.list_blobs(name_starts_with=name_starts_with)
            return [b.name for b in blobs]
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

    def list_blob_infos(self, name_starts_with=None) -> list:
        """
        Lists blobs with basic metadata (name, last_modified, size).
        """
        try:
            blobs = self.container_client.list_blobs(name_starts_with=name_starts_with)
            return [
                {
                    "name": b.name,
                    "last_modified": b.last_modified,
                    "size": b.size,
                    "etag": getattr(b, "etag", None),
                }
                for b in blobs
            ]
        except Exception as e:
            logger.error(f"Error listing blob infos: {e}")
            raise

    def delete_file(self, remote_path: str):
        """
        Deletes a file from the container.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            # Only delete if it exists? Or let SDK raise?
            # Test expects success if it deletes, usually idempotent or silent logic is preferred?
            # Standard delete_blob raises ResourceNotFoundError if not found.
            if blob_client.exists():
                blob_client.delete_blob()
                logger.info(f"Deleted blob: {remote_path}")
            else:
                logger.warning(f"Attempted to delete non-existent blob: {remote_path}")
        except Exception as e:
            logger.error(f"Error deleting file {remote_path}: {e}")
            raise

    def delete_prefix(self, prefix: Optional[str] = None) -> int:
        """
        Deletes all blobs under a prefix. If prefix is None/empty, deletes all blobs in the container.
        Returns the number of blobs deleted.
        """
        try:
            normalized = prefix or None
            deleted = 0
            blobs = self.container_client.list_blobs(name_starts_with=normalized)
            for blob in blobs:
                try:
                    self.container_client.delete_blob(blob.name)
                    deleted += 1
                except Exception as exc:
                    logger.warning(f"Failed to delete blob {blob.name}: {exc}")
            if normalized:
                logger.info(f"Deleted {deleted} blob(s) under prefix '{normalized}' in {self.container_name}.")
            else:
                logger.info(f"Deleted {deleted} blob(s) in container {self.container_name}.")
            return deleted
        except Exception as e:
            logger.error(f"Error deleting blobs for prefix '{prefix}': {e}")
            raise

    def has_blobs(self, prefix: Optional[str] = None) -> bool:
        """
        Returns True if any blobs exist under the prefix (or in the container if prefix is None/empty).
        """
        try:
            normalized = prefix or None
            blobs = self.container_client.list_blobs(name_starts_with=normalized)
            for _ in blobs:
                return True
            return False
        except Exception as e:
            logger.error(f"Error checking blobs for prefix '{prefix}': {e}")
            raise

    def read_csv(self, remote_path: str) -> pd.DataFrame:
        """
        Reads a CSV from Azure Blob Storage into a Pandas DataFrame.
        Returns None if the file does not exist or is empty.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                logger.debug(f"File not found in blob storage: {remote_path}")
                return None
            
            download_stream = blob_client.download_blob()
            data = download_stream.readall()
            
            if not data:
                return None

            return pd.read_csv(io.BytesIO(data))
        except Exception as e:
            logger.error(f"Error reading {remote_path}: {e}")
            return None

    def write_csv(self, remote_path: str, df: pd.DataFrame, index=False):
        """
        Writes a Pandas DataFrame to a CSV in Azure Blob Storage.
        """
        try:
            output = io.StringIO()
            df.to_csv(output, index=index)
            data = output.getvalue()
            
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Successfully wrote to blob: {remote_path}")
        except Exception as e:
            logger.error(f"Error writing to {remote_path}: {e}")
            raise

    def upload_file(self, local_path: str, remote_path: str):
        """
        Uploads a local file to Azure Blob Storage.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Uploaded {local_path} to {remote_path}")
        except Exception as e:
            logger.error(f"Error uploading {local_path}: {e}")
            raise


    def download_file(self, remote_path: str, local_path: str):
        """
        Downloads a file from Azure Blob Storage to local path.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            with open(local_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            logger.info(f"Downloaded {remote_path} to {local_path}")
        except Exception as e:
            logger.error(f"Error downloading {remote_path}: {e}")
            raise

    def download_data(self, remote_path: str) -> bytes:
        """
        Downloads a blob as bytes.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                return None
            return blob_client.download_blob().readall()
        except Exception as e:
            logger.error(f"Error downloading data {remote_path}: {e}")
            raise

    def upload_data(self, remote_path: str, data: bytes, overwrite: bool = True):
        """
        Uploads bytes to a blob.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.upload_blob(data, overwrite=overwrite)
            logger.info(f"Uploaded data to {remote_path}")
        except Exception as e:
            logger.error(f"Error uploading data to {remote_path}: {e}")
            raise

    def get_last_modified(self, remote_path: str) -> datetime:
        """
        Gets the last modified timestamp of a blob. Returns None if check fails/doesn't exist.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                return None
            return blob_client.get_blob_properties().last_modified
        except Exception as e:
            logger.error(f"Error getting properties for {remote_path}: {e}")
            return None

    def read_parquet(self, remote_path: str) -> pd.DataFrame:
        """
        Reads a Parquet file from Azure Blob Storage into a Pandas DataFrame.
        """
        try:
            blob_client = self.container_client.get_blob_client(remote_path)
            if not blob_client.exists():
                return None
            
            data = blob_client.download_blob().readall()
            return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            logger.error(f"Error reading parquet {remote_path}: {e}")
            return None

    def write_parquet(self, remote_path: str, df: pd.DataFrame):
        """
        Writes a Pandas DataFrame to a Parquet file in Azure Blob Storage.
        """
        try:
            output = io.BytesIO()
            df.to_parquet(output, index=False)
            data = output.getvalue()
            
            blob_client = self.container_client.get_blob_client(remote_path)
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Successfully wrote parquet to blob: {remote_path}")
        except Exception as e:
            logger.error(f"Error writing parquet to {remote_path}: {e}")
            raise
