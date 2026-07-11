"""Optional Azure Blob / ADLS Gen2 upload for production ingestion."""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class AzureLandingClient:
    """
    Upload locally landed files to Azure Data Lake Storage Gen2.

    Requires ``azure-storage-blob`` and valid Azure credentials in environment
    variables. Used when deploying the ADF-linked production path.
    """

    def __init__(
        self,
        storage_account: str,
        container: str,
        credential: object | None = None,
    ) -> None:
        """
        Initialize Azure landing client.

        Args:
            storage_account: ADLS storage account name.
            container: Filesystem/container name.
            credential: Optional Azure credential; defaults to DefaultAzureCredential.
        """
        self.storage_account = storage_account
        self.container = container
        self._credential = credential
        self._service_client = None

    def _get_client(self):
        """Lazy initialization of the Azure Blob service client."""
        if self._service_client is not None:
            return self._service_client

        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise ImportError(
                "azure-storage-blob and azure-identity are required for Azure uploads. "
                "Install with: pip install azure-storage-blob azure-identity"
            ) from exc

        account_url = f"https://{self.storage_account}.blob.core.windows.net"
        credential = self._credential or DefaultAzureCredential(
            exclude_interactive_browser_credential=False
        )
        self._service_client = BlobServiceClient(account_url, credential=credential)
        return self._service_client

    def upload_directory(self, local_dir: Path, target_prefix: str) -> int:
        """
        Upload all files under a local directory to ADLS.

        Args:
            local_dir: Local directory root.
            target_prefix: Target path prefix inside the container.

        Returns:
            Number of files uploaded.
        """
        if not local_dir.exists():
            raise FileNotFoundError(f"Local directory not found: {local_dir}")

        client = self._get_client()
        container_client = client.get_container_client(self.container)
        uploaded = 0

        for file_path in local_dir.rglob("*"):
            if not file_path.is_file():
                continue
            relative = file_path.relative_to(local_dir).as_posix()
            blob_name = f"{target_prefix.rstrip('/')}/{relative}"
            logger.info("Uploading %s -> %s", file_path, blob_name)
            with file_path.open("rb") as handle:
                container_client.upload_blob(
                    name=blob_name, data=handle, overwrite=True
                )
            uploaded += 1

        return uploaded

    @classmethod
    def from_environment(cls) -> AzureLandingClient:
        """Create client from AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_CONTAINER env vars."""
        account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
        container = os.getenv("AZURE_STORAGE_CONTAINER", "raw")
        if not account:
            raise ValueError("AZURE_STORAGE_ACCOUNT is not set.")
        return cls(storage_account=account, container=container)
