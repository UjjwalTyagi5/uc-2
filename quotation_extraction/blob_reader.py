"""Download quotation files from Azure Blob Storage to a local temp directory."""

from __future__ import annotations

import pathlib
import tempfile

from azure.storage.blob import BlobServiceClient
from loguru import logger

from .config import ExtractionConfig


class BlobReader:
    """Downloads blobs to a temporary working directory."""

    def __init__(self, config: ExtractionConfig) -> None:
        self._client = BlobServiceClient(
            account_url=config.BLOB_ACCOUNT_URL,
            credential=config.BLOB_ACCOUNT_KEY,
        )
        self._container = config.BLOB_CONTAINER_NAME
        self._work_root = pathlib.Path(
            tempfile.mkdtemp(prefix="qe_work_")
        )

    @property
    def work_dir(self) -> pathlib.Path:
        return self._work_root

    def download(self, blob_path: str) -> pathlib.Path:
        """Download *blob_path* (container-relative) and return the local path.

        If the file already exists locally (e.g. from a previous pipeline
        stage), the download is skipped.
        """
        local = self._work_root / blob_path
        if local.exists():
            logger.debug("Already local: {}", local)
            return local

        local.parent.mkdir(parents=True, exist_ok=True)
        blob_client = self._client.get_blob_client(
            container=self._container, blob=blob_path
        )
        with open(local, "wb") as f:
            stream = blob_client.download_blob()
            stream.readinto(f)

        logger.debug("Downloaded blob → {}", local)
        return local

    def download_many(self, blob_paths: list[str]) -> list[pathlib.Path]:
        """Download a batch of blobs. Returns local paths in the same order."""
        return [self.download(p) for p in blob_paths]
