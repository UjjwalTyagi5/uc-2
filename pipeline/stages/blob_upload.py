"""
pipeline.stages.blob_upload
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage: upload purchase requisition attachments to Azure Blob Storage
and sync the BI dashboard view for the given PR.

Delegates all business logic to AttachmentBlobSync — this class is purely a
pipeline adapter (thin wrapper that conforms to the BaseStage contract).
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.stages.base import BaseStage


class BlobUploadStage(BaseStage):
    """
    Stage 1 — Blob upload + BI dashboard sync.

    Completion marker: ras_tracker.current_stage_fk = 'blob_uploadation_done'

    To skip this stage for a PR that was already processed, the orchestrator's
    repository query filters it out via the LEFT JOIN on ras_tracker.
    """

    NAME = "blob_upload"

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._config = config

    def execute(self, purchase_req_no: str) -> None:
        """
        Runs AttachmentBlobSync for the given PR.
        Raises on any unrecoverable error (propagated from AttachmentBlobSync).
        """
        AttachmentBlobSync(self._config).run(purchase_req_no)
