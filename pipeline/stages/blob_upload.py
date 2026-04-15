"""
pipeline.stages.blob_upload
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 3 — BLOB_UPLOAD.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 3
  STAGE_NAME: BLOB_UPLOAD
  STAGE_DESC: File Uploading on blob
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 3

Responsibility
--------------
Uploads all binary attachments for the PR to Azure Blob Storage and syncs
the BI dashboard view from on-prem into Azure SQL.  On success, advances
ras_tracker.current_stage_fk to 'BLOB_UPLOAD'.

Delegates all business logic to AttachmentBlobSync — this class is purely a
pipeline adapter (thin wrapper that conforms to the BaseStage contract).
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.stages.base import BaseStage


class BlobUploadStage(BaseStage):
    """
    ATTACHMENT domain — stage 3 of 5.

    Prerequisites : EMBED_DOC_EXTRACTION (stage 2) completed
    Completion    : ras_tracker.current_stage_fk = 'BLOB_UPLOAD'
    Next stage    : CLASSIFICATION (stage 4)
    """

    NAME     = "BLOB_UPLOAD"
    STAGE_ID = 3

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._config = config

    def execute(self, purchase_req_no: str) -> None:
        """
        Runs AttachmentBlobSync for the given PR.
        AttachmentBlobSync handles its own ras_tracker update internally.
        Raises on any unrecoverable error.
        """
        AttachmentBlobSync(self._config).run(purchase_req_no)
