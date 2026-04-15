"""
pipeline.stages.blob_upload
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage: upload purchase requisition attachments to Azure Blob Storage
and sync the BI dashboard view for the given PR.

DB stage reference
------------------
  STAGE_ID : 3
  STAGE_NAME: BLOB_UPLOAD
  STAGE_DESC: File Uploading on blob
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 3

Completion marker: ras_tracker.current_stage_fk = 'BLOB_UPLOAD'

Delegates all business logic to AttachmentBlobSync — this class is purely a
pipeline adapter (thin wrapper that conforms to the BaseStage contract).
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.models import PipelineStage
from pipeline.stages.base import BaseStage


class BlobUploadStage(BaseStage):
    """
    ATTACHMENT domain — stage 3 of 5.

    Prerequisites : EMBED_DOC_EXTRACTION (stage 2) done  [or pipeline entry point]
    Completion    : ras_tracker.current_stage_fk = PipelineStage.BLOB_UPLOAD
    Next stage    : CLASSIFICATION (stage 4)
    """

    NAME     = PipelineStage.BLOB_UPLOAD   # "BLOB_UPLOAD"
    STAGE_ID = 3

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._config = config

    def execute(self, purchase_req_no: str) -> None:
        """
        Runs AttachmentBlobSync for the given PR.
        Raises on any unrecoverable error (propagated from AttachmentBlobSync).
        """
        AttachmentBlobSync(self._config).run(purchase_req_no)
