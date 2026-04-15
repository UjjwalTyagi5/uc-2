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
Downloads all binary attachments for the PR from the DB and saves them
to the local work/ directory:

    work/procurement/{safe_pr_no}/{att_id}/{filename}

Does NOT upload to Azure Blob — that happens in EmbedDocExtractionStage
AFTER embedded files have been extracted, so the entire folder (parent
files + extracted files) is uploaded in one go.

On success, advances ras_tracker.current_stage_fk to 'BLOB_UPLOAD'.
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class BlobUploadStage(BaseStage):
    """
    ATTACHMENT domain — stage 3 (runs second in execution order).

    Prerequisites : INGESTION (stage 1) completed
    Completion    : ras_tracker.current_stage_fk = 'BLOB_UPLOAD'
    Next stage    : EMBED_DOC_EXTRACTION (stage 2, runs third)
    """

    NAME     = "BLOB_UPLOAD"
    STAGE_ID = 3

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._config  = config
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        """
        Saves all binary attachments to work/ directory.
        The actual Azure Blob upload is deferred to EmbedDocExtractionStage
        so extracted files are included in the same upload batch.
        """
        saved = AttachmentBlobSync(self._config).save_locally(purchase_req_no)
        self._log.info(
            f"Saved {saved} attachment(s) locally for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)
