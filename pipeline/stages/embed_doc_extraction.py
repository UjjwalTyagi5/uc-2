"""
pipeline.stages.embed_doc_extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 2 — EMBED_DOC_EXTRACTION.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 2
  STAGE_NAME: EMBED_DOC_EXTRACTION
  STAGE_DESC: Document extraction embedded
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 2

Current status: STUB
--------------------
No extraction logic is implemented yet.
The stage logs that it was reached, advances ras_tracker to
current_stage_fk = 'EMBED_DOC_EXTRACTION', and exits successfully
so the pipeline continues to BLOB_UPLOAD.

When the real logic is ready:
    Replace the TODO block in execute() with the actual implementation.
    The tracker advance call at the end should remain unchanged.
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class EmbedDocExtractionStage(BaseStage):
    """
    ATTACHMENT domain — stage 2 of 5.

    Prerequisites : INGESTION (stage 1) completed
    Completion    : ras_tracker.current_stage_fk = 'EMBED_DOC_EXTRACTION'
    Next stage    : BLOB_UPLOAD (stage 3)
    """

    NAME     = "EMBED_DOC_EXTRACTION"
    STAGE_ID = 2

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        # TODO: implement document extraction / embedding logic
        self._log.info(
            f"EMBED_DOC_EXTRACTION not yet implemented — "
            f"passing through for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)
