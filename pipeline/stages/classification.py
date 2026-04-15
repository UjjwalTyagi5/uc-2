"""
pipeline.stages.classification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 4 — CLASSIFICATION.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 4
  STAGE_NAME: CLASSIFICATION
  STAGE_DESC: Document type classification using AI/ML
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 4

Current status: STUB
--------------------
No AI/ML classification logic is implemented yet.
The stage logs that it was reached, advances ras_tracker to
current_stage_fk = 'CLASSIFICATION', and exits successfully.

When the real logic is ready:
    Replace the TODO block in execute() with the actual implementation.
    The tracker advance call at the end should remain unchanged.
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class ClassificationStage(BaseStage):
    """
    ATTACHMENT domain — stage 4 of 5.

    Prerequisites : BLOB_UPLOAD (stage 3) completed
    Completion    : ras_tracker.current_stage_fk = 'CLASSIFICATION'
    Next stage    : METADATA_EXTRACTION (stage 5)
    """

    NAME     = "CLASSIFICATION"
    STAGE_ID = 4

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        # TODO: implement document type classification logic (AI/ML)
        self._log.info(
            f"CLASSIFICATION not yet implemented — "
            f"passing through for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)
