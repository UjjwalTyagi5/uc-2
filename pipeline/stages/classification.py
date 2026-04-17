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
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class ClassificationStage(BaseStage):
    """
    ATTACHMENT domain — stage 4 of 5.

    Prerequisites : BLOB_UPLOAD (stage 3) completed
    Completion    : ras_tracker.current_stage_fk = 'CLASSIFICATION'
                    ras_tracker.last_processed_at = GETUTCDATE()
                    vw_get_ras_data_for_bidashboard row synced for this PR
    Next stage    : METADATA_EXTRACTION (stage 5)
    """

    NAME     = "CLASSIFICATION"
    STAGE_ID = 4

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._config  = config
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        # TODO: implement document type classification logic (AI/ML)
        self._log.info(
            f"CLASSIFICATION not yet implemented — "
            f"passing through for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)

        # Stamp last_processed_at so SourceChangeDetector can detect future changes
        self._tracker.set_last_processed_at(purchase_req_no)

        # Sync BI dashboard row for this PR now that processing is complete.
        # Runs on both first-time processing and re-processing after source changes.
        try:
            AttachmentBlobSync(self._config).sync_bi_dashboard_for_pr(purchase_req_no)
        except Exception as exc:
            # BI dashboard sync failure is non-fatal — log it but do not fail the stage.
            self._log.error(
                f"BI dashboard sync failed for PR={purchase_req_no!r} "
                f"(non-fatal, continuing): {exc}"
            )
