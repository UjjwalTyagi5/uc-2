"""
pipeline.stages.ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 1 — INGESTION.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 1
  STAGE_NAME: INGESTION
  STAGE_DESC: Document received and stored
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 1

Responsibility
--------------
Pipeline entry point for a PR.  Creates the ras_tracker row (or advances it
if it somehow already exists) and sets current_stage_fk = 'INGESTION'.
Also enriches the row with ras_justification, currency, and datetime fields
pulled directly from purchase_req_mst in the same MERGE statement.

All subsequent stages depend on this row existing in ras_tracker.
"""

from __future__ import annotations

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class IngestionStage(BaseStage):
    """
    ATTACHMENT domain — stage 1 of 5.

    Prerequisites : None  (pipeline entry point)
    Completion    : ras_tracker row created with current_stage_fk = 'INGESTION'
    Next stage    : EMBED_DOC_EXTRACTION (stage 2)
    """

    NAME     = "INGESTION"
    STAGE_ID = 1

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        """
        Creates or updates the ras_tracker row for this PR.
        Uses MERGE so re-running is safe (idempotent).
        """
        self._log.info(f"Recording ingestion for PR={purchase_req_no!r}")
        self._tracker.upsert_stage(purchase_req_no, self.NAME)
