"""
pipeline.stages.metadata_extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 5 — METADATA_EXTRACTION.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 5
  STAGE_NAME: METADATA_EXTRACTION
  STAGE_DESC: Quotation metadata extraction
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 5

Responsibility
--------------
  For each quotation attachment classified in stage 4, call the LLM-based
  quotation extractor to produce structured line-item rows and write them
  to [ras_procurement].[quotation_extracted_items].

  Raises RuntimeError if no attachments were classified as 'Quotation',
  which causes the orchestrator to mark the PR as EXCEPTION (stage 99) and
  insert a record in ras_pipeline_exceptions — no further stages run.

On success, advances ras_tracker.current_stage_fk to 'METADATA_EXTRACTION'.
"""

from __future__ import annotations

from utils.config import AppConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker
from quotation_extraction.run import run_extraction


class MetadataExtractionStage(BaseStage):
    """
    ATTACHMENT domain — stage 5.

    Prerequisites : CLASSIFICATION (stage 4) completed
                    (attachment_classification rows with doc_type populated)
    Completion    : ras_tracker.current_stage_fk = 5 (METADATA_EXTRACTION)
                    quotation_extracted_items rows written for this PR
    """

    NAME     = "METADATA_EXTRACTION"
    STAGE_ID = 5

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._config  = config
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        # run_extraction raises RuntimeError if no quotation sources found —
        # BaseStage.run() catches it and the orchestrator records the exception.
        items = run_extraction(
            purchase_req_no,
            config=self._config,
            write_to_db=True,
        )

        self._log.info(
            f"Extracted {len(items)} item(s) for PR={purchase_req_no!r}"
        )

        self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)
