"""
pipeline.stages.complete
~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 8 — COMPLETE.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 8
  STAGE_NAME: COMPLETE
  STAGE_DESC: Pipeline fully completed
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 8

Responsibility
--------------
  Marker stage — no business logic.  Advances ras_tracker to COMPLETE
  once all preceding stages (including PRICE_BENCHMARK) have succeeded.
  A PR at stage 8 is fully done and will not be picked up again.
"""

from __future__ import annotations

from utils.config import AppConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class CompleteStage(BaseStage):
    """
    ATTACHMENT domain — stage 8.

    Prerequisites : PRICE_BENCHMARK (stage 7) completed
    Completion    : ras_tracker.current_stage_fk = 8 (COMPLETE)
    """

    NAME     = "COMPLETE"
    STAGE_ID = 8

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)
        self._log.info(f"PR={purchase_req_no!r} marked as fully complete")
