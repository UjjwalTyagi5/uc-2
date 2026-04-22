"""
pipeline.stages.embeddings
~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 6 — EMBEDDINGS.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 6
  STAGE_NAME: EMBEDDINGS
  STAGE_DESC: Quotation item vector embeddings
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 6

Responsibility
--------------
  For each PR that completed METADATA_EXTRACTION (stage 5), embed the
  is_selected_quote=1 items using Azure OpenAI text-embedding-3-large and
  upsert the resulting vectors into Pinecone.

  Each vector carries four metadata fields:
    purchase_req_no, purchase_dtl_id, extracted_item_uuid_pk,
    commodity_tag, item_created_date

  Vector IDs are stable (dtl_{purchase_dtl_id}) so re-runs overwrite rather
  than duplicate.

On success, advances ras_tracker.current_stage_fk to 'EMBEDDINGS'.
"""

from __future__ import annotations

from utils.config import AppConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker
from quotation_embedding.run import run_embedding


class EmbeddingsStage(BaseStage):
    """
    ATTACHMENT domain — stage 6.

    Prerequisites : METADATA_EXTRACTION (stage 5) completed
                    (quotation_extracted_items rows with is_selected_quote=1)
    Completion    : ras_tracker.current_stage_fk = 6 (EMBEDDINGS)
                    Pinecone vectors upserted for this PR
    """

    NAME     = "EMBEDDINGS"
    STAGE_ID = 6

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._config  = config
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        count = run_embedding(purchase_req_no, config=self._config)

        self._log.info(
            f"Upserted {count} vector(s) to Pinecone for PR={purchase_req_no!r}"
        )

        self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)
