"""
pipeline.stages.price_benchmark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 7 — PRICE_BENCHMARK.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 7
  STAGE_NAME: PRICE_BENCHMARK
  STAGE_DESC: Price benchmark analysis
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 7

Responsibility
--------------
  For each PR that completed EMBEDDINGS (stage 6), run the price benchmark:
    1. Query Pinecone for similar historical items.
    2. Fetch their pricing from quotation_extracted_items.
    3. Call LLM for a benchmark price recommendation and summary.
    4. Upsert results to benchmark_result.

On success, advances ras_tracker.current_stage_fk to 'PRICE_BENCHMARK'.
"""

from __future__ import annotations

from utils.config import AppConfig
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker
from price_benchmark.run import run_benchmark


class PriceBenchmarkStage(BaseStage):
    """
    ATTACHMENT domain — stage 7.

    Prerequisites : EMBEDDINGS (stage 6) completed
    Completion    : ras_tracker.current_stage_fk = 7 (PRICE_BENCHMARK)
                    benchmark_result rows upserted for this PR
    """

    NAME     = "PRICE_BENCHMARK"
    STAGE_ID = 7

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._config  = config
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        count = run_benchmark(purchase_req_no, self._config)
        self._log.info(
            f"{count} benchmark row(s) written for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)
