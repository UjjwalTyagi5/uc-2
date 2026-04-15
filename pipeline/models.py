"""
pipeline.models
~~~~~~~~~~~~~~~
Immutable data contracts used across the pipeline layer.
No business logic lives here — only plain data structures.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class PipelineStage(str, Enum):
    """
    Canonical stage names — must match STAGE_NAME in the pipeline_stages table.

    Using (str, Enum) so every member IS a plain string and can be used
    directly in SQL parameters without calling .value.

    Domain breakdown
    ----------------
    ATTACHMENT  : INGESTION → EMBED_DOC_EXTRACTION → BLOB_UPLOAD
                  → CLASSIFICATION → METADATA_EXTRACTION
    LINE_ITEM   : ITEM_INGESTION → QUOTATION_DETECTION → ITEM_EXTRACTION
                  → SPEC_ENRICHMENT → EMBEDDING_GENERATION → VECTOR_INDEXING
    BENCHMARK   : BENCHMARK_INGESTION → SIMILARITY_SEARCH → PRICE_BENCHMARKING
                  → RISK_EVALUATION → LLM_ANALYSIS
    GLOBAL      : COMPLETED | DELIVERED | EXCEPTION
    """

    # ── ATTACHMENT domain (stage IDs 1-5) ──────────────────────────────
    INGESTION            = "INGESTION"             # ID  1
    EMBED_DOC_EXTRACTION = "EMBED_DOC_EXTRACTION"  # ID  2
    BLOB_UPLOAD          = "BLOB_UPLOAD"            # ID  3  ← current stage
    CLASSIFICATION       = "CLASSIFICATION"         # ID  4
    METADATA_EXTRACTION  = "METADATA_EXTRACTION"    # ID  5

    # ── LINE_ITEM domain (stage IDs 10-15) ─────────────────────────────
    ITEM_INGESTION       = "ITEM_INGESTION"         # ID 10
    QUOTATION_DETECTION  = "QUOTATION_DETECTION"    # ID 11
    ITEM_EXTRACTION      = "ITEM_EXTRACTION"        # ID 12
    SPEC_ENRICHMENT      = "SPEC_ENRICHMENT"        # ID 13
    EMBEDDING_GENERATION = "EMBEDDING_GENERATION"   # ID 14
    VECTOR_INDEXING      = "VECTOR_INDEXING"        # ID 15

    # ── BENCHMARK domain (stage IDs 20-24) ─────────────────────────────
    BENCHMARK_INGESTION  = "BENCHMARK_INGESTION"    # ID 20
    SIMILARITY_SEARCH    = "SIMILARITY_SEARCH"      # ID 21
    PRICE_BENCHMARKING   = "PRICE_BENCHMARKING"     # ID 22
    RISK_EVALUATION      = "RISK_EVALUATION"        # ID 23
    LLM_ANALYSIS         = "LLM_ANALYSIS"           # ID 24

    # ── GLOBAL terminal states (stage IDs 90-99) ───────────────────────
    COMPLETED            = "COMPLETED"              # ID 90
    DELIVERED            = "DELIVERED"              # ID 91
    EXCEPTION            = "EXCEPTION"              # ID 99


class StageStatus(Enum):
    SUCCESS = "success"
    FAILED  = "failed"
    SKIPPED = "skipped"   # earlier stage failed; this stage was never attempted


@dataclass(frozen=True)
class StageResult:
    """Outcome of a single pipeline stage for one PR."""

    stage_name:    str
    status:        StageStatus
    duration_secs: float               = 0.0
    error:         Optional[Exception] = field(default=None, compare=False)

    @property
    def succeeded(self) -> bool:
        return self.status is StageStatus.SUCCESS

    def __str__(self) -> str:
        tag = self.status.value.upper()
        dur = f"{self.duration_secs:.2f}s"
        base = f"[{tag}] stage={self.stage_name!r} duration={dur}"
        if self.error:
            base += f" error={self.error!r}"
        return base


@dataclass(frozen=True)
class PRResult:
    """Aggregated outcome of running all pipeline stages for one PR."""

    purchase_req_no: str
    stage_results:   List[StageResult]
    started_at:      float = field(default_factory=time.monotonic)

    @property
    def succeeded(self) -> bool:
        """True only if every stage completed with SUCCESS."""
        return all(r.succeeded for r in self.stage_results)

    @property
    def failed_stage(self) -> Optional[StageResult]:
        """First stage that failed, or None if all succeeded."""
        for r in self.stage_results:
            if r.status is StageStatus.FAILED:
                return r
        return None

    @property
    def total_duration_secs(self) -> float:
        return sum(r.duration_secs for r in self.stage_results)

    def __str__(self) -> str:
        status = "OK" if self.succeeded else "FAILED"
        dur    = f"{self.total_duration_secs:.2f}s"
        return f"PR={self.purchase_req_no!r} status={status} total_duration={dur}"
