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
