"""
pipeline.stages.base
~~~~~~~~~~~~~~~~~~~~
Abstract base class for every pipeline stage.

Pattern — Template Method:
    Subclasses implement `execute()`.  The public `run()` method here adds
    timing, structured logging, and converts exceptions into StageResult
    objects so the orchestrator never has to handle raw exceptions per stage.

Adding a new stage:
    1. Create pipeline/stages/your_stage.py
    2. Subclass BaseStage, set NAME, implement execute()
    3. Append an instance to PipelineOrchestrator.STAGES
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod

from loguru import logger

from pipeline.models import StageResult, StageStatus


class BaseStage(ABC):
    """
    Abstract pipeline stage.

    Subclasses MUST:
        - set a unique class-level NAME string
        - implement execute(purchase_req_no)

    execute() should raise on any unrecoverable failure; the exception
    will be caught by run() and surfaced as a FAILED StageResult.
    """

    NAME: str = ""          # overridden by each subclass

    def __init__(self) -> None:
        if not self.NAME:
            raise NotImplementedError(f"{type(self).__name__} must define a non-empty NAME")
        self._log = logger.bind(stage=self.NAME)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self.NAME

    def run(self, purchase_req_no: str) -> StageResult:
        """
        Template method — wraps execute() with timing + error handling.
        Always returns a StageResult; never raises.
        """
        self._log.info(f"Starting stage={self.NAME!r} for PR={purchase_req_no!r}")
        start = time.perf_counter()

        try:
            self.execute(purchase_req_no)
            duration = time.perf_counter() - start
            self._log.success(
                f"Stage={self.NAME!r} succeeded for PR={purchase_req_no!r} "
                f"in {duration:.2f}s"
            )
            return StageResult(
                stage_name=self.NAME,
                status=StageStatus.SUCCESS,
                duration_secs=duration,
            )

        except Exception as exc:
            duration = time.perf_counter() - start
            self._log.error(
                f"Stage={self.NAME!r} failed for PR={purchase_req_no!r} "
                f"after {duration:.2f}s: {exc}"
            )
            return StageResult(
                stage_name=self.NAME,
                status=StageStatus.FAILED,
                duration_secs=duration,
                error=exc,
            )

    # ------------------------------------------------------------------
    # Abstract
    # ------------------------------------------------------------------

    @abstractmethod
    def execute(self, purchase_req_no: str) -> None:
        """
        Run the stage's business logic for the given PR.
        Raise any exception to signal failure; return normally to signal success.
        """
