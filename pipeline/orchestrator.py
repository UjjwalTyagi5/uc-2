"""
pipeline.orchestrator
~~~~~~~~~~~~~~~~~~~~~
Central coordinator for the RAS attachment processing pipeline.

Responsibilities:
    1. Ask the repository for all unprocessed PRs.
    2. Run each PR through every registered stage in order.
    3. If a stage fails, skip remaining stages for that PR (fail-fast per PR)
       but continue to the next PR so one bad PR never blocks the whole batch.
    4. Log a structured summary when the run is complete.

Extending the pipeline
----------------------
To add a new processing stage (e.g. classification):

    1. Create pipeline/stages/classification.py  →  ClassificationStage(BaseStage)
    2. Import it here and append to STAGES:

        from pipeline.stages.classification import ClassificationStage
        ...
        STAGES = [
            BlobUploadStage(config),
            ClassificationStage(config),   # <-- new
        ]

    ClassificationStage.execute() should:
        - Do its work for the given PURCHASE_REQ_NO
        - Update ras_tracker.current_stage_fk to a new value on success
        - Raise on failure (BaseStage.run() handles the rest)
"""

from __future__ import annotations

from typing import List, Optional

from loguru import logger

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.models import PRResult, StageResult, StageStatus
from pipeline.repository import PipelineRepository
from pipeline.stages.base import BaseStage
from pipeline.stages.blob_upload import BlobUploadStage


class PipelineOrchestrator:
    """
    Orchestrates the end-to-end processing pipeline for untracked PRs.

    Parameters
    ----------
    config:
        Shared BlobSyncConfig used by all stages (holds DB and Blob creds).
    limit:
        Optional cap on the number of PRs processed per run.  Useful for
        gradually draining a large backlog.  None = process everything.
    stages:
        Override the default stage list — primarily useful for testing.
    """

    def __init__(
        self,
        config: BlobSyncConfig,
        limit: Optional[int] = None,
        stages: Optional[List[BaseStage]] = None,
    ) -> None:
        self._config     = config
        self._repository = PipelineRepository(config.get_azure_conn_str(), limit=limit)
        self._stages     = stages if stages is not None else self._build_default_stages(config)
        self._log        = logger.bind(component="PipelineOrchestrator")

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> List[PRResult]:
        """
        Fetches all pending PRs and runs each through the stage pipeline.

        Returns the list of PRResult objects (one per PR) so callers can
        inspect outcomes programmatically if needed.

        This method never raises — all errors are captured in PRResult.
        """
        self._log.info("Pipeline run started")
        self._log.info(
            f"Registered stages: {[s.name for s in self._stages]}"
        )

        try:
            pending = self._repository.fetch_pending_prs()
        except Exception as exc:
            self._log.critical(f"Cannot fetch pending PRs — aborting run: {exc}")
            return []

        total = len(pending)
        self._log.info(f"Pending PRs to process: {total}")

        if not pending:
            self._log.info("Nothing to process — pipeline run finished")
            return []

        results: List[PRResult] = []
        for idx, pr_no in enumerate(pending, 1):
            self._log.info(f"--- [{idx}/{total}] PR: {pr_no!r} ---")
            result = self._process_pr(pr_no)
            results.append(result)

        self._log_summary(results)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _process_pr(self, pr_no: str) -> PRResult:
        """
        Runs all stages for a single PR in registration order.

        - On stage success → continue to next stage.
        - On stage failure → mark remaining stages as SKIPPED, stop early.
        """
        stage_results: List[StageResult] = []
        pipeline_failed = False

        for stage in self._stages:
            if pipeline_failed:
                self._log.warning(
                    f"Skipping stage={stage.name!r} for PR={pr_no!r} "
                    f"(previous stage failed)"
                )
                stage_results.append(
                    StageResult(stage_name=stage.name, status=StageStatus.SKIPPED)
                )
                continue

            result = stage.run(pr_no)
            stage_results.append(result)

            if not result.succeeded:
                pipeline_failed = True

        pr_result = PRResult(purchase_req_no=pr_no, stage_results=stage_results)
        if pr_result.succeeded:
            self._log.success(f"PR={pr_no!r} completed all stages successfully")
        else:
            failed = pr_result.failed_stage
            self._log.error(
                f"PR={pr_no!r} failed at stage={failed.stage_name!r}: {failed.error}"
            )
        return pr_result

    def _log_summary(self, results: List[PRResult]) -> None:
        """Logs a structured summary table at the end of the run."""
        total     = len(results)
        succeeded = sum(1 for r in results if r.succeeded)
        failed    = total - succeeded

        self._log.info("=" * 60)
        self._log.info("Pipeline run summary")
        self._log.info(f"  Total PRs   : {total}")
        self._log.info(f"  Succeeded   : {succeeded}")
        self._log.info(f"  Failed      : {failed}")

        if failed:
            self._log.info("  Failed PRs:")
            for r in results:
                if not r.succeeded and r.failed_stage:
                    self._log.error(
                        f"    {r.purchase_req_no!r} — stage={r.failed_stage.stage_name!r} "
                        f"error={r.failed_stage.error!r}"
                    )

        self._log.info("=" * 60)

    @staticmethod
    def _build_default_stages(config: BlobSyncConfig) -> List[BaseStage]:
        """
        Returns the ordered list of stages for a standard pipeline run.
        Edit this list to add / reorder / remove stages.
        """
        return [
            BlobUploadStage(config),
            # ClassificationStage(config),   # uncomment when ready
            # ExtractionStage(config),        # uncomment when ready
        ]
