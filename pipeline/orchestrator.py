"""
pipeline.orchestrator
~~~~~~~~~~~~~~~~~~~~~
Central coordinator for the RAS attachment processing pipeline.

Responsibilities:
    1. Load the pipeline_stages table from DB via StageRegistry.
    2. Validate all stage classes against the DB — fail fast if anything drifts.
    3. Ask the repository for all unprocessed PRs.
    4. Run each PR through every registered stage in order.
    5. If a stage fails, skip remaining stages for that PR (fail-fast per PR)
       but continue to the next PR so one bad PR never blocks the whole batch.
    6. Log a structured summary when the run is complete.

Extending the pipeline
----------------------
To add a new processing stage (e.g. METADATA_EXTRACTION):

    1. Create pipeline/stages/metadata_extraction.py
    2. Subclass BaseStage, set NAME = "METADATA_EXTRACTION", STAGE_ID = 5
    3. In execute(): do the work, then call
           self._tracker.advance_stage(purchase_req_no, self.NAME)
    4. Append MetadataExtractionStage(config) to _build_default_stages() below

The stage NAME and STAGE_ID must match a row in the pipeline_stages DB table —
StageRegistry validates this at startup and raises ValueError if they don't.
"""

from __future__ import annotations

from typing import List, Optional

from loguru import logger

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.models import PRResult, StageResult, StageStatus
from pipeline.repository import PipelineRepository
from pipeline.stage_registry import StageRegistry
from pipeline.stages.base import BaseStage
from pipeline.stages.blob_upload import BlobUploadStage
from pipeline.stages.classification import ClassificationStage
from pipeline.stages.embed_doc_extraction import EmbedDocExtractionStage
from pipeline.stages.ingestion import IngestionStage
from pipeline.tracker import PipelineTracker


class PipelineOrchestrator:
    """
    Orchestrates the end-to-end processing pipeline for untracked PRs.

    Parameters
    ----------
    config:
        Shared BlobSyncConfig used by all stages (holds DB and Blob creds).
    limit:
        Optional cap on the number of PRs processed per run.
        None = process everything.
    stages:
        Override the default stage list — primarily useful for testing.
        If provided, these stages are still validated against the DB table.

    Raises
    ------
    ValueError
        At init time if any stage class has a NAME or STAGE_ID that does
        not match the pipeline_stages table in the DB.
    pyodbc.Error
        At init time if the DB is unreachable or pipeline_stages is missing.
    """

    def __init__(
        self,
        config: BlobSyncConfig,
        limit: Optional[int] = None,
        stages: Optional[List[BaseStage]] = None,
    ) -> None:
        self._config     = config
        self._log        = logger.bind(component="PipelineOrchestrator")
        self._repository = PipelineRepository(config.get_azure_conn_str(), limit=limit)
        self._tracker    = PipelineTracker(config.get_azure_conn_str())

        # Load stage definitions from DB — single source of truth
        self._registry = StageRegistry(config.get_azure_conn_str())

        # Build stages, then validate every NAME/STAGE_ID against the DB table
        self._stages = stages if stages is not None else self._build_default_stages(config)
        self._registry.validate_stages(self._stages)

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
            f"Active stages: "
            f"{[f'{s.STAGE_ID}:{s.NAME}' for s in self._stages]}"
        )

        # The last registered stage is the completion marker.
        # PRs already at this stage are skipped; everything else is retried.
        completed_stage = self._stages[-1].NAME

        try:
            pending = self._repository.fetch_pending_prs(completed_stage)
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
                    f"Skipping stage={stage.NAME!r} for PR={pr_no!r} "
                    f"(previous stage failed)"
                )
                stage_results.append(
                    StageResult(stage_name=stage.NAME, status=StageStatus.SKIPPED)
                )
                continue

            result = stage.run(pr_no)
            stage_results.append(result)

            if not result.succeeded:
                pipeline_failed = True
                # Move PR to EXCEPTION stage and write to exception table
                self._handle_stage_failure(pr_no, result)

        pr_result = PRResult(purchase_req_no=pr_no, stage_results=stage_results)
        if pr_result.succeeded:
            self._log.success(f"PR={pr_no!r} completed all stages successfully")
        else:
            failed = pr_result.failed_stage
            self._log.opt(exception=failed.error).error(
                f"PR={pr_no!r} failed at stage={failed.stage_name!r}: {failed.error}"
            )
        return pr_result

    def _handle_stage_failure(self, pr_no: str, result: StageResult) -> None:
        """
        On stage failure:
          1. Mark ras_tracker.current_stage_fk = 'EXCEPTION'
          2. Insert a row into ras_pipeline_exceptions

        If the DB write itself fails, log the error but do NOT re-raise —
        the PR is already counted as failed in the run summary.
        """
        error_message = str(result.error) if result.error else "Unknown error"
        try:
            self._tracker.record_exception(
                purchase_req_no=pr_no,
                stage_name=result.stage_name,
                error_message=error_message,
            )
        except Exception as exc:
            self._log.opt(exception=True).error(
                f"Could not write exception record for PR={pr_no!r}: {exc}"
            )

    def _log_summary(self, results: List[PRResult]) -> None:
        """Logs a structured summary table at the end of the run."""
        total     = len(results)
        succeeded = sum(1 for r in results if r.succeeded)
        failed    = total - succeeded

        self._log.info("=" * 60)
        self._log.info("Pipeline run summary")
        self._log.info(f"  Total PRs : {total}")
        self._log.info(f"  Succeeded : {succeeded}")
        self._log.info(f"  Failed    : {failed}")

        if failed:
            self._log.info("  Failed PRs:")
            for r in results:
                if not r.succeeded and r.failed_stage:
                    self._log.error(
                        f"    {r.purchase_req_no!r} — "
                        f"stage={r.failed_stage.stage_name!r} "
                        f"error={r.failed_stage.error!r}"
                    )

        self._log.info("=" * 60)

    @staticmethod
    def _build_default_stages(config: BlobSyncConfig) -> List[BaseStage]:
        """
        Ordered stage list for the ATTACHMENT domain pipeline.

        Current state
        -------------
        Stage 1 — INGESTION           : records entry, creates ras_tracker row
        Stage 2 — EMBED_DOC_EXTRACTION: stub (no work, marks stage)
        Stage 3 — BLOB_UPLOAD         : uploads attachments to Azure Blob Storage
        Stage 4 — CLASSIFICATION      : stub (no work, marks stage)

        To add the next stage (e.g. METADATA_EXTRACTION):
            1. Create pipeline/stages/metadata_extraction.py  (NAME="METADATA_EXTRACTION", STAGE_ID=5)
            2. Append MetadataExtractionStage(config) to this list
            3. Ensure STAGE_NAME=METADATA_EXTRACTION, STAGE_ID=5 exists in pipeline_stages table
        """
        return [
            IngestionStage(config),            # stage 1  — real work
            EmbedDocExtractionStage(config),   # stage 2  — stub
            BlobUploadStage(config),           # stage 3  — real work
            ClassificationStage(config),       # stage 4  — stub
            # MetadataExtractionStage(config), # stage 5  — add when ready
        ]
