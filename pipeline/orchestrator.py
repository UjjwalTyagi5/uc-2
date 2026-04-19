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

import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

from loguru import logger

from utils.config import AppConfig
from pipeline.attachment_classification_repository import AttachmentClassificationRepository
from pipeline.models import PRResult, StageResult, StageStatus
from pipeline.repository import PipelineRepository
from pipeline.stage_registry import StageRegistry
from pipeline.stages.base import BaseStage
from pipeline.stages.blob_upload import BlobUploadStage
from pipeline.stages.classification import ClassificationStage
from pipeline.stages.embed_doc_extraction import EmbedDocExtractionStage
from pipeline.stages.ingestion import IngestionStage
from pipeline.stages.metadata_extraction import MetadataExtractionStage
from pipeline.tracker import PipelineTracker


class PipelineOrchestrator:
    """
    Orchestrates the end-to-end processing pipeline for untracked PRs.

    Parameters
    ----------
    config:
        Shared AppConfig used by all stages (holds DB and Blob creds).
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
        config: AppConfig,
        limit: Optional[int] = None,
        stages: Optional[List[BaseStage]] = None,
    ) -> None:
        self._config     = config
        self._log        = logger.bind(component="PipelineOrchestrator")
        self._repository = PipelineRepository(config.get_azure_conn_str(), limit=limit)
        self._tracker    = PipelineTracker(config.get_azure_conn_str())
        self._att_repo   = AttachmentClassificationRepository(config.get_azure_conn_str())

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
        completed_stage_id = self._stages[-1].STAGE_ID

        try:
            pending = self._repository.fetch_pending_prs(completed_stage_id)
        except Exception as exc:
            self._log.critical(f"Cannot fetch pending PRs — aborting run: {exc}")
            return []

        total = len(pending)
        self._log.info(f"Pending PRs to process: {total}")

        if not pending:
            self._log.info("Nothing to process — pipeline run finished")
            return []

        workers = self._config.PIPELINE_WORKERS
        if workers > 1:
            self._log.info(f"Processing {total} PRs with {workers} parallel workers")
            results = self._run_parallel(pending, workers)
        else:
            results = []
            for idx, pr_no in enumerate(pending, 1):
                self._log.info(f"--- [{idx}/{total}] PR: {pr_no!r} ---")
                results.append(self._process_pr(pr_no))

        self._log_summary(results)
        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_parallel(self, pr_nos: List[str], workers: int) -> List[PRResult]:
        """
        Process pr_nos concurrently using a thread pool.

        Uses ThreadPoolExecutor — appropriate because all pipeline work is
        I/O-bound (DB queries, blob uploads, file I/O).  Each thread opens
        its own pyodbc connections and writes to its own work subfolder so
        there is no shared mutable state between workers.

        Results are returned in completion order (not submission order) for
        live progress logging; the summary at the end is unaffected.
        """
        total     = len(pr_nos)
        results: List[PRResult] = []
        counter_lock = threading.Lock()
        completed    = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_pr = {
                executor.submit(self._process_pr, pr_no): pr_no
                for pr_no in pr_nos
            }
            for future in as_completed(future_to_pr):
                pr_no = future_to_pr[future]
                with counter_lock:
                    completed += 1
                    idx = completed
                try:
                    result = future.result()
                    results.append(result)
                    status = "OK" if result.succeeded else "FAILED"
                    self._log.info(f"[{idx}/{total}] PR={pr_no!r} {status}")
                except Exception as exc:
                    self._log.opt(exception=True).error(
                        f"[{idx}/{total}] PR={pr_no!r} raised unexpectedly: {exc}"
                    )
                    results.append(PRResult(
                        purchase_req_no=pr_no,
                        stage_results=[StageResult(
                            stage_name="UNKNOWN",
                            status=StageStatus.FAILED,
                            error=exc,
                        )],
                    ))

        return results

    def _process_pr(self, pr_no: str) -> PRResult:
        """
        Runs stages in order for a single PR.

        Before stages begin, clears all prior pipeline output for this PR so
        every run starts from a clean slate — handles both first-time PRs
        (no-op) and retries (removes stale rows and local files).

        On stage failure:
          - Writes to ras_pipeline_exceptions + marks ras_tracker = EXCEPTION
          - Immediately stops all further stages for this PR  (break)
          - Returns so the caller loop can continue to the next PR
        """
        stage_results: List[StageResult] = []

        # ── Pre-run cleanup ───────────────────────────────────────────────
        # Delete all pipeline output rows for this PR from the DB (via SP),
        # then wipe the local work folder so we download fresh from source.
        # Safe for new PRs — the SP and rmtree are both no-ops when nothing exists.
        try:
            self._att_repo.cleanup_for_pr(pr_no)
        except Exception as exc:
            self._log.opt(exception=True).error(
                f"Pre-run DB cleanup failed for PR={pr_no!r} — aborting PR: {exc}"
            )
            self._handle_stage_failure(pr_no, StageResult(
                stage_name="PRE_RUN_CLEANUP",
                status=StageStatus.FAILED,
                error=exc,
            ))
            return PRResult(
                purchase_req_no=pr_no,
                stage_results=[StageResult(
                    stage_name="PRE_RUN_CLEANUP",
                    status=StageStatus.FAILED,
                    error=exc,
                )],
            )

        safe_pr    = pr_no.replace("/", "_")
        work_folder = Path(self._config.WORK_DIR) / "procurement" / safe_pr
        if work_folder.exists():
            shutil.rmtree(work_folder)
            self._log.debug(f"Removed local work folder: {work_folder}")

        for stage in self._stages:
            result = stage.run(pr_no)
            stage_results.append(result)

            if not result.succeeded:
                # Record in exception table then stop — no further stages for this PR
                self._handle_stage_failure(pr_no, result)
                break

        pr_result = PRResult(purchase_req_no=pr_no, stage_results=stage_results)
        if pr_result.succeeded:
            self._log.success(f"PR={pr_no!r} completed all stages successfully")
        else:
            failed = pr_result.failed_stage
            self._log.opt(exception=failed.error).error(
                f"PR={pr_no!r} stopped at stage={failed.stage_name!r}: {failed.error}"
            )
        return pr_result

    def _handle_stage_failure(self, pr_no: str, result: StageResult) -> None:
        """
        On stage failure:
          1. Mark ras_tracker.current_stage_fk = 'EXCEPTION'
          2. Insert a row into ras_pipeline_exceptions

        If stage_id == 0 (PRE_RUN_CLEANUP has no DB stage row), skip the
        ras_pipeline_exceptions insert — just mark the tracker as EXCEPTION.

        If the DB write itself fails, log the error but do NOT re-raise —
        the PR is already counted as failed in the run summary.
        """
        error_message = str(result.error) if result.error else "Unknown error"
        if result.stage_id == 0:
            # Not a real pipeline stage — can't FK to pipeline_stages; just log.
            self._log.warning(
                f"Stage failure recorded (no DB exception row) for PR={pr_no!r} "
                f"stage={result.stage_name!r}: {error_message}"
            )
            return
        try:
            self._tracker.record_exception(
                purchase_req_no=pr_no,
                stage_id=result.stage_id,
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
    def _build_default_stages(config: AppConfig) -> List[BaseStage]:
        """
        Ordered stage list for the ATTACHMENT domain pipeline.

        Current state
        -------------
        Stage 1 — INGESTION            : records entry, creates ras_tracker row
        Stage 2 — EMBED_DOC_EXTRACTION : downloads attachments + extracts embedded docs
        Stage 3 — BLOB_UPLOAD          : uploads complete work folder to Azure Blob
        Stage 4 — CLASSIFICATION       : classifies each attachment by doc_type via LLM
        Stage 5 — METADATA_EXTRACTION  : extracts structured line items from quotations
        """
        return [
            IngestionStage(config),             # stage 1
            EmbedDocExtractionStage(config),    # stage 2
            BlobUploadStage(config),            # stage 3
            ClassificationStage(config),        # stage 4
            MetadataExtractionStage(config),    # stage 5
        ]
