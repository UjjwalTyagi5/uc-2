"""
pipeline.stages.classification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 4 — CLASSIFICATION.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 4
  STAGE_NAME: CLASSIFICATION
  STAGE_DESC: Document type classification using AI/ML
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 4

Responsibility
--------------
  For every parent and embedded file belonging to a PR, classify the
  document type using LLM-based classification (GPT-4o-mini with
  escalation to GPT-4o when confidence < 0.75).

  Updates the following columns in both attachment_classification and
  embedded_attachment_classification tables:
      doc_type            — one of: Quotation, MPBC, RFQ, BER,
                            E-Auction Results, Others
      classification_conf — DECIMAL(5,2) e.g. 0.95

  On success:
      1. advances ras_tracker.current_stage_fk to 4 (CLASSIFICATION)
      2. stamps ras_tracker.last_processed_at for change detection
      3. syncs BI dashboard row for this PR
"""

from __future__ import annotations

from pathlib import Path

from utils.config import AppConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.attachment_classification_repository import (
    AttachmentClassificationRepository,
)
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker

_WORK_DIR = Path("work")

# Map classifier output values → DB-expected values
_CLASSIFICATION_MAP = {
    "E-Auction": "E-Auction Results",
    "Other": "Others",
}

# Extensions the file_classifier can handle
_SUPPORTED_EXTENSIONS = {
    ".xlsx", ".xls", ".csv", ".pdf", ".docx", ".doc",
    ".pptx", ".ppt", ".txt", ".html", ".htm",
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp",
}


class ClassificationStage(BaseStage):
    """
    ATTACHMENT domain — stage 4.

    Prerequisites : BLOB_UPLOAD (stage 3) completed
    Completion    : ras_tracker.current_stage_fk = 4 (CLASSIFICATION)
                    ras_tracker.last_processed_at = SYSUTCDATETIME()
                    vw_get_ras_data_for_bidashboard row synced for this PR
    Next stage    : METADATA_EXTRACTION (stage 5)
    """

    NAME     = "CLASSIFICATION"
    STAGE_ID = 4

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._config   = config
        self._tracker  = PipelineTracker(config.get_azure_conn_str())
        self._att_repo = AttachmentClassificationRepository(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        safe_pr     = purchase_req_no.replace("/", "_")
        pr_work_dir = _WORK_DIR / "procurement" / safe_pr

        if not pr_work_dir.exists():
            self._log.warning(
                f"No work directory for PR={purchase_req_no!r} — "
                f"skipping classification"
            )
            self._finish(purchase_req_no)
            return

        # Lazy import — avoids loading heavy LLM / extraction deps at module level
        from file_classifier.classifier.classifier import classify_file

        classified_count = 0
        error_count      = 0

        for att_dir in sorted(pr_work_dir.iterdir()):
            if not att_dir.is_dir():
                continue

            att_id = att_dir.name

            # ── Classify parent files ─────────────────────────────────────
            for file_path in sorted(att_dir.iterdir()):
                if not file_path.is_file():
                    continue

                doc_type, confidence = self._classify_single_file(
                    file_path, classify_file,
                )
                try:
                    self._att_repo.update_parent_classification(
                        attachment_id       = att_id,
                        doc_type            = doc_type,
                        classification_conf = confidence,
                    )
                    classified_count += 1
                    self._log.info(
                        f"Classified parent {file_path.name!r}: "
                        f"{doc_type} (conf={confidence:.2f})"
                    )
                except Exception as exc:
                    self._log.opt(exception=True).error(
                        f"DB update failed for parent file "
                        f"{file_path.name!r}: {exc}"
                    )
                    error_count += 1

            # ── Classify embedded files ───────────────────────────────────
            extracted_dir = att_dir / "extracted"
            if not extracted_dir.exists() or not extracted_dir.is_dir():
                continue

            parent_pk = self._att_repo.get_parent_pk(att_id)
            if parent_pk is None:
                self._log.warning(
                    f"No parent PK for att_id={att_id!r} — "
                    f"skipping embedded classification"
                )
                continue

            for emb_file in sorted(extracted_dir.iterdir()):
                if not emb_file.is_file():
                    continue

                doc_type, confidence = self._classify_single_file(
                    emb_file, classify_file,
                )
                blob_path = (
                    f"procurement/{safe_pr}/{att_id}/extracted/{emb_file.name}"
                )
                try:
                    self._att_repo.update_embedded_classification(
                        attachment_classification_id = parent_pk,
                        file_path                    = blob_path,
                        doc_type                     = doc_type,
                        classification_conf          = confidence,
                    )
                    classified_count += 1
                    self._log.info(
                        f"Classified embedded {emb_file.name!r}: "
                        f"{doc_type} (conf={confidence:.2f})"
                    )
                except Exception as exc:
                    self._log.opt(exception=True).error(
                        f"DB update failed for embedded file "
                        f"{emb_file.name!r}: {exc}"
                    )
                    error_count += 1

        self._log.info(
            f"Classification complete: {classified_count} file(s) classified, "
            f"{error_count} error(s) for PR={purchase_req_no!r}"
        )
        self._finish(purchase_req_no)

    # ── Helpers ───────────────────────────────────────────────────────────

    def _finish(self, purchase_req_no: str) -> None:
        """Advance tracker, stamp last_processed_at, and sync BI dashboard."""
        self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)

        # Stamp last_processed_at so SourceChangeDetector can detect future changes
        self._tracker.set_last_processed_at(purchase_req_no)

        # Sync BI dashboard row for this PR now that processing is complete.
        # Runs on both first-time processing and re-processing after source changes.
        try:
            AttachmentBlobSync(self._config).sync_bi_dashboard_for_pr(purchase_req_no)
        except Exception as exc:
            # BI dashboard sync failure is non-fatal — log it but do not fail the stage.
            self._log.error(
                f"BI dashboard sync failed for PR={purchase_req_no!r} "
                f"(non-fatal, continuing): {exc}"
            )

    def _classify_single_file(
        self,
        file_path: Path,
        classify_file_fn,
    ) -> tuple[str, float]:
        """
        Classify a single file.  Returns (doc_type, confidence).
        On failure or unsupported type returns ("Others", 0.0).
        """
        ext = file_path.suffix.lower()
        if ext not in _SUPPORTED_EXTENSIONS:
            self._log.debug(
                f"Unsupported extension {ext!r} for {file_path.name!r} "
                f"— marking as Others"
            )
            return "Others", 0.0

        try:
            file_bytes = file_path.read_bytes()
            result     = classify_file_fn(file_bytes, file_path.name)

            raw_classification = result["classification"]
            confidence         = result["confidence"]

            # Map classifier values to DB-expected values
            doc_type = _CLASSIFICATION_MAP.get(
                raw_classification, raw_classification,
            )
            return doc_type, confidence

        except Exception as exc:
            self._log.opt(exception=True).warning(
                f"Classification failed for {file_path.name!r}: {exc}"
            )
            return "Others", 0.0
