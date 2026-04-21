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

from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Image extensions eligible for the pre-LLM "trivial image" heuristic.
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".gif"}


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

        # ── Collect every file in this PR into a flat task list ──────────────
        # Tasks are tagged "parent" or "embedded" so the worker knows which
        # repository method to call and how to build the blob_path.
        file_tasks: list[dict] = []

        for att_dir in sorted(pr_work_dir.iterdir()):
            if not att_dir.is_dir():
                continue
            att_id = att_dir.name

            for file_path in sorted(att_dir.iterdir()):
                if file_path.is_file():
                    file_tasks.append({
                        "kind":       "parent",
                        "file_path":  file_path,
                        "att_id":     att_id,
                    })

            extracted_dir = att_dir / "extracted"
            if not extracted_dir.exists() or not extracted_dir.is_dir():
                continue

            # Resolve parent_pk once per attachment (outside the worker pool)
            # rather than re-querying for every embedded file.
            parent_pk = self._att_repo.get_parent_pk(att_id)
            if parent_pk is None:
                self._log.warning(
                    f"No parent PK for att_id={att_id!r} — "
                    f"skipping embedded classification"
                )
                continue

            for emb_file in sorted(extracted_dir.iterdir()):
                if emb_file.is_file():
                    file_tasks.append({
                        "kind":       "embedded",
                        "file_path":  emb_file,
                        "att_id":     att_id,
                        "parent_pk":  parent_pk,
                        "blob_path":  f"procurement/{safe_pr}/{att_id}/extracted/{emb_file.name}",
                    })

        if not file_tasks:
            self._log.info(
                f"No files to classify for PR={purchase_req_no!r}"
            )
            self._finish(purchase_req_no)
            return

        # ── Classify files in parallel ───────────────────────────────────────
        # LLM calls dominate wall-clock; threads let multiple requests stay
        # in-flight against Azure OpenAI at once.
        max_workers = max(
            1, min(self._config.CLASSIFICATION_WORKERS, len(file_tasks))
        )
        classified_count = 0
        error_count      = 0

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"classify-{safe_pr}",
        ) as pool:
            futures = {
                pool.submit(self._process_file_task, task, classify_file): task
                for task in file_tasks
            }
            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                    classified_count += 1
                except Exception as exc:
                    self._log.opt(exception=True).error(
                        f"Classification task failed for "
                        f"{task['file_path'].name!r}: {exc}"
                    )
                    error_count += 1

        self._log.info(
            f"Classification complete: {classified_count} file(s) classified, "
            f"{error_count} error(s) for PR={purchase_req_no!r} "
            f"(workers={max_workers})"
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

    def _process_file_task(self, task: dict, classify_file_fn) -> None:
        """
        Worker body for the per-PR ThreadPoolExecutor: classify one file and
        write the result to the appropriate table. Raises on DB failure so the
        main loop can count it as an error.
        """
        file_path = task["file_path"]

        # Short-circuit trivial images (logos, signatures, watermarks,
        # thumbnails) before paying for an LLM call.
        trivial, reason = self._is_trivial_image(file_path)
        if trivial:
            self._log.info(
                f"Skipped trivial image {file_path.name!r} ({reason}) — "
                f"marking as Others"
            )
            doc_type, confidence = "Others", 0.0
        else:
            doc_type, confidence = self._classify_single_file(file_path, classify_file_fn)

        if task["kind"] == "parent":
            self._att_repo.update_parent_classification(
                attachment_id       = task["att_id"],
                doc_type            = doc_type,
                classification_conf = confidence,
            )
            self._log.info(
                f"Classified parent {file_path.name!r}: "
                f"{doc_type} (conf={confidence:.2f})"
            )
        else:
            self._att_repo.update_embedded_classification(
                attachment_classification_id = task["parent_pk"],
                file_path                    = task["blob_path"],
                doc_type                     = doc_type,
                classification_conf          = confidence,
            )
            self._log.info(
                f"Classified embedded {file_path.name!r}: "
                f"{doc_type} (conf={confidence:.2f})"
            )

    def _is_trivial_image(self, file_path: Path) -> tuple[bool, str]:
        """
        Heuristic pre-filter that returns (True, reason) if an image is almost
        certainly a logo, signature, watermark, icon, or thumbnail — i.e. a
        file the LLM would classify as "Others" anyway. Returns (False, "")
        for non-images and for images that look like real document scans.

        Signals used (all derived from pixel data, not file size):
          • Long-edge < 200px               → icon/logo
          • Aspect ratio > 5:1              → header strip / divider / barcode
          • Long < 400 and short < 200      → sub-thumbnail
          • > 97% near-white pixels         → signature / watermark / stamp
          • < 16 unique colours             → flat logo artwork
        """
        if file_path.suffix.lower() not in _IMAGE_EXTENSIONS:
            return False, ""

        try:
            from PIL import Image
        except ImportError:
            return False, ""

        try:
            with Image.open(file_path) as img:
                img.load()
                w, h = img.size
                long_edge  = max(w, h)
                short_edge = max(min(w, h), 1)

                if long_edge < 200:
                    return True, f"tiny {w}x{h}"

                if long_edge / short_edge > 5:
                    return True, f"extreme aspect {w}x{h}"

                if long_edge < 400 and short_edge < 200:
                    return True, f"sub-thumbnail {w}x{h}"

                gray = img.convert("L")
                hist = gray.histogram()
                total = sum(hist)
                if total:
                    near_white_ratio = sum(hist[230:]) / total
                    if near_white_ratio > 0.97:
                        return True, f"mostly white ({near_white_ratio:.0%})"

                try:
                    colors = img.convert("RGB").getcolors(maxcolors=256)
                    if colors is not None and len(colors) < 16:
                        return True, f"flat palette ({len(colors)} colors)"
                except Exception:
                    pass

        except Exception as exc:
            # If we can't even decode it, the LLM won't do better — skip.
            return True, f"unreadable ({exc.__class__.__name__})"

        return False, ""

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
