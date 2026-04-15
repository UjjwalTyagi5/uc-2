"""
pipeline.stages.embed_doc_extraction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 2 — EMBED_DOC_EXTRACTION.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 2
  STAGE_NAME: EMBED_DOC_EXTRACTION
  STAGE_DESC: Document extraction embedded
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 2

Responsibility
--------------
For each attachment already saved to the local work/ directory by
BLOB_UPLOAD, scan the file for embedded documents (Office or PDF) and
extract them to a sibling extracted/ sub-folder.

After extraction, records are upserted into:
  AttachmentClassification          — one row per parent attachment file
  EmbeddedAttachmentClassification  — one row per embedded file found

Local path structure
--------------------
    work/procurement/{safe_pr_no}/{att_id}/{parent_file}
                                           └─ extracted/
                                                  └─ {parent_file}__{embedded}

DB path (stored in file_path columns) mirrors blob path:
    procurement/{safe_pr_no}/{att_id}/{parent_file}
    procurement/{safe_pr_no}/{att_id}/extracted/{embedded_file}

NOTE: This stage runs AFTER BLOB_UPLOAD in the execution list (even though
its STAGE_ID is lower) because it needs the local files that BLOB_UPLOAD
saves to work/.  The execution order in _build_default_stages() controls
which stage runs first — STAGE_ID is for DB identification only.
"""

from __future__ import annotations

from pathlib import Path

from attachment_blob_sync.config import BlobSyncConfig
from embed_doc_extraction.extractor import SUPPORTED_PARENTS, FileExtractor
from pipeline.attachment_classification_repository import (
    AttachmentClassificationRepository,
)
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker

_WORK_DIR = Path("work")


class EmbedDocExtractionStage(BaseStage):
    """
    ATTACHMENT domain — stage 2 (runs after BLOB_UPLOAD in execution order).

    Prerequisites : BLOB_UPLOAD completed (local work/ files must exist)
    Completion    : ras_tracker.current_stage_fk = 'EMBED_DOC_EXTRACTION'
    Next stage    : CLASSIFICATION (stage 4)
    """

    NAME     = "EMBED_DOC_EXTRACTION"
    STAGE_ID = 2

    def __init__(self, config: BlobSyncConfig) -> None:
        super().__init__()
        self._tracker    = PipelineTracker(config.get_azure_conn_str())
        self._att_repo   = AttachmentClassificationRepository(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        safe_pr      = purchase_req_no.replace("/", "_")
        pr_work_dir  = _WORK_DIR / "procurement" / safe_pr

        if not pr_work_dir.exists():
            self._log.warning(
                f"No local files for PR={purchase_req_no!r} at {pr_work_dir} "
                f"— skipping embed extraction"
            )
            self._tracker.advance_stage(purchase_req_no, self.NAME)
            return

        # Fetch rass_uuid_pk once — needed as FK for AttachmentClassification rows
        rass_uuid = self._att_repo.get_tracker_uuid(purchase_req_no)
        if rass_uuid is None:
            raise RuntimeError(
                f"No ras_tracker row for PR={purchase_req_no!r}. "
                f"INGESTION must complete before EMBED_DOC_EXTRACTION."
            )

        extractor       = FileExtractor()
        total_extracted = 0

        # Each sub-folder under pr_work_dir is one attachment ({att_id}/).
        for att_dir in sorted(pr_work_dir.iterdir()):
            if not att_dir.is_dir() or att_dir.name == "extracted":
                continue

            att_id     = att_dir.name                          # e.g. "452205"
            output_dir = att_dir / "extracted"
            output_dir.mkdir(exist_ok=True)

            for file_path in sorted(att_dir.iterdir()):
                if not file_path.is_file():
                    continue
                if file_path.suffix.lower() not in SUPPORTED_PARENTS:
                    continue

                # ── Extract embedded files ─────────────────────────────────
                extractor.parent_prefix = file_path.stem
                count_before = extractor.extracted_count
                extractor.process_file(str(file_path), str(output_dir))
                embedded_files = sorted(output_dir.iterdir()) if output_dir.exists() else []
                # Only files added in THIS call (by matching prefix)
                new_embedded = [
                    f for f in embedded_files
                    if f.is_file() and f.stem.startswith(extractor.parent_prefix)
                ]
                delta = extractor.extracted_count - count_before
                total_extracted += delta

                # ── Blob-style paths for DB storage ───────────────────────
                parent_blob_path = (
                    f"procurement/{safe_pr}/{att_id}/{file_path.name}"
                )

                # ── Upsert parent into AttachmentClassification ───────────
                try:
                    parent_pk = self._att_repo.upsert_parent(
                        purchase_req_no  = purchase_req_no,
                        rass_uuid_pk     = rass_uuid,
                        attachment_id    = att_id,
                        file_path        = parent_blob_path,
                        embedded_file_flag  = delta > 0,
                        embedded_file_count = delta,
                    )
                except Exception as exc:
                    self._log.opt(exception=True).error(
                        f"Failed to upsert AttachmentClassification for "
                        f"att_id={att_id!r} file={file_path.name!r}: {exc}"
                    )
                    continue  # skip embedded upserts for this file but continue the loop

                # ── Upsert each embedded file ──────────────────────────────
                for emb_file in new_embedded:
                    embedded_blob_path = (
                        f"procurement/{safe_pr}/{att_id}/extracted/{emb_file.name}"
                    )
                    try:
                        self._att_repo.upsert_embedded(
                            attachment_classify_uuid_pk = parent_pk,
                            parent_attachment_id        = att_id,
                            file_path                   = embedded_blob_path,
                        )
                    except Exception as exc:
                        self._log.opt(exception=True).error(
                            f"Failed to upsert EmbeddedAttachmentClassification "
                            f"for file={emb_file.name!r}: {exc}"
                        )

                if delta:
                    self._log.debug(
                        f"  {file_path.name}: {delta} embedded file(s) → "
                        f"{output_dir.relative_to(_WORK_DIR)}"
                    )

        self._log.info(
            f"Embed extraction complete: {total_extracted} embedded file(s) "
            f"for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)
