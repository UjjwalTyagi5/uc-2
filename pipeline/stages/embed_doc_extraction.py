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
  1. Download binary attachments from the DB and save to work/:
         work/procurement/{safe_pr_no}/{att_id}/{filename}

  2. For each saved file, scan for embedded documents (Office / PDF) and
     extract them to a sibling extracted/ folder:
         work/procurement/{safe_pr_no}/{att_id}/extracted/{parent_stem}__{embedded}

  3. Record parent attachment and embedded file metadata in DB:
         AttachmentClassification          — one row per parent file
         EmbeddedAttachmentClassification  — one row per extracted file

On success, advances ras_tracker.current_stage_fk to 'EMBED_DOC_EXTRACTION'.
Next stage (BLOB_UPLOAD) uploads the complete work folder to Azure Blob.
"""

from __future__ import annotations

from pathlib import Path

from utils.config import AppConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from embed_doc_extraction.extractor import SUPPORTED_PARENTS, FileExtractor
from pipeline.attachment_classification_repository import (
    AttachmentClassificationRepository,
)
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker

class EmbedDocExtractionStage(BaseStage):
    """
    ATTACHMENT domain — stage 2.

    Prerequisites : INGESTION (stage 1) completed
    Completion    : ras_tracker.current_stage_fk = 'EMBED_DOC_EXTRACTION'
    Next stage    : BLOB_UPLOAD (stage 3) — uploads the work folder to blob
    """

    NAME     = "EMBED_DOC_EXTRACTION"
    STAGE_ID = 2

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._config    = config
        self._work_dir  = Path(config.WORK_DIR)
        self._tracker   = PipelineTracker(config.get_azure_conn_str())
        self._att_repo  = AttachmentClassificationRepository(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        safe_pr     = purchase_req_no.replace("/", "_")
        pr_work_dir = self._work_dir / "procurement" / safe_pr

        # ── Step 1: Download attachments + sync BI dashboard ─────────────────
        # Both reads happen here because we're already connected to on-prem.
        # BI dashboard data must be in Azure before METADATA_EXTRACTION reads it.
        blob_sync = AttachmentBlobSync(self._config)
        saved = blob_sync.save_locally(purchase_req_no)
        self._log.info(
            f"Saved {saved} attachment(s) to work/ for PR={purchase_req_no!r}"
        )

        try:
            blob_sync.sync_bi_dashboard_for_pr(purchase_req_no)
        except Exception as exc:
            # Non-fatal — log and continue; context_builder handles missing data gracefully
            self._log.error(
                f"BI dashboard sync failed for PR={purchase_req_no!r} "
                f"(non-fatal, continuing): {exc}"
            )

        if not pr_work_dir.exists() or saved == 0:
            self._log.warning(
                f"No local files for PR={purchase_req_no!r} — skipping embed extraction"
            )
            self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)
            return

        # ── Step 2: Extract embedded files + record in DB ─────────────────
        rass_uuid = self._att_repo.get_tracker_uuid(purchase_req_no)
        if rass_uuid is None:
            raise RuntimeError(
                f"No ras_tracker row for PR={purchase_req_no!r}. "
                f"INGESTION must complete before EMBED_DOC_EXTRACTION."
            )

        extractor       = FileExtractor()
        total_extracted = 0

        for att_dir in sorted(pr_work_dir.iterdir()):
            if not att_dir.is_dir() or att_dir.name == "extracted":
                continue

            att_id     = att_dir.name
            output_dir = att_dir / "extracted"
            output_dir.mkdir(exist_ok=True)

            for file_path in sorted(att_dir.iterdir()):
                if not file_path.is_file():
                    continue
                if file_path.suffix.lower() not in SUPPORTED_PARENTS:
                    continue

                extractor.parent_prefix = file_path.stem
                count_before = extractor.extracted_count
                extractor.process_file(str(file_path), str(output_dir))
                delta = extractor.extracted_count - count_before
                total_extracted += delta

                new_embedded = sorted(
                    f for f in output_dir.iterdir()
                    if f.is_file() and f.stem.startswith(extractor.parent_prefix)
                ) if output_dir.exists() else []

                parent_blob_path = f"procurement/{safe_pr}/{att_id}/{file_path.name}"

                try:
                    parent_pk = self._att_repo.upsert_parent(
                        purchase_req_no     = purchase_req_no,
                        ras_uuid_pk        = rass_uuid,
                        attachment_id       = att_id,
                        file_path           = parent_blob_path,
                        embedded_file_flag  = delta > 0,
                        embedded_file_count = delta,
                    )
                except Exception as exc:
                    self._log.opt(exception=True).error(
                        f"Failed to upsert AttachmentClassification "
                        f"att_id={att_id!r} file={file_path.name!r}: {exc}"
                    )
                    continue

                for emb_file in new_embedded:
                    try:
                        self._att_repo.upsert_embedded(
                            attachment_classify_uuid_pk = parent_pk,
                            parent_attachment_id        = att_id,
                            file_path = (
                                f"procurement/{safe_pr}/{att_id}/extracted/{emb_file.name}"
                            ),
                        )
                    except Exception as exc:
                        self._log.opt(exception=True).error(
                            f"Failed to upsert EmbeddedAttachmentClassification "
                            f"file={emb_file.name!r}: {exc}"
                        )

                if delta:
                    self._log.debug(
                        f"  {file_path.name}: {delta} embedded file(s) → "
                        f"{output_dir.relative_to(self._work_dir)}"
                    )

        self._log.info(
            f"Embed extraction complete: {total_extracted} embedded file(s) "
            f"for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.STAGE_ID)
