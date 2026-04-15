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
extract them to a sibling extracted/ sub-folder:

    work/procurement/{pr_no}/{att_id}/{parent_file}
                                      └─ extracted/
                                             └─ {parent_file}__{embedded_file}

NOTE: This stage runs AFTER BLOB_UPLOAD in the execution list (even though
its STAGE_ID is lower) because it needs the local files that BLOB_UPLOAD
saves to work/.  The execution order in _build_default_stages() controls
which stage runs first — STAGE_ID is for DB identification only.
"""

from __future__ import annotations

from pathlib import Path

from attachment_blob_sync.config import BlobSyncConfig
from embed_doc_extraction.extractor import SUPPORTED_PARENTS, FileExtractor
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
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        safe_pr    = purchase_req_no.replace("/", "_")
        pr_work_dir = _WORK_DIR / "procurement" / safe_pr

        if not pr_work_dir.exists():
            self._log.warning(
                f"No local files for PR={purchase_req_no!r} at {pr_work_dir} "
                f"— skipping embed extraction"
            )
            self._tracker.advance_stage(purchase_req_no, self.NAME)
            return

        extractor       = FileExtractor()
        total_extracted = 0

        # Each sub-folder under pr_work_dir is one attachment ({att_id}/).
        # Process every supported parent file inside it and extract
        # embedded docs to {att_id}/extracted/.
        for att_dir in sorted(pr_work_dir.iterdir()):
            if not att_dir.is_dir() or att_dir.name == "extracted":
                continue

            output_dir = att_dir / "extracted"
            output_dir.mkdir(exist_ok=True)

            for file_path in sorted(att_dir.iterdir()):
                if not file_path.is_file():
                    continue
                if not file_path.suffix.lower() in SUPPORTED_PARENTS:
                    continue

                extractor.parent_prefix = file_path.stem
                count_before            = extractor.extracted_count
                extractor.process_file(str(file_path), str(output_dir))
                delta = extractor.extracted_count - count_before

                if delta:
                    self._log.debug(
                        f"  {file_path.name}: {delta} embedded file(s) → "
                        f"{output_dir.relative_to(_WORK_DIR)}"
                    )

            total_extracted += extractor.extracted_count

        self._log.info(
            f"Embed extraction: {total_extracted} file(s) extracted "
            f"for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)
