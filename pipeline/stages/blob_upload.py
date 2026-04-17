"""
pipeline.stages.blob_upload
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Pipeline stage 3 — BLOB_UPLOAD.

DB stage reference  (pipeline_stages table)
-------------------------------------------
  STAGE_ID  : 3
  STAGE_NAME: BLOB_UPLOAD
  STAGE_DESC: File Uploading on blob
  DOMAIN    : ATTACHMENT
  SEQUENCE  : 3

Responsibility
--------------
Uploads the entire work/procurement/{safe_pr_no}/ folder to Azure Blob
Storage in one go — parent attachment files AND any embedded files
extracted by EMBED_DOC_EXTRACTION are included together.

Blob path mirrors the local path relative to work/:
    work/procurement/R_1_2020/452205/invoice.pdf
    → blob: procurement/R_1_2020/452205/invoice.pdf

    work/procurement/R_1_2020/452205/extracted/invoice__embed.pdf
    → blob: procurement/R_1_2020/452205/extracted/invoice__embed.pdf

On success, advances ras_tracker.current_stage_fk to 'BLOB_UPLOAD'.
"""

from __future__ import annotations

from utils.config import AppConfig
from attachment_blob_sync.sync import AttachmentBlobSync
from pipeline.stages.base import BaseStage
from pipeline.tracker import PipelineTracker


class BlobUploadStage(BaseStage):
    """
    ATTACHMENT domain — stage 3.

    Prerequisites : EMBED_DOC_EXTRACTION (stage 2) completed
                    (local work/ folder with all files must exist)
    Completion    : ras_tracker.current_stage_fk = 'BLOB_UPLOAD'
    Next stage    : CLASSIFICATION (stage 4)
    """

    NAME     = "BLOB_UPLOAD"
    STAGE_ID = 3

    def __init__(self, config: AppConfig) -> None:
        super().__init__()
        self._config  = config
        self._tracker = PipelineTracker(config.get_azure_conn_str())

    def execute(self, purchase_req_no: str) -> None:
        uploaded = AttachmentBlobSync(self._config).upload_work_folder_to_blob(
            purchase_req_no
        )
        self._log.info(
            f"Uploaded {uploaded} file(s) to blob for PR={purchase_req_no!r}"
        )
        self._tracker.advance_stage(purchase_req_no, self.NAME)
