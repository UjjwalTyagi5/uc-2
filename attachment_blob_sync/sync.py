"""
attachment_blob_sync.sync
~~~~~~~~~~~~~~~~~~~~~~~~~
Core logic for downloading attachments and uploading them to Azure Blob.

Public API (used by pipeline stages)
--------------------------------------
save_locally(pr_no)
    Downloads all binary attachments for the PR from the on-prem DB and
    saves them to work/procurement/{safe_pr}/{att_id}/{filename}.
    Does NOT upload to blob, does NOT touch ras_tracker.
    Called by BlobUploadStage.

upload_work_folder_to_blob(pr_no)
    Walks work/procurement/{safe_pr}/ recursively and uploads every file
    to Azure Blob at the same relative path (blob path mirrors local path
    under work/).  Includes parent files AND any extracted sub-files added
    by EmbedDocExtractionStage.
    Does NOT touch ras_tracker.
    Called by EmbedDocExtractionStage after extraction is complete.

run(pr_no)                           [CLI / standalone use]
    Convenience method that calls save_locally() + upload_work_folder_to_blob()
    + updates ras_tracker + syncs BI dashboard.
    Used by `python -m attachment_blob_sync --pr-no <PR>`.
"""

from pathlib import Path

import pyodbc
from loguru import logger
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

from attachment_blob_sync.config import BlobSyncConfig

_MAX_RETRIES = 3
_WORK_DIR    = Path("work")


class AttachmentBlobSync:
    def __init__(self, config: BlobSyncConfig):
        self._config = config

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def save_locally(self, purchase_req_no: str) -> int:
        """
        Downloads all attachments for the PR from the DB and saves them to:
            work/procurement/{safe_pr}/{att_id}/{filename}

        Returns the number of files saved.
        Does NOT upload to blob and does NOT update ras_tracker.

        Raises on any unrecoverable DB error.
        """
        logger.info(f"Saving attachments locally for PR: {purchase_req_no}")
        primary_conn = pyodbc.connect(self._config.get_azure_conn_str(), autocommit=False, timeout=0)
        ras_conn     = pyodbc.connect(self._config.get_ras_conn_str(),     autocommit=False, timeout=0)
        try:
            attachments = self._fetch_attachments(primary_conn, purchase_req_no)
            if not attachments:
                logger.warning(f"No attachments found for PR: {purchase_req_no}")
                return 0

            logger.info(f"Found {len(attachments)} attachment(s) for PR: {purchase_req_no}")
            attachment_ids = [att_id for att_id, _ in attachments]
            binary_docs    = self._fetch_binary_docs(ras_conn, attachment_ids)

            if not binary_docs:
                logger.warning(
                    f"No binary data found in ras_attachments for PR: {purchase_req_no} "
                    f"(attachment_ids: {attachment_ids})"
                )
                return 0

            saved = 0
            for att_id, files_name in attachments:
                doc_bytes = binary_docs.get(att_id)
                if doc_bytes is None:
                    logger.warning(
                        f"Skipping attachment_id={att_id} ({files_name}) — no binary doc found"
                    )
                    continue
                self._save_local(purchase_req_no, att_id, files_name, doc_bytes)
                saved += 1

            logger.info(f"Saved {saved} file(s) locally for PR: {purchase_req_no}")
            return saved

        finally:
            primary_conn.close()
            ras_conn.close()

    def upload_work_folder_to_blob(self, purchase_req_no: str) -> int:
        """
        Uploads the entire work/procurement/{safe_pr}/ folder to Azure Blob,
        preserving directory structure.

        Blob path = file path relative to work/:
            local:  work/procurement/R_134984_2020/452205/invoice.pdf
            blob:   procurement/R_134984_2020/452205/invoice.pdf

            local:  work/procurement/R_134984_2020/452205/extracted/doc__embed.pdf
            blob:   procurement/R_134984_2020/452205/extracted/doc__embed.pdf

        Returns the number of files uploaded.
        Does NOT update ras_tracker.

        Raises RuntimeError if the local folder doesn't exist or any upload fails.
        """
        safe_pr      = self._sanitize_pr_no(purchase_req_no)
        local_folder = _WORK_DIR / "procurement" / safe_pr

        if not local_folder.exists():
            raise RuntimeError(
                f"Local work folder not found: {local_folder} — "
                f"ensure save_locally() ran before upload_work_folder_to_blob()."
            )

        container_client = (
            BlobServiceClient(
                account_url=self._config.BLOB_ACCOUNT_URL,
                credential=self._config.BLOB_ACCOUNT_KEY,
            )
            .get_container_client(self._config.BLOB_CONTAINER_NAME)
        )

        all_files = sorted(f for f in local_folder.rglob("*") if f.is_file())
        if not all_files:
            logger.warning(f"No files found in {local_folder} to upload")
            return 0

        logger.info(
            f"Uploading {len(all_files)} file(s) from {local_folder} to blob "
            f"for PR: {purchase_req_no}"
        )

        success_count = 0
        fail_count    = 0
        for file_path in all_files:
            # blob path mirrors the local path relative to work/
            blob_path = file_path.relative_to(_WORK_DIR).as_posix()
            data      = file_path.read_bytes()
            if self._upload_blob(container_client, blob_path, data):
                success_count += 1
            else:
                fail_count += 1

        logger.info(
            f"PR {purchase_req_no}: {success_count} uploaded, {fail_count} failed"
        )

        if fail_count > 0:
            raise RuntimeError(
                f"{fail_count} file(s) failed to upload for PR {purchase_req_no}"
            )

        return success_count

    def run(self, purchase_req_no: str) -> None:
        """
        Standalone / CLI entry point — runs the full attachment pipeline:
          1. Save all binary attachments locally (DB → work/)
          2. Upload entire work folder to blob (work/ → Azure Blob)
          3. Update ras_tracker.current_stage_fk = 'BLOB_UPLOAD'
          4. Sync BI dashboard view

        Pipeline stages call save_locally() and upload_work_folder_to_blob()
        individually so they can manage their own tracker updates.
        """
        logger.info(f"=== attachment_blob_sync started for PR: {purchase_req_no} ===")

        primary_conn = pyodbc.connect(self._config.get_azure_conn_str(), autocommit=False, timeout=0)
        ras_conn     = pyodbc.connect(self._config.get_ras_conn_str(),     autocommit=False, timeout=0)

        run_error: Exception | None = None
        try:
            try:
                saved = self.save_locally(purchase_req_no)
                if saved > 0:
                    self.upload_work_folder_to_blob(purchase_req_no)
                    self._upsert_tracker(primary_conn, purchase_req_no)
                    primary_conn.commit()
                    logger.info(
                        f"Tracker updated: current_stage_fk = 'BLOB_UPLOAD' "
                        f"for PR {purchase_req_no}"
                    )
            except Exception as exc:
                run_error = exc
                logger.opt(exception=True).error(
                    f"Attachment sync failed for PR {purchase_req_no}: {exc}"
                )

            # BI dashboard sync always runs regardless of upload result
            try:
                self._sync_bi_dashboard(ras_conn, primary_conn, purchase_req_no)
            except Exception as exc:
                logger.error(f"BI dashboard sync failed for PR {purchase_req_no}: {exc}")

            if run_error is not None:
                raise run_error

        finally:
            primary_conn.close()
            ras_conn.close()
            logger.info(f"=== attachment_blob_sync finished for PR: {purchase_req_no} ===")

    # ──────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────

    def _fetch_attachments(self, conn: pyodbc.Connection, purchase_req_no: str) -> list:
        sql = """
            SELECT pa.[ATTACHMENT_ID], pa.[FILES_NAME]
            FROM [ras_procurement].[purchase_req_mst]  prm
            JOIN [ras_procurement].[purchase_attachments] pa
              ON prm.[PURCHASE_REQ_ID] = pa.[PURCHASE_ID]
            WHERE prm.[PURCHASE_REQ_NO] = ?
              AND pa.[ATTACHMENT_ID] IS NOT NULL
              AND pa.[FILES_NAME]    IS NOT NULL
        """
        cursor = conn.cursor()
        cursor.execute(sql, purchase_req_no)
        rows = cursor.fetchall()
        cursor.close()
        return [(int(row[0]), str(row[1])) for row in rows]

    def _fetch_binary_docs(self, conn: pyodbc.Connection, attachment_ids: list) -> dict:
        if not attachment_ids:
            return {}
        placeholders = ", ".join("?" * len(attachment_ids))
        sql = f"""
            SELECT [attachment_id], [doc]
            FROM [ras_attachments]
            WHERE [attachment_id] IN ({placeholders})
              AND [doc] IS NOT NULL
        """
        cursor = conn.cursor()
        cursor.execute(sql, attachment_ids)
        rows = cursor.fetchall()
        cursor.close()
        return {int(row[0]): bytes(row[1]) for row in rows}

    @staticmethod
    def _sanitize_pr_no(pr_no: str) -> str:
        """Replace '/' in PURCHASE_REQ_NO to avoid unintended blob sub-directories."""
        return pr_no.replace("/", "_")

    def _upload_blob(self, container_client, blob_path: str, data: bytes) -> bool:
        """
        Uploads data to blob_path with up to _MAX_RETRIES attempts on AzureError.
        Returns True on success, False after all retries are exhausted.
        """
        blob_client = container_client.get_blob_client(blob_path)
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                blob_client.upload_blob(data, overwrite=True)
                logger.debug(f"Uploaded: {blob_path}")
                return True
            except AzureError as exc:
                if attempt < _MAX_RETRIES:
                    logger.warning(
                        f"Upload attempt {attempt}/{_MAX_RETRIES} failed for {blob_path}: {exc}"
                    )
                else:
                    logger.error(
                        f"Upload failed after {_MAX_RETRIES} attempts for {blob_path}: {exc}"
                    )
        return False

    def _save_local(self, pr_no: str, att_id: int, files_name: str, data: bytes) -> None:
        """
        Saves binary data to:
            work/procurement/{sanitized_pr_no}/{att_id}/{files_name}
        """
        safe_pr    = self._sanitize_pr_no(pr_no)
        local_path = _WORK_DIR / "procurement" / safe_pr / str(att_id) / files_name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        logger.debug(f"Saved locally: {local_path}")

    def _sync_bi_dashboard(
        self,
        onprem_conn: pyodbc.Connection,
        azure_conn: pyodbc.Connection,
        purchase_req_no: str,
    ) -> None:
        src_cursor = onprem_conn.cursor()
        src_cursor.execute(
            "SELECT * FROM [dbo].[vw_get_ras_data_for_bidashboard] WHERE [PURCHASE_REQ_NO] = ?",
            purchase_req_no,
        )
        rows    = src_cursor.fetchall()
        columns = [col[0] for col in src_cursor.description]
        src_cursor.close()

        az_cursor = azure_conn.cursor()
        az_cursor.execute(
            "DELETE FROM [ras_procurement].[vw_get_ras_data_for_bidashboard] WHERE [PURCHASE_REQ_NO] = ?",
            purchase_req_no,
        )
        deleted = az_cursor.rowcount

        if rows:
            col_list     = ", ".join(f"[{c}]" for c in columns)
            placeholders = ", ".join("?" * len(columns))
            insert_sql   = (
                f"INSERT INTO [ras_procurement].[vw_get_ras_data_for_bidashboard] "
                f"({col_list}) VALUES ({placeholders})"
            )
            az_cursor.fast_executemany = True
            az_cursor.executemany(insert_sql, [list(row) for row in rows])

        azure_conn.commit()
        az_cursor.close()
        logger.info(
            f"BI dashboard sync for PR {purchase_req_no}: "
            f"deleted {deleted} old row(s), inserted {len(rows)} row(s)"
        )

    def _upsert_tracker(self, conn: pyodbc.Connection, purchase_req_no: str) -> None:
        sql = """
            MERGE [ras_procurement].[ras_tracker] WITH (HOLDLOCK) AS target
            USING (
                SELECT
                    [PURCHASE_REQ_NO]            AS purchase_req_no_fk,
                    [JUSTIFICATION]              AS ras_justification,
                    [CURRENCY]                   AS currency,
                    [C_DATETIME]                 AS ras_created_at,
                    [U_DATETIME]                 AS ras_updated_at,
                    [PURCHASEFINALAPPROVALSTATUS] AS ras_status
                FROM [ras_procurement].[purchase_req_mst]
                WHERE [PURCHASE_REQ_NO] = ?
            ) AS src
              ON target.[purchase_req_no_fk] = src.[purchase_req_no_fk]
            WHEN MATCHED THEN
                UPDATE SET
                    [current_stage_fk] = 'BLOB_UPLOAD',
                    [updated_at]       = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (
                    [purchase_req_no_fk], [ras_justification], [currency],
                    [ras_created_at], [ras_updated_at], [ras_status], [current_stage_fk]
                )
                VALUES (
                    src.[purchase_req_no_fk], src.[ras_justification], src.[currency],
                    src.[ras_created_at], src.[ras_updated_at], src.[ras_status], 'BLOB_UPLOAD'
                );
        """
        cursor = conn.cursor()
        cursor.execute(sql, purchase_req_no)
        cursor.close()
