from pathlib import Path

import pyodbc
from loguru import logger
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError

from attachment_blob_sync.config import BlobSyncConfig

_MAX_RETRIES = 3
_WORK_DIR    = Path("work")   # local mirror root — relative to cwd


class AttachmentBlobSync:
    def __init__(self, config: BlobSyncConfig):
        self._config = config

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, purchase_req_no: str) -> None:
        logger.info(f"=== attachment_blob_sync started for PR: {purchase_req_no} ===")

        primary_conn = pyodbc.connect(self._config.get_azure_conn_str(), autocommit=False, timeout=0)
        ras_conn     = pyodbc.connect(self._config.get_ras_conn_str(),     autocommit=False, timeout=0)
        container_client = (
            BlobServiceClient(
                account_url=self._config.BLOB_ACCOUNT_URL,
                credential=self._config.BLOB_ACCOUNT_KEY,
            )
            .get_container_client(self._config.BLOB_CONTAINER_NAME)
        )

        blob_upload_error: Exception | None = None
        try:
            # ── Step 1: Blob upload ────────────────────────────────────────
            try:
                attachments = self._fetch_attachments(primary_conn, purchase_req_no)
                if not attachments:
                    logger.warning(f"No attachments found for PR: {purchase_req_no}")
                else:
                    logger.info(f"Found {len(attachments)} attachment(s) for PR: {purchase_req_no}")

                    attachment_ids = [att_id for att_id, _ in attachments]
                    binary_docs = self._fetch_binary_docs(ras_conn, attachment_ids)

                    if not binary_docs:
                        logger.warning(
                            f"No binary data found in ras_attachments for PR: {purchase_req_no} "
                            f"(attachment_ids: {attachment_ids})"
                        )
                    else:
                        success_count = 0
                        fail_count    = 0

                        for att_id, files_name in attachments:
                            doc_bytes = binary_docs.get(att_id)
                            if doc_bytes is None:
                                logger.warning(
                                    f"Skipping attachment_id={att_id} ({files_name}) — no binary doc found"
                                )
                                continue

                            uploaded = self._upload(container_client, purchase_req_no, att_id, files_name, doc_bytes)
                            if uploaded:
                                success_count += 1
                                self._save_local(purchase_req_no, att_id, files_name, doc_bytes)
                            else:
                                fail_count += 1

                        logger.info(
                            f"PR {purchase_req_no}: {success_count} uploaded, "
                            f"{fail_count} failed, "
                            f"{len(attachments) - success_count - fail_count} skipped (no doc)"
                        )

                        if fail_count == 0:
                            self._upsert_tracker(primary_conn, purchase_req_no)
                            primary_conn.commit()
                            logger.info(
                                f"Tracker updated: current_stage_fk = 'BLOB_UPLOAD' "
                                f"for PR {purchase_req_no}"
                            )
                        else:
                            logger.error(
                                f"PR {purchase_req_no}: {fail_count} upload(s) failed — tracker NOT updated"
                            )
            except Exception as exc:
                blob_upload_error = exc
                logger.opt(exception=True).error(
                    f"Blob upload step failed for PR {purchase_req_no}: {exc}"
                )

            # ── Step 2: BI dashboard sync ──────────────────────────────────
            # Runs unconditionally — blob upload failure does not skip this step.
            try:
                self._sync_bi_dashboard(ras_conn, primary_conn, purchase_req_no)
            except Exception as exc:
                logger.error(f"BI dashboard sync failed for PR {purchase_req_no}: {exc}")

            # Re-raise blob upload error after BI sync so the pipeline stage is
            # still marked as FAILED in the orchestrator if Step 1 had an error.
            if blob_upload_error is not None:
                raise blob_upload_error

        finally:
            primary_conn.close()
            ras_conn.close()
            logger.info(f"=== attachment_blob_sync finished for PR: {purchase_req_no} ===")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_attachments(self, conn: pyodbc.Connection, purchase_req_no: str) -> list:
        """
        Returns list of (ATTACHMENT_ID, FILES_NAME) for the given PURCHASE_REQ_NO.
        Joins purchase_req_mst → purchase_attachments.
        """
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
        """
        Returns dict {attachment_id: bytes} for the given list of IDs.
        Queries ras_attachments on the RAS DB.
        Only rows where doc IS NOT NULL are returned.
        """
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
        """
        PURCHASE_REQ_NO values like 'R_3451/2026' contain '/' which would create
        unintended sub-directories in the blob path. Replace with '_'.
        """
        return pr_no.replace("/", "_")

    def _upload(
        self,
        container_client,
        pr_no: str,
        att_id: int,
        files_name: str,
        data: bytes,
    ) -> bool:
        """
        Uploads binary data to Azure Blob at:
            procurement/{sanitized_pr_no}/{att_id}/{files_name}

        Retries up to _MAX_RETRIES times on AzureError.
        Returns True on success, False after all retries are exhausted.
        """
        safe_pr = self._sanitize_pr_no(pr_no)
        blob_path = f"procurement/{safe_pr}/{att_id}/{files_name}"
        blob_client = container_client.get_blob_client(blob_path)

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                blob_client.upload_blob(data, overwrite=True)
                logger.info(f"Uploaded: {blob_path}")
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

    def _save_local(
        self,
        pr_no: str,
        att_id: int,
        files_name: str,
        data: bytes,
    ) -> None:
        """
        Mirrors the blob path under the local work/ directory:
            work/procurement/{sanitized_pr_no}/{att_id}/{files_name}
        Creates parent directories as needed.  Overwrites if the file
        already exists (idempotent, same as blob overwrite=True).
        """
        safe_pr   = self._sanitize_pr_no(pr_no)
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
        """
        Syncs BI dashboard data for the given PR from the on-prem view into the Azure table.
        Steps:
          1. SELECT all rows from on-prem [dbo].[vw_get_ras_data_for_bidashboard]
             where PURCHASE_REQ_NO = ?
          2. DELETE existing rows from Azure [ras_procurement].[vw_get_ras_data_for_bidashboard]
             for this PR (prevents duplicates on re-runs)
          3. INSERT fetched rows into Azure table
        """
        # Fetch from on-prem view
        src_cursor = onprem_conn.cursor()
        src_cursor.execute(
            "SELECT * FROM [dbo].[vw_get_ras_data_for_bidashboard] WHERE [PURCHASE_REQ_NO] = ?",
            purchase_req_no,
        )
        rows = src_cursor.fetchall()
        columns = [col[0] for col in src_cursor.description]
        src_cursor.close()

        # Delete + insert on Azure in a single transaction
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
        """
        UPSERT ras_tracker for the given PR.
        - If row exists: update current_stage_fk = 'BLOB_UPLOAD'.
        - If row is new: insert with data pulled directly from purchase_req_mst
          (justification, currency, c_datetime, u_datetime, purchasefinalapprovalstatus).
        """
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
                    [purchase_req_no_fk],
                    [ras_justification],
                    [currency],
                    [ras_created_at],
                    [ras_updated_at],
                    [ras_status],
                    [current_stage_fk]
                )
                VALUES (
                    src.[purchase_req_no_fk],
                    src.[ras_justification],
                    src.[currency],
                    src.[ras_created_at],
                    src.[ras_updated_at],
                    src.[ras_status],
                    'BLOB_UPLOAD'
                );
        """
        cursor = conn.cursor()
        cursor.execute(sql, purchase_req_no)
        cursor.close()
