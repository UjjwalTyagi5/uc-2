"""
attachment_blob_sync.sync
~~~~~~~~~~~~~~~~~~~~~~~~~
Downloads attachments from the on-prem DB, saves locally, uploads to
Azure Blob, and keeps the BI dashboard row current.

Public API
----------
save_locally(pr_no)
    Downloads binary attachments from DB → work/procurement/{safe_pr}/{att_id}/{filename}.
    Does NOT upload to blob. Called by EmbedDocExtractionStage.

upload_work_folder_to_blob(pr_no)
    Uploads the entire work/procurement/{safe_pr}/ folder to Azure Blob,
    preserving directory structure. Called by BlobUploadStage.

sync_bi_dashboard_for_pr(pr_no)
    Refreshes the BI dashboard row for this PR (DELETE + re-INSERT).
    Called by ClassificationStage after every successful pipeline run.

run(pr_no)
    Convenience for CLI / standalone use: save_locally + upload + tracker update.
"""

from __future__ import annotations

from pathlib import Path

from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient

from db.crud import BaseRepository
from db.tables import AzureTables, OnPremTables
from utils.config import ALLOWED_ATTACHMENT_EXTENSIONS, AppConfig



# ── SQL (module-level, like reference pattern) ─────────────────────────────

_FETCH_ATTACHMENTS_SQL = f"""
    SELECT pa.[ATTACHMENT_ID], pa.[FILES_NAME]
    FROM   {AzureTables.PURCHASE_REQ_MST}  prm
    JOIN   {AzureTables.PURCHASE_ATTACHMENTS} pa
      ON   prm.[PURCHASE_REQ_ID] = pa.[PURCHASE_ID]
    WHERE  prm.[PURCHASE_REQ_NO] = ?
      AND  pa.[ATTACHMENT_ID]    IS NOT NULL
      AND  pa.[FILES_NAME]       IS NOT NULL
"""

_FETCH_BINARY_DOCS_SQL = f"""
    SELECT [attachment_id], [doc]
    FROM   {OnPremTables.RAS_ATTACHMENTS}
    WHERE  [attachment_id] IN ({{placeholders}})
      AND  [doc] IS NOT NULL
"""

_FETCH_BI_DASHBOARD_SQL = f"""
    SELECT * FROM {OnPremTables.BI_DASHBOARD}
    WHERE  [PURCHASE_REQ_NO] = ?
"""

_DELETE_BI_DASHBOARD_SQL = f"""
    DELETE FROM {AzureTables.BI_DASHBOARD}
    WHERE  [PURCHASE_REQ_NO] = ?
"""

_UPSERT_TRACKER_SQL = f"""
    MERGE {AzureTables.RAS_TRACKER} WITH (HOLDLOCK) AS target
    USING (
        SELECT
            [PURCHASE_REQ_NO]            AS purchase_req_no,
            [PURCHASEFINALAPPROVALSTATUS] AS ras_status
        FROM {AzureTables.PURCHASE_REQ_MST}
        WHERE [PURCHASE_REQ_NO] = ?
    ) AS src
      ON target.[purchase_req_no] = src.[purchase_req_no]
    WHEN MATCHED THEN
        UPDATE SET
            [current_stage_fk] = 3,
            [updated_at]       = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (
            [purchase_req_no], [ras_status], [current_stage_fk]
        )
        VALUES (
            src.[purchase_req_no], src.[ras_status], 3
        );
"""


# ── Repository class ───────────────────────────────────────────────────────

class AttachmentBlobSync(BaseRepository):
    """
    Manages the full lifecycle of PR attachments:
      DB download → local save → Azure Blob upload → BI dashboard sync.

    Inherits BaseRepository (Azure SQL as primary).
    Uses _ras_conn_str for on-prem SQL Server reads.
    """

    def __init__(self, config: AppConfig) -> None:
        super().__init__(config.get_azure_conn_str())
        self._ras_conn_str     = config.get_ras_conn_str()
        self._config           = config
        self._work_dir         = Path(config.WORK_DIR)
        self._max_blob_retries = config.BLOB_MAX_RETRIES

    # ── Public API ─────────────────────────────────────────────────────────

    def save_locally(self, purchase_req_no: str) -> int:
        """
        Downloads all attachments for the PR from the DB and saves them to:
            work/procurement/{sanitized_pr}/{att_id}/{filename}

        Returns the number of files saved.
        Does NOT upload to blob and does NOT update ras_tracker.
        """
        self._log.info(f"Saving attachments locally for PR: {purchase_req_no}")

        attachments = self._get_attachments(purchase_req_no)
        if not attachments:
            self._log.warning(f"No attachments found for PR: {purchase_req_no}")
            return 0

        self._log.info(f"Found {len(attachments)} attachment(s) for PR: {purchase_req_no}")

        attachment_ids = [att_id for att_id, _ in attachments]
        binary_docs    = self._get_binary_docs(attachment_ids)

        if not binary_docs:
            self._log.warning(
                f"No binary data found for PR: {purchase_req_no} "
                f"(attachment_ids: {attachment_ids})"
            )
            return 0

        saved = 0
        for att_id, files_name in attachments:
            ext = Path(files_name).suffix.lower()
            if ext not in ALLOWED_ATTACHMENT_EXTENSIONS:
                self._log.warning(
                    f"Skipping att_id={att_id} ({files_name}) — "
                    f"extension {ext!r} not in allowed list"
                )
                continue

            doc_bytes = binary_docs.get(att_id)
            if doc_bytes is None:
                self._log.warning(f"Skipping att_id={att_id} ({files_name}) — no binary doc")
                continue

            self._save_local(purchase_req_no, att_id, files_name, doc_bytes)
            saved += 1

        self._log.info(f"Saved {saved} file(s) locally for PR: {purchase_req_no}")
        return saved

    def upload_work_folder_to_blob(self, purchase_req_no: str) -> int:
        """
        Uploads the entire work/procurement/{safe_pr}/ folder to Azure Blob,
        preserving directory structure under work/.

        Returns the number of files uploaded.
        Does NOT update ras_tracker.
        """
        safe_pr      = _sanitize_pr_no(purchase_req_no)
        local_folder = self._work_dir / "procurement" / safe_pr

        if not local_folder.exists():
            raise RuntimeError(
                f"Local work folder not found: {local_folder} — "
                f"run save_locally() before upload_work_folder_to_blob()."
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
            self._log.warning(f"No files found in {local_folder} to upload")
            return 0

        self._log.info(
            f"Uploading {len(all_files)} file(s) from {local_folder} "
            f"for PR: {purchase_req_no}"
        )

        success_count = 0
        fail_count    = 0
        for file_path in all_files:
            blob_path = file_path.relative_to(self._work_dir).as_posix()
            if self._upload_blob(container_client, blob_path, file_path.read_bytes()):
                success_count += 1
            else:
                fail_count += 1

        self._log.info(
            f"PR {purchase_req_no}: {success_count} uploaded, {fail_count} failed"
        )

        if fail_count > 0:
            raise RuntimeError(
                f"{fail_count} file(s) failed to upload for PR {purchase_req_no}"
            )

        return success_count

    def sync_bi_dashboard_for_pr(self, purchase_req_no: str) -> None:
        """
        Refreshes the BI dashboard row for a PR: reads from on-prem view,
        deletes the old Azure row, inserts fresh data.

        Called by ClassificationStage on every pipeline run (first-time
        and re-process).
        """
        self._log.info(f"Syncing BI dashboard for PR: {purchase_req_no}")

        # Read source from on-prem (get column names too for dynamic INSERT)
        rows, columns = self._fetch_from_with_columns(
            self._ras_conn_str, _FETCH_BI_DASHBOARD_SQL, purchase_req_no
        )

        # Delete old + insert fresh in one Azure transaction
        conn = self._connect_write()
        try:
            cursor = conn.cursor()

            cursor.execute(_DELETE_BI_DASHBOARD_SQL, purchase_req_no)
            deleted = cursor.rowcount

            if rows:
                col_list     = ", ".join(f"[{c}]" for c in columns)
                placeholders = ", ".join("?" * len(columns))
                insert_sql   = (
                    f"INSERT INTO {AzureTables.BI_DASHBOARD} "
                    f"({col_list}) VALUES ({placeholders})"
                )
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, [list(row) for row in rows])

            conn.commit()
            self._log.info(
                f"BI dashboard sync for PR {purchase_req_no}: "
                f"deleted {deleted} row(s), inserted {len(rows)} row(s)"
            )
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def run(self, purchase_req_no: str) -> None:
        """
        Standalone / CLI entry point — runs the full attachment pipeline:
          1. Save all binary attachments locally (DB → work/)
          2. Upload entire work folder to blob  (work/ → Azure Blob)
          3. Update ras_tracker.current_stage_fk = 'BLOB_UPLOAD'
          4. Sync BI dashboard row

        Pipeline stages call save_locally() and upload_work_folder_to_blob()
        individually so they can manage their own tracker updates.
        """
        self._log.info(f"=== attachment_blob_sync started for PR: {purchase_req_no} ===")

        run_error: Exception | None = None
        try:
            saved = self.save_locally(purchase_req_no)
            if saved > 0:
                self.upload_work_folder_to_blob(purchase_req_no)
                self._execute(_UPSERT_TRACKER_SQL, purchase_req_no)
                self._log.info(
                    f"Tracker updated: current_stage_fk = 'BLOB_UPLOAD' "
                    f"for PR {purchase_req_no}"
                )
        except Exception as exc:
            run_error = exc
            self._log.opt(exception=True).error(
                f"Attachment sync failed for PR {purchase_req_no}: {exc}"
            )

        # BI dashboard sync always runs regardless of upload result
        try:
            self.sync_bi_dashboard_for_pr(purchase_req_no)
        except Exception as exc:
            self._log.error(f"BI dashboard sync failed for PR {purchase_req_no}: {exc}")

        self._log.info(f"=== attachment_blob_sync finished for PR: {purchase_req_no} ===")

        if run_error is not None:
            raise run_error

    # ── Internal helpers ───────────────────────────────────────────────────

    def _get_attachments(self, purchase_req_no: str) -> list[tuple[int, str]]:
        """Returns [(attachment_id, files_name), ...] from Azure SQL."""
        rows = self._fetch(_FETCH_ATTACHMENTS_SQL, purchase_req_no)
        return [(int(row[0]), str(row[1])) for row in rows]

    def _get_binary_docs(self, attachment_ids: list[int]) -> dict[int, bytes]:
        """Returns {attachment_id: doc_bytes} from the on-prem RAS database."""
        if not attachment_ids:
            return {}
        placeholders = ", ".join("?" * len(attachment_ids))
        sql  = _FETCH_BINARY_DOCS_SQL.format(placeholders=placeholders)
        rows = self._fetch_from(self._ras_conn_str, sql, *attachment_ids)
        return {int(row[0]): bytes(row[1]) for row in rows}

    def _upload_blob(self, container_client, blob_path: str, data: bytes) -> bool:
        """Upload `data` to `blob_path` with up to self._max_blob_retries attempts."""
        blob_client = container_client.get_blob_client(blob_path)
        for attempt in range(1, self._max_blob_retries + 1):
            try:
                blob_client.upload_blob(data, overwrite=True)
                self._log.debug(f"Uploaded: {blob_path}")
                return True
            except AzureError as exc:
                if attempt < self._max_blob_retries:
                    self._log.warning(
                        f"Upload attempt {attempt}/{self._max_blob_retries} failed "
                        f"for {blob_path}: {exc}"
                    )
                else:
                    self._log.error(
                        f"Upload failed after {self._max_blob_retries} attempts "
                        f"for {blob_path}: {exc}"
                    )
        return False

    def _save_local(
        self, pr_no: str, att_id: int, files_name: str, data: bytes
    ) -> None:
        """Saves binary data to work/procurement/{safe_pr}/{att_id}/{files_name}."""
        safe_pr    = _sanitize_pr_no(pr_no)
        local_path = self._work_dir / "procurement" / safe_pr / str(att_id) / files_name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(data)
        self._log.debug(f"Saved locally: {local_path}")


# ── Module-level utility ───────────────────────────────────────────────────

def _sanitize_pr_no(pr_no: str) -> str:
    """Replace '/' in PURCHASE_REQ_NO to avoid unintended blob sub-directories."""
    return pr_no.replace("/", "_")
