"""
pipeline.attachment_classification_repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CRUD service for the attachment classification tables.

Tables
------
attachment_classification          — one row per parent attachment file
                                     linked to ras_tracker via ras_uuid_pk
embedded_attachment_classification — one row per file extracted from a parent
                                     linked via attachment_classification_id

Write path (called from EmbedDocExtractionStage)
-------------------------------------------------
1. upsert_parent(...)
       MERGE on attachment_id (UNIQUE constraint).
       Returns the attachment_classify_uuid_pk so embedded rows can FK to it.

2. upsert_embedded(attachment_classification_id, parent_attachment_id, file_path)
       MERGE on (attachment_classification_id, file_path).

Cleanup path (called from PipelineOrchestrator before each PR run)
------------------------------------------------------------------
3. cleanup_for_pr(purchase_req_no)
       Calls stored procedure usp_cleanup_pr_data which deletes all
       PR-specific rows in FK-safe order inside a single DB transaction.

       Tables cleaned (in order):
         quotation_extracted_items          (FK child of both classification tables)
         embedded_attachment_classification (FK child of attachment_classification)
         attachment_classification
         vw_get_ras_data_for_bidashboard    (BI dashboard)

       ras_tracker and ras_pipeline_exceptions are intentionally NOT cleaned.
"""

from __future__ import annotations

import random
import time

import pyodbc
from loguru import logger

_DEADLOCK_STATE  = "40001"
_DEADLOCK_RETRIES = 4
_DEADLOCK_BASE_DELAY = 0.5   # seconds

from db.tables import AzureTables


class AttachmentClassificationRepository:
    """
    Data-access layer for attachment_classification and
    embedded_attachment_classification tables.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL DB.
    """

    # ── SQL ────────────────────────────────────────────────────────────────

    # MERGE on attachment_id (has UNIQUE constraint — single key is sufficient)
    _MERGE_PARENT_SQL = f"""
        MERGE {AzureTables.ATTACHMENT_CLASSIFICATION} WITH (HOLDLOCK) AS target
        USING (
            SELECT ? AS ras_uuid_pk,
                   ? AS attachment_id
        ) AS src
          ON  target.[attachment_id] = src.[attachment_id]
        WHEN MATCHED THEN
            UPDATE SET
                [file_path]           = ?,
                [embedded_file_flag]  = ?,
                [embedded_file_count] = ?,
                [updated_at]          = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                [ras_uuid_pk],
                [attachment_id],
                [file_path],
                [embedded_file_flag],
                [embedded_file_count]
            )
            VALUES (?, ?, ?, ?, ?);
    """

    _SELECT_PARENT_PK_SQL = f"""
        SELECT [attachment_classify_uuid_pk]
        FROM   {AzureTables.ATTACHMENT_CLASSIFICATION}
        WHERE  [attachment_id] = ?
    """

    _MERGE_EMBEDDED_SQL = f"""
        MERGE {AzureTables.EMBEDDED_ATTACHMENT_CLASSIFICATION} WITH (HOLDLOCK) AS target
        USING (
            SELECT ? AS attachment_classification_id,
                   ? AS file_path
        ) AS src
          ON  target.[attachment_classification_id] = src.[attachment_classification_id]
          AND target.[file_path]                    = src.[file_path]
        WHEN MATCHED THEN
            UPDATE SET
                [parent_attachment_id] = ?,
                [updated_at]           = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                [attachment_classification_id],
                [parent_attachment_id],
                [file_path]
            )
            VALUES (?, ?, ?);
    """

    _GET_TRACKER_UUID_SQL = f"""
        SELECT [ras_uuid_pk]
        FROM   {AzureTables.RAS_TRACKER}
        WHERE  [purchase_req_no] = ?
    """

    _UPDATE_PARENT_CLASSIFICATION_SQL = f"""
        UPDATE {AzureTables.ATTACHMENT_CLASSIFICATION}
        SET    [doc_type]            = ?,
               [classification_conf] = ?
        WHERE  [attachment_id]       = ?
    """

    _UPDATE_EMBEDDED_CLASSIFICATION_SQL = f"""
        UPDATE {AzureTables.EMBEDDED_ATTACHMENT_CLASSIFICATION}
        SET    [doc_type]            = ?,
               [classification_conf] = ?
        WHERE  [attachment_classification_id] = ?
          AND  [file_path]                    = ?
    """

    _CLEANUP_SP_SQL = "EXEC [ras_procurement].[usp_cleanup_pr_data] ?"

    # Explicit delete for quotation_extracted_items — ensures rows are removed
    # even if usp_cleanup_pr_data does not cover this table.
    _DELETE_QUOTATION_ITEMS_SQL = f"""
        DELETE qi
          FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS} qi
          JOIN {AzureTables.ATTACHMENT_CLASSIFICATION} ac
            ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
          JOIN {AzureTables.RAS_TRACKER} rt
            ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
         WHERE rt.[purchase_req_no] = ?
    """

    def __init__(self, conn_str: str) -> None:
        self._conn_str = conn_str
        self._log      = logger.bind(component="AttachmentClassificationRepository")

    # ── Public interface ───────────────────────────────────────────────────

    def get_tracker_uuid(self, purchase_req_no: str) -> str | None:
        """Returns the ras_uuid_pk from ras_tracker for the given PR, or None."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._GET_TRACKER_UUID_SQL, purchase_req_no)
            row = cursor.fetchone()
            return str(row[0]) if row else None
        except pyodbc.Error as exc:
            self._log.error(
                f"get_tracker_uuid failed for PR={purchase_req_no!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def cleanup_for_pr(self, purchase_req_no: str) -> None:
        """
        Deletes all pipeline output rows for this PR before (re-)processing.

        Skipped entirely for brand-new PRs (no ras_tracker row) — nothing to clean.

        For existing PRs delegates to usp_cleanup_pr_data (single transaction):
          1. quotation_extracted_items
          2. embedded_attachment_classification
          3. attachment_classification
          4. vw_get_ras_data_for_bidashboard
        """
        if self.get_tracker_uuid(purchase_req_no) is None:
            self._log.info(
                f"PR={purchase_req_no!r} is new — no existing DB rows to clean up"
            )
            return

        self._log.info(
            f"PR={purchase_req_no!r} already exists in tracker — "
            f"this is a retry; DB cleanup started"
        )

        for attempt in range(_DEADLOCK_RETRIES + 1):
            conn = self._connect()
            try:
                cursor = conn.cursor()
                cursor.execute(self._DELETE_QUOTATION_ITEMS_SQL, purchase_req_no)
                deleted_qi = cursor.rowcount
                cursor.execute(self._CLEANUP_SP_SQL, purchase_req_no)
                conn.commit()
                self._log.info(
                    f"DB cleanup done for PR={purchase_req_no!r} "
                    f"(deleted {deleted_qi} quotation_extracted_items row(s))"
                )
                return  # success
            except pyodbc.Error as exc:
                conn.rollback()
                is_deadlock = exc.args[0] == _DEADLOCK_STATE
                if not is_deadlock or attempt >= _DEADLOCK_RETRIES:
                    self._log.error(f"cleanup_for_pr failed for PR={purchase_req_no!r}: {exc}")
                    raise
                delay = _DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.5)
                self._log.warning(
                    f"Deadlock in cleanup_for_pr for PR={purchase_req_no!r} "
                    f"(attempt {attempt + 1}/{_DEADLOCK_RETRIES}), retrying in {delay:.1f}s"
                )
                time.sleep(delay)
            finally:
                conn.close()

    def upsert_parent(
        self,
        purchase_req_no: str,
        ras_uuid_pk: str,
        attachment_id: str,
        file_path: str,
        embedded_file_flag: bool,
        embedded_file_count: int,
    ) -> str:
        """
        INSERT or UPDATE a row in attachment_classification.

        Matched on attachment_id (UNIQUE constraint).
        On INSERT: sets ras_uuid_pk FK to ras_tracker.
        On UPDATE: refreshes file_path, embedded_file_flag, embedded_file_count.

        Returns the attachment_classify_uuid_pk of the upserted row.
        """
        self._log.debug(
            f"upsert_parent PR={purchase_req_no!r} att_id={attachment_id!r} "
            f"embedded={embedded_file_flag} count={embedded_file_count}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()

            cursor.execute(
                self._MERGE_PARENT_SQL,
                # USING source
                ras_uuid_pk, attachment_id,
                # WHEN MATCHED UPDATE
                file_path, embedded_file_flag, embedded_file_count,
                # WHEN NOT MATCHED INSERT
                ras_uuid_pk, attachment_id, file_path,
                embedded_file_flag, embedded_file_count,
            )

            cursor.execute(self._SELECT_PARENT_PK_SQL, attachment_id)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    f"upsert_parent: cannot find attachment_classification row after MERGE "
                    f"for PR={purchase_req_no!r} att_id={attachment_id!r}"
                )
            pk = str(row[0])

            conn.commit()
            self._log.info(
                f"attachment_classification upserted: PR={purchase_req_no!r} "
                f"att_id={attachment_id!r} pk={pk} embedded_count={embedded_file_count}"
            )
            return pk

        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"upsert_parent failed PR={purchase_req_no!r} att_id={attachment_id!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def upsert_embedded(
        self,
        attachment_classify_uuid_pk: str,
        parent_attachment_id: str,
        file_path: str,
    ) -> None:
        """
        INSERT or UPDATE a row in embedded_attachment_classification.

        Matched on (attachment_classification_id, file_path).
        """
        self._log.debug(
            f"upsert_embedded parent_pk={attachment_classify_uuid_pk!r} "
            f"file={file_path!r}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                self._MERGE_EMBEDDED_SQL,
                # USING source
                attachment_classify_uuid_pk, file_path,
                # WHEN MATCHED UPDATE
                parent_attachment_id,
                # WHEN NOT MATCHED INSERT
                attachment_classify_uuid_pk, parent_attachment_id, file_path,
            )
            conn.commit()
            self._log.debug(
                f"embedded_attachment_classification upserted: "
                f"parent_pk={attachment_classify_uuid_pk!r} file={file_path!r}"
            )

        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"upsert_embedded failed for file={file_path!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def get_parent_pk(
        self,
        attachment_id: str,
    ) -> str | None:
        """
        Returns the attachment_classify_uuid_pk for a given attachment_id,
        or None if the row doesn't exist.
        """
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._SELECT_PARENT_PK_SQL, attachment_id)
            row = cursor.fetchone()
            return str(row[0]) if row else None
        except pyodbc.Error as exc:
            self._log.error(
                f"get_parent_pk failed for att_id={attachment_id!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def update_parent_classification(
        self,
        attachment_id: str,
        doc_type: str,
        classification_conf: float,
    ) -> None:
        """
        Updates doc_type and classification_conf on an existing
        attachment_classification row.
        """
        self._log.debug(
            f"update_parent_classification att_id={attachment_id!r} "
            f"doc_type={doc_type!r} conf={classification_conf}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                self._UPDATE_PARENT_CLASSIFICATION_SQL,
                doc_type, classification_conf, attachment_id,
            )
            conn.commit()
            self._log.info(
                f"attachment_classification updated: att_id={attachment_id!r} "
                f"doc_type={doc_type!r} conf={classification_conf}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"update_parent_classification failed att_id={attachment_id!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def update_embedded_classification(
        self,
        attachment_classification_id: str,
        file_path: str,
        doc_type: str,
        classification_conf: float,
    ) -> None:
        """
        Updates doc_type and classification_conf on an existing
        embedded_attachment_classification row.
        """
        self._log.debug(
            f"update_embedded_classification parent_pk={attachment_classification_id!r} "
            f"file={file_path!r} doc_type={doc_type!r} conf={classification_conf}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                self._UPDATE_EMBEDDED_CLASSIFICATION_SQL,
                doc_type, classification_conf,
                attachment_classification_id, file_path,
            )
            conn.commit()
            self._log.debug(
                f"embedded_attachment_classification updated: "
                f"parent_pk={attachment_classification_id!r} "
                f"file={file_path!r} doc_type={doc_type!r}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"update_embedded_classification failed for "
                f"file={file_path!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    # ── Private helpers ────────────────────────────────────────────────────

    def _connect(self) -> pyodbc.Connection:
        from db.connection import connect_with_retry
        return connect_with_retry(self._conn_str, autocommit=False)
