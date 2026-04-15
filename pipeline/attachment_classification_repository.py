"""
pipeline.attachment_classification_repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
CRUD service for the two attachment-classification tables.

Tables
------
AttachmentClassification          — one row per parent attachment file
EmbeddedAttachmentClassification  — one row per file extracted from a parent

Write path (called from EmbedDocExtractionStage)
-------------------------------------------------
1. upsert_parent(...)
       MERGE on (purchase_req_no_fk, attachment_id).
       Returns the attachment_classify_uuid_pk so embedded rows can FK to it.

2. upsert_embedded(attachment_classify_uuid_pk, parent_attachment_id, file_path)
       MERGE on (attachment_classify_uuid_pk, file_path).

Both methods open a fresh connection per call and commit in a finally block.
"""

from __future__ import annotations

import pyodbc
from loguru import logger


class AttachmentClassificationRepository:
    """
    Data-access layer for AttachmentClassification and
    EmbeddedAttachmentClassification tables.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL DB
        (the same one used by PipelineTracker).
    """

    # ── SQL ────────────────────────────────────────────────────────────────

    _MERGE_PARENT_SQL = """
        MERGE [ras_procurement].[AttachmentClassification] WITH (HOLDLOCK) AS target
        USING (
            SELECT ? AS purchase_req_no_fk,
                   ? AS attachment_id
        ) AS src
          ON  target.[purchase_req_no_fk] = src.[purchase_req_no_fk]
          AND target.[attachment_id]      = src.[attachment_id]
        WHEN MATCHED THEN
            UPDATE SET
                [file_path]           = ?,
                [embedded_file_flag]  = ?,
                [embedded_file_count] = ?,
                [updated_at]          = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT (
                [purchase_req_no_fk],
                [ras_uuid_pk],
                [attachment_id],
                [file_path],
                [embedded_file_flag],
                [embedded_file_count]
            )
            VALUES (?, ?, ?, ?, ?, ?);
    """

    _SELECT_PARENT_PK_SQL = """
        SELECT [attachment_classify_uuid_pk]
        FROM   [ras_procurement].[AttachmentClassification]
        WHERE  [purchase_req_no_fk] = ?
          AND  [attachment_id]      = ?
    """

    _MERGE_EMBEDDED_SQL = """
        MERGE [ras_procurement].[EmbeddedAttachmentClassification] WITH (HOLDLOCK) AS target
        USING (
            SELECT ? AS attachment_classify_uuid_pk,
                   ? AS file_path
        ) AS src
          ON  target.[attachment_classify_uuid_pk] = src.[attachment_classify_uuid_pk]
          AND target.[file_path]                   = src.[file_path]
        WHEN MATCHED THEN
            UPDATE SET
                [parent_attachment_id] = ?,
                [updated_at]           = GETUTCDATE()
        WHEN NOT MATCHED THEN
            INSERT (
                [attachment_classify_uuid_pk],
                [parent_attachment_id],
                [file_path]
            )
            VALUES (?, ?, ?);
    """

    _GET_TRACKER_UUID_SQL = """
        SELECT [ras_uuid_pk]
        FROM   [ras_procurement].[ras_tracker]
        WHERE  [purchase_req_no_fk] = ?
    """

    def __init__(self, conn_str: str) -> None:
        self._conn_str = conn_str
        self._log      = logger.bind(component="AttachmentClassificationRepository")

    # ── Public interface ───────────────────────────────────────────────────

    def get_tracker_uuid(self, purchase_req_no: str) -> str | None:
        """
        Returns the ras_uuid_pk from ras_tracker for the given PR,
        or None if the row doesn't exist yet.
        """
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._GET_TRACKER_UUID_SQL, purchase_req_no)
            row = cursor.fetchone()
            cursor.close()
            return str(row[0]) if row else None
        except pyodbc.Error as exc:
            self._log.error(
                f"get_tracker_uuid failed for PR={purchase_req_no!r}: {exc}"
            )
            raise
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
        INSERT or UPDATE a row in AttachmentClassification.

        Matched on (purchase_req_no_fk, attachment_id).
        On INSERT: populates ras_uuid_pk FK to ras_tracker.
        On UPDATE: refreshes file_path, embedded_file_flag, embedded_file_count.

        Returns
        -------
        str
            The attachment_classify_uuid_pk of the upserted row.

        Raises
        ------
        pyodbc.Error  : on any DB failure.
        RuntimeError  : if the row cannot be found after the MERGE.
        """
        self._log.debug(
            f"upsert_parent PR={purchase_req_no!r} att_id={attachment_id!r} "
            f"embedded={embedded_file_flag} count={embedded_file_count}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()

            # MERGE — INSERT or UPDATE
            cursor.execute(
                self._MERGE_PARENT_SQL,
                # USING source
                purchase_req_no, attachment_id,
                # WHEN MATCHED UPDATE
                file_path, embedded_file_flag, embedded_file_count,
                # WHEN NOT MATCHED INSERT
                purchase_req_no, ras_uuid_pk, attachment_id,
                file_path, embedded_file_flag, embedded_file_count,
            )

            # SELECT the PK (auto-generated NEWSEQUENTIALID on first insert)
            cursor.execute(self._SELECT_PARENT_PK_SQL, purchase_req_no, attachment_id)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    f"upsert_parent: cannot find AttachmentClassification row after MERGE "
                    f"for PR={purchase_req_no!r} att_id={attachment_id!r}"
                )
            pk = str(row[0])

            conn.commit()
            cursor.close()
            self._log.info(
                f"AttachmentClassification upserted: PR={purchase_req_no!r} "
                f"att_id={attachment_id!r} pk={pk} "
                f"embedded_count={embedded_file_count}"
            )
            return pk

        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"upsert_parent failed PR={purchase_req_no!r} "
                f"att_id={attachment_id!r}: {exc}"
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
        INSERT or UPDATE a row in EmbeddedAttachmentClassification.

        Matched on (attachment_classify_uuid_pk, file_path).
        On INSERT: creates the embedded file record linked to its parent.
        On UPDATE: refreshes parent_attachment_id.

        Raises
        ------
        pyodbc.Error  : on any DB failure.
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
            cursor.close()
            self._log.debug(
                f"EmbeddedAttachmentClassification upserted: "
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

    # ── Private helpers ────────────────────────────────────────────────────

    def _connect(self) -> pyodbc.Connection:
        from pipeline.db_utils import connect_with_retry
        return connect_with_retry(self._conn_str, autocommit=False)
