"""
pipeline.tracker
~~~~~~~~~~~~~~~~
Shared service for ras_tracker and ras_pipeline_exceptions writes.

Operations
----------
upsert_stage(pr_no, stage_id)
    MERGE — INSERT the tracker row if it doesn't exist yet, otherwise UPDATE.
    Pulls ras_status from purchase_req_mst on first insert.
    Use for: INGESTION (pipeline entry point).

advance_stage(pr_no, stage_id)
    Plain UPDATE — moves current_stage_fk forward on an existing row.
    Use for: EMBED_DOC_EXTRACTION, BLOB_UPLOAD, CLASSIFICATION, and future stages.

set_last_processed_at(pr_no)
    Stamps last_processed_at = SYSUTCDATETIME(). Called at CLASSIFICATION
    so SourceChangeDetector can detect future source changes.

record_exception(pr_no, stage_id, error_message)
    Called when any stage fails for a PR:
      1. MERGE ras_tracker → current_stage_fk = 99 (EXCEPTION)
      2. SELECT ras_tracker.ras_uuid_pk for this PR
      3. INSERT into ras_pipeline_exceptions (ras_tracker_id, stage_id, exception_message)
    After this the PR is at terminal stage EXCEPTION (ID 99) and will NOT be
    retried by fetch_pending_prs() on the next run.
"""

from __future__ import annotations

import pyodbc
from loguru import logger

from db.connection import connect_with_retry
from db.tables import AzureTables

# Terminal stage IDs — must match pipeline_stages table
_STAGE_EXCEPTION = 99


class PipelineTracker:
    """
    Thin service layer for ras_tracker and ras_pipeline_exceptions writes.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL DB (ras_procurement schema).
    """

    _UPSERT_SQL = f"""
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
                [current_stage_fk] = ?,
                [updated_at]       = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                [purchase_req_no],
                [ras_status],
                [current_stage_fk]
            )
            VALUES (
                src.[purchase_req_no],
                src.[ras_status],
                ?
            );
    """

    _ADVANCE_SQL = f"""
        UPDATE {AzureTables.RAS_TRACKER}
        SET    [current_stage_fk] = ?,
               [updated_at]       = SYSUTCDATETIME()
        WHERE  [purchase_req_no] = ?
    """

    _GET_TRACKER_ID_SQL = f"""
        SELECT [ras_uuid_pk]
        FROM   {AzureTables.RAS_TRACKER}
        WHERE  [purchase_req_no] = ?
    """

    _INSERT_EXCEPTION_SQL = f"""
        INSERT INTO {AzureTables.RAS_PIPELINE_EXCEPTIONS}
            ([ras_tracker_id], [stage_id], [exception_message])
        VALUES
            (?, ?, ?)
    """

    _SET_LAST_PROCESSED_SQL = f"""
        UPDATE {AzureTables.RAS_TRACKER}
        SET    [last_processed_at] = SYSUTCDATETIME(),
               [updated_at]        = SYSUTCDATETIME()
        WHERE  [purchase_req_no] = ?
    """

    def __init__(self, conn_str: str) -> None:
        self._conn_str = conn_str
        self._log      = logger.bind(component="PipelineTracker")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def upsert_stage(self, purchase_req_no: str, stage_id: int) -> None:
        """
        INSERT or UPDATE ras_tracker for the given PR.

        On INSERT: pulls ras_status from purchase_req_mst.
        On UPDATE: advances current_stage_fk and updated_at only.

        Parameters
        ----------
        stage_id : pipeline_stages.stage_id value (e.g. 1 for INGESTION)
        """
        self._log.debug(f"upsert_stage PR={purchase_req_no!r} stage_id={stage_id}")
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._UPSERT_SQL, purchase_req_no, stage_id, stage_id)
            conn.commit()
            self._log.info(
                f"ras_tracker upserted: PR={purchase_req_no!r} stage_id={stage_id}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"upsert_stage failed PR={purchase_req_no!r} stage_id={stage_id}: {exc}"
            )
            raise
        finally:
            conn.close()

    def advance_stage(self, purchase_req_no: str, stage_id: int) -> None:
        """
        Advance current_stage_fk on an existing ras_tracker row.

        Parameters
        ----------
        stage_id : pipeline_stages.stage_id value (e.g. 2 for EMBED_DOC_EXTRACTION)
        """
        self._log.debug(f"advance_stage PR={purchase_req_no!r} stage_id={stage_id}")
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._ADVANCE_SQL, stage_id, purchase_req_no)
            affected = cursor.rowcount
            conn.commit()

            if affected == 0:
                raise RuntimeError(
                    f"advance_stage: no ras_tracker row for PR={purchase_req_no!r}. "
                    f"INGESTION must run before stage_id={stage_id}."
                )
            self._log.info(
                f"ras_tracker advanced: PR={purchase_req_no!r} stage_id={stage_id}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"advance_stage failed PR={purchase_req_no!r} stage_id={stage_id}: {exc}"
            )
            raise
        finally:
            conn.close()

    def set_last_processed_at(self, purchase_req_no: str) -> None:
        """
        Stamps last_processed_at = SYSUTCDATETIME() on the tracker row.
        Called by the final pipeline stage (CLASSIFICATION) after it completes.
        """
        self._log.debug(f"set_last_processed_at PR={purchase_req_no!r}")
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._SET_LAST_PROCESSED_SQL, purchase_req_no)
            conn.commit()
            self._log.info(f"last_processed_at set for PR={purchase_req_no!r}")
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"set_last_processed_at failed PR={purchase_req_no!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def record_exception(
        self,
        purchase_req_no: str,
        stage_id: int,
        error_message: str,
    ) -> None:
        """
        Called when a pipeline stage fails for a PR.

        Steps (single transaction):
          1. MERGE ras_tracker → current_stage_fk = 99 (EXCEPTION)
          2. SELECT ras_tracker.ras_uuid_pk for this PR
          3. INSERT into ras_pipeline_exceptions (ras_tracker_id, stage_id, exception_message)
        """
        self._log.debug(
            f"record_exception PR={purchase_req_no!r} stage_id={stage_id!r}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()

            # 1. Mark tracker as EXCEPTION (stage_id = 99)
            cursor.execute(
                self._UPSERT_SQL, purchase_req_no, _STAGE_EXCEPTION, _STAGE_EXCEPTION
            )

            # 2. Fetch ras_uuid_pk
            cursor.execute(self._GET_TRACKER_ID_SQL, purchase_req_no)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    f"record_exception: cannot find ras_tracker row for "
                    f"PR={purchase_req_no!r} after upsert."
                )
            tracker_id = row[0]

            # 3. Insert exception record
            cursor.execute(
                self._INSERT_EXCEPTION_SQL,
                tracker_id,
                stage_id,
                error_message,
            )

            conn.commit()
            self._log.warning(
                f"Exception recorded: PR={purchase_req_no!r} "
                f"stage_id={stage_id!r} tracker_id={tracker_id}"
            )

        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"record_exception DB write failed for PR={purchase_req_no!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> pyodbc.Connection:
        return connect_with_retry(self._conn_str, autocommit=False)
