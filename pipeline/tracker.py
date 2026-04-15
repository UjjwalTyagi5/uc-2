"""
pipeline.tracker
~~~~~~~~~~~~~~~~
Shared service for ras_tracker and ras_pipeline_exceptions writes.

Operations
----------
upsert_stage(pr_no, stage)
    MERGE — INSERT the tracker row if it doesn't exist yet, otherwise UPDATE.
    Enriches the row from purchase_req_mst on first insert.
    Use for: INGESTION (pipeline entry point).

advance_stage(pr_no, stage)
    Plain UPDATE — moves current_stage_fk forward on an existing row.
    Use for: EMBED_DOC_EXTRACTION, CLASSIFICATION, and future stages.

record_exception(pr_no, stage_name, error_message)
    Called when any stage fails for a PR:
      1. MERGE ras_tracker → current_stage_fk = 'EXCEPTION'
         (creates the row first if it doesn't exist, e.g. if INGESTION itself failed)
      2. SELECT ras_tracker.id for this PR
      3. INSERT into ras_pipeline_exceptions
         (id=NEWID(), ras_tracker_id, stage_name, exception_message, timestamps)
    After this the PR is at terminal stage EXCEPTION and will NOT be
    retried by fetch_pending_prs() on the next run.
"""

from __future__ import annotations

import pyodbc
from loguru import logger


class PipelineTracker:
    """
    Thin service layer for ras_tracker and ras_pipeline_exceptions writes.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL DB (ras_procurement schema).
    """

    _UPSERT_SQL = """
        MERGE [ras_procurement].[ras_tracker] WITH (HOLDLOCK) AS target
        USING (
            SELECT
                [PURCHASE_REQ_NO]             AS purchase_req_no_fk,
                [JUSTIFICATION]               AS ras_justification,
                [CURRENCY]                    AS currency,
                [C_DATETIME]                  AS ras_created_at,
                [U_DATETIME]                  AS ras_updated_at,
                [PURCHASEFINALAPPROVALSTATUS]  AS ras_status
            FROM [ras_procurement].[purchase_req_mst]
            WHERE [PURCHASE_REQ_NO] = ?
        ) AS src
          ON target.[purchase_req_no_fk] = src.[purchase_req_no_fk]
        WHEN MATCHED THEN
            UPDATE SET
                [current_stage_fk] = ?,
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
                ?
            );
    """

    _ADVANCE_SQL = """
        UPDATE [ras_procurement].[ras_tracker]
        SET    [current_stage_fk] = ?,
               [updated_at]       = GETUTCDATE()
        WHERE  [purchase_req_no_fk] = ?
    """

    # Gets the PK of the tracker row so we can FK to it from the exception table
    _GET_TRACKER_ID_SQL = """
        SELECT [id]
        FROM   [ras_procurement].[ras_tracker]
        WHERE  [purchase_req_no_fk] = ?
    """

    _INSERT_EXCEPTION_SQL = """
        INSERT INTO [ras_procurement].[ras_pipeline_exceptions]
            ([id], [ras_tracker_id], [stage_name], [exception_message],
             [created_at], [updated_at])
        VALUES
            (NEWID(), ?, ?, ?, GETUTCDATE(), GETUTCDATE())
    """

    def __init__(self, conn_str: str) -> None:
        self._conn_str = conn_str
        self._log      = logger.bind(component="PipelineTracker")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def upsert_stage(self, purchase_req_no: str, stage: str) -> None:
        """
        INSERT or UPDATE ras_tracker for the given PR.

        On INSERT: populates ras_justification, currency, and datetime fields
        from purchase_req_mst in the same statement.
        On UPDATE: only advances current_stage_fk and updated_at.

        Raises:
            pyodbc.Error: on any DB failure.
        """
        self._log.debug(f"upsert_stage PR={purchase_req_no!r} stage={stage!r}")
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._UPSERT_SQL, purchase_req_no, stage, stage)
            conn.commit()
            cursor.close()
            self._log.info(
                f"ras_tracker upserted: PR={purchase_req_no!r} stage={stage!r}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"upsert_stage failed PR={purchase_req_no!r} stage={stage!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def advance_stage(self, purchase_req_no: str, stage: str) -> None:
        """
        Advance current_stage_fk on an existing ras_tracker row.

        Raises:
            pyodbc.Error : on any DB failure.
            RuntimeError : if no row exists (INGESTION must run first).
        """
        self._log.debug(f"advance_stage PR={purchase_req_no!r} stage={stage!r}")
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._ADVANCE_SQL, stage, purchase_req_no)
            affected = cursor.rowcount
            conn.commit()
            cursor.close()

            if affected == 0:
                raise RuntimeError(
                    f"advance_stage: no ras_tracker row for PR={purchase_req_no!r}. "
                    f"INGESTION must run before {stage!r}."
                )
            self._log.info(
                f"ras_tracker advanced: PR={purchase_req_no!r} stage={stage!r}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"advance_stage failed PR={purchase_req_no!r} stage={stage!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def record_exception(
        self,
        purchase_req_no: str,
        stage_name: str,
        error_message: str,
    ) -> None:
        """
        Called when a pipeline stage fails for a PR.

        Steps (single transaction):
          1. MERGE ras_tracker → current_stage_fk = 'EXCEPTION'
             (safe even if the row doesn't exist yet — e.g. INGESTION itself failed)
          2. SELECT ras_tracker.id for this PR
          3. INSERT into ras_pipeline_exceptions

        After this the PR is at the terminal 'EXCEPTION' stage and will not
        be picked up by fetch_pending_prs() on subsequent runs.

        Parameters
        ----------
        purchase_req_no:
            The PR that failed.
        stage_name:
            NAME of the stage that raised the exception.
        error_message:
            str(exception) — the human-readable error detail.

        Raises:
            pyodbc.Error: if the DB write itself fails (logged + re-raised).
        """
        self._log.debug(
            f"record_exception PR={purchase_req_no!r} "
            f"stage={stage_name!r}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()

            # 1. Mark tracker row as EXCEPTION (creates row if missing)
            cursor.execute(self._UPSERT_SQL, purchase_req_no, "EXCEPTION", "EXCEPTION")

            # 2. Fetch ras_tracker.id
            cursor.execute(self._GET_TRACKER_ID_SQL, purchase_req_no)
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(
                    f"record_exception: cannot find ras_tracker row for "
                    f"PR={purchase_req_no!r} after upsert — check DB constraints."
                )
            tracker_id = row[0]

            # 3. Insert exception record
            cursor.execute(
                self._INSERT_EXCEPTION_SQL,
                tracker_id,
                stage_name,
                error_message,
            )

            conn.commit()
            cursor.close()
            self._log.warning(
                f"Exception recorded: PR={purchase_req_no!r} "
                f"stage={stage_name!r} tracker_id={tracker_id}"
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
        try:
            return pyodbc.connect(self._conn_str, autocommit=False, timeout=0)
        except pyodbc.Error as exc:
            self._log.error(f"Cannot connect to Azure SQL DB: {exc}")
            raise
