"""
pipeline.tracker
~~~~~~~~~~~~~~~~
Shared service for updating ras_tracker.current_stage_fk.

Used by pipeline stages that do not own their own DB transaction
(i.e. every stage except BlobUploadStage, which drives tracker updates
internally via AttachmentBlobSync._upsert_tracker).

Two operations
--------------
upsert_stage(pr_no, stage)
    MERGE — INSERT the tracker row if it doesn't exist yet, otherwise UPDATE.
    Also pulls ras_justification, currency, C_DATETIME, U_DATETIME,
    PURCHASEFINALAPPROVALSTATUS from purchase_req_mst so the row is
    fully populated on first write.
    Use for: INGESTION (the pipeline entry point that creates the row).

advance_stage(pr_no, stage)
    Plain UPDATE — moves current_stage_fk forward on an existing row.
    Use for: EMBED_DOC_EXTRACTION, CLASSIFICATION, and any future stage
    that runs after INGESTION has already created the row.
"""

from __future__ import annotations

import pyodbc
from loguru import logger


class PipelineTracker:
    """
    Thin service layer for ras_tracker writes.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL DB (ras_procurement schema).
    """

    # Full MERGE — creates row if missing, otherwise advances stage.
    # Also enriches the row with metadata from purchase_req_mst on INSERT.
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

    # Simple UPDATE — advances the stage on a row that already exists.
    _ADVANCE_SQL = """
        UPDATE [ras_procurement].[ras_tracker]
        SET    [current_stage_fk] = ?,
               [updated_at]       = GETUTCDATE()
        WHERE  [purchase_req_no_fk] = ?
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
        self._log.debug(
            f"upsert_stage PR={purchase_req_no!r} stage={stage!r}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            # MERGE needs: (1) WHERE param, (2) MATCHED stage, (3) NOT MATCHED stage
            cursor.execute(self._UPSERT_SQL, purchase_req_no, stage, stage)
            conn.commit()
            cursor.close()
            self._log.info(
                f"ras_tracker upserted: PR={purchase_req_no!r} "
                f"current_stage_fk={stage!r}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"upsert_stage failed for PR={purchase_req_no!r} "
                f"stage={stage!r}: {exc}"
            )
            raise
        finally:
            conn.close()

    def advance_stage(self, purchase_req_no: str, stage: str) -> None:
        """
        Advance current_stage_fk on an existing ras_tracker row.

        Raises:
            pyodbc.Error  : on any DB failure.
            RuntimeError  : if no row was found for the given PR
                            (suggests a pipeline ordering bug).
        """
        self._log.debug(
            f"advance_stage PR={purchase_req_no!r} stage={stage!r}"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(self._ADVANCE_SQL, stage, purchase_req_no)
            affected = cursor.rowcount
            conn.commit()
            cursor.close()

            if affected == 0:
                raise RuntimeError(
                    f"advance_stage: no ras_tracker row found for "
                    f"PR={purchase_req_no!r}. "
                    f"INGESTION stage must run before {stage!r}."
                )

            self._log.info(
                f"ras_tracker advanced: PR={purchase_req_no!r} "
                f"current_stage_fk={stage!r}"
            )
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(
                f"advance_stage failed for PR={purchase_req_no!r} "
                f"stage={stage!r}: {exc}"
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
