"""
pipeline.change_detector
~~~~~~~~~~~~~~~~~~~~~~~~
Detects PRs whose source data changed after the pipeline last processed them
and re-queues them for re-processing.

How it works
------------
1. Query ras_tracker for PRs that completed the full pipeline
   (current_stage_fk = 'CLASSIFICATION') AND have a last_processed_at stamp
   AND whose purchase_req_mst.U_DATETIME is newer than last_processed_at.

2. The U_DATETIME on purchase_req_mst is kept fresh by two on-prem triggers:
     TR_Detail_BubbleUp_UDatetime      — fires on INSERT/UPDATE/DELETE in
                                         purchase_req_detail
     TR_Attachments_BubbleUp_UDatetime — fires on INSERT/UPDATE/DELETE in
                                         purchase_attachments
   Any change to a child row automatically bumps the parent PR's U_DATETIME.

3. For each changed PR, reset current_stage_fk = 'INGESTION'.
   fetch_pending_prs() picks these up on the next pipeline run and re-processes
   them end-to-end.  All pipeline operations are idempotent, so re-runs are safe.

Approval filter
---------------
Only PRs with PURCHASEFINALAPPROVALSTATUS in APPROVED_STATUSES are ever
processed.  Un-approved PRs are skipped at both the initial pick-up and the
re-queue steps.

Requires DB migration
---------------------
    ALTER TABLE [ras_procurement].[ras_tracker]
      ADD [last_processed_at] DATETIME NULL;
"""

from __future__ import annotations

from typing import List

import pyodbc
from loguru import logger

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.db_utils import connect_with_retry

# PRs with these approval statuses are eligible for processing.
# Case-insensitive comparison is done in SQL with UPPER().
APPROVED_STATUSES = ("APPROVED BY ALL", "APPROVED BY ALL EXCEPTION")


class SourceChangeDetector:
    """
    Finds completed PRs whose source data changed and re-queues them for
    re-processing by resetting current_stage_fk to 'INGESTION'.

    Parameters
    ----------
    config:
        Shared BlobSyncConfig — only the Azure SQL connection string is used.
    """

    _CHANGED_PRS_SQL = """
        SELECT prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        JOIN   [ras_procurement].[ras_tracker]      rt
          ON   rt.[purchase_req_no_fk] = prm.[PURCHASE_REQ_NO]
        WHERE  rt.[current_stage_fk]  = 'CLASSIFICATION'
          AND  rt.[last_processed_at] IS NOT NULL
          AND  prm.[U_DATETIME]       > rt.[last_processed_at]
          AND  UPPER(prm.[PURCHASEFINALAPPROVALSTATUS])
                   IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
    """

    _REQUEUE_SQL = """
        UPDATE [ras_procurement].[ras_tracker]
        SET    [current_stage_fk] = 'INGESTION',
               [updated_at]       = GETUTCDATE()
        WHERE  [purchase_req_no_fk] = ?
    """

    def __init__(self, config: BlobSyncConfig) -> None:
        self._conn_str = config.get_azure_conn_str()
        self._log      = logger.bind(component="SourceChangeDetector")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def detect_and_requeue(self) -> int:
        """
        Detects completed PRs with source data changes and resets them to
        'INGESTION' so the next pipeline run re-processes them.

        Returns
        -------
        int
            Number of PRs re-queued.

        Raises
        ------
        pyodbc.Error
            If the DB query or update fails (logged + re-raised).
        """
        self._log.info("Scanning for completed PRs with source data changes...")
        conn = self._connect()
        try:
            changed = self._find_changed_prs(conn)
            if not changed:
                self._log.info("No completed PRs have changed source data — nothing to re-queue")
                return 0

            self._log.info(
                f"Found {len(changed)} PR(s) with updated source data: {changed}"
            )
            self._requeue(conn, changed)
            conn.commit()
            self._log.info(
                f"Re-queued {len(changed)} PR(s) for re-processing "
                f"(current_stage_fk reset to 'INGESTION')"
            )
            return len(changed)

        except pyodbc.Error as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            self._log.error(f"detect_and_requeue failed: {exc}")
            raise
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _find_changed_prs(self, conn: pyodbc.Connection) -> List[str]:
        """Returns PURCHASE_REQ_NO values for completed PRs that have changed."""
        cursor = conn.cursor()
        cursor.execute(self._CHANGED_PRS_SQL)
        rows = cursor.fetchall()
        cursor.close()
        return [str(row[0]) for row in rows]

    def _requeue(self, conn: pyodbc.Connection, pr_nos: List[str]) -> None:
        """Resets current_stage_fk = 'INGESTION' for each PR in the list."""
        cursor = conn.cursor()
        for pr_no in pr_nos:
            cursor.execute(self._REQUEUE_SQL, pr_no)
            self._log.debug(f"Re-queued PR={pr_no!r}")
        cursor.close()

    def _connect(self) -> pyodbc.Connection:
        return connect_with_retry(self._conn_str, autocommit=False)
