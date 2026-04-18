"""
pipeline.repository
~~~~~~~~~~~~~~~~~~~
Data-access layer for the pipeline.

Responsible for all SQL interactions that the orchestrator needs:
    - fetch_pending_prs(completed_stage_id)
          Returns PRs that still need processing:
            • no row in ras_tracker at all  (brand new)
            • row exists but current_stage_fk is NOT the completed_stage_id
              and NOT a terminal state (90=COMPLETED / 91=DELIVERED / 99=EXCEPTION)
          This makes the pipeline resumable: if a run crashes mid-way,
          the stuck PR is retried on the next run. All stage operations
          are idempotent so re-running from the top is safe.

    - fetch_prs_at_stage(stage_id)
          Returns PRs whose current_stage_fk matches the given stage ID.
          Used by future stages to find their own input set.

Rules:
    - Opens a fresh connection per public method call; closes it in a finally
      block so connection leaks are impossible even under exceptions.
    - All queries are parameterised; no string interpolation of external data.
    - autocommit=True for read-only queries (no implicit transaction overhead).
"""

from __future__ import annotations

from typing import List, Optional

import pyodbc
from loguru import logger

# Terminal stage IDs — PRs at these stages are never reprocessed
_TERMINAL_STAGE_IDS = (90, 91, 99)  # COMPLETED, DELIVERED, EXCEPTION


class PipelineRepository:
    """
    Read-only data-access layer for the pipeline orchestrator.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL database that holds the
        ras_procurement schema (purchase_req_mst, ras_tracker, pipeline_stages).
    limit:
        Optional cap on the number of PRs returned per query.
        Pass None (default) to return all matching PRs.
    """

    # Picks up:
    #   1. Brand-new PRs  (no tracker row)
    #   2. PRs stuck mid-pipeline  (tracker row exists but not at completed_stage_id
    #                               and not at a terminal state)
    #
    # Only process PRs that have been fully approved.
    # Un-approved / in-progress PRs are skipped until their status advances.
    # Allowlist approach: only fully-approved PRs enter the pipeline.
    # PRs with REJECTED / REJECT status are naturally excluded — no extra NOT IN needed.
    _PENDING_SQL = """
        SELECT prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no]
        WHERE (rt.[purchase_req_no] IS NULL
               OR rt.[current_stage_fk] NOT IN (?, 90, 91, 99))
          AND UPPER(prm.[PURCHASEFINALAPPROVALSTATUS])
                  IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
        ORDER BY prm.[C_DATETIME] ASC
    """

    _PENDING_SQL_LIMITED = """
        SELECT TOP (?) prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no]
        WHERE (rt.[purchase_req_no] IS NULL
               OR rt.[current_stage_fk] NOT IN (?, 90, 91, 99))
          AND UPPER(prm.[PURCHASEFINALAPPROVALSTATUS])
                  IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
        ORDER BY prm.[C_DATETIME] ASC
    """

    _AT_STAGE_SQL = """
        SELECT rt.[purchase_req_no]
        FROM   [ras_procurement].[ras_tracker] rt
        WHERE  rt.[current_stage_fk] = ?
        ORDER BY rt.[updated_at] ASC
    """

    _AT_STAGE_SQL_LIMITED = """
        SELECT TOP (?) rt.[purchase_req_no]
        FROM   [ras_procurement].[ras_tracker] rt
        WHERE  rt.[current_stage_fk] = ?
        ORDER BY rt.[updated_at] ASC
    """

    def __init__(self, conn_str: str, limit: Optional[int] = None) -> None:
        self._conn_str = conn_str
        self._limit    = limit
        self._log      = logger.bind(component="PipelineRepository")

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_pending_prs(self, completed_stage_id: int) -> List[str]:
        """
        Returns all PURCHASE_REQ_NO values that still need processing,
        ordered by C_DATETIME ASC (oldest PRs first).

        A PR needs processing if:
          - It has no row in ras_tracker yet  (brand new), OR
          - Its current_stage_fk is not `completed_stage_id` and not a terminal
            state (90=COMPLETED / 91=DELIVERED / 99=EXCEPTION).

        Parameters
        ----------
        completed_stage_id:
            STAGE_ID of the last stage in the pipeline (e.g. 4 for CLASSIFICATION).
            PRs already at this stage are considered done and excluded.
        """
        self._log.debug(
            f"Querying pending PRs (completed_stage_id={completed_stage_id})"
            + (f" TOP {self._limit}" if self._limit else "")
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._limit is not None:
                cursor.execute(self._PENDING_SQL_LIMITED, self._limit, completed_stage_id)
            else:
                cursor.execute(self._PENDING_SQL, completed_stage_id)

            pr_list = [str(row[0]) for row in cursor.fetchall()]
            self._log.info(
                f"Pending PRs found: {len(pr_list)} "
                f"(not yet at stage_id={completed_stage_id})"
            )
            return pr_list

        except pyodbc.Error as exc:
            self._log.error(f"Failed to fetch pending PRs: {exc}")
            raise

        finally:
            conn.close()

    def fetch_prs_at_stage(self, stage_id: int) -> List[str]:
        """
        Returns PURCHASE_REQ_NO values for PRs whose current_stage_fk equals
        `stage_id`, ordered by updated_at ascending.

        Parameters
        ----------
        stage_id:
            pipeline_stages.stage_id value to filter on.
        """
        self._log.debug(
            f"Querying PRs at stage_id={stage_id}"
            + (f" TOP {self._limit}" if self._limit else "")
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._limit is not None:
                cursor.execute(self._AT_STAGE_SQL_LIMITED, self._limit, stage_id)
            else:
                cursor.execute(self._AT_STAGE_SQL, stage_id)

            pr_list = [str(row[0]) for row in cursor.fetchall()]
            self._log.info(f"PRs at stage_id={stage_id} found: {len(pr_list)}")
            return pr_list

        except pyodbc.Error as exc:
            self._log.error(f"Failed to fetch PRs at stage_id={stage_id}: {exc}")
            raise

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> pyodbc.Connection:
        from db.connection import connect_with_retry
        return connect_with_retry(self._conn_str, autocommit=True)
