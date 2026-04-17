"""
pipeline.repository
~~~~~~~~~~~~~~~~~~~
Data-access layer for the pipeline.

Responsible for all SQL interactions that the orchestrator needs:
    - fetch_pending_prs(completed_stage)
          Returns PRs that still need processing:
            • no row in ras_tracker at all  (brand new)
            • row exists but current_stage_fk is NOT the completed_stage
              and NOT a terminal state (COMPLETED / DELIVERED / EXCEPTION)
          This makes the pipeline resumable: if a run crashes mid-way,
          the stuck PR is retried on the next run. All stage operations
          are idempotent so re-running from the top is safe.

    - fetch_prs_at_stage(completed_stage)
          Returns PRs whose current_stage_fk matches a given value.
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

# These stage names always mean "done — do not reprocess"
_TERMINAL_STAGES = ("COMPLETED", "DELIVERED", "EXCEPTION")


class PipelineRepository:
    """
    Read-only data-access layer for the pipeline orchestrator.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the Azure SQL database that holds the
        ras_procurement schema (purchase_req_mst, ras_tracker, pipeline_stages).
    limit:
        Optional cap on the number of PRs returned per query.  Useful for
        processing large backlogs in controlled batches (e.g. 100 per run).
        Pass None (default) to return all matching PRs.
    """

    # Picks up:
    #   1. Brand-new PRs  (no tracker row)
    #   2. PRs stuck mid-pipeline  (tracker row exists but not at completed_stage
    #                               and not at a terminal state)
    #
    # ? placeholders: completed_stage, COMPLETED, DELIVERED, EXCEPTION
    # (pyodbc doesn't support IN (?) with a list, so we expand to 4 literals)
    # Only process PRs that have been fully approved.
    # Un-approved / in-progress PRs are skipped until their status advances.
    # Allowlist approach: only fully-approved PRs enter the pipeline.
    # PRs with REJECTED / REJECT status (or any other non-approved status)
    # are naturally excluded and never picked up — no extra NOT IN needed.
    _PENDING_SQL = """
        SELECT prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no_fk]
        WHERE (rt.[purchase_req_no_fk] IS NULL
               OR rt.[current_stage_fk] NOT IN (?, 'COMPLETED', 'DELIVERED', 'EXCEPTION'))
          AND UPPER(prm.[PURCHASEFINALAPPROVALSTATUS])
                  IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
        ORDER BY prm.[C_DATETIME] ASC
    """

    _PENDING_SQL_LIMITED = """
        SELECT TOP (?) prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no_fk]
        WHERE (rt.[purchase_req_no_fk] IS NULL
               OR rt.[current_stage_fk] NOT IN (?, 'COMPLETED', 'DELIVERED', 'EXCEPTION'))
          AND UPPER(prm.[PURCHASEFINALAPPROVALSTATUS])
                  IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
        ORDER BY prm.[C_DATETIME] ASC
    """

    _AT_STAGE_SQL = """
        SELECT rt.[purchase_req_no_fk]
        FROM   [ras_procurement].[ras_tracker] rt
        WHERE  rt.[current_stage_fk] = ?
        ORDER BY rt.[updated_at] ASC
    """

    _AT_STAGE_SQL_LIMITED = """
        SELECT TOP (?) rt.[purchase_req_no_fk]
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

    def fetch_pending_prs(self, completed_stage: str) -> List[str]:
        """
        Returns all PURCHASE_REQ_NO values that still need processing,
        ordered by C_DATETIME ASC (oldest PRs first).

        A PR needs processing if:
          - It has no row in ras_tracker yet  (brand new), OR
          - Its current_stage_fk is not `completed_stage` and not a terminal
            state (COMPLETED / DELIVERED / EXCEPTION).

        This means the pipeline is automatically resumable: if a run
        crashes after INGESTION but before CLASSIFICATION, the PR remains
        visible on the next run and is retried from the beginning.
        All stage operations are idempotent so this is safe.

        Parameters
        ----------
        completed_stage:
            STAGE_NAME of the last stage in the pipeline (e.g. 'CLASSIFICATION').
            PRs already at this stage are considered done and excluded.

        Raises:
            pyodbc.Error: on any database connectivity or query failure.
        """
        self._log.debug(
            f"Querying pending PRs (completed_stage={completed_stage!r})"
            + (f" TOP {self._limit}" if self._limit else "")
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._limit is not None:
                # TOP (N), completed_stage
                cursor.execute(self._PENDING_SQL_LIMITED, self._limit, completed_stage)
            else:
                cursor.execute(self._PENDING_SQL, completed_stage)

            pr_list = [str(row[0]) for row in cursor.fetchall()]
            cursor.close()
            self._log.info(
                f"Pending PRs found: {len(pr_list)} "
                f"(not yet at stage={completed_stage!r})"
            )
            return pr_list

        except pyodbc.Error as exc:
            self._log.error(f"Failed to fetch pending PRs: {exc}")
            raise

        finally:
            conn.close()

    def fetch_prs_at_stage(self, completed_stage: str) -> List[str]:
        """
        Returns PURCHASE_REQ_NO values for PRs whose current_stage_fk equals
        `completed_stage`, ordered by updated_at ascending.

        Use this when a future stage needs its own input set — for example,
        to fetch all PRs that finished BLOB_UPLOAD and are ready for
        CLASSIFICATION:

            repo.fetch_prs_at_stage('BLOB_UPLOAD')

        Raises:
            pyodbc.Error: on any database connectivity or query failure.
        """
        self._log.debug(
            f"Querying PRs at stage={completed_stage!r}"
            + (f" TOP {self._limit}" if self._limit else "")
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._limit is not None:
                cursor.execute(self._AT_STAGE_SQL_LIMITED, self._limit, completed_stage)
            else:
                cursor.execute(self._AT_STAGE_SQL, completed_stage)

            pr_list = [str(row[0]) for row in cursor.fetchall()]
            cursor.close()
            self._log.info(f"PRs at stage={completed_stage!r} found: {len(pr_list)}")
            return pr_list

        except pyodbc.Error as exc:
            self._log.error(f"Failed to fetch PRs at stage={completed_stage!r}: {exc}")
            raise

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> pyodbc.Connection:
        """Opens a read-only (autocommit) connection to the Azure SQL DB."""
        from db.connection import connect_with_retry
        return connect_with_retry(self._conn_str, autocommit=True)
