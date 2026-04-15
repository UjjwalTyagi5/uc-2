"""
pipeline.repository
~~~~~~~~~~~~~~~~~~~
Data-access layer for the pipeline.

Responsible for all SQL interactions that the orchestrator needs:
    - fetch_pending_prs()      : PRs with no row in ras_tracker yet
                                 (pipeline entry point — before stage BLOB_UPLOAD)
    - fetch_prs_at_stage()     : PRs whose current_stage_fk matches a given
                                 PipelineStage value (used by future stages to
                                 find their own input set, e.g. BLOB_UPLOAD → CLASSIFICATION)

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

    # PRs that have never been processed — no row in ras_tracker at all.
    # This is the entry condition for the full ATTACHMENT pipeline.
    _PIPELINE_ENTRY_SQL = """
        SELECT prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no_fk]
        WHERE rt.[purchase_req_no_fk] IS NULL
        ORDER BY prm.[C_DATETIME] ASC
    """

    _PIPELINE_ENTRY_SQL_LIMITED = """
        SELECT TOP (?) prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no_fk]
        WHERE rt.[purchase_req_no_fk] IS NULL
        ORDER BY prm.[C_DATETIME] ASC
    """

    # PRs that completed a specific stage and are ready for the next one.
    # Used by each stage to fetch its own input set.
    # e.g. fetch_prs_at_stage(PipelineStage.BLOB_UPLOAD)
    #      → returns PRs ready for CLASSIFICATION
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

    def fetch_pending_prs(self) -> List[str]:
        """
        Returns PURCHASE_REQ_NO values for every PR that has never been
        processed (no row in ras_tracker), ordered by C_DATETIME ascending
        so oldest unprocessed PRs are handled first.

        This is the pipeline entry-point query — used before the first stage
        (BLOB_UPLOAD) runs for a given PR.

        Raises:
            pyodbc.Error: on any database connectivity or query failure.
        """
        self._log.debug(
            "Querying pipeline-entry PRs (not in ras_tracker)"
            + (f" TOP {self._limit}" if self._limit else "")
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._limit is not None:
                cursor.execute(self._PIPELINE_ENTRY_SQL_LIMITED, self._limit)
            else:
                cursor.execute(self._PIPELINE_ENTRY_SQL)

            pr_list = [str(row[0]) for row in cursor.fetchall()]
            cursor.close()
            self._log.info(f"Pipeline-entry PRs found: {len(pr_list)}")
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

            repo.fetch_prs_at_stage(PipelineStage.BLOB_UPLOAD)

        Parameters
        ----------
        completed_stage:
            The PipelineStage value that marks a PR as ready for the next step.
            Must match STAGE_NAME in the pipeline_stages table.

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
            self._log.info(
                f"PRs at stage={completed_stage!r} found: {len(pr_list)}"
            )
            return pr_list

        except pyodbc.Error as exc:
            self._log.error(
                f"Failed to fetch PRs at stage={completed_stage!r}: {exc}"
            )
            raise

        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> pyodbc.Connection:
        """Opens a read-only (autocommit) connection to the Azure SQL DB."""
        try:
            return pyodbc.connect(self._conn_str, autocommit=True, timeout=0)
        except pyodbc.Error as exc:
            self._log.error(f"Cannot connect to Azure SQL DB: {exc}")
            raise
