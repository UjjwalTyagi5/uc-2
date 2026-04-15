"""
pipeline.repository
~~~~~~~~~~~~~~~~~~~
Data-access layer for the pipeline.

Responsible for all SQL interactions that the orchestrator needs:
    - Fetching the list of pending PRs (not yet in ras_tracker)

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
        ras_procurement schema (purchase_req_mst, ras_tracker).
    limit:
        Optional cap on the number of pending PRs returned.  Useful for
        processing large backlogs in controlled batches (e.g. 100 per run).
        Pass None (default) to return all pending PRs.
    """

    _PENDING_SQL = """
        SELECT prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no_fk]
        WHERE rt.[purchase_req_no_fk] IS NULL
        ORDER BY prm.[C_DATETIME] ASC
    """

    _PENDING_SQL_LIMITED = """
        SELECT TOP (?) prm.[PURCHASE_REQ_NO]
        FROM   [ras_procurement].[purchase_req_mst] prm
        LEFT JOIN [ras_procurement].[ras_tracker]   rt
          ON prm.[PURCHASE_REQ_NO] = rt.[purchase_req_no_fk]
        WHERE rt.[purchase_req_no_fk] IS NULL
        ORDER BY prm.[C_DATETIME] ASC
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
        processed (no row in ras_tracker), ordered oldest-first.

        A limit set at construction time caps the result set so large backlogs
        can be processed incrementally across multiple pipeline runs.

        Raises:
            pyodbc.Error: on any database connectivity or query failure.
        """
        conn = self._connect()
        try:
            cursor = conn.cursor()
            if self._limit is not None:
                self._log.debug(f"Querying pending PRs with TOP {self._limit}")
                cursor.execute(self._PENDING_SQL_LIMITED, self._limit)
            else:
                self._log.debug("Querying all pending PRs (no limit)")
                cursor.execute(self._PENDING_SQL)

            rows = cursor.fetchall()
            cursor.close()
            pr_list = [str(row[0]) for row in rows]
            self._log.info(f"Pending PRs found: {len(pr_list)}")
            return pr_list

        except pyodbc.Error as exc:
            self._log.error(f"Failed to fetch pending PRs: {exc}")
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
