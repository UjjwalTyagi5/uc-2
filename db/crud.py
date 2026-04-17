"""
db.crud
~~~~~~~
Base repository class shared by every data-access class in this repo.

Usage
-----
Subclass BaseRepository and call the protected helpers instead of writing
the open/execute/commit/close boilerplate in every method:

    from db.crud import BaseRepository
    from db.tables import AzureTables

    class MyRepository(BaseRepository):

        _MY_SQL = f"SELECT * FROM {AzureTables.RAS_TRACKER} WHERE [purchase_req_no_fk] = ?"

        def get_row(self, pr_no: str):
            return self._fetch_one(self._MY_SQL, pr_no)

        def update_stage(self, pr_no: str, stage: str) -> int:
            return self._execute(
                f"UPDATE {AzureTables.RAS_TRACKER} SET [current_stage_fk] = ? "
                f"WHERE [purchase_req_no_fk] = ?",
                stage, pr_no,
            )

        def cleanup(self, pr_no: str) -> None:
            self._call_sp("[ras_procurement].[usp_cleanup_pr_data]", pr_no)

Helpers
-------
_fetch(sql, *params)        → list[Row]   SELECT, autocommit, no tx overhead
_fetch_one(sql, *params)    → Row | None  SELECT single row
_execute(sql, *params)      → int         DML — commits, returns rowcount
_execute_many(sql, rows)    → None        Bulk DML via fast_executemany
_call_sp(sp_expr, *params)  → None        Stored procedure call, commits

All helpers open a fresh connection per call and close it in a finally block
so connection leaks are impossible even under exceptions.
"""

from __future__ import annotations

from typing import Any, List, Optional, Sequence

import pyodbc
from loguru import logger

from db.connection import connect_with_retry


class BaseRepository:
    """
    Abstract base for all data-access classes.

    Parameters
    ----------
    conn_str:
        pyodbc connection string for the target database.
    autocommit_reads:
        If True (default), read helpers open connections with autocommit=True
        to avoid implicit transaction overhead on SELECT-only queries.
    """

    def __init__(self, conn_str: str, autocommit_reads: bool = True) -> None:
        self._conn_str        = conn_str
        self._autocommit_reads = autocommit_reads
        self._log             = logger.bind(component=type(self).__name__)

    # ── Connection helpers ─────────────────────────────────────────────────

    def _connect_read(self) -> pyodbc.Connection:
        """Open a read-only connection (autocommit, no implicit transaction)."""
        return connect_with_retry(self._conn_str, autocommit=self._autocommit_reads)

    def _connect_write(self) -> pyodbc.Connection:
        """Open a write connection (autocommit=False, caller must commit/rollback)."""
        return connect_with_retry(self._conn_str, autocommit=False)

    # ── Read helpers ───────────────────────────────────────────────────────

    def _fetch(self, sql: str, *params: Any) -> List[pyodbc.Row]:
        """
        Execute a SELECT and return all matching rows.

        Uses an autocommit connection — no transaction overhead.
        """
        conn = self._connect_read()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()
        except pyodbc.Error as exc:
            self._log.error(f"_fetch failed: {exc}\nSQL: {sql[:200]}")
            raise
        finally:
            conn.close()

    def _fetch_one(self, sql: str, *params: Any) -> Optional[pyodbc.Row]:
        """Execute a SELECT and return the first row, or None."""
        rows = self._fetch(sql, *params)
        return rows[0] if rows else None

    def _fetch_with_columns(
        self, sql: str, *params: Any
    ) -> tuple[List[pyodbc.Row], List[str]]:
        """
        Execute a SELECT and return (rows, column_names).

        Use when you need column names alongside the data — e.g. when
        building a dynamic INSERT from the result set.
        """
        conn = self._connect_read()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description] if cursor.description else []
            return cursor.fetchall(), columns
        except pyodbc.Error as exc:
            self._log.error(f"_fetch_with_columns failed: {exc}\nSQL: {sql[:200]}")
            raise
        finally:
            conn.close()

    def _fetch_from(
        self, conn_str: str, sql: str, *params: Any
    ) -> List[pyodbc.Row]:
        """
        Execute a SELECT against a *different* database than self._conn_str.

        Use when a repository needs to read from a secondary DB — e.g.
        AttachmentBlobSync reading binary docs from the on-prem RAS DB.
        """
        conn = connect_with_retry(conn_str, autocommit=True)
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            return cursor.fetchall()
        except pyodbc.Error as exc:
            self._log.error(f"_fetch_from failed: {exc}\nSQL: {sql[:200]}")
            raise
        finally:
            conn.close()

    def _fetch_from_with_columns(
        self, conn_str: str, sql: str, *params: Any
    ) -> tuple[List[pyodbc.Row], List[str]]:
        """_fetch_from variant that also returns column names."""
        conn = connect_with_retry(conn_str, autocommit=True)
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            columns = [col[0] for col in cursor.description] if cursor.description else []
            return cursor.fetchall(), columns
        except pyodbc.Error as exc:
            self._log.error(f"_fetch_from_with_columns failed: {exc}\nSQL: {sql[:200]}")
            raise
        finally:
            conn.close()

    # ── Write helpers ──────────────────────────────────────────────────────

    def _execute(self, sql: str, *params: Any) -> int:
        """
        Execute a single DML statement (INSERT / UPDATE / DELETE / MERGE).

        Commits on success, rolls back on error.

        Returns
        -------
        int
            Number of rows affected (cursor.rowcount).
        """
        conn = self._connect_write()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            affected = cursor.rowcount
            conn.commit()
            return affected
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(f"_execute failed: {exc}\nSQL: {sql[:200]}")
            raise
        finally:
            conn.close()

    def _execute_many(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        """
        Bulk DML using fast_executemany.

        Commits on success, rolls back on error.
        Skips silently if `rows` is empty.
        """
        if not rows:
            return
        conn = self._connect_write()
        try:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(sql, [list(r) for r in rows])
            conn.commit()
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(f"_execute_many failed: {exc}\nSQL: {sql[:200]}")
            raise
        finally:
            conn.close()

    def _call_sp(self, sp_expr: str, *params: Any) -> None:
        """
        Call a stored procedure.  `sp_expr` is the full EXEC expression
        including parameter placeholders, e.g.:
            "[ras_procurement].[usp_cleanup_pr_data] ?"

        Commits on success, rolls back on error.
        """
        sql = f"EXEC {sp_expr}"
        conn = self._connect_write()
        try:
            conn.cursor().execute(sql, params)
            conn.commit()
        except pyodbc.Error as exc:
            conn.rollback()
            self._log.error(f"_call_sp failed: {exc}\nSP: {sp_expr[:200]}")
            raise
        finally:
            conn.close()
