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

        _MY_SQL = f"SELECT * FROM {AzureTables.RAS_TRACKER} WHERE [purchase_req_no] = ?"

        def get_row(self, pr_no: str):
            return self._fetch_one(self._MY_SQL, pr_no)

        def update_stage(self, pr_no: str, stage_id: int) -> int:
            return self._execute(
                f"UPDATE {AzureTables.RAS_TRACKER} SET [current_stage_fk] = ? "
                f"WHERE [purchase_req_no] = ?",
                stage_id, pr_no,
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

Retry behaviour
---------------
All helpers retry automatically on transient Azure SQL / network errors
(throttling, brief failover, connection reset).  Each retry opens a fresh
pooled connection so stale connections are never re-used after a fault.

    Read helpers  : up to _MAX_RETRIES extra attempts, no side-effects
    Write helpers : rollback on each failed attempt, then retry from scratch
                    (safe because all write SQL in this repo is idempotent —
                    INSERT…MERGE, UPDATE, SP — so re-running is harmless)

Non-transient errors (bad SQL, wrong column name, auth failure) are
re-raised immediately without retrying.
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable, List, Optional, Sequence, TypeVar

import pyodbc
from loguru import logger

from db.connection import connect_with_retry, is_transient_error

_T = TypeVar("_T")

_MAX_RETRIES = 3       # extra attempts after first failure (4 total)
_BASE_DELAY  = 2.0     # starting back-off in seconds; doubles each retry


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
        self._conn_str         = conn_str
        self._autocommit_reads = autocommit_reads
        self._log              = logger.bind(component=type(self).__name__)

    # ── Connection helpers ─────────────────────────────────────────────────

    def _connect_read(self) -> pyodbc.Connection:
        return connect_with_retry(self._conn_str, autocommit=self._autocommit_reads)

    def _connect_write(self) -> pyodbc.Connection:
        return connect_with_retry(self._conn_str, autocommit=False)

    # ── Retry wrapper ──────────────────────────────────────────────────────

    def _retrying(self, fn: Callable[[], _T]) -> _T:
        """
        Call fn() and retry up to _MAX_RETRIES times on transient DB errors.

        fn() must open its own connection and close it in a finally block.
        On each retry a fresh connection is obtained so stale connections
        from a brief failover are never reused.
        """
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return fn()
            except pyodbc.Error as exc:
                if not is_transient_error(exc) or attempt == _MAX_RETRIES:
                    raise
                delay = _BASE_DELAY * (2 ** attempt) * (0.8 + 0.4 * random.random())
                self._log.warning(
                    f"Transient DB error — retrying query in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{_MAX_RETRIES + 1}): {exc}"
                )
                time.sleep(delay)
        raise RuntimeError("unreachable")  # satisfies type checkers

    # ── Read helpers ───────────────────────────────────────────────────────

    def _fetch(self, sql: str, *params: Any) -> List[pyodbc.Row]:
        """Execute a SELECT and return all matching rows."""
        def _run():
            conn = self._connect_read()
            try:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                return cursor.fetchall()
            except pyodbc.Error:
                self._log.error(f"_fetch failed\nSQL: {sql[:200]}")
                raise
            finally:
                conn.close()
        return self._retrying(_run)

    def _fetch_one(self, sql: str, *params: Any) -> Optional[pyodbc.Row]:
        """Execute a SELECT and return the first row, or None."""
        rows = self._fetch(sql, *params)
        return rows[0] if rows else None

    def _fetch_with_columns(
        self, sql: str, *params: Any
    ) -> tuple[List[pyodbc.Row], List[str]]:
        """Execute a SELECT and return (rows, column_names)."""
        def _run():
            conn = self._connect_read()
            try:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description] if cursor.description else []
                return cursor.fetchall(), columns
            except pyodbc.Error:
                self._log.error(f"_fetch_with_columns failed\nSQL: {sql[:200]}")
                raise
            finally:
                conn.close()
        return self._retrying(_run)

    def _fetch_from(
        self, conn_str: str, sql: str, *params: Any
    ) -> List[pyodbc.Row]:
        """Execute a SELECT against a different database than self._conn_str."""
        def _run():
            conn = connect_with_retry(conn_str, autocommit=True)
            try:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                return cursor.fetchall()
            except pyodbc.Error:
                self._log.error(f"_fetch_from failed\nSQL: {sql[:200]}")
                raise
            finally:
                conn.close()
        return self._retrying(_run)

    def _fetch_from_with_columns(
        self, conn_str: str, sql: str, *params: Any
    ) -> tuple[List[pyodbc.Row], List[str]]:
        """_fetch_from variant that also returns column names."""
        def _run():
            conn = connect_with_retry(conn_str, autocommit=True)
            try:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                columns = [col[0] for col in cursor.description] if cursor.description else []
                return cursor.fetchall(), columns
            except pyodbc.Error:
                self._log.error(f"_fetch_from_with_columns failed\nSQL: {sql[:200]}")
                raise
            finally:
                conn.close()
        return self._retrying(_run)

    # ── Write helpers ──────────────────────────────────────────────────────

    def _execute(self, sql: str, *params: Any) -> int:
        """
        Execute a single DML statement (INSERT / UPDATE / DELETE / MERGE).
        Commits on success, rolls back on error.
        Returns number of rows affected.
        """
        def _run():
            conn = self._connect_write()
            try:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                affected = cursor.rowcount
                conn.commit()
                return affected
            except pyodbc.Error:
                conn.rollback()
                self._log.error(f"_execute failed\nSQL: {sql[:200]}")
                raise
            finally:
                conn.close()
        return self._retrying(_run)

    def _execute_many(self, sql: str, rows: Sequence[Sequence[Any]]) -> None:
        """
        Bulk DML using fast_executemany.
        Commits on success, rolls back on error. Skips if rows is empty.
        """
        if not rows:
            return

        def _run():
            conn = self._connect_write()
            try:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                cursor.executemany(sql, [list(r) for r in rows])
                conn.commit()
            except pyodbc.Error:
                conn.rollback()
                self._log.error(f"_execute_many failed\nSQL: {sql[:200]}")
                raise
            finally:
                conn.close()
        self._retrying(_run)

    def _call_sp(self, sp_expr: str, *params: Any) -> None:
        """
        Call a stored procedure.  sp_expr is the full EXEC expression
        including parameter placeholders, e.g.:
            "[ras_procurement].[usp_cleanup_pr_data] ?"
        Commits on success, rolls back on error.
        """
        sql = f"EXEC {sp_expr}"

        def _run():
            conn = self._connect_write()
            try:
                conn.cursor().execute(sql, params)
                conn.commit()
            except pyodbc.Error:
                conn.rollback()
                self._log.error(f"_call_sp failed\nSP: {sp_expr[:200]}")
                raise
            finally:
                conn.close()
        self._retrying(_run)
