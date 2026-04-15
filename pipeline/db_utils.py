"""
pipeline.db_utils
~~~~~~~~~~~~~~~~~
Shared database connection helpers for the pipeline.

connect_with_retry(conn_str, autocommit, max_attempts, base_delay)
    Opens a pyodbc connection with exponential-backoff retry on transient
    Azure SQL errors.  Replaces bare pyodbc.connect() in all pipeline
    components so a momentary server hiccup doesn't kill a 5-lakh-record run.

Transient errors retried
------------------------
Azure SQL / SQL Server error numbers that are safe to retry:
    40613  Database not currently available
    40197  Service encountered an error processing the request
    40501  Service is currently busy
    40614  Elastic pool is too busy
    49918  Cannot process request — not enough resources
    49919  Cannot create or update request — too many create/update operations
    49920  Too many operations in progress
    10928  Resource ID reached limit
    10929  Resource ID below minimum / above maximum
    10053  Transport-level error — connection reset
    10054  Existing connection forcibly closed by remote host
    10060  Connection attempt failed (timeout reaching server)

SQL states retried
------------------
    08S01  Communication link failure (mid-session network drop)
    HYT00  Timeout expired (connection attempt)
    HYT01  Connection timeout expired

All other errors are re-raised immediately without retry.
"""

from __future__ import annotations

import random
import time

import pyodbc
from loguru import logger

# Azure SQL numeric error codes that are transient / safe to retry
_TRANSIENT_CODES = {
    40613, 40197, 40501, 40614,
    49918, 49919, 49920,
    10928, 10929,
    10053, 10054, 10060,
}

# ODBC SQL states that indicate a transient connectivity failure
_TRANSIENT_STATES = {"08S01", "HYT00", "HYT01"}


def _is_transient(exc: pyodbc.Error) -> bool:
    """Returns True if the exception looks like a retryable transient error."""
    # Check SQL state (first element of the exception args tuple)
    sql_state = exc.args[0] if exc.args else ""
    if str(sql_state) in _TRANSIENT_STATES:
        return True

    # Check for known error numbers embedded in the message string
    message = str(exc)
    return any(str(code) in message for code in _TRANSIENT_CODES)


def connect_with_retry(
    conn_str: str,
    autocommit: bool = False,
    max_attempts: int = 5,
    base_delay: float = 2.0,
) -> pyodbc.Connection:
    """
    Opens a pyodbc connection with exponential-backoff retry.

    Parameters
    ----------
    conn_str:
        pyodbc connection string.
    autocommit:
        Passed directly to pyodbc.connect().
    max_attempts:
        Maximum number of attempts before re-raising the last error.
        Default 5 → delays of ~2 s, ~4 s, ~8 s, ~16 s before final attempt.
    base_delay:
        Base delay in seconds for the first retry (doubles each attempt,
        ±20 % random jitter).

    Raises
    ------
    pyodbc.Error
        On non-transient errors (immediately) or after all attempts are
        exhausted on transient errors.
    """
    last_exc: pyodbc.Error | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            # timeout=30 — fail the *connection attempt* quickly instead of
            # hanging forever; does not limit query execution time.
            return pyodbc.connect(conn_str, autocommit=autocommit, timeout=30)

        except pyodbc.Error as exc:
            last_exc = exc

            if not _is_transient(exc) or attempt == max_attempts:
                logger.error(
                    f"DB connection failed "
                    f"(attempt {attempt}/{max_attempts}): {exc}"
                )
                raise

            delay = base_delay * (2 ** (attempt - 1)) * (0.8 + 0.4 * random.random())
            logger.warning(
                f"Transient DB error — retrying in {delay:.1f}s "
                f"(attempt {attempt}/{max_attempts}): {exc}"
            )
            time.sleep(delay)

    raise last_exc  # unreachable, but satisfies type checkers
