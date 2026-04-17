"""
db.connection
~~~~~~~~~~~~~
One place for all database connections across the entire repo.

    from db.connection import get_connection

    conn = get_connection(config.get_azure_conn_str())   # Azure SQL
    conn = get_connection(config.get_ras_conn_str())     # On-prem SQL Server

How it works
------------
1. Connection pooling
   pyodbc.pooling = True tells the ODBC Driver Manager to keep idle
   connections alive and reuse them.  Opening a "new" connection just
   checks out one from the pool — no TCP handshake, no TLS negotiation.
   Works for both Azure SQL and on-prem SQL Server automatically.

2. Automatic retry on timeouts / transient errors
   Azure SQL and on-prem SQL Server both occasionally return temporary
   errors (throttling, brief network blip, connection reset).  Instead of
   crashing the whole pipeline we wait a moment and try again.
   Strategy: up to 5 attempts, delay doubles each time (~2s → 4s → 8s → 16s)
   with ±20% random jitter so multiple workers don't all retry at once.

3. Fast-fail on real errors
   Wrong password, unknown database, invalid connection string — these will
   never succeed on retry, so we re-raise immediately without waiting.
"""

from __future__ import annotations

import random
import time

import pyodbc
from loguru import logger

# ── Connection pooling ────────────────────────────────────────────────────
# The ODBC Driver Manager maintains a pool of open connections per
# connection string.  When your code calls pyodbc.connect() it hands back
# an idle connection from the pool instead of opening a new one.
# Set once at import time — affects all connections in this process.
pyodbc.pooling = True


# ── Transient errors: what we retry ──────────────────────────────────────

# Azure SQL throttling / resource errors  (numeric error codes in the message)
_RETRY_CODES = frozenset({
    40613,  # Database not currently available (restart / failover)
    40197,  # Service error processing the request (temporary)
    40501,  # Service is busy — try again in 10 seconds
    40614,  # Elastic pool is too busy
    49918,  # Not enough resources to process the request
    49919,  # Too many create/update operations in progress
    49920,  # Service is busy processing multiple requests
    10928,  # Resource limit reached — request will be terminated
    10929,  # Resource below minimum / above maximum
    10053,  # Transport-level error: connection reset by server
    10054,  # Existing connection forcibly closed by remote host
    10060,  # Connection attempt failed — timeout reaching server
})

# ODBC SQL states  (first element of the exception args tuple)
_RETRY_STATES = frozenset({
    "08S01",  # Communication link failure (mid-session network drop)
    "HYT00",  # Timeout expired — connection attempt timed out
    "HYT01",  # Connection timeout expired
})


def _is_transient(exc: pyodbc.Error) -> bool:
    """Return True if this error is temporary and safe to retry."""
    sql_state = str(exc.args[0]) if exc.args else ""
    if sql_state in _RETRY_STATES:
        return True
    # Numeric codes appear in the string representation of the exception
    message = str(exc)
    return any(str(code) in message for code in _RETRY_CODES)


# ── Main public function ──────────────────────────────────────────────────

def get_connection(
    conn_str: str,
    autocommit: bool = False,
    max_retries: int = 4,
    base_delay: float = 2.0,
) -> pyodbc.Connection:
    """
    Return a pooled database connection, retrying on transient errors.

    Parameters
    ----------
    conn_str    : pyodbc connection string (Azure or on-prem)
    autocommit  : True for read-only queries; False (default) for writes
    max_retries : how many extra attempts after the first failure  (default 4
                  means up to 5 total: 1 try + 4 retries)
    base_delay  : starting back-off in seconds; doubles each retry

    Raises
    ------
    pyodbc.Error
        Immediately on non-transient errors (bad credentials, unknown DB…)
        After all retries are exhausted on transient errors.

    Examples
    --------
        conn = get_connection(config.get_azure_conn_str())
        conn = get_connection(config.get_ras_conn_str(), autocommit=True)
    """
    last_error: pyodbc.Error | None = None

    for attempt in range(max_retries + 1):   # 0 = first try, 1..max = retries
        try:
            # timeout=30: fail the *connection attempt* in 30 s instead of
            # hanging forever. Does not limit query execution time.
            return pyodbc.connect(conn_str, autocommit=autocommit, timeout=30)

        except pyodbc.Error as exc:
            last_error = exc

            # Non-transient or out of retries → fail immediately
            if not _is_transient(exc) or attempt == max_retries:
                logger.error(
                    f"DB connection failed (attempt {attempt + 1}/{max_retries + 1}): {exc}"
                )
                raise

            # Transient → wait and try again
            delay = base_delay * (2 ** attempt) * (0.8 + 0.4 * random.random())
            logger.warning(
                f"Transient DB error — retrying in {delay:.1f}s "
                f"(attempt {attempt + 1}/{max_retries + 1}): {exc}"
            )
            time.sleep(delay)

    raise last_error  # unreachable — satisfies type checkers


# Alias kept so callers that already use connect_with_retry() don't need
# to change — both names point to the same function.
connect_with_retry = get_connection
