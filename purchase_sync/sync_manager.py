import pyodbc
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from loguru import logger

from .db_manager import BaseSyncManager, _quote_table
from .config import SYNC_CONTROL_TABLE, SYNC_STATUS_TABLE, BATCH_SIZE, AZURE_BATCH_SIZE, PARALLEL_WORKERS

_CTRL = _quote_table(SYNC_CONTROL_TABLE)
_STAT = _quote_table(SYNC_STATUS_TABLE)

_MAX_RETRIES = 3        # retry a single chunk on transient connection failure
_MAX_TABLE_RETRIES = 2  # retry a full table sync on connection failure

GET_SYNC_CONTROL_DATA = f"""
    SELECT ETLId, Source, Destination, SyncHours
    FROM {_CTRL}
    WHERE EnableSync = 1
    ORDER BY ETLId ASC
"""

GET_SYNC_STATUS_BY_ID = f"""
    SELECT TOP 1 LastSyncTime
    FROM {_STAT}
    WHERE ETLId = ?
    ORDER BY LastSyncTime DESC
"""

INSERT_SYNC_STATUS = f"""
    INSERT INTO {_STAT}
        (ETLId, LastSyncTime, SyncStatus, NumberOfRecordFetched, ErrorMessage)
    VALUES (?, ?, ?, ?, ?)
"""


class PurchaseSyncManager(BaseSyncManager):

    def __init__(self, conn_str: str, source_manager: BaseSyncManager):
        super().__init__(conn_str)
        self.source_manager = source_manager

    def sync_all_tables(self):
        try:
            self.connect()
            self.cursor.execute(GET_SYNC_CONTROL_DATA)
            configs = self.cursor.fetchall()
            if not configs:
                logger.warning(f"No enabled sync entries found in {SYNC_CONTROL_TABLE}")
                return

            target_tables = [cfg[2] for cfg in configs]

            # Disable FK constraints on all target tables (sequential, main connection)
            for table in target_tables:
                self._toggle_fk_constraints(table, enable=False)

            # Sync all tables in parallel — each worker gets its own DB connections
            with ThreadPoolExecutor(max_workers=len(configs)) as executor:
                futures = {
                    executor.submit(self._sync_table_worker, cfg): cfg
                    for cfg in configs
                }
                for future in as_completed(futures):
                    cfg = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(
                            f"Worker failed for '{cfg[1]}' → '{cfg[2]}': {e}"
                        )

            # Re-enable and validate FK constraints after all tables are loaded
            for table in target_tables:
                self._toggle_fk_constraints(table, enable=True)

        except Exception as e:
            logger.error(f"Error in sync_all_tables: {e}")
            raise e
        finally:
            self.close()

    def _sync_table_worker(self, cfg):
        """Runs sync_single_table in its own thread with dedicated DB connections.

        Retries the full table up to _MAX_TABLE_RETRIES times on connection
        failures. Before each retry the staging table is explicitly dropped and
        logged so we always start from a clean state.
        """
        etl_id, source_table, target_table, sync_hours = cfg
        staging_table = self._get_staging_name(target_table)

        for attempt in range(1, _MAX_TABLE_RETRIES + 1):
            worker_source = BaseSyncManager(self.source_manager.conn_str)
            worker = PurchaseSyncManager(self.conn_str, worker_source)
            try:
                worker.connect()
                worker_source.connect()

                # Ensure any leftover staging from a previous attempt is gone
                worker._drop_table_if_exists(staging_table)
                worker.conn.commit()

                worker.sync_single_table(etl_id, source_table, target_table, sync_hours)
                return  # success — exit retry loop

            except pyodbc.OperationalError as e:
                err = (repr(e) or str(e)).replace("\n", " ")[:300]
                if attempt < _MAX_TABLE_RETRIES:
                    logger.warning(
                        f"[ETLId={etl_id}] Table sync failed "
                        f"(attempt {attempt}/{_MAX_TABLE_RETRIES}), will retry "
                        f"'{source_table}' → '{target_table}': {err}"
                    )
                else:
                    logger.error(
                        f"[ETLId={etl_id}] All {_MAX_TABLE_RETRIES} attempts failed "
                        f"for '{source_table}' → '{target_table}': {err}"
                    )
                    raise

            finally:
                try:
                    worker_source.close()
                except Exception:
                    pass
                try:
                    worker.close()
                except Exception:
                    pass

    def _load_staging_parallel(self, source_table: str, staging_table: str,
                               total_source: int, insert_sql: str) -> int:
        """Loads all source rows into staging using PARALLEL_WORKERS threads.

        Each worker gets an exclusive row slice (by OFFSET/FETCH), opens its
        own fresh source + Azure connections, and inserts via AZURE_BATCH_SIZE
        commits with retry. No shared connection means no idle timeouts.
        Returns total rows inserted.
        """
        segment_size = max(1, (total_source + PARALLEL_WORKERS - 1) // PARALLEL_WORKERS)
        total_inserted = 0
        lock = threading.Lock()

        def worker(start_offset: int, max_rows: int):
            nonlocal total_inserted

            current_offset = start_offset
            rows_done = 0

            while rows_done < max_rows:
                remaining = max_rows - rows_done

                # Fresh source + Azure connections for every batch —
                # guarantees no stale/idle connection can time out
                src = BaseSyncManager(self.source_manager.conn_str)
                src.connect()
                az_conn = pyodbc.connect(self.conn_str, autocommit=False)
                az_conn.timeout = 0
                az_cur = az_conn.cursor()

                try:
                    fetch_size = min(BATCH_SIZE, remaining)
                    src.cursor.execute(
                        f"SELECT * FROM {_quote_table(source_table)} "
                        f"ORDER BY (SELECT NULL) "
                        f"OFFSET {current_offset} ROWS FETCH NEXT {fetch_size} ROWS ONLY"
                    )
                    batch_rows = src.cursor.fetchall()

                    if not batch_rows:
                        break  # no more rows

                    for ci in range(0, len(batch_rows), AZURE_BATCH_SIZE):
                        chunk = batch_rows[ci:ci + AZURE_BATCH_SIZE]

                        for attempt in range(1, _MAX_RETRIES + 1):
                            try:
                                az_cur.fast_executemany = True
                                az_cur.executemany(insert_sql, chunk)
                                az_conn.commit()
                                break
                            except pyodbc.OperationalError as e:
                                try:
                                    az_conn.rollback()
                                except Exception:
                                    pass
                                if attempt < _MAX_RETRIES:
                                    logger.warning(
                                        f"  Azure chunk retry "
                                        f"{attempt}/{_MAX_RETRIES}: {e}"
                                    )
                                    try:
                                        az_conn.close()
                                    except Exception:
                                        pass
                                    az_conn = pyodbc.connect(
                                        self.conn_str, autocommit=False
                                    )
                                    az_conn.timeout = 0
                                    az_cur = az_conn.cursor()
                                else:
                                    raise

                        with lock:
                            total_inserted += len(chunk)
                            pct = total_inserted / total_source * 100
                            logger.info(
                                f"  '{staging_table}' — "
                                f"{total_inserted:,} / {total_source:,} "
                                f"rows ({pct:.1f}%)"
                            )

                    current_offset += len(batch_rows)
                    rows_done += len(batch_rows)

                except pyodbc.OperationalError as e:
                    logger.warning(
                        f"  On-prem connection lost at offset {current_offset}, "
                        f"reconnecting and retrying batch: {e}"
                    )
                    # current_offset is unchanged — next loop iteration
                    # reopens connections and retries the same batch
                finally:
                    try:
                        src.close()
                    except Exception:
                        pass
                    try:
                        az_conn.close()
                    except Exception:
                        pass

        with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
            futures = []
            for i in range(PARALLEL_WORKERS):
                start = i * segment_size
                rows = min(segment_size, total_source - start)
                if rows > 0:
                    futures.append(executor.submit(worker, start, rows))
            for f in as_completed(futures):
                f.result()  # re-raise any worker exception

        return total_inserted

    def sync_single_table(self, etl_id: int, source_table: str, target_table: str, sync_hours: int):
        sync_time = datetime.now()
        staging_table = self._get_staging_name(target_table)
        logger.info(f"Syncing '{source_table}' → '{target_table}' (ETLId={etl_id})")

        # Fresh cursors for every table — prevents stale state from a previous
        # table's error (e.g. Invalid cursor state, leftover result sets)
        self.reset_cursor()
        self.source_manager.reset_cursor()

        # Clean up any leftover staging table from a previous failed run
        self._drop_table_if_exists(staging_table)

        try:
            # Get total source row count upfront for progress reporting
            total_source = self.source_manager.get_row_count(source_table)
            logger.info(f"  Source has {total_source:,} rows — batch size: {BATCH_SIZE:,}")

            if total_source == 0:
                logger.warning(f"No data in source '{source_table}', skipping.")
                self._log_sync_status(etl_id, sync_time, "SUCCESS", 0, None)
                self.conn.commit()
                return

            # Step 1 — create staging table with same schema as target (empty)
            self._create_staging_table(target_table, staging_table)
            self.conn.commit()
            logger.info(f"  Created staging table '{staging_table}'")

            # Step 2 — load all rows into staging using parallel workers.
            # Each worker owns an independent slice of rows (by OFFSET), opens
            # its own fresh source + Azure connections, and inserts via small
            # AZURE_BATCH_SIZE commits. No shared connection = no idle timeouts.
            columns = self.source_manager.get_columns(source_table)
            insert_sql = self._build_insert_sql(staging_table, columns)
            logger.info(
                f"  Loading into staging with {PARALLEL_WORKERS} parallel workers "
                f"(Azure batch: {AZURE_BATCH_SIZE:,} rows each)..."
            )
            total_rows = self._load_staging_parallel(
                source_table, staging_table, total_source, insert_sql
            )

            # Step 3 — all batches loaded into staging successfully.
            # Now atomically swap: TRUNCATE target + copy from staging in ONE commit.
            logger.info(f"  All {total_rows:,} rows in staging — swapping into '{target_table}'...")
            self.truncate_table(target_table)
            quoted_target = _quote_table(target_table)
            quoted_staging = _quote_table(staging_table)
            self.execute_query(f"INSERT INTO {quoted_target} SELECT * FROM {quoted_staging}")
            self._log_sync_status(etl_id, sync_time, "SUCCESS", total_rows, None)
            self.conn.commit()  # single commit: staging insert + truncate + copy + status log
            logger.info(f"  Synced {total_rows:,} rows into '{target_table}' — committed.")

            # Step 4 — drop staging (cleanup; not part of data transaction)
            self._drop_table_if_exists(staging_table)
            self.conn.commit()

        except Exception as e:
            # Connection may be dead — rollback only if it's still alive
            try:
                self.conn.rollback()
            except Exception:
                pass
            # Drop staging so it doesn't block the next run
            try:
                self._drop_table_if_exists(staging_table)
                self.conn.commit()
            except Exception:
                pass

            error_msg = (repr(e) or str(e) or "unknown error").replace("\n", " ")[:500]
            logger.error(f"Error syncing '{source_table}' → '{target_table}': {error_msg}")
            logger.error(traceback.format_exc())
            logger.warning(f"  Target table '{target_table}' was NOT truncated — old data is intact.")

            try:
                self._log_sync_status(etl_id, sync_time, "FAILED", 0, error_msg)
                self.conn.commit()
            except Exception as log_err:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                logger.critical(f"Failed to write error status for ETLId={etl_id}: {log_err}")

    def _ensure_connected(self):
        """Ping the Azure connection; silently reconnect if it has been dropped.

        Azure SQL / the intervening load-balancer can kill idle TCP connections
        in as little as ~4 minutes (and sometimes sooner). This is called before
        every Azure write so a dropped connection is recovered transparently.
        Previously committed staging batches survive the reconnect because they
        are already persisted in the staging table.
        """
        try:
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
        except Exception:
            logger.warning("Azure connection lost — reconnecting...")
            try:
                self.conn.close()
            except Exception:
                pass
            self.connect()

    def _get_staging_name(self, table_name: str) -> str:
        """Returns staging table name, preserving schema if present.
        e.g. 'dbo.purchase_req_mst' → 'dbo.purchase_req_mst_staging'
             'purchase_req_mst'      → 'purchase_req_mst_staging'
        """
        if "." in table_name:
            schema, name = table_name.rsplit(".", 1)
            return f"{schema}.{name}_staging"
        return f"{table_name}_staging"

    def _create_staging_table(self, source_table: str, staging_table: str):
        """Creates an empty staging table mirroring the target table's schema."""
        quoted_source = _quote_table(source_table)
        quoted_staging = _quote_table(staging_table)
        self.execute_query(f"SELECT TOP 0 * INTO {quoted_staging} FROM {quoted_source}")

    def _drop_table_if_exists(self, table_name: str):
        """Drops a table if it exists and logs when it actually does."""
        quoted = _quote_table(table_name)
        safe_name = table_name.replace(chr(39), chr(39) + chr(39))
        # Check existence first so we can log accurately
        self.cursor.execute(
            f"SELECT 1 WHERE OBJECT_ID(N'{safe_name}', N'U') IS NOT NULL"
        )
        exists = self.cursor.fetchone() is not None
        if exists:
            self.execute_query(f"DROP TABLE {quoted}")
            logger.info(f"  Dropped staging table '{table_name}'")

    def _build_insert_sql(self, table_name: str, columns: list) -> str:
        col_list = ", ".join(f"[{col}]" for col in columns)
        placeholders = ", ".join("?" for _ in columns)
        return f"INSERT INTO {_quote_table(table_name)} ({col_list}) VALUES ({placeholders})"

    def _toggle_fk_constraints(self, table_name: str, enable: bool):
        quoted = _quote_table(table_name)
        action = "WITH CHECK CHECK CONSTRAINT ALL" if enable else "NOCHECK CONSTRAINT ALL"
        label = "Re-enabled" if enable else "Disabled"
        try:
            self.execute_query(f"ALTER TABLE {quoted} {action}")
            self.conn.commit()
            logger.info(f"{label} FK constraints on '{table_name}'")
        except Exception as e:
            self.conn.rollback()
            logger.warning(
                f"Could not toggle FK constraints on '{table_name}' (skipping — "
                f"no FK constraints defined or insufficient permissions): {e}"
            )

    def _log_sync_status(self, etl_id: int, sync_time: datetime,
                         status: str, record_count: int, error_msg):
        self.execute_query(
            INSERT_SYNC_STATUS,
            (etl_id, sync_time, status, record_count, error_msg)
        )
