import pyodbc
import traceback
from datetime import datetime
from loguru import logger

from .db_manager import BaseSyncManager, _quote_table
from .config import SYNC_CONTROL_TABLE, SYNC_STATUS_TABLE, BATCH_SIZE

_CTRL = _quote_table(SYNC_CONTROL_TABLE)
_STAT = _quote_table(SYNC_STATUS_TABLE)

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
            self.source_manager.connect()
            self.cursor.execute(GET_SYNC_CONTROL_DATA)
            configs = self.cursor.fetchall()
            if not configs:
                logger.warning(f"No enabled sync entries found in {SYNC_CONTROL_TABLE}")
                return

            target_tables = [cfg[2] for cfg in configs]

            # Disable FK constraints on all target tables before any truncate/insert
            for table in target_tables:
                self._toggle_fk_constraints(table, enable=False)

            # Sync in ETLId order (parent table first, children after)
            for cfg in configs:
                self.sync_single_table(*cfg)

            # Re-enable and validate FK constraints after all tables are loaded
            for table in target_tables:
                self._toggle_fk_constraints(table, enable=True)

        except Exception as e:
            logger.error(f"Error in sync_all_tables: {e}")
            raise e
        finally:
            self.source_manager.close()
            self.close()

    def sync_single_table(self, etl_id: int, source_table: str, target_table: str, sync_hours: int):
        sync_time = datetime.now()
        staging_table = self._get_staging_name(target_table)
        logger.info(f"Syncing '{source_table}' → '{target_table}' (ETLId={etl_id})")

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

            total_rows = 0
            insert_sql = None

            # Step 2 — stream ALL batches into staging (no commit between batches)
            for batch_rows, columns, col_types in self.source_manager.get_table_data_in_batches(source_table, BATCH_SIZE):
                if insert_sql is None:
                    insert_sql = self._build_insert_sql(staging_table, columns)
                    self.cursor.fast_executemany = True

                # Compute actual max string length per column in this batch.
                # Avoids OOM (no huge pre-allocation) and truncation (buffer = real max).
                input_sizes = []
                for i, t in enumerate(col_types):
                    if t == str:
                        max_len = max(
                            (len(row[i]) for row in batch_rows if row[i] is not None),
                            default=1
                        )
                        input_sizes.append((pyodbc.SQL_WVARCHAR, max_len, 0))
                    else:
                        input_sizes.append(None)
                self.cursor.setinputsizes(input_sizes)

                self.cursor.executemany(insert_sql, batch_rows)
                # No commit here — keep everything in the transaction
                total_rows += len(batch_rows)
                pct = (total_rows / total_source * 100) if total_source else 0
                logger.info(f"  '{staging_table}' — {total_rows:,} / {total_source:,} rows ({pct:.1f}%)")

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
            self.conn.rollback()
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
                self.conn.rollback()
                logger.critical(f"Failed to write error status for ETLId={etl_id}: {log_err}")

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
        """Drops a table only if it exists (safe to call even if absent)."""
        quoted = _quote_table(table_name)
        # OBJECT_ID with 'U' checks for user tables only
        self.execute_query(
            f"IF OBJECT_ID(N'{table_name.replace(chr(39), chr(39)+chr(39))}', N'U') IS NOT NULL "
            f"DROP TABLE {quoted}"
        )

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
