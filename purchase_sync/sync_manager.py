import pyodbc
import traceback
from datetime import datetime
from loguru import logger

from .db_manager import BaseSyncManager, _quote_table
from .config import SYNC_CONTROL_TABLE, SYNC_STATUS_TABLE

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
        logger.info(f"Syncing '{source_table}' → '{target_table}' (ETLId={etl_id})")

        try:
            self.truncate_table(target_table)
            self.conn.commit()

            total_rows = 0
            insert_sql = None

            for batch_rows, columns, col_types in self.source_manager.get_table_data_in_batches(source_table):
                if insert_sql is None:
                    insert_sql = self._build_insert_sql(target_table, columns)
                    # Fix fast_executemany string truncation: explicitly set max size
                    # for all string columns so pyodbc doesn't infer from first row.
                    # SQL_WVARCHAR with 4000 covers nvarchar(n); SQL_WLONGVARCHAR
                    # with 1073741823 covers nvarchar(max).
                    input_sizes = [
                        (pyodbc.SQL_WLONGVARCHAR, 1073741823, 0) if t == str else None
                        for t in col_types
                    ]
                    self.cursor.setinputsizes(input_sizes)
                    self.cursor.fast_executemany = True

                self.cursor.executemany(insert_sql, batch_rows)
                self.conn.commit()
                total_rows += len(batch_rows)
                logger.info(f"  '{target_table}' — {total_rows} rows inserted so far...")

            if total_rows == 0:
                logger.warning(f"No data found in source table '{source_table}', skipping.")

            logger.info(f"Synced {total_rows} rows into '{target_table}'")
            self._log_sync_status(etl_id, sync_time, "SUCCESS", total_rows, None)
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            error_msg = (repr(e) or str(e) or "unknown error").replace("\n", " ")[:500]
            logger.error(f"Error syncing '{source_table}' → '{target_table}': {error_msg}")
            logger.error(traceback.format_exc())
            try:
                self._log_sync_status(etl_id, sync_time, "FAILED", 0, error_msg)
                self.conn.commit()
            except Exception as log_err:
                self.conn.rollback()
                logger.critical(f"Failed to write error status for ETLId={etl_id}: {log_err}")

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
