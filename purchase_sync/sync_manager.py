from datetime import datetime
from loguru import logger

from .db_manager import BaseSyncManager
from .config import SYNC_CONTROL_TABLE, SYNC_STATUS_TABLE


GET_SYNC_CONTROL_DATA = f"""
    SELECT ETLId, Source, Destination, SyncHours
    FROM {SYNC_CONTROL_TABLE}
    WHERE EnableSync = 1
    ORDER BY ETLId ASC
"""

GET_SYNC_STATUS_BY_ID = f"""
    SELECT TOP 1 LastSyncTime
    FROM {SYNC_STATUS_TABLE}
    WHERE ETLId = ?
    ORDER BY LastSyncTime DESC
"""

INSERT_SYNC_STATUS = f"""
    INSERT INTO {SYNC_STATUS_TABLE}
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
            rows, columns = self.source_manager.get_table_data(source_table)

            if not rows:
                logger.warning(f"No data found in source table '{source_table}', skipping.")
                self._log_sync_status(etl_id, sync_time, "SUCCESS", 0, None)
                self.conn.commit()
                return

            self.truncate_table(target_table)

            insert_sql = self._build_insert_sql(target_table, columns)
            self.cursor.fast_executemany = True
            self.cursor.executemany(insert_sql, rows)
            self.conn.commit()

            logger.info(f"Synced {len(rows)} rows into '{target_table}'")

            self._log_sync_status(etl_id, sync_time, "SUCCESS", len(rows), None)
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            error_msg = str(e).replace("\n", " ")[:300]
            logger.error(f"Error syncing '{source_table}' → '{target_table}': {error_msg}")
            try:
                self._log_sync_status(etl_id, sync_time, "FAILED", 0, error_msg)
                self.conn.commit()
            except Exception as log_err:
                self.conn.rollback()
                logger.critical(f"Failed to write error status for ETLId={etl_id}: {log_err}")

    def _build_insert_sql(self, table_name: str, columns: list) -> str:
        col_list = ", ".join(f"[{col}]" for col in columns)
        placeholders = ", ".join("?" for _ in columns)
        return f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

    def _toggle_fk_constraints(self, table_name: str, enable: bool):
        if enable:
            self.execute_query(f"ALTER TABLE {table_name} WITH CHECK CHECK CONSTRAINT ALL")
            logger.info(f"Re-enabled FK constraints on '{table_name}'")
        else:
            self.execute_query(f"ALTER TABLE {table_name} NOCHECK CONSTRAINT ALL")
            logger.info(f"Disabled FK constraints on '{table_name}'")
        self.conn.commit()

    def _log_sync_status(self, etl_id: int, sync_time: datetime,
                         status: str, record_count: int, error_msg):
        self.execute_query(
            INSERT_SYNC_STATUS,
            (etl_id, sync_time, status, record_count, error_msg)
        )
