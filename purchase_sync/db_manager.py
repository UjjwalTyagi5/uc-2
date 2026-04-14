import pyodbc
from abc import ABC
from loguru import logger


def _quote_table(table_name: str) -> str:
    """
    Wraps each part of a schema-qualified table name in square brackets.
    Handles both plain names and schema.table format.
    e.g. 'my-schema.my-table' → '[my-schema].[my-table]'
         'purchase_req_mst'   → '[purchase_req_mst]'
    """
    return ".".join(f"[{part.strip('[]')}]" for part in table_name.split("."))


class BaseSyncManager(ABC):

    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = pyodbc.connect(self.conn_str, autocommit=False)
            self.conn.timeout = 0  # no query execution timeout
            self.cursor = self.conn.cursor()
            logger.info("Connected to DB")
        except Exception as e:
            logger.error(f"Error while making connection with database: {e}")
            raise e

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("DB connection closed")

    def reset_cursor(self):
        """Close the current cursor and open a fresh one on the same connection."""
        try:
            if self.cursor:
                self.cursor.close()
        except Exception:
            pass
        self.cursor = self.conn.cursor()

    def get_row_count(self, table_name: str) -> int:
        """Returns total row count for a table."""
        self.cursor.execute(f"SELECT COUNT(1) FROM {_quote_table(table_name)}")
        return self.cursor.fetchone()[0]

    def get_table_data_in_batches(self, table_name: str, batch_size: int = 5000):
        """Fetches rows in batches using OFFSET/FETCH pagination.

        Each batch is a separate short-lived query instead of one streaming
        cursor held open for the full table duration. This prevents on-prem
        server timeouts on large tables (a streaming SELECT * on 575K rows
        can run for 4+ minutes and get killed by the server).
        """
        quoted = _quote_table(table_name)
        offset = 0
        columns = None
        col_types = None

        while True:
            self.cursor.execute(
                f"SELECT * FROM {quoted} "
                f"ORDER BY (SELECT NULL) "
                f"OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
            )
            rows = self.cursor.fetchall()
            if not rows:
                break
            if columns is None:
                columns = [desc[0] for desc in self.cursor.description]
                col_types = [desc[1] for desc in self.cursor.description]
            yield rows, columns, col_types
            offset += len(rows)

    def execute_query(self, query: str, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def truncate_table(self, table_name: str):
        self.cursor.execute(f"TRUNCATE TABLE {_quote_table(table_name)}")
        logger.info(f"Truncated table: {table_name}")
