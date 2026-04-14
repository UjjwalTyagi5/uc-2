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

    def get_row_count(self, table_name: str, where_clause: str = None) -> int:
        """Returns total row count for a table, with an optional WHERE filter."""
        where_sql = f" WHERE {where_clause}" if where_clause else ""
        self.cursor.execute(f"SELECT COUNT(1) FROM {_quote_table(table_name)}{where_sql}")
        return self.cursor.fetchone()[0]

    def get_columns(self, table_name: str) -> list:
        """Returns column names for a table without fetching any rows."""
        self.cursor.execute(f"SELECT TOP 0 * FROM {_quote_table(table_name)}")
        return [desc[0] for desc in self.cursor.description]

    def get_table_data_in_batches(self, table_name: str, batch_size: int = 5000,
                                   start_offset: int = 0, max_rows: int = None):
        """Fetches rows in batches using OFFSET/FETCH pagination.

        Each batch is a separate short-lived query — no long-running cursor.
        start_offset and max_rows allow parallel workers to each own a slice.
        """
        quoted = _quote_table(table_name)
        offset = start_offset
        rows_yielded = 0
        columns = None
        col_types = None

        while True:
            fetch_size = batch_size
            if max_rows is not None:
                remaining = max_rows - rows_yielded
                if remaining <= 0:
                    break
                fetch_size = min(batch_size, remaining)

            self.cursor.execute(
                f"SELECT * FROM {quoted} "
                f"ORDER BY (SELECT NULL) "
                f"OFFSET {offset} ROWS FETCH NEXT {fetch_size} ROWS ONLY"
            )
            rows = self.cursor.fetchall()
            if not rows:
                break
            if columns is None:
                columns = [desc[0] for desc in self.cursor.description]
                col_types = [desc[1] for desc in self.cursor.description]
            yield rows, columns, col_types
            offset += len(rows)
            rows_yielded += len(rows)

    def execute_query(self, query: str, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def truncate_table(self, table_name: str):
        self.cursor.execute(f"TRUNCATE TABLE {_quote_table(table_name)}")
        logger.info(f"Truncated table: {table_name}")
