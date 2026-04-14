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

    def get_table_data_in_batches(self, table_name: str, batch_size: int = 2000):
        """Fetches rows in batches. Also yields Python type per column for setinputsizes."""
        self.cursor.execute(f"SELECT * FROM {_quote_table(table_name)}")
        columns = [desc[0] for desc in self.cursor.description]
        col_types = [desc[1] for desc in self.cursor.description]  # Python type objects
        while True:
            rows = self.cursor.fetchmany(batch_size)
            if not rows:
                break
            yield rows, columns, col_types

    def execute_query(self, query: str, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def truncate_table(self, table_name: str):
        self.cursor.execute(f"TRUNCATE TABLE {_quote_table(table_name)}")
        logger.info(f"Truncated table: {table_name}")
