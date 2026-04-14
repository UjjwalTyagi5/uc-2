import pyodbc
from abc import ABC
from loguru import logger


class BaseSyncManager(ABC):

    def __init__(self, conn_str: str):
        self.conn_str = conn_str
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = pyodbc.connect(self.conn_str, autocommit=False)
            self.cursor = self.conn.cursor()
            logger.info("Connected to DB")
        except Exception as e:
            logger.error(f"Error while making connection with database: {e}")
            raise e

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("DB connection closed")

    def get_table_data(self, table_name: str):
        self.cursor.execute(f"SELECT * FROM {table_name}")
        rows = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        return rows, columns

    def execute_query(self, query: str, params=None):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

    def truncate_table(self, table_name: str):
        self.cursor.execute(f"TRUNCATE TABLE {table_name}")
        logger.info(f"Truncated table: {table_name}")
