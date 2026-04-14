import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is missing or empty. Check your .env file.")
    return value


SYNC_CONTROL_TABLE = _require_env("SYNC_CONTROL_TABLE")
SYNC_STATUS_TABLE = _require_env("SYNC_STATUS_TABLE")


class DBConfig:
    def __init__(self):
        self.ONPREM_SERVER = _require_env("ONPREM_SERVER")
        self.ONPREM_DB = _require_env("ONPREM_DB")
        self.ONPREM_USER = _require_env("ONPREM_USER")
        self.ONPREM_PASS = _require_env("ONPREM_PASS")
        self.AZURE_SERVER = _require_env("AZURE_SERVER")
        self.AZURE_DB = _require_env("AZURE_DB")
        self.AZURE_USER = _require_env("AZURE_USER")
        self.AZURE_PASS = _require_env("AZURE_PASS")

    def get_onprem_conn_str(self) -> str:
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.ONPREM_SERVER};"
            f"DATABASE={self.ONPREM_DB};"
            f"UID={self.ONPREM_USER};PWD={self.ONPREM_PASS}"
        )

    def get_azure_conn_str(self) -> str:
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.AZURE_SERVER};DATABASE={self.AZURE_DB};"
            f"UID={self.AZURE_USER};PWD={self.AZURE_PASS}"
        )
