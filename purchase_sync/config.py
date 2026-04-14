import os
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is missing or empty. Check your .env file.")
    return value


def _optional_env(key: str, default: str) -> str:
    return os.getenv(key) or default


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
        # Configurable driver — set ODBC_DRIVER in .env if the machine has Driver 18
        self.ODBC_DRIVER = _optional_env("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

    def get_onprem_conn_str(self) -> str:
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.ONPREM_SERVER};"
            f"DATABASE={self.ONPREM_DB};"
            f"UID={self.ONPREM_USER};PWD={self.ONPREM_PASS};"
            f"TrustServerCertificate=yes;"
        )

    def get_azure_conn_str(self) -> str:
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.AZURE_SERVER};DATABASE={self.AZURE_DB};"
            f"UID={self.AZURE_USER};PWD={self.AZURE_PASS};"
            f"Encrypt=yes;TrustServerCertificate=no;"
        )
