import os
import pyodbc
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is missing or empty. Check your .env file.")
    return value


def _detect_sql_driver() -> str:
    """
    Returns the driver from ODBC_DRIVER env var if set,
    otherwise auto-detects the best available SQL Server ODBC driver.
    """
    from_env = os.getenv("ODBC_DRIVER")
    if from_env:
        return from_env

    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
    ]
    installed = pyodbc.drivers()
    for driver in preferred:
        if driver in installed:
            logger.info(f"Auto-detected ODBC driver: {driver}")
            return driver

    raise EnvironmentError(
        f"No SQL Server ODBC driver found on this machine.\n"
        f"Installed drivers: {installed}\n"
        f"Download from: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
    )


SYNC_CONTROL_TABLE = _require_env("SYNC_CONTROL_TABLE")
SYNC_STATUS_TABLE = _require_env("SYNC_STATUS_TABLE")
BATCH_SIZE = int(_optional_env("BATCH_SIZE", "5000"))


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
        self.ODBC_DRIVER = _detect_sql_driver()
        logger.info(f"Using ODBC driver: {self.ODBC_DRIVER}")

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
