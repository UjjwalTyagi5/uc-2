import os
import pyodbc
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is missing or empty. Check your .env file."
        )
    return value


def _optional_env(key: str, default: str) -> str:
    return os.getenv(key) or default


def _detect_odbc_driver() -> str:
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


class BlobSyncConfig:
    def __init__(self):
        # Azure SQL DB — holds ras_procurement schema (purchase_req_mst, purchase_attachments, ras_tracker)
        self.AZURE_SERVER = _require_env("AZURE_SERVER")
        self.AZURE_DB     = _require_env("AZURE_DB")
        self.AZURE_USER   = _require_env("AZURE_USER")
        self.AZURE_PASS   = _require_env("AZURE_PASS")

        # RAS on-prem DB (separate server — holds ras_attachments with binary docs)
        self.RAS_SERVER = _require_env("RAS_SERVER")
        self.RAS_DB     = _require_env("RAS_DB")
        self.RAS_USER   = _require_env("RAS_USER")
        self.RAS_PASS   = _require_env("RAS_PASS")

        # Azure Blob Storage
        self.BLOB_ACCOUNT_URL    = _require_env("BLOB_ACCOUNT_URL")
        self.BLOB_ACCOUNT_KEY    = _require_env("BLOB_ACCOUNT_KEY")
        self.BLOB_CONTAINER_NAME = _require_env("BLOB_CONTAINER_NAME")

        self.ODBC_DRIVER = _detect_odbc_driver()
        logger.info(f"Using ODBC driver: {self.ODBC_DRIVER}")

    def get_azure_conn_str(self) -> str:
        """Connection string for the Azure SQL DB (holds ras_procurement schema)."""
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.AZURE_SERVER};DATABASE={self.AZURE_DB};"
            f"UID={self.AZURE_USER};PWD={self.AZURE_PASS};"
            f"Encrypt=yes;TrustServerCertificate=no;"
        )

    def get_ras_conn_str(self) -> str:
        """Connection string for the on-prem RAS DB (holds ras_attachments binary docs)."""
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.RAS_SERVER};"
            f"DATABASE={self.RAS_DB};"
            f"UID={self.RAS_USER};PWD={self.RAS_PASS};"
            f"TrustServerCertificate=yes;"
        )
