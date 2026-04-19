"""
utils.config
~~~~~~~~~~~~
Single source of truth for every configuration value used across this repo.

All packages — pipeline, attachment_blob_sync, purchase_sync — import from
here instead of each having their own copy of the same settings.

Usage
-----
    from utils.config import AppConfig

    config = AppConfig()
    conn   = get_connection(config.get_azure_conn_str())
    conn   = get_connection(config.get_onprem_conn_str())

Environment variables (.env file)
----------------------------------
Required — Azure SQL (holds ras_procurement schema):
    AZURE_SERVER        e.g. myserver.database.windows.net
    AZURE_DB            e.g. ras_db
    AZURE_USER
    AZURE_PASS

Required — On-prem SQL Server (holds ras_attachments binary docs):
    ONPREM_SERVER
    ONPREM_DB
    ONPREM_USER
    ONPREM_PASS

Required — Azure Blob Storage (attachment files):
    BLOB_ACCOUNT_URL    e.g. https://myaccount.blob.core.windows.net
    BLOB_ACCOUNT_KEY
    BLOB_CONTAINER_NAME

Required — ETL sync control tables:
    SYNC_CONTROL_TABLE  e.g. ras_procurement.sync_control
    SYNC_STATUS_TABLE   e.g. ras_procurement.sync_status

Optional — ODBC driver (auto-detected if not set):
    ODBC_DRIVER         e.g. ODBC Driver 18 for SQL Server

Optional — Connection retry settings:
    DB_MAX_RETRIES      max extra attempts after first failure   (default: 4)
    DB_BASE_DELAY       starting back-off in seconds             (default: 2.0)
    DB_CONNECT_TIMEOUT  seconds to wait for a connection attempt (default: 30)

Optional — ETL batch / concurrency settings:
    BATCH_SIZE          rows fetched from on-prem per query      (default: 100000)
    AZURE_BATCH_SIZE    rows per Azure INSERT commit             (default: 2000)
    PARALLEL_WORKERS    parallel threads per table sync          (default: 4)

Optional — Pipeline concurrency:
    PIPELINE_WORKERS    parallel workers for PR processing       (default: 1)

Optional — Azure OpenAI (required only by LLM-based stages: CLASSIFICATION, METADATA_EXTRACTION):
    AZURE_OPENAI_ENDPOINT       e.g. https://myaccount.openai.azure.com/
    AZURE_OPENAI_API_KEY
    AZURE_OPENAI_DEPLOYMENT     model deployment name  (default: gpt-4o)
    AZURE_OPENAI_API_VERSION    API version string     (default: 2025-04-01-preview)

Optional — Quotation extraction tuning:
    EXTRACTION_MAX_PAGES        max PDF pages to render per file  (default: 20)
    EXTRACTION_LLM_TEMPERATURE  LLM temperature                   (default: 0)
    EXTRACTION_LLM_MAX_TOKENS   max tokens in LLM response        (default: 16000)
    LLM_MAX_RETRIES             extra LLM attempts after failure  (default: 3)
    LLM_RETRY_BASE_DELAY        starting back-off in seconds      (default: 5.0)
                        Set to 2–8 to process multiple PRs at the same time.
                        Each worker opens its own DB connections and work folder.
                        Keep at 1 if the DB connection limit is tight.
"""

from __future__ import annotations

import os

import pyodbc
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


# ── Business constants ─────────────────────────────────────────────────────

# File extensions that are accepted from the DB.
# Attachments whose extension is NOT in this set are silently skipped during
# download so they never reach the local work folder, blob, or classification.
ALLOWED_ATTACHMENT_EXTENSIONS: frozenset[str] = frozenset({
    ".pdf",
    ".xls",  ".xlsx",
    ".doc",  ".docx",
    ".jpg",  ".jpeg",
    ".png",
    ".tif",  ".tiff",
    ".ppt",  ".pptx",
    ".zip",
    ".txt",
    ".rar",
    ".msg",
})


# ── Helpers ────────────────────────────────────────────────────────────────

def _require(key: str) -> str:
    """Return the env-var value or raise a clear error if it is missing."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is missing or empty. "
            f"Check your .env file."
        )
    return value


def _optional(key: str, default: str) -> str:
    """Return the env-var value, or `default` if it is not set."""
    return os.getenv(key) or default


def _detect_odbc_driver() -> str:
    """
    Return the SQL Server ODBC driver name to use.

    Uses ODBC_DRIVER env var if set; otherwise picks the newest available
    driver from the list below.
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
            logger.debug(f"Auto-detected ODBC driver: {driver}")
            return driver

    raise EnvironmentError(
        f"No SQL Server ODBC driver found on this machine.\n"
        f"Installed drivers: {installed}\n"
        f"Download: https://learn.microsoft.com/en-us/sql/connect/odbc/"
        f"download-odbc-driver-for-sql-server"
    )


# ── Main config class ──────────────────────────────────────────────────────

class AppConfig:
    """
    Loads all application settings from environment variables.

    Instantiate once at startup and pass around — do not re-create per request.

    Attributes
    ----------
    Connection credentials
        AZURE_SERVER / AZURE_DB / AZURE_USER / AZURE_PASS
        ONPREM_SERVER / ONPREM_DB / ONPREM_USER / ONPREM_PASS
        BLOB_ACCOUNT_URL / BLOB_ACCOUNT_KEY / BLOB_CONTAINER_NAME

    Connection behaviour
        DB_MAX_RETRIES      : extra attempts after first failure (default 4)
        DB_BASE_DELAY       : starting retry back-off in seconds (default 2.0)
        DB_CONNECT_TIMEOUT  : per-attempt connection timeout in seconds (default 30)

    ETL sync
        BATCH_SIZE          : rows fetched from on-prem per query (default 100 000)
        AZURE_BATCH_SIZE    : rows per Azure INSERT commit (default 2 000)
        PARALLEL_WORKERS    : parallel threads per table (default 4)
        SYNC_CONTROL_TABLE  : fully qualified control table name
        SYNC_STATUS_TABLE   : fully qualified status/log table name
    """

    def __init__(self) -> None:
        # ── Azure SQL ──────────────────────────────────────────────────────
        self.AZURE_SERVER = _require("AZURE_SERVER")
        self.AZURE_DB     = _require("AZURE_DB")
        self.AZURE_USER   = _require("AZURE_USER")
        self.AZURE_PASS   = _require("AZURE_PASS")

        # ── On-prem SQL Server ─────────────────────────────────────────────
        # Optional at startup — only needed by ETL sync and pipeline stages
        # that read binary attachments.  Validated lazily in get_onprem_conn_str().
        self.ONPREM_SERVER = _optional("ONPREM_SERVER", "")
        self.ONPREM_DB     = _optional("ONPREM_DB",     "")
        self.ONPREM_USER   = _optional("ONPREM_USER",   "")
        self.ONPREM_PASS   = _optional("ONPREM_PASS",   "")

        # ── Azure Blob Storage ─────────────────────────────────────────────
        self.BLOB_ACCOUNT_URL    = _require("BLOB_ACCOUNT_URL")
        self.BLOB_ACCOUNT_KEY    = _require("BLOB_ACCOUNT_KEY")
        self.BLOB_CONTAINER_NAME = _require("BLOB_CONTAINER_NAME")

        # ── ETL sync tables ────────────────────────────────────────────────
        self.SYNC_CONTROL_TABLE = _require("SYNC_CONTROL_TABLE")
        self.SYNC_STATUS_TABLE  = _require("SYNC_STATUS_TABLE")

        # ── ODBC driver ────────────────────────────────────────────────────
        self.ODBC_DRIVER = _detect_odbc_driver()
        logger.info(f"Using ODBC driver: {self.ODBC_DRIVER}")

        # ── Connection retry / timeout settings ────────────────────────────
        # These are passed to get_connection() in db.connection.
        self.DB_MAX_RETRIES     = int(_optional("DB_MAX_RETRIES",    "4"))
        self.DB_BASE_DELAY      = float(_optional("DB_BASE_DELAY",   "2.0"))
        self.DB_CONNECT_TIMEOUT = int(_optional("DB_CONNECT_TIMEOUT", "30"))

        # ── ETL batch / concurrency settings ──────────────────────────────
        self.BATCH_SIZE        = int(_optional("BATCH_SIZE",        "100000"))
        self.AZURE_BATCH_SIZE  = int(_optional("AZURE_BATCH_SIZE",  "2000"))
        self.PARALLEL_WORKERS  = int(_optional("PARALLEL_WORKERS",  "4"))

        # ── Pipeline concurrency ───────────────────────────────────────────
        # Number of PRs to process in parallel (ThreadPoolExecutor workers).
        # 1 = sequential (safe default); raise to 2-8 to speed up large batches.
        self.PIPELINE_WORKERS  = int(_optional("PIPELINE_WORKERS",  "1"))

        # Number of files within a single PR to classify in parallel.
        # Combined with PIPELINE_WORKERS this gives PIPELINE_WORKERS *
        # CLASSIFICATION_WORKERS concurrent LLM calls — size it against the
        # Azure OpenAI deployment's RPM/TPM quota.
        self.CLASSIFICATION_WORKERS = int(_optional("CLASSIFICATION_WORKERS", "4"))

        # ── Blob upload settings ───────────────────────────────────────────
        self.BLOB_MAX_RETRIES = int(_optional("BLOB_MAX_RETRIES", "3"))
        self.WORK_DIR         = _optional("WORK_DIR", "work")

        # ── Azure OpenAI ───────────────────────────────────────────────────
        # Optional at startup — required only by LLM-based stages.
        # Validated lazily: stages that use LLM will fail with a clear message
        # if these are missing.
        self.AOAI_ENDPOINT    = _optional("AZURE_OPENAI_ENDPOINT",   "")
        self.AOAI_API_KEY     = _optional("AZURE_OPENAI_API_KEY",    "")
        self.AOAI_DEPLOYMENT  = _optional("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        self.AOAI_API_VERSION = _optional("AZURE_OPENAI_API_VERSION","2025-04-01-preview")

        # ── Quotation extraction tuning ────────────────────────────────────
        self.MAX_PAGES            = int(_optional("EXTRACTION_MAX_PAGES",           "20"))
        self.LLM_TEMPERATURE      = float(_optional("EXTRACTION_LLM_TEMPERATURE",  "0"))
        self.LLM_MAX_TOKENS       = int(_optional("EXTRACTION_LLM_MAX_TOKENS",     "16000"))
        self.LLM_MAX_RETRIES      = int(_optional("LLM_MAX_RETRIES",               "3"))
        self.LLM_RETRY_BASE_DELAY = float(_optional("LLM_RETRY_BASE_DELAY",        "5.0"))

    # ── Connection string builders ─────────────────────────────────────────

    def get_azure_conn_str(self) -> str:
        """Connection string for the Azure SQL database (ras_procurement schema)."""
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.AZURE_SERVER};"
            f"DATABASE={self.AZURE_DB};"
            f"UID={self.AZURE_USER};"
            f"PWD={self.AZURE_PASS};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )

    def get_onprem_conn_str(self) -> str:
        """Connection string for the on-prem SQL Server database."""
        missing = [v for v in ("ONPREM_SERVER", "ONPREM_DB", "ONPREM_USER", "ONPREM_PASS")
                   if not getattr(self, v)]
        if missing:
            raise EnvironmentError(
                f"On-prem SQL Server connection requires env vars that are not set: "
                f"{', '.join(missing)}"
            )
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.ONPREM_SERVER};"
            f"DATABASE={self.ONPREM_DB};"
            f"UID={self.ONPREM_USER};"
            f"PWD={self.ONPREM_PASS};"
            f"TrustServerCertificate=yes;"
        )

    # Alias — attachment_blob_sync calls this name
    def get_ras_conn_str(self) -> str:
        """Alias for get_onprem_conn_str() — used by attachment_blob_sync."""
        return self.get_onprem_conn_str()
