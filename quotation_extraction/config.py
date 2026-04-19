"""Configuration for the quotation-extraction module.

Loads Azure OpenAI, Azure SQL, and Azure Blob settings from environment
variables (via .env).  Every required variable is validated at import time
so mis-configuration fails fast.
"""

from __future__ import annotations

import os
import pathlib

from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).resolve().parents[1] / ".env")

_PROMPTS_DIR = pathlib.Path(__file__).resolve().parent / "prompts"


def _require(var: str) -> str:
    val = os.getenv(var, "").strip()
    if not val:
        raise EnvironmentError(f"Missing required env var: {var}")
    return val


def _detect_odbc_driver() -> str:
    import pyodbc  # noqa: delayed import so tests can mock

    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
    ]
    available = pyodbc.drivers()
    for drv in preferred:
        if drv in available:
            return drv
    raise RuntimeError(
        f"No supported SQL Server ODBC driver found. Available: {available}"
    )


class ExtractionConfig:
    """Centralised config for quotation extraction."""

    def __init__(self) -> None:
        # ── Azure OpenAI ──
        self.AOAI_ENDPOINT: str = _require("AZURE_OPENAI_ENDPOINT")
        self.AOAI_API_KEY: str = _require("AZURE_OPENAI_API_KEY")
        self.AOAI_DEPLOYMENT: str = os.getenv(
            "AZURE_OPENAI_DEPLOYMENT", "gpt-5.2"
        )
        self.AOAI_API_VERSION: str = os.getenv(
            "AZURE_OPENAI_API_VERSION", "2025-04-01-preview"
        )

        # ── Azure SQL (target) ──
        self.AZURE_SERVER: str = _require("AZURE_SERVER")
        self.AZURE_DB: str = _require("AZURE_DB")
        self.AZURE_USER: str = _require("AZURE_USER")
        self.AZURE_PASS: str = _require("AZURE_PASS")

        # ── Azure Blob ──
        self.BLOB_ACCOUNT_URL: str = _require("BLOB_ACCOUNT_URL")
        self.BLOB_ACCOUNT_KEY: str = _require("BLOB_ACCOUNT_KEY")
        self.BLOB_CONTAINER: str = _require("BLOB_CONTAINER_NAME")

        # ── Driver ──
        env_driver = os.getenv("ODBC_DRIVER", "").strip()
        self.ODBC_DRIVER: str = env_driver or _detect_odbc_driver()

        # ── Tuning ──
        self.MAX_PAGES: int = int(os.getenv("EXTRACTION_MAX_PAGES", "20"))
        self.LLM_TEMPERATURE: float = float(
            os.getenv("EXTRACTION_LLM_TEMPERATURE", "0")
        )
        self.LLM_MAX_TOKENS: int = int(
            os.getenv("EXTRACTION_LLM_MAX_TOKENS", "16000")
        )

    # ── helpers ──

    def get_azure_conn_str(self) -> str:
        return (
            f"DRIVER={{{self.ODBC_DRIVER}}};"
            f"SERVER={self.AZURE_SERVER};"
            f"DATABASE={self.AZURE_DB};"
            f"UID={self.AZURE_USER};"
            f"PWD={self.AZURE_PASS};"
            "TrustServerCertificate=yes;"
        )

    @staticmethod
    def prompts_dir() -> pathlib.Path:
        return _PROMPTS_DIR
