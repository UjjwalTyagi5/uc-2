"""
run_etl_sync.py
---------------
Purchase requisition ETL sync — root entry point.

Syncs all purchase requisition tables from the on-prem SQL Server
into the Azure SQL database.

Usage
-----
    python run_etl_sync.py

Logs
----
    Console : INFO and above (loguru default sink)
"""
import sys

from loguru import logger

from purchase_sync.config import DBConfig
from purchase_sync.db_manager import BaseSyncManager
from purchase_sync.sync_manager import PurchaseSyncManager


def main() -> None:
    logger.info("=== purchase_sync ETL run started ===")
    try:
        config         = DBConfig()
        source_manager = BaseSyncManager(config.get_onprem_conn_str())
        azure_manager  = PurchaseSyncManager(
            conn_str       = config.get_azure_conn_str(),
            source_manager = source_manager,
        )
        azure_manager.sync_all_tables()
        logger.info("=== purchase_sync ETL run completed successfully ===")
    except Exception as exc:
        logger.opt(exception=True).error(f"ETL sync failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
