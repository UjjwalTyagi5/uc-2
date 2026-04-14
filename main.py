import sys
from loguru import logger

from purchase_sync.config import DBConfig
from purchase_sync.db_manager import BaseSyncManager
from purchase_sync.sync_manager import PurchaseSyncManager


def main():
    logger.info("=== purchase_sync ETL run started ===")
    try:
        config = DBConfig()
        source_manager = BaseSyncManager(config.get_onprem_conn_str())
        azure_manager = PurchaseSyncManager(
            conn_str=config.get_azure_conn_str(),
            source_manager=source_manager
        )
        azure_manager.sync_all_tables()
        logger.info("=== purchase_sync ETL run completed successfully ===")
    except Exception as e:
        logger.error(f"=== purchase_sync ETL run failed: {e} ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
