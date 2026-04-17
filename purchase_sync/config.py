"""
purchase_sync.config
~~~~~~~~~~~~~~~~~~~~
Backwards-compatibility shim.

The real configuration now lives in utils.config.AppConfig.
DBConfig and the module-level constants below are preserved so existing
code that imports from this module continues to work unchanged.
"""

from utils.config import AppConfig

# Direct alias — DBConfig IS AppConfig
DBConfig = AppConfig

# Module-level constants imported by sync_manager.py.
# Instantiate a temporary config to read the values — environment must
# already be loaded (dotenv is called inside AppConfig.__init__).
_cfg = AppConfig()

SYNC_CONTROL_TABLE = _cfg.SYNC_CONTROL_TABLE
SYNC_STATUS_TABLE  = _cfg.SYNC_STATUS_TABLE
BATCH_SIZE         = _cfg.BATCH_SIZE
AZURE_BATCH_SIZE   = _cfg.AZURE_BATCH_SIZE
PARALLEL_WORKERS   = _cfg.PARALLEL_WORKERS
