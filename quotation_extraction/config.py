# All configuration now lives in the shared utils.config.AppConfig.
# This alias keeps every import in this package unchanged.
from utils.config import AppConfig as ExtractionConfig

__all__ = ["ExtractionConfig"]
