"""Root entry-point for quotation extraction.

Mirrors the pattern of ``run_etl_sync.py`` and ``run_pipeline.py``.

Usage:
    python run_extraction.py --pr-no "R_260647/2026"
    python run_extraction.py --pr-no "R_260647/2026" --all --dry-run
"""

from quotation_extraction.__main__ import main

if __name__ == "__main__":
    main()
