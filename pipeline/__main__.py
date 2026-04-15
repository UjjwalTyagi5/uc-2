"""
Entry point for the pipeline package.

Usage
-----
    python -m pipeline                     # process all pending PRs
    python -m pipeline --limit 50          # process at most 50 PRs this run

Logs
----
    Console : INFO level and above
    File    : logs/pipeline_YYYY-MM-DD.log
              DEBUG level and above, full stack traces on errors,
              rotated daily, kept for 30 days
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.orchestrator import PipelineOrchestrator

# ── Log directory (created next to where the script is run from) ──────────
_LOG_DIR = Path("logs")


def _configure_logging() -> None:
    """
    Sets up two loguru sinks:
      1. Console  — INFO+, concise format
      2. File     — DEBUG+, full format with stack traces, daily rotation,
                    30-day retention

    backtrace=True  : shows the full call stack on exceptions
    diagnose=True   : shows local variable values in tracebacks
                      (set to False in production if logs contain secrets)
    """
    _LOG_DIR.mkdir(exist_ok=True)

    # Remove the default loguru sink (plain stderr)
    logger.remove()

    # Console sink — INFO and above, compact
    logger.add(
        sys.stderr,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[component]}</cyan> | "
            "{message}"
        ),
        colorize=True,
        backtrace=False,
        diagnose=False,
        filter=lambda r: "component" in r["extra"],
    )

    # Console sink for messages without a component binding (root logger)
    logger.add(
        sys.stderr,
        level="INFO",
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "{message}"
        ),
        colorize=True,
        backtrace=False,
        diagnose=False,
        filter=lambda r: "component" not in r["extra"],
    )

    # File sink — DEBUG and above, full detail, daily rotation
    logger.add(
        _LOG_DIR / "pipeline_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
            "{level: <8} | "
            "{extra} | "
            "{name}:{function}:{line} | "
            "{message}"
        ),
        rotation="00:00",       # new file at midnight
        retention="30 days",    # keep last 30 days
        compression="zip",      # compress rotated files
        backtrace=True,         # full call stack on exceptions
        diagnose=True,          # local variable values in tracebacks
        encoding="utf-8",
        enqueue=True,           # async write — won't slow down main thread
    )

    logger.info(f"Logs writing to: {_LOG_DIR.resolve()}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline",
        description=(
            "RAS attachment processing pipeline.\n"
            "Fetches every unprocessed PURCHASE_REQ_NO and runs it through "
            "all registered pipeline stages (blob upload, classification, …)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Cap the number of PRs processed in this run.  "
            "Omit to process every pending PR."
        ),
    )
    return parser


def main() -> None:
    _configure_logging()
    args = _build_parser().parse_args()

    try:
        config = BlobSyncConfig()
    except EnvironmentError as exc:
        logger.critical(f"Configuration error — cannot start pipeline: {exc}")
        sys.exit(1)

    try:
        results = PipelineOrchestrator(config, limit=args.limit).run()
    except Exception as exc:
        logger.opt(exception=True).critical(f"Unexpected pipeline error: {exc}")
        sys.exit(1)

    # Exit with a non-zero code if any PR failed so CI/schedulers can detect it
    any_failed = any(not r.succeeded for r in results)
    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
