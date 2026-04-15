"""
Entry point for the pipeline package.

Usage
-----
    python -m pipeline                     # process all pending PRs
    python -m pipeline --limit 50          # process at most 50 PRs this run
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger

from attachment_blob_sync.config import BlobSyncConfig
from pipeline.orchestrator import PipelineOrchestrator


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
    args = _build_parser().parse_args()

    try:
        config = BlobSyncConfig()
    except EnvironmentError as exc:
        logger.critical(f"Configuration error — cannot start pipeline: {exc}")
        sys.exit(1)

    try:
        results = PipelineOrchestrator(config, limit=args.limit).run()
    except Exception as exc:
        logger.critical(f"Unexpected pipeline error: {exc}")
        sys.exit(1)

    # Exit with a non-zero code if any PR failed, so CI/schedulers can detect failure
    any_failed = any(not r.succeeded for r in results)
    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()
