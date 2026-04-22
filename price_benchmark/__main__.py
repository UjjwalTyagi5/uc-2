"""CLI entry point for the price benchmark module.

Usage
-----
    # Benchmark all pending PRs (older purchase orders first):
    python -m price_benchmark

    # Benchmark a single PR:
    python -m price_benchmark --pr-no R_260647/2026

    # Cap at 50 PRs:
    python -m price_benchmark --limit 50

    # Combine: single-PR rerun with explicit config loading from env:
    python -m price_benchmark --pr-no R_260647/2026
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger

from utils.config import AppConfig

from .run import run_all_pending, run_benchmark


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m price_benchmark",
        description="Benchmark procurement line-item prices against historical Pinecone data.",
    )
    parser.add_argument(
        "--pr-no",
        metavar="PURCHASE_REQ_NO",
        help="Benchmark a single PR (e.g. R_260647/2026). "
             "If omitted, all pending PRs are processed.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Maximum number of pending PRs to process (ignored with --pr-no).",
    )
    return parser.parse_args()


def main() -> None:
    args   = _parse_args()
    config = AppConfig()

    if args.pr_no:
        logger.info("Single-PR benchmark: PR={!r}", args.pr_no)
        count = run_benchmark(args.pr_no, config)
        logger.info("Done — {} benchmark row(s) written", count)
    else:
        done = run_all_pending(config, limit=args.limit)
        logger.info("Done — benchmarked {} PR(s)", len(done))

    sys.exit(0)


if __name__ == "__main__":
    main()
