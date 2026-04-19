"""CLI entry point for quotation extraction.

Usage:
    # Extract from a specific PR (reads from local work/ folder)
    python -m quotation_extraction --pr-no "R_260647/2026"

    # Extract ALL attachments (ignores doc_type classification)
    python -m quotation_extraction --pr-no "R_260647/2026" --all

    # Custom work directory
    python -m quotation_extraction --pr-no "R_260647/2026" --work-dir ./work

    # Dry-run: extract but don't write to DB
    python -m quotation_extraction --pr-no "R_260647/2026" --dry-run

    # Extract from a single file for testing
    python -m quotation_extraction --pr-no "R_260647/2026" --file ./quotation.pdf
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import uuid
from dataclasses import asdict
from datetime import date
from decimal import Decimal

from loguru import logger

from .config import ExtractionConfig
from .context_builder import build_ras_context
from .extractor import QuotationExtractor, compute_quote_ranks
from .models import ExtractedItem, QuotationSource
from .run import run_extraction
from .writer import ExtractionWriter


def _json_serializer(obj: object) -> str:
    if isinstance(obj, (date,)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def _configure_logging() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )
    logger.add(
        "logs/quotation_extraction_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        rotation="1 day",
        retention="30 days",
        compression="gz",
        enqueue=True,
    )


def _run_single_file(args: argparse.Namespace) -> None:
    """Extract from one specific file (for quick testing)."""
    config = ExtractionConfig()
    ras_ctx = build_ras_context(config, args.pr_no)

    fp = pathlib.Path(args.file)
    source = QuotationSource(
        blob_path=fp.name,
        attachment_classify_fk=None,
        embedded_classify_fk=None,
    )

    extractor = QuotationExtractor(config)
    items = extractor.extract(str(fp), source, ras_ctx)
    compute_quote_ranks(items)

    if not args.dry_run and items:
        writer = ExtractionWriter(config)
        writer.write(items)

    _print_summary(items)


def _run_full(args: argparse.Namespace) -> None:
    """Standard extraction flow for a PR."""
    items = run_extraction(
        args.pr_no,
        include_all_attachments=args.all,
        work_dir=args.work_dir,
        write_to_db=not args.dry_run,
    )
    _print_summary(items)


def _print_summary(items: list[ExtractedItem]) -> None:
    logger.info("=== Extraction Summary ===")
    logger.info("Total items extracted: {}", len(items))

    if not items:
        return

    matched = sum(1 for i in items if i.purchase_dtl_id is not None)
    logger.info("Matched to RAS line items: {}/{}", matched, len(items))

    as_dicts = [asdict(i) for i in items]
    print(json.dumps(as_dicts, indent=2, default=_json_serializer))


def main() -> None:
    _configure_logging()

    parser = argparse.ArgumentParser(
        description="Extract structured data from quotation documents",
    )
    parser.add_argument(
        "--pr-no",
        required=True,
        help="PURCHASE_REQ_NO (e.g. R_260647/2026)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Process ALL attachments, not just doc_type=Quotation",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Root of local work directory (default: uc-2/work/)",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Extract from a single file (testing mode)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Extract but don't write to the database",
    )

    args = parser.parse_args()

    try:
        if args.file:
            _run_single_file(args)
        else:
            _run_full(args)
    except Exception:
        logger.exception("Extraction failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
