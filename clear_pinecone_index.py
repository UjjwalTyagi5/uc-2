"""
clear_pinecone_index.py
-----------------------
Delete ALL vectors from the configured Pinecone index / namespace.

Reads PINECONE_API_KEY, PINECONE_INDEX_NAME, and PINECONE_NAMESPACE from .env
(same values used by the pipeline).

Usage
-----
    # Shows stats and prompts for confirmation:
    python clear_pinecone_index.py

    # Skip the confirmation prompt:
    python clear_pinecone_index.py --force
"""

from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv
from loguru import logger
from pinecone import Pinecone

load_dotenv()


def _load_settings() -> tuple[str, str, str | None]:
    """Return (api_key, index_name, namespace) from environment."""
    import os
    api_key    = os.getenv("PINECONE_API_KEY", "").strip()
    index_name = os.getenv("PINECONE_INDEX_NAME", "").strip()
    namespace  = os.getenv("PINECONE_NAMESPACE", "").strip() or None

    missing = [k for k, v in [
        ("PINECONE_API_KEY",    api_key),
        ("PINECONE_INDEX_NAME", index_name),
    ] if not v]

    if missing:
        logger.critical(
            f"Missing required env vars: {', '.join(missing)}. "
            f"Check your .env file."
        )
        sys.exit(1)

    return api_key, index_name, namespace


def _get_vector_count(index, namespace: str | None) -> int:
    stats = index.describe_index_stats()
    if namespace:
        ns_stats = (stats.namespaces or {}).get(namespace)
        return ns_stats.vector_count if ns_stats else 0
    return stats.total_vector_count or 0


def main() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")

    parser = argparse.ArgumentParser(
        prog="python clear_pinecone_index.py",
        description="Delete ALL vectors from the configured Pinecone index/namespace.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Skip the confirmation prompt and delete immediately.",
    )
    args = parser.parse_args()

    api_key, index_name, namespace = _load_settings()

    ns_display = repr(namespace) if namespace else "(default namespace)"
    logger.info(f"Index     : {index_name}")
    logger.info(f"Namespace : {ns_display}")

    pc    = Pinecone(api_key=api_key)
    index = pc.Index(index_name)

    count = _get_vector_count(index, namespace)
    logger.info(f"Vectors currently in index/namespace: {count:,}")

    if count == 0:
        logger.info("Nothing to delete — index/namespace is already empty.")
        sys.exit(0)

    if not args.force:
        answer = input(
            f"\nAbout to delete ALL {count:,} vectors from index={index_name!r} "
            f"namespace={ns_display}.\nType 'yes' to confirm: "
        ).strip().lower()
        if answer != "yes":
            logger.info("Aborted — no vectors were deleted.")
            sys.exit(0)

    logger.info("Deleting all vectors ...")
    index.delete(delete_all=True, namespace=namespace)

    after = _get_vector_count(index, namespace)
    logger.info(f"Done. Vectors remaining: {after:,}")


if __name__ == "__main__":
    main()
