import argparse
import sys

from loguru import logger

from attachment_blob_sync.config import BlobSyncConfig
from attachment_blob_sync.sync import AttachmentBlobSync


def main():
    parser = argparse.ArgumentParser(
        prog="python -m attachment_blob_sync",
        description="Upload purchase requisition attachments from RAS DB to Azure Blob Storage.",
    )
    parser.add_argument(
        "--pr-no",
        required=True,
        metavar="PURCHASE_REQ_NO",
        help="The PURCHASE_REQ_NO to process (e.g. R_3451/2026)",
    )
    args = parser.parse_args()

    try:
        config = BlobSyncConfig()
        AttachmentBlobSync(config).run(args.pr_no)
    except Exception as exc:
        logger.error(f"attachment_blob_sync failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
