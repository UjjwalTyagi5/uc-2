"""High-level runner: for a given PR, extract all quotation attachments.

Files are read from the local ``work/`` directory that the earlier pipeline
stages (EMBED_DOC_EXTRACTION → BLOB_UPLOAD) already populated.  The blob
path stored in AttachmentClassification / EmbeddedAttachmentClassification
maps 1-to-1 to the local path under ``work/``.

Typical usage:
    from quotation_extraction.run import run_extraction
    results = run_extraction("R_260647/2026")
"""

from __future__ import annotations

import pathlib
from typing import Optional

from loguru import logger

from .config import ExtractionConfig
from .context_builder import build_ras_context
from .extractor import QuotationExtractor, run_selection_llm_query
from .models import ExtractedItem, QuotationSource, RASContext
from .source_resolver import resolve_quotation_sources
from .writer import ExtractionWriter

def run_extraction(
    purchase_req_no: str,
    *,
    config: Optional[ExtractionConfig] = None,
    include_all_attachments: bool = False,
    work_dir: Optional[str] = None,
    write_to_db: bool = True,
) -> list[ExtractedItem]:
    """Run end-to-end extraction for one purchase requisition.

    Parameters
    ----------
    purchase_req_no:
        The PURCHASE_REQ_NO value (e.g. "R_260647/2026").
    config:
        Extraction configuration.  Created from env if not supplied.
    include_all_attachments:
        When True, processes ALL attachments regardless of doc_type
        classification.  Useful for testing.
    work_dir:
        Root of the local work directory.  Overrides config.WORK_DIR.
        Files are expected at ``{work_dir}/{blob_path}``
        (e.g. ``work/procurement/R_260647_2026/1000831/file.pdf``).
    write_to_db:
        Write extracted items to quotation_extracted_items table.

    Returns
    -------
    list[ExtractedItem]
        All extracted items across all quotation attachments.
    """
    if config is None:
        config = ExtractionConfig()

    # Use the same WORK_DIR as the blob-sync and pipeline stages so all
    # modules resolve files from the same root (configurable via WORK_DIR env).
    work_root = pathlib.Path(work_dir) if work_dir else pathlib.Path(config.WORK_DIR)

    # 1. Build RAS context (line items + metadata)
    ras_ctx: RASContext = build_ras_context(config, purchase_req_no)
    if not ras_ctx.line_items:
        logger.warning(
            "No line items found for {} — nothing to extract",
            purchase_req_no,
        )
        return []

    # 2. Resolve quotation sources from DB
    sources: list[QuotationSource] = resolve_quotation_sources(
        config, purchase_req_no, include_all=include_all_attachments
    )
    if not sources:
        raise RuntimeError(
            f"No quotation documents found for PR={purchase_req_no!r}. "
            f"None of the attachments are classified as 'Quotation' — "
            f"this RAS cannot proceed to benchmarking."
        )

    # 3. Extract from each quotation file (read from local work/ folder)
    extractor = QuotationExtractor(config)
    all_items: list[ExtractedItem] = []

    for src in sources:
        try:
            file_path = work_root / src.blob_path

            if not file_path.exists():
                logger.warning(
                    "File not found locally: {} — skipping "
                    "(has the pipeline run for this PR?)",
                    file_path,
                )
                continue

            logger.info(
                "Extracting: {} (att_fk={}, emb_fk={})",
                src.blob_path,
                src.attachment_classify_fk,
                src.embedded_classify_fk,
            )

            items = extractor.extract(str(file_path), src, ras_ctx)
            all_items.extend(items)

        except Exception:
            logger.exception("Failed to extract from {}", src.blob_path)
            continue

    if not all_items:
        logger.warning("No items extracted for {}", purchase_req_no)
        return []

    # 4. Single LLM call: determine is_selected_quote + quote_rank per DTL_ID
    #    Falls back to programmatic ranking if LLM fails
    run_selection_llm_query(all_items, ras_ctx, config)

    logger.info(
        "Extraction complete for {}: {} items from {} quotation(s)",
        purchase_req_no,
        len(all_items),
        len(sources),
    )

    # 5. Write to DB
    if write_to_db:
        writer = ExtractionWriter(config)
        writer.write(all_items)

    return all_items
