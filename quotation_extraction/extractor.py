"""Core extraction orchestrator.

Flow per quotation file:
  1. Load document  →  DocumentContent  (text or images)
  2. Build the LLM prompt from RAS context + prompt templates
  3. Call Azure OpenAI GPT-5.2
  4. Parse JSON response into ExtractedItem list
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Optional

from loguru import logger

from .config import ExtractionConfig
from .document_loader import load_document
from .llm_client import ExtractionLLMClient
from .models import (
    DocumentContent,
    ExtractedItem,
    LineItemContext,
    QuotationSource,
    RASContext,
)

_PROMPTS_CACHE: dict[str, str] = {}


def _read_prompt(name: str, config: ExtractionConfig) -> str:
    if name not in _PROMPTS_CACHE:
        path = config.prompts_dir() / name
        _PROMPTS_CACHE[name] = path.read_text(encoding="utf-8")
    return _PROMPTS_CACHE[name]


# ── Prompt building ──


def _build_line_items_table(items: list[LineItemContext]) -> str:
    rows = [
        "| DTL_ID | Item No | Description | Qty | Type | UOM "
        "| Unit Price | Total (REQ_VALUE) | Discount | Currency "
        "| Supplier | Delivery Date | Payment | Comments |"
    ]
    rows.append(
        "|--------|---------|-------------|-----|------|-----"
        "|------------|-------------------|----------|----------"
        "|----------|---------------|---------|----------|"
    )
    for li in items:
        rows.append(
            f"| {li.purchase_dtl_id} "
            f"| {li.item_no} "
            f"| {li.item_description or 'N/A'} "
            f"| {li.quantity or 'N/A'} "
            f"| {li.item_type or 'N/A'} "
            f"| {li.uom or 'N/A'} "
            f"| {li.unit_price or 'N/A'} "
            f"| {li.req_value or 'N/A'} "
            f"| {li.discount or 'N/A'} "
            f"| {li.currency or 'N/A'} "
            f"| {li.supplier_name or 'N/A'} "
            f"| {li.delivery_date or 'N/A'} "
            f"| {li.payment_details or 'N/A'} "
            f"| {li.comments or 'N/A'} |"
        )
    return "\n".join(rows)


def _build_user_prompt(
    config: ExtractionConfig,
    ctx: RASContext,
    doc: DocumentContent,
) -> str:
    tpl = _read_prompt("extraction.txt", config)
    taxonomy = _read_prompt("item_taxonomy.txt", config)

    doc_content_str: str
    if doc.is_image_based:
        doc_content_str = (
            f"[{doc.page_count} page(s) attached as images below]"
        )
    else:
        doc_content_str = doc.text or "[No content extracted]"

    return tpl.format(
        purchase_req_no=ctx.purchase_req_no,
        purchase_req_id=ctx.purchase_req_id,
        justification=ctx.justification or "N/A",
        supplier_name=ctx.supplier_name or "N/A",
        currency=ctx.currency or "N/A",
        enquiry_no=ctx.enquiry_no or "N/A",
        classification=ctx.classification or "N/A",
        department=ctx.department or "N/A",
        negotiated_by=ctx.negotiated_by or "N/A",
        address=ctx.address or "N/A",
        contract_no=ctx.contract_no or "N/A",
        order_no=ctx.order_no or "N/A",
        purchase_value=ctx.purchase_value or "N/A",
        category=ctx.category or "N/A",
        sub_category=ctx.sub_category or "N/A",
        site_region=ctx.site_region or "N/A",
        site_country=ctx.site_country or "N/A",
        site=ctx.site or "N/A",
        division=ctx.division or "N/A",
        requisition_type=ctx.requisition_type or "N/A",
        parent_supplier=ctx.parent_supplier or "N/A",
        supplier_type=ctx.supplier_type or "N/A",
        supplier_country=ctx.supplier_country or "N/A",
        payment_days=ctx.payment_days or "N/A",
        po_date=ctx.po_date or "N/A",
        category_buyer=ctx.category_buyer or "N/A",
        line_items_table=_build_line_items_table(ctx.line_items),
        item_taxonomy=taxonomy,
        document_content=doc_content_str,
    )


# ── JSON parsing helpers ──


def _safe_date(val: object) -> Optional[date]:
    if val is None or val == "":
        return None
    try:
        return date.fromisoformat(str(val))
    except (ValueError, TypeError):
        return None


def _safe_decimal(val: object) -> Optional[Decimal]:
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, TypeError):
        return None


def _safe_int(val: object) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(val)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None


def _strip_json_fences(raw: str) -> str:
    """Remove optional ```json ... ``` fences from LLM output."""
    raw = raw.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return raw.strip()


def _parse_llm_response(
    raw: str,
    source: QuotationSource,
    ras_supplier: Optional[str],
) -> list[ExtractedItem]:
    """Parse the raw JSON string into a list of :class:`ExtractedItem`."""

    raw = _strip_json_fences(raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("LLM returned invalid JSON: {}", exc)
        return []

    header = data if isinstance(data, dict) else {}
    items_raw: list[dict] = header.get("items", [])

    if not items_raw:
        logger.warning("LLM returned zero items for {}", source.blob_path)
        return []

    # shared header fields
    h_supplier = header.get("supplier_name")
    h_address = header.get("supplier_address")
    h_country = header.get("supplier_country")
    h_ref = header.get("quotation_ref_no")
    h_date = _safe_date(header.get("quotation_date"))
    h_currency = header.get("currency")
    h_validity_date = _safe_date(header.get("validity_date"))
    h_validity_days = _safe_int(header.get("validity_days"))
    h_payment = header.get("payment_terms")

    # is_selected_quote: supplier name in quotation matches the RAS primary supplier
    is_selected = False
    if ras_supplier and h_supplier:
        is_selected = (
            ras_supplier.strip().lower() in h_supplier.strip().lower()
            or h_supplier.strip().lower() in ras_supplier.strip().lower()
        )

    results: list[ExtractedItem] = []
    for item in items_raw:
        results.append(
            ExtractedItem(
                attachment_classify_fk=source.attachment_classify_fk,
                embedded_classify_fk=source.embedded_classify_fk,
                purchase_dtl_id=_safe_int(item.get("purchase_dtl_id")),
                is_selected_quote=is_selected,
                supplier_match_conf=_safe_decimal(
                    item.get("supplier_match_conf")
                ),
                quote_rank=None,  # computed later across all quotations
                supplier_name=h_supplier,
                supplier_address=h_address,
                supplier_country=h_country,
                quotation_ref_no=h_ref,
                quotation_date=h_date,
                currency=h_currency,
                validity_date=h_validity_date,
                validity_days=h_validity_days,
                payment_terms=h_payment,
                item_name=item.get("item_name"),
                item_description=item.get("item_description"),
                quantity=_safe_decimal(item.get("quantity")),
                unit=item.get("unit"),
                unit_price=_safe_decimal(item.get("unit_price")),
                total_price=_safe_decimal(item.get("total_price")),
                discount=_safe_decimal(item.get("discount")),
                taxation_details=item.get("taxation_details"),
                delivery_date=_safe_date(item.get("delivery_date")),
                delivery_time_days=_safe_int(item.get("delivery_time_days")),
                item_level_1=item.get("item_level_1") or None,
                item_level_2=item.get("item_level_2") or None,
                item_level_3=item.get("item_level_3") or None,
                item_level_4=item.get("item_level_4") or None,
                item_level_5=item.get("item_level_5") or None,
                item_level_6=item.get("item_level_6") or None,
                item_level_7=item.get("item_level_7") or None,
                item_level_8=item.get("item_level_8") or None,
                commodity_tag=item.get("commodity_tag"),
                item_summary=item.get("item_summary"),
            )
        )

    logger.info(
        "Parsed {} items from LLM response (supplier={})",
        len(results),
        h_supplier,
    )
    return results


def _align_to_ras_line_items(
    items: list[ExtractedItem],
    ras_context: RASContext,
    source: QuotationSource,
) -> list[ExtractedItem]:
    """Align extracted items to exactly the RAS line items.

    1. Drop any extracted item whose purchase_dtl_id is not in the RAS
       (e.g. shipping charges, extras the LLM invented).
    2. For any RAS line item (DTL_ID) not covered by the extraction,
       create a stub row with DTL_ID populated but item fields as None.

    Result: exactly len(ras_context.line_items) rows per quotation source.
    """
    valid_dtl_ids = {li.purchase_dtl_id for li in ras_context.line_items}

    # Keep only items that map to a real RAS line item
    matched = [i for i in items if i.purchase_dtl_id in valid_dtl_ids]
    dropped = len(items) - len(matched)
    if dropped:
        logger.info(
            "Dropped {} extracted item(s) not matching any RAS line item",
            dropped,
        )

    # Fill missing RAS line items with stub rows
    covered_dtl_ids = {i.purchase_dtl_id for i in matched}
    missing = valid_dtl_ids - covered_dtl_ids

    if missing:
        header_donor = matched[0] if matched else None

        for li in ras_context.line_items:
            if li.purchase_dtl_id not in missing:
                continue
            matched.append(
                ExtractedItem(
                    attachment_classify_fk=source.attachment_classify_fk,
                    embedded_classify_fk=source.embedded_classify_fk,
                    purchase_dtl_id=li.purchase_dtl_id,
                    is_selected_quote=header_donor.is_selected_quote if header_donor else False,
                    supplier_match_conf=Decimal("0"),
                    quote_rank=None,
                    supplier_name=header_donor.supplier_name if header_donor else None,
                    supplier_address=header_donor.supplier_address if header_donor else None,
                    supplier_country=header_donor.supplier_country if header_donor else None,
                    quotation_ref_no=header_donor.quotation_ref_no if header_donor else None,
                    quotation_date=header_donor.quotation_date if header_donor else None,
                    currency=header_donor.currency if header_donor else None,
                    validity_date=header_donor.validity_date if header_donor else None,
                    validity_days=header_donor.validity_days if header_donor else None,
                    payment_terms=header_donor.payment_terms if header_donor else None,
                )
            )

        logger.info(
            "Filled {} stub row(s) for RAS line items not found in quotation",
            len(missing),
        )

    return matched


# ── Quote ranking ──


def compute_quote_ranks(
    all_items: list[ExtractedItem],
) -> None:
    """Assign *quote_rank* per purchase_dtl_id across all quotation sources.

    Rank 1 = lowest unit_price for that line item.  Items with no
    purchase_dtl_id or no unit_price are left with quote_rank = None.
    Mutates the items in-place.
    """

    from collections import defaultdict

    by_dtl: dict[int, list[ExtractedItem]] = defaultdict(list)
    for item in all_items:
        if item.purchase_dtl_id is not None and item.unit_price is not None:
            by_dtl[item.purchase_dtl_id].append(item)

    for dtl_id, group in by_dtl.items():
        sorted_group = sorted(group, key=lambda x: x.unit_price)  # type: ignore[arg-type]
        for rank, item in enumerate(sorted_group, 1):
            item.quote_rank = rank


# ── Public API ──


class QuotationExtractor:
    """Extracts structured item data from one quotation file."""

    def __init__(self, config: ExtractionConfig) -> None:
        self._config = config
        self._llm = ExtractionLLMClient(config)

    def extract(
        self,
        file_path: str,
        source: QuotationSource,
        ras_context: RASContext,
    ) -> list[ExtractedItem]:
        """Run the full extraction pipeline on a single quotation file.

        Returns a list of :class:`ExtractedItem` (one per extracted line).
        """
        doc: DocumentContent = load_document(
            file_path, max_pages=self._config.MAX_PAGES
        )

        system_prompt = _read_prompt("system.txt", self._config)
        user_prompt = _build_user_prompt(self._config, ras_context, doc)

        raw_response = self._llm.extract(system_prompt, user_prompt, doc)

        items = _parse_llm_response(
            raw_response,
            source,
            ras_context.supplier_name,
        )

        items = _align_to_ras_line_items(items, ras_context, source)

        return items
