"""Core extraction orchestrator.

Flow per quotation file:
  1. Load document  →  DocumentContent  (text or images)
  2. Build the LLM prompt from RAS context + prompt templates
  3. Call Azure OpenAI GPT-5.2
  4. Parse JSON response into ExtractedItem list
"""

from __future__ import annotations

import json
import pathlib
import re
from decimal import Decimal
from typing import Optional

from loguru import logger
from pydantic import ValidationError

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

_PROMPTS_DIR = pathlib.Path(__file__).parent / "prompts"

_PROMPTS_CACHE: dict[str, str] = {}


def _read_prompt(name: str) -> str:
    if name not in _PROMPTS_CACHE:
        _PROMPTS_CACHE[name] = (_PROMPTS_DIR / name).read_text(encoding="utf-8")
    return _PROMPTS_CACHE[name]


# ── Prompt building ──


def _na(val: object) -> str:
    """Return the value as string, or 'N/A' if None/empty."""
    if val is None or str(val).strip() == "":
        return "N/A"
    return str(val)


def _build_line_items_table(items: list[LineItemContext]) -> str:
    header = (
        "| DTL_ID | Item No | Item Code | Description | Qty | UOM | Type "
        "| Unit Price | Original Value | Initial Offer | Negotiated "
        "| Req Value | Discount | Currency | Supplier "
        "| Delivery Date | Prepayment | Payment Terms | Comments |"
    )
    sep = (
        "|--------|---------|-----------|-------------|-----|-----|------"
        "|------------|----------------|---------------|----------"
        "|-----------|----------|----------|----------"
        "|---------------|------------|---------------|----------|"
    )
    rows = [header, sep]
    for li in items:
        rows.append(
            f"| {_na(li.purchase_dtl_id)} "
            f"| {_na(li.item_no)} "
            f"| {_na(li.item_code)} "
            f"| {_na(li.item_description)} "
            f"| {_na(li.quantity)} "
            f"| {_na(li.uom)} "
            f"| {_na(li.item_type)} "
            f"| {_na(li.unit_price)} "
            f"| {_na(li.original_value)} "
            f"| {_na(li.initial_offer)} "
            f"| {_na(li.negotiation)} "
            f"| {_na(li.req_value)} "
            f"| {_na(li.discount)} "
            f"| {_na(li.currency)} "
            f"| {_na(li.supplier_name)} "
            f"| {_na(li.delivery_date)} "
            f"| {_na(li.prepayment)} "
            f"| {_na(li.payment_details)} "
            f"| {_na(li.comments)} |"
        )
    return "\n".join(rows)


def _build_user_prompt(
    config: ExtractionConfig,
    ctx: RASContext,
    doc: DocumentContent,
) -> str:
    tpl = _read_prompt("extraction.txt")
    taxonomy = _read_prompt("item_taxonomy.txt")

    doc_content_str: str
    if doc.is_image_based:
        doc_content_str = (
            f"[{doc.page_count} page(s) attached as images below]"
        )
    else:
        doc_content_str = doc.text or "[No content extracted]"

    def _f(val: object) -> str:
        return str(val) if val is not None else "N/A"

    return tpl.format(
        purchase_req_no=ctx.purchase_req_no,
        purchase_req_id=ctx.purchase_req_id,
        justification=_f(ctx.justification),
        supplier_name=_f(ctx.supplier_name),
        currency=_f(ctx.currency),
        enquiry_no=_f(ctx.enquiry_no),
        classification=_f(ctx.classification),
        department=_f(ctx.department),
        negotiated_by=_f(ctx.negotiated_by),
        address=_f(ctx.address),
        contract_no=_f(ctx.contract_no),
        order_no=_f(ctx.order_no),
        purchase_value=_f(ctx.purchase_value),
        category=_f(ctx.category),
        sub_category=_f(ctx.sub_category),
        l3=_f(ctx.l3),
        l4=_f(ctx.l4),
        purchase_category=_f(ctx.purchase_category),
        ras_title=_f(ctx.ras_title),
        site_region=_f(ctx.site_region),
        site_country=_f(ctx.site_country),
        site=_f(ctx.site),
        division=_f(ctx.division),
        requisition_type=_f(ctx.requisition_type),
        parent_supplier=_f(ctx.parent_supplier),
        supplier_type=_f(ctx.supplier_type),
        supplier_country=_f(ctx.supplier_country),
        payment_days=_f(ctx.payment_days),
        po_date=_f(ctx.po_date),
        category_buyer=_f(ctx.category_buyer),
        line_items_table=_build_line_items_table(ctx.line_items),
        item_taxonomy=taxonomy,
        document_content=doc_content_str,
    )


# ── JSON parsing helpers ──


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
    """Parse the raw JSON string into a list of :class:`ExtractedItem`.

    Pydantic handles all type coercions (str→Decimal, str→date, str→int)
    and normalises empty strings to None via the model's pre-validator.
    Invalid individual items are skipped with a warning rather than
    failing the whole file.
    """
    raw = _strip_json_fences(raw)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("LLM returned invalid JSON: {}", exc)
        return []

    header: dict = data if isinstance(data, dict) else {}
    items_raw: list[dict] = header.get("items", [])

    if not items_raw:
        logger.warning("LLM returned zero items for {}", source.blob_path)
        return []

    # Determine if the quotation supplier matches the RAS primary supplier
    h_supplier: Optional[str] = header.get("supplier_name") or None
    is_selected = False
    if ras_supplier and h_supplier:
        is_selected = (
            ras_supplier.strip().lower() in h_supplier.strip().lower()
            or h_supplier.strip().lower() in ras_supplier.strip().lower()
        )

    # Header fields shared across every item row in this quotation
    header_fields = {
        "supplier_name":    h_supplier,
        "supplier_address": header.get("supplier_address") or None,
        "supplier_country": header.get("supplier_country") or None,
        "quotation_ref_no": header.get("quotation_ref_no") or None,
        "quotation_date":   header.get("quotation_date"),
        "currency":         header.get("currency") or None,
        "validity_date":    header.get("validity_date"),
        "validity_days":    header.get("validity_days"),
        "payment_terms":    header.get("payment_terms") or None,
    }

    results: list[ExtractedItem] = []
    for raw_item in items_raw:
        try:
            item = ExtractedItem.model_validate({
                # source linkage — set programmatically, never from LLM
                "attachment_classify_fk": source.attachment_classify_fk,
                "embedded_classify_fk":   source.embedded_classify_fk,
                "is_selected_quote":      is_selected,
                "quote_rank":             None,
                # header fields (shared across all items in this quotation)
                **header_fields,
                # item-level fields from LLM — Pydantic coerces types
                **raw_item,
            })
            results.append(item)
        except ValidationError as exc:
            logger.warning(
                "Skipping invalid item from LLM response for {}: {}",
                source.blob_path, exc,
            )

    logger.info(
        "Parsed {} item(s) from LLM response (supplier={})",
        len(results), h_supplier,
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
            file_path,
            max_pages=self._config.MAX_PAGES,
            work_dir=pathlib.Path(file_path).parent,
        )

        system_prompt = _read_prompt("system.txt")
        user_prompt = _build_user_prompt(self._config, ras_context, doc)

        raw_response = self._llm.extract(system_prompt, user_prompt, doc)

        items = _parse_llm_response(
            raw_response,
            source,
            ras_context.supplier_name,
        )

        items = _align_to_ras_line_items(items, ras_context, source)

        return items
