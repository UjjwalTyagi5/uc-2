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
from difflib import SequenceMatcher
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


# ── Supplier matching ──

_SELECTED_THRESHOLD = Decimal("0.70")   # min overall score to mark as selected quote
_PRICE_TOLERANCE    = Decimal("0.05")   # 5 % — prices within this band count as matching
_PRICE_MAX_BOOST    = Decimal("0.10")   # max confidence boost from price alignment



def _compute_supplier_match(
    extracted_supplier: Optional[str],
    ras_context: RASContext,
) -> tuple[bool, Decimal]:
    """Fuzzy-match extracted supplier name against all RAS supplier sources.

    Sources checked (in priority order):
      1. purchase_req_mst.SUPPLIER_NAME      → ras_context.supplier_name
      2. purchase_req_detail.SUPPLIER_NAME   → each li.supplier_name
      3. vw_get_ras_data.Parent_Supplier     → ras_context.parent_supplier

    Returns (is_selected, match_confidence 0.00–1.00).
    Substring containment scores 0.90; SequenceMatcher ratio otherwise.
    is_selected = True when best confidence ≥ 0.70.
    """
    if not extracted_supplier:
        return False, Decimal("0")

    known: set[str] = set()
    if ras_context.supplier_name:
        known.add(ras_context.supplier_name.strip())
    if ras_context.parent_supplier:
        known.add(ras_context.parent_supplier.strip())
    for li in ras_context.line_items:
        if li.supplier_name:
            known.add(li.supplier_name.strip())

    if not known:
        return False, Decimal("0")

    ext = extracted_supplier.strip().lower()
    best = 0.0

    for name in known:
        n = name.lower()
        if ext in n or n in ext:
            best = max(best, 0.90)
        else:
            best = max(best, SequenceMatcher(None, ext, n).ratio())

    conf = Decimal(str(round(best, 4)))
    return conf >= _SELECTED_THRESHOLD, conf


def _apply_price_alignment_boost(
    items: list[ExtractedItem],
    ras_context: RASContext,
) -> None:
    """Boost supplier_match_conf by up to 0.10 when extracted unit prices
    align with RAS line item prices within 5 % tolerance.

    Logic: if ≥50 % of matched line items have a price within tolerance,
    boost all items in this quotation proportionally.  Mutates in-place.
    """
    dtl_price: dict[int, Decimal] = {
        li.purchase_dtl_id: li.unit_price
        for li in ras_context.line_items
        if li.unit_price is not None and li.unit_price > 0
    }
    if not dtl_price:
        return

    matches = comparable = 0
    for item in items:
        if item.purchase_dtl_id in dtl_price and item.unit_price is not None:
            comparable += 1
            ras_p = dtl_price[item.purchase_dtl_id]
            if abs(item.unit_price - ras_p) / ras_p <= _PRICE_TOLERANCE:
                matches += 1

    if comparable == 0:
        return

    price_hit_ratio = matches / comparable
    if price_hit_ratio < 0.5:
        return

    boost = Decimal(str(round(price_hit_ratio * float(_PRICE_MAX_BOOST), 4)))
    for item in items:
        if item.supplier_match_conf is not None:
            item.supplier_match_conf = min(Decimal("1.0"), item.supplier_match_conf + boost)

    logger.debug(
        "Price alignment boost +{} applied ({}/{} items within {}% tolerance)",
        boost, matches, comparable, int(_PRICE_TOLERANCE * 100),
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
    ras_context: RASContext,
) -> list[ExtractedItem]:
    """Parse the raw JSON string into a list of :class:`ExtractedItem`.

    Supplier matching checks all three RAS sources:
      purchase_req_mst.SUPPLIER_NAME, purchase_req_detail.SUPPLIER_NAME,
      and vw_get_ras_data.Parent_Supplier.

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

    # Multi-source supplier match — stored per item; final is_selected_quote
    # is resolved later by resolve_selected_quote after all files are extracted.
    h_supplier: Optional[str] = header.get("supplier_name") or None
    _, match_conf = _compute_supplier_match(h_supplier, ras_context)

    logger.info(
        "Supplier match: extracted={!r} conf={}",
        h_supplier, match_conf,
    )

    # Header fields shared across every item row in this quotation
    header_fields = {
        "supplier_name":      h_supplier,
        "supplier_address":   header.get("supplier_address") or None,
        "supplier_country":   header.get("supplier_country") or None,
        "quotation_ref_no":   header.get("quotation_ref_no") or None,
        "quotation_date":     header.get("quotation_date"),
        "currency":           header.get("currency") or None,
        "validity_date":      header.get("validity_date"),
        "validity_days":      header.get("validity_days"),
        "payment_terms":      header.get("payment_terms") or None,
        "supplier_match_conf": match_conf,
    }

    results: list[ExtractedItem] = []
    for raw_item in items_raw:
        try:
            item = ExtractedItem.model_validate({
                "attachment_classify_fk": source.attachment_classify_fk,
                "embedded_classify_fk":   source.embedded_classify_fk,
                "is_selected_quote":      False,   # resolved later by resolve_selected_quote
                "quote_rank":             None,
                **header_fields,
                **raw_item,
            })
            results.append(item)
        except ValidationError as exc:
            logger.warning(
                "Skipping invalid item from LLM response for {}: {}",
                source.blob_path, exc,
            )

    logger.info(
        "Parsed {} item(s) from LLM response (supplier={} conf={})",
        len(results), h_supplier, match_conf,
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
                    is_selected_quote=False,
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


# ── LLM-based selection (post-extraction) ──


def _build_ras_selection_table(ras_context: RASContext) -> str:
    """Compact line-items table for the selection prompt."""
    header = "| DTL_ID | Description | Qty | UOM | Unit Price | Supplier |"
    sep    = "|--------|-------------|-----|-----|------------|----------|"
    rows   = [header, sep]
    for li in ras_context.line_items:
        rows.append(
            f"| {_na(li.purchase_dtl_id)} "
            f"| {_na(li.item_description)} "
            f"| {_na(li.quantity)} "
            f"| {_na(li.uom)} "
            f"| {_na(li.unit_price)} "
            f"| {_na(li.supplier_name)} |"
        )
    return "\n".join(rows)


def _build_sources_summary(
    source_keys: list[tuple],
    groups: dict[tuple, list[ExtractedItem]],
    ras_context: RASContext,
) -> str:
    """One section per extracted source showing supplier, date, and per-DTL prices."""
    ras_prices = {li.purchase_dtl_id: li.unit_price for li in ras_context.line_items}

    parts: list[str] = []
    for idx, key in enumerate(source_keys, 1):
        group = groups[key]
        rep   = group[0]
        price_by_dtl = {
            item.purchase_dtl_id: item.unit_price
            for item in group
            if item.purchase_dtl_id is not None
        }

        # Count how many line items have actual extracted prices vs stubs
        priced_count = sum(1 for p in price_by_dtl.values() if p is not None)
        total_count  = len(ras_prices)
        conf         = rep.supplier_match_conf or Decimal("0")

        price_rows = [
            "| DTL_ID | Extracted Unit Price | RAS Unit Price |",
            "|--------|---------------------|----------------|",
        ]
        for dtl_id, ras_price in ras_prices.items():
            price_rows.append(
                f"| {dtl_id} | {_na(price_by_dtl.get(dtl_id))} | {_na(ras_price)} |"
            )

        parts.append(
            f"### Source {idx}\n"
            f"Supplier             : {_na(rep.supplier_name)}\n"
            f"Supplier Match Conf  : {conf} (0=no match, 1=exact match)\n"
            f"Items with Price     : {priced_count}/{total_count} line items extracted\n"
            f"Quotation Date       : {_na(rep.quotation_date)}\n"
            f"Ref No               : {_na(rep.quotation_ref_no)}\n"
            f"Currency             : {_na(rep.currency)}\n\n"
            + "\n".join(price_rows)
        )

    return "\n\n".join(parts)


def _apply_llm_rankings(
    all_items: list[ExtractedItem],
    source_keys: list[tuple],
    groups: dict[tuple, list[ExtractedItem]],
    dtl_rankings: dict,
) -> None:
    """Apply per-DTL rankings returned by the selection LLM query."""
    rank_map: dict[tuple, Optional[int]] = {}

    for dtl_id_str, ranks in dtl_rankings.items():
        try:
            dtl_id = int(dtl_id_str)
        except (ValueError, TypeError):
            continue
        if not isinstance(ranks, list):
            continue
        for idx, rank in enumerate(ranks):
            if idx >= len(source_keys):
                break
            key = source_keys[idx]
            rank_map[(key, dtl_id)] = int(rank) if rank is not None else None

    for item in all_items:
        if item.purchase_dtl_id is None:
            continue
        key  = (item.attachment_classify_fk, item.embedded_classify_fk)
        rank = rank_map.get((key, item.purchase_dtl_id))
        item.quote_rank = rank


def run_selection_llm_query(
    all_items: list[ExtractedItem],
    ras_context: RASContext,
    config: ExtractionConfig,
) -> None:
    """Determine is_selected_quote and quote_rank for all extracted items.

    Algorithm (fully deterministic — no LLM needed):

    Step 1 — Rank
        compute_quote_ranks() assigns quote_rank per DTL_ID across all sources.
        Rank 1 = lowest unit_price; no-price stubs rank last.

    Step 2 — Select
        Only sources with ≥1 non-stub item are candidates.
        Each candidate is scored:
            supplier_match_conf   (60 %) — how well supplier name matches RAS
            rank-1 fraction       (40 %) — fraction of non-stub items ranked 1

        The candidate with the highest score wins.  Its non-stub items get
        is_selected_quote = True; all other items get False.

    Mutates items in-place.
    """
    from collections import defaultdict

    def _has_data(item: ExtractedItem) -> bool:
        return (
            item.unit_price      is not None
            or item.item_name        is not None
            or item.item_description is not None
        )

    # ── Step 1: rank across all sources ─────────────────────────────────
    compute_quote_ranks(all_items)

    # ── Step 2: mark rank-1 data items as selected ───────────────────────
    selected = 0
    for item in all_items:
        if item.quote_rank == 1 and _has_data(item):
            item.is_selected_quote = True
            selected += 1
        else:
            item.is_selected_quote = False

    logger.info(
        "Selection: {} item(s) marked is_selected_quote=True (rank=1 with data)",
        selected,
    )


# ── Quote ranking ──


def compute_quote_ranks(
    all_items: list[ExtractedItem],
) -> None:
    """Assign *quote_rank* per purchase_dtl_id across all quotation sources.

    Sorting rules (applied in order):
      1. unit_price ascending  — lower price = better rank; items with no
         unit_price are placed after all priced items.
      2. quotation_date descending (tie-breaker) — when two quotes have the
         same unit_price, the more recent quotation wins rank 1.
         Quotes with no date sort after those with a date.

    Every item for a DTL_ID gets a rank as long as at least one source for
    that DTL_ID has a unit_price.  DTL_IDs where every source has no price
    are left with quote_rank = None.
    Items with no purchase_dtl_id are always left with quote_rank = None.
    Mutates the items in-place.
    """

    from collections import defaultdict

    _MAX_PRICE = Decimal("999999999999")

    def _sort_key(item: ExtractedItem):
        d = item.quotation_date
        date_key = (-d.toordinal()) if d is not None else 1  # 1 > any negative
        price    = item.unit_price if item.unit_price is not None else _MAX_PRICE
        return (price, date_key)

    by_dtl: dict[int, list[ExtractedItem]] = defaultdict(list)
    for item in all_items:
        if item.purchase_dtl_id is not None:
            by_dtl[item.purchase_dtl_id].append(item)

    for dtl_id, group in by_dtl.items():
        # Skip DTL_IDs where no source extracted a price — nothing meaningful to rank
        if not any(item.unit_price is not None for item in group):
            continue
        sorted_group = sorted(group, key=_sort_key)
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

        items = _parse_llm_response(raw_response, source, ras_context)

        items = _align_to_ras_line_items(items, ras_context, source)

        # Boost supplier_match_conf when extracted prices align with RAS prices
        _apply_price_alignment_boost(items, ras_context)

        return items
