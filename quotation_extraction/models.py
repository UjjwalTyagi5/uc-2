"""Domain models for the quotation-extraction pipeline."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional


# ── RAS / Line-item context (read from DB, passed to the LLM) ──


@dataclass(frozen=True)
class LineItemContext:
    """One row from purchase_req_detail, enriched with RAS-level data."""

    purchase_dtl_id: int
    purchase_req_id: int
    item_no: int
    quantity: Optional[Decimal]
    item_type: Optional[str]
    item_description: Optional[str]
    unit_price: Optional[Decimal] = None
    uom: Optional[str] = None
    supplier_name: Optional[str] = None
    discount: Optional[Decimal] = None
    req_value: Optional[Decimal] = None
    currency: Optional[str] = None
    delivery_date: Optional[str] = None
    payment_details: Optional[str] = None
    original_value: Optional[Decimal] = None
    initial_offer: Optional[Decimal] = None
    negotiation: Optional[Decimal] = None
    comments: Optional[str] = None
    prepayment: Optional[str] = None
    item_code: Optional[str] = None


@dataclass(frozen=True)
class RASContext:
    """Everything the LLM needs to know about a RAS before reading a quotation."""

    purchase_req_no: str
    purchase_req_id: int
    justification: Optional[str]
    supplier_name: Optional[str]
    currency: Optional[str]
    enquiry_no: Optional[str] = None
    classification: Optional[str] = None
    department: Optional[str] = None
    negotiated_by: Optional[str] = None
    address: Optional[str] = None
    contract_no: Optional[str] = None
    order_no: Optional[str] = None
    purchase_value: Optional[Decimal] = None
    category: Optional[str] = None
    sub_category: Optional[str] = None
    site_country: Optional[str] = None
    site_region: Optional[str] = None
    site: Optional[str] = None
    division: Optional[str] = None
    requisition_type: Optional[str] = None
    parent_supplier: Optional[str] = None
    supplier_type: Optional[str] = None
    supplier_country: Optional[str] = None
    payment_days: Optional[str] = None
    po_date: Optional[str] = None
    category_buyer: Optional[str] = None
    l5: Optional[str] = None
    l6: Optional[str] = None
    l7: Optional[str] = None
    l8: Optional[str] = None
    line_items: list[LineItemContext] = field(default_factory=list)


# ── Document content (produced by document_loader) ──


@dataclass
class DocumentContent:
    """LLM-consumable representation of a single quotation file.

    Exactly one of *text* or *images* is populated:
    * text   – plain/markdown string  (for XLSX, DOCX, TXT, …)
    * images – list of base-64 PNG strings  (for PDF pages, scanned images, …)
    """

    text: Optional[str] = None
    images: Optional[list[str]] = None
    source_path: str = ""
    page_count: int = 0

    @property
    def is_image_based(self) -> bool:
        return bool(self.images)


# ── Extracted item (LLM output → silver table row) ──


@dataclass
class ExtractedItem:
    """One row destined for [ras_procurement].[quotation_extracted_items]."""

    # ── linkage ──
    attachment_classify_fk: Optional[uuid.UUID] = None
    embedded_classify_fk: Optional[uuid.UUID] = None
    purchase_dtl_id: Optional[int] = None
    is_selected_quote: bool = False
    supplier_match_conf: Optional[Decimal] = None
    quote_rank: Optional[int] = None

    # ── supplier / header ──
    supplier_name: Optional[str] = None
    supplier_address: Optional[str] = None
    supplier_country: Optional[str] = None
    quotation_ref_no: Optional[str] = None
    quotation_date: Optional[date] = None
    currency: Optional[str] = None
    validity_date: Optional[date] = None
    validity_days: Optional[int] = None
    payment_terms: Optional[str] = None

    # ── item ──
    item_name: Optional[str] = None
    item_description: Optional[str] = None
    quantity: Optional[Decimal] = None
    unit: Optional[str] = None
    unit_price: Optional[Decimal] = None
    total_price: Optional[Decimal] = None
    discount: Optional[Decimal] = None
    taxation_details: Optional[str] = None
    delivery_date: Optional[date] = None
    delivery_time_days: Optional[int] = None

    # ── taxonomy ──
    item_level_1: Optional[str] = None
    item_level_2: Optional[str] = None
    item_level_3: Optional[str] = None
    item_level_4: Optional[str] = None
    item_level_5: Optional[str] = None
    item_level_6: Optional[str] = None
    item_level_7: Optional[str] = None
    item_level_8: Optional[str] = None

    # ── meta ──
    commodity_tag: Optional[str] = None
    item_summary: Optional[str] = None


@dataclass(frozen=True)
class QuotationSource:
    """Identifies one quotation file to be processed.

    The file lives in Azure Blob; *blob_path* is the container-relative key.
    Exactly one of the FK fields will be set (matching the DB constraint).
    """

    blob_path: str
    attachment_classify_fk: Optional[uuid.UUID] = None
    embedded_classify_fk: Optional[uuid.UUID] = None
    attachment_id: Optional[str] = None
