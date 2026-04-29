"""Domain models for the price benchmark module."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional


@dataclass
class HistoricalItem:
    """One similar item fetched from quotation_extracted_items via Pinecone match."""

    purchase_dtl_id:       int
    unit_price:            Optional[Decimal]
    total_price:           Optional[Decimal]
    quantity:              Optional[Decimal]
    unit:                  Optional[str]
    currency:              Optional[str]
    quotation_date:        Optional[date]
    supplier_name:         Optional[str]
    unit_price_eur:        Optional[Decimal] = None
    total_price_eur:       Optional[Decimal] = None
    # Fields added for FK traceability and CPI inflation
    extracted_item_uuid_pk: Optional[str]  = None  # PK from quotation_extracted_items
    supplier_country:       Optional[str]  = None  # for CPI country resolution
    purchase_req_no:        Optional[str]  = None  # PR this item belongs to
    pr_c_datetime:          Optional[date] = None  # purchase_req_mst.C_DATETIME for its PR


@dataclass
class BenchmarkResult:
    """One row destined for [ras_procurement].[benchmark_result]."""

    extracted_item_uuid_fk: str            # UUID str FK → quotation_extracted_items
    purchase_dtl_id:         int
    bp_unit_price:           Optional[Decimal]
    bp_total_price:          Optional[Decimal]
    # FK references to the actual low/last historical rows (replaces raw price cols)
    low_hist_item_fk:        Optional[str]     # extracted_item_uuid_pk of low-price row
    last_hist_item_fk:       Optional[str]     # extracted_item_uuid_pk of most-recent row
    inflation_pct:           Optional[Decimal] # LLM-estimated inflation %
    cpi_inflation_pct:       Optional[Decimal] # World Bank CPI-based inflation %
    similar_dtl_ids:         Optional[str]     # JSON array string, e.g. "[123, 456]"
    summary:                 Optional[str]
