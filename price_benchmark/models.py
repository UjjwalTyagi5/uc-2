"""Domain models for the price benchmark module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional


@dataclass
class HistoricalItem:
    """One similar item fetched from quotation_extracted_items via Pinecone match."""

    purchase_dtl_id: int
    unit_price:      Optional[Decimal]
    total_price:     Optional[Decimal]
    quantity:        Optional[Decimal]
    unit:            Optional[str]
    currency:        Optional[str]
    quotation_date:  Optional[date]
    supplier_name:   Optional[str]
    unit_price_eur:  Optional[Decimal] = None
    total_price_eur: Optional[Decimal] = None


@dataclass
class BenchmarkResult:
    """One row destined for [ras_procurement].[benchmark_result]."""

    extracted_item_uuid_fk: str           # UUID str FK → quotation_extracted_items
    purchase_dtl_id:         int
    bp_unit_price:           Optional[Decimal]
    bp_total_price:          Optional[Decimal]
    low_hist_unit_price:     Optional[Decimal]
    low_hist_total_price:    Optional[Decimal]
    last_hist_unit_price:    Optional[Decimal]
    last_hist_total_price:   Optional[Decimal]
    inflation_pct:           Optional[Decimal]
    similar_dtl_ids:         Optional[str]   # JSON array string, e.g. "[123, 456]"
    summary:                 Optional[str]
