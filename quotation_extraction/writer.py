"""Persist extracted items to quotation_extracted_items.

All DB access goes through BaseRepository so writes get automatic retry
on transient Azure SQL errors, consistent connection management, and
bulk-insert via fast_executemany for better performance.
"""

from __future__ import annotations

from decimal import Decimal

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables
from utils.currency_converter import CurrencyConverter

from .config import ExtractionConfig
from .models import ExtractedItem

_INSERT_SQL = f"""
INSERT INTO {AzureTables.QUOTATION_EXTRACTED_ITEMS} (
    [attachment_classify_fk],
    [embedded_classify_fk],
    [purchase_dtl_id],
    [is_selected_quote],
    [supplier_match_conf],
    [quote_rank],
    [supplier_name],
    [supplier_address],
    [supplier_country],
    [quotation_ref_no],
    [quotation_date],
    [currency],
    [validity_date],
    [validity_days],
    [payment_terms],
    [item_name],
    [item_description],
    [quantity],
    [unit],
    [unit_price],
    [total_price],
    [discount],
    [taxation_details],
    [delivery_date],
    [delivery_time_days],
    [item_level_1],
    [item_level_2],
    [item_level_3],
    [item_level_4],
    [item_level_5],
    [item_level_6],
    [item_level_7],
    [item_level_8],
    [commodity_tag],
    [item_summary],
    [unit_price_eur],
    [total_price_eur]
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?
)
"""

_DELETE_BY_ATTACHMENT_SQL = f"""
DELETE FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS}
 WHERE [attachment_classify_fk] = ?
   AND [embedded_classify_fk] IS NULL
"""

_DELETE_BY_EMBEDDED_SQL = f"""
DELETE FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS}
 WHERE [embedded_classify_fk] = ?
"""


def _p(val: object) -> object:
    """Convert Python types to pyodbc-safe values.

    Decimal → float: pyodbc fast_executemany fails when Decimal objects have
    varying scale across rows (e.g. Decimal("1.00000") vs Decimal("1.5")).
    SQL Server accepts float for all DECIMAL columns and rounds to column scale.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, Decimal):
        return float(val)
    return val


def _item_to_row(item: ExtractedItem) -> list:
    """Convert one ExtractedItem to the ordered INSERT parameter list."""
    return [
        _p(str(item.attachment_classify_fk) if item.attachment_classify_fk else None),
        _p(str(item.embedded_classify_fk)   if item.embedded_classify_fk   else None),
        _p(item.purchase_dtl_id),
        _p(item.is_selected_quote),
        _p(item.supplier_match_conf),
        _p(item.quote_rank),
        _p(item.supplier_name),
        _p(item.supplier_address),
        _p(item.supplier_country),
        _p(item.quotation_ref_no),
        _p(item.quotation_date),
        _p(item.currency),
        _p(item.validity_date),
        _p(item.validity_days),
        _p(item.payment_terms),
        _p(item.item_name),
        _p(item.item_description),
        _p(item.quantity),
        _p(item.unit),
        _p(item.unit_price),
        _p(item.total_price),
        _p(item.discount),
        _p(item.taxation_details),
        _p(item.delivery_date),
        _p(item.delivery_time_days),
        _p(item.item_level_1),
        _p(item.item_level_2),
        _p(item.item_level_3),
        _p(item.item_level_4),
        _p(item.item_level_5),
        _p(item.item_level_6),
        _p(item.item_level_7),
        _p(item.item_level_8),
        _p(item.commodity_tag),
        _p(item.item_summary),
        _p(item.unit_price_eur),
        _p(item.total_price_eur),
    ]


class ExtractionWriter(BaseRepository):
    """Writes :class:`ExtractedItem` rows to the silver table."""

    def __init__(self, config: ExtractionConfig) -> None:
        super().__init__(config.get_azure_conn_str())

    def write(self, items: list[ExtractedItem], *, replace: bool = True) -> int:
        """Insert items into the silver table.

        If *replace* is True, existing rows for the same attachment /
        embedded FK are deleted first (idempotent re-runs).

        Returns the number of rows written.
        """
        if not items:
            return 0

        if replace:
            self._delete_existing(items)

        converter = CurrencyConverter(self._conn_str)
        for item in items:
            eur_unit  = converter.to_eur(item.unit_price,  item.currency, item.quotation_date)
            eur_total = converter.to_eur(item.total_price, item.currency, item.quotation_date)
            # Quantize to match DB column precision: DECIMAL(18,4) and DECIMAL(18,2)
            item.unit_price_eur  = eur_unit.quantize(Decimal("0.0001"))  if eur_unit  is not None else None
            item.total_price_eur = eur_total.quantize(Decimal("0.01"))   if eur_total is not None else None

        rows = [_item_to_row(item) for item in items]
        self._execute_many(_INSERT_SQL, rows)

        logger.info(
            "Wrote {} row(s) to {}",
            len(rows), AzureTables.QUOTATION_EXTRACTED_ITEMS,
        )
        return len(rows)

    def _delete_existing(self, items: list[ExtractedItem]) -> None:
        """Delete pre-existing rows for the same source FK(s).

        Embedded items carry both FKs — delete by the more-specific key
        (embedded FK) so rows from sibling embedded files are untouched.
        Parent-only items are deleted by their attachment FK.
        """
        deleted_emb: set[str] = set()
        deleted_att: set[str] = set()

        for item in items:
            if item.embedded_classify_fk:
                key = str(item.embedded_classify_fk)
                if key not in deleted_emb:
                    self._execute(_DELETE_BY_EMBEDDED_SQL, key)
                    deleted_emb.add(key)
            elif item.attachment_classify_fk:
                key = str(item.attachment_classify_fk)
                if key not in deleted_att:
                    self._execute(_DELETE_BY_ATTACHMENT_SQL, key)
                    deleted_att.add(key)
