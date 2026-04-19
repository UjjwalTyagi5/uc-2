"""Persist extracted items to [ras_procurement].[quotation_extracted_items]."""

from __future__ import annotations

import pyodbc
from loguru import logger

from pipeline.db_utils import connect_with_retry

from .config import ExtractionConfig
from .models import ExtractedItem

_SCHEMA = "ras_procurement"
_TABLE = "quotation_extracted_items"

_INSERT_SQL = f"""
INSERT INTO [{_SCHEMA}].[{_TABLE}] (
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
    [item_summary]
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?
)
"""

_DELETE_BY_ATTACHMENT_SQL = f"""
DELETE FROM [{_SCHEMA}].[{_TABLE}]
 WHERE [attachment_classify_fk] = ?
"""

_DELETE_BY_EMBEDDED_SQL = f"""
DELETE FROM [{_SCHEMA}].[{_TABLE}]
 WHERE [embedded_classify_fk] = ?
"""


def _to_param(val: object) -> object:
    """Convert Python types to pyodbc-friendly values."""
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    return val


class ExtractionWriter:
    """Writes :class:`ExtractedItem` rows to the silver table."""

    def __init__(self, config: ExtractionConfig) -> None:
        self._conn_str = config.get_azure_conn_str()

    def write(self, items: list[ExtractedItem], *, replace: bool = True) -> int:
        """Insert items into the silver table.

        If *replace* is True, existing rows for the same attachment /
        embedded FK are deleted first (idempotent re-runs).

        Returns the number of rows written.
        """
        if not items:
            return 0

        conn: pyodbc.Connection = connect_with_retry(
            self._conn_str, autocommit=False
        )
        try:
            cursor = conn.cursor()

            if replace:
                self._delete_existing(cursor, items)

            written = 0
            for item in items:
                params = (
                    _to_param(str(item.attachment_classify_fk) if item.attachment_classify_fk else None),
                    _to_param(str(item.embedded_classify_fk) if item.embedded_classify_fk else None),
                    _to_param(item.purchase_dtl_id),
                    _to_param(item.is_selected_quote),
                    _to_param(item.supplier_match_conf),
                    _to_param(item.quote_rank),
                    _to_param(item.supplier_name),
                    _to_param(item.supplier_address),
                    _to_param(item.supplier_country),
                    _to_param(item.quotation_ref_no),
                    _to_param(item.quotation_date),
                    _to_param(item.currency),
                    _to_param(item.validity_date),
                    _to_param(item.validity_days),
                    _to_param(item.payment_terms),
                    _to_param(item.item_name),
                    _to_param(item.item_description),
                    _to_param(item.quantity),
                    _to_param(item.unit),
                    _to_param(item.unit_price),
                    _to_param(item.total_price),
                    _to_param(item.discount),
                    _to_param(item.taxation_details),
                    _to_param(item.delivery_date),
                    _to_param(item.delivery_time_days),
                    _to_param(item.item_level_1),
                    _to_param(item.item_level_2),
                    _to_param(item.item_level_3),
                    _to_param(item.item_level_4),
                    _to_param(item.item_level_5),
                    _to_param(item.item_level_6),
                    _to_param(item.item_level_7),
                    _to_param(item.item_level_8),
                    _to_param(item.commodity_tag),
                    _to_param(item.item_summary),
                )
                cursor.execute(_INSERT_SQL, params)
                written += 1

            conn.commit()
            logger.info("Wrote {} rows to {}.{}", written, _SCHEMA, _TABLE)
            return written

        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _delete_existing(
        cursor: pyodbc.Cursor,
        items: list[ExtractedItem],
    ) -> None:
        """Delete pre-existing rows for the same source FK(s)."""
        deleted_att: set[str] = set()
        deleted_emb: set[str] = set()

        for item in items:
            if item.attachment_classify_fk:
                key = str(item.attachment_classify_fk)
                if key not in deleted_att:
                    cursor.execute(_DELETE_BY_ATTACHMENT_SQL, key)
                    deleted_att.add(key)
            elif item.embedded_classify_fk:
                key = str(item.embedded_classify_fk)
                if key not in deleted_emb:
                    cursor.execute(_DELETE_BY_EMBEDDED_SQL, key)
                    deleted_emb.add(key)
