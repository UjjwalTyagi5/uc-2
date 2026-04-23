"""Fetch is_selected_quote=1 items for a PR with full pricing data."""

from __future__ import annotations

from db.crud import BaseRepository
from db.tables import AzureTables

_FETCH_SQL = f"""
    SELECT
        qi.[extracted_item_uuid_pk],
        qi.[purchase_dtl_id],
        qi.[item_name],
        qi.[item_description],
        qi.[item_level_1],
        qi.[item_level_2],
        qi.[item_level_3],
        qi.[item_level_4],
        qi.[item_level_5],
        qi.[item_level_6],
        qi.[item_level_7],
        qi.[item_level_8],
        qi.[commodity_tag],
        qi.[item_summary],
        qi.[unit_price],
        qi.[total_price],
        qi.[quantity],
        qi.[unit],
        qi.[currency],
        qi.[quotation_date],
        qi.[supplier_name],
        qi.[unit_price_eur],
        qi.[total_price_eur],
        prd.[C_DATETIME]  AS [item_created_date],
        prm.[C_DATETIME]  AS [pr_c_datetime]
    FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS} qi
    JOIN {AzureTables.ATTACHMENT_CLASSIFICATION} ac
      ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
    JOIN {AzureTables.RAS_TRACKER} rt
      ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
    LEFT JOIN {AzureTables.PURCHASE_REQ_DETAIL} prd
      ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
    LEFT JOIN {AzureTables.PURCHASE_REQ_MST} prm
      ON rt.[purchase_req_no] = prm.[PURCHASE_REQ_NO]
    WHERE rt.[purchase_req_no] = ?
      AND qi.[is_selected_quote] = 1
    ORDER BY qi.[purchase_dtl_id]
"""


class BenchmarkItemReader(BaseRepository):
    """Fetches is_selected_quote=1 rows for a PR with pricing columns included."""

    def fetch(self, purchase_req_no: str) -> list[dict]:
        """Return one dict per unique purchase_dtl_id (first row wins on ties)."""
        rows, cols = self._fetch_with_columns(_FETCH_SQL, purchase_req_no)

        seen: dict[int, dict] = {}
        for row in rows:
            record = dict(zip(cols, row))
            dtl_id = record.get("purchase_dtl_id")
            if dtl_id is not None and dtl_id not in seen:
                seen[dtl_id] = record

        return list(seen.values())
