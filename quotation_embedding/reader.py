"""Read selected quotation items from Azure SQL for embedding."""

from __future__ import annotations

from db.crud import BaseRepository
from db.tables import AzureTables

_FETCH_SQL = f"""
    SELECT
        qi.[extracted_item_uuid_pk],
        qi.[purchase_dtl_id],
        qi.[commodity_tag],
        qi.[item_level_1],
        qi.[item_level_2],
        qi.[item_level_3],
        qi.[item_level_4],
        qi.[item_level_5],
        qi.[item_level_6],
        qi.[item_level_7],
        qi.[item_level_8],
        qi.[item_summary],
        prd.[C_DATETIME] AS [item_created_date]
    FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS} qi
    JOIN {AzureTables.ATTACHMENT_CLASSIFICATION} ac
      ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
    JOIN {AzureTables.RAS_TRACKER} rt
      ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
    LEFT JOIN {AzureTables.PURCHASE_REQ_DETAIL} prd
      ON qi.[purchase_dtl_id] = prd.[PURCHASE_REQ_DTL_ID]
    WHERE rt.[purchase_req_no] = ?
      AND qi.[is_selected_quote] = 1
    ORDER BY qi.[purchase_dtl_id]
"""


class EmbeddingItemReader(BaseRepository):
    """Fetches is_selected_quote=1 rows for a PR, deduplicated by purchase_dtl_id."""

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
