"""Find PRs that need price benchmarking, ordered by purchase date (older first)."""

from __future__ import annotations

from typing import Optional

from db.crud import BaseRepository
from db.tables import AzureTables

_EMBEDDINGS_STAGE_ID = 6  # PRs that completed EMBEDDINGS are ready for benchmarking

# Find PRs where current_stage_fk = 6 (EMBEDDINGS done, benchmark not yet run).
# Ordered by purchase_req_mst.C_DATETIME ASC so older purchase orders are
# benchmarked before newer ones.
_PENDING_SQL = f"""
    SELECT   rt.[purchase_req_no]
    FROM     {AzureTables.RAS_TRACKER}  rt
    JOIN     {AzureTables.PURCHASE_REQ_MST} mst
               ON  mst.[PURCHASE_REQ_NO] = rt.[purchase_req_no]
    WHERE    rt.[current_stage_fk] = {_EMBEDDINGS_STAGE_ID}
    ORDER BY mst.[C_DATETIME] ASC
"""

_PENDING_TOP_SQL = f"""
    SELECT   TOP (?) rt.[purchase_req_no]
    FROM     {AzureTables.RAS_TRACKER}  rt
    JOIN     {AzureTables.PURCHASE_REQ_MST} mst
               ON  mst.[PURCHASE_REQ_NO] = rt.[purchase_req_no]
    WHERE    rt.[current_stage_fk] = {_EMBEDDINGS_STAGE_ID}
    ORDER BY mst.[C_DATETIME] ASC
"""


class PendingPRReader(BaseRepository):
    """Finds PRs at the EMBEDDINGS stage (current_stage_fk = 6) that need benchmarking.

    Results are ordered by purchase_req_mst.C_DATETIME ascending so older
    purchase orders are benchmarked before newer ones.
    """

    def fetch(self, limit: Optional[int] = None) -> list[str]:
        """Return purchase_req_no values for unbenchmarked PRs.

        Parameters
        ----------
        limit:
            Cap the number of PRs returned.  None = return all pending.
        """
        if limit is not None:
            rows = self._fetch(_PENDING_TOP_SQL, limit)
        else:
            rows = self._fetch(_PENDING_SQL)
        return [str(row[0]) for row in rows]
