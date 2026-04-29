"""Persist BenchmarkResult rows to [ras_procurement].[benchmark_result]."""

from __future__ import annotations

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables

from .models import BenchmarkResult

_MERGE_SQL = f"""
    MERGE {AzureTables.BENCHMARK_RESULT} WITH (HOLDLOCK) AS target
    USING (SELECT ? AS purchase_dtl_id) AS src
      ON  target.[purchase_dtl_id] = src.[purchase_dtl_id]
    WHEN MATCHED THEN
        UPDATE SET
            [bp_unit_price]       = ?,
            [bp_total_price]      = ?,
            [low_hist_item_fk]    = ?,
            [last_hist_item_fk]   = ?,
            [inflation_pct]       = ?,
            [cpi_inflation_pct]   = ?,
            [similar_dtl_ids]     = ?,
            [summary]             = ?,
            [updated_at]          = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (
            [extracted_item_uuid_fk],
            [purchase_dtl_id],
            [bp_unit_price],
            [bp_total_price],
            [low_hist_item_fk],
            [last_hist_item_fk],
            [inflation_pct],
            [cpi_inflation_pct],
            [similar_dtl_ids],
            [summary]
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


class BenchmarkWriter(BaseRepository):
    """Writes BenchmarkResult rows to benchmark_result using MERGE (idempotent)."""

    def write(self, results: list[BenchmarkResult]) -> int:
        """Upsert all results.  Returns the number of rows written."""
        if not results:
            return 0

        for result in results:
            self._execute(
                _MERGE_SQL,
                # USING source key
                result.purchase_dtl_id,
                # WHEN MATCHED UPDATE
                result.bp_unit_price,
                result.bp_total_price,
                result.low_hist_item_fk,
                result.last_hist_item_fk,
                result.inflation_pct,
                result.cpi_inflation_pct,
                result.similar_dtl_ids,
                result.summary,
                # WHEN NOT MATCHED INSERT
                result.extracted_item_uuid_fk,
                result.purchase_dtl_id,
                result.bp_unit_price,
                result.bp_total_price,
                result.low_hist_item_fk,
                result.last_hist_item_fk,
                result.inflation_pct,
                result.cpi_inflation_pct,
                result.similar_dtl_ids,
                result.summary,
            )

        logger.info(
            "benchmark_result: upserted {} row(s) to {}",
            len(results), AzureTables.BENCHMARK_RESULT,
        )
        return len(results)
