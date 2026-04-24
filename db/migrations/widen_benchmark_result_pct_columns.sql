-- ============================================================
-- Migration: widen inflation_pct columns in benchmark_result
--
-- inflation_pct (LLM estimate) can exceed 999% for old items,
-- causing DECIMAL overflow.  Widen both pct columns to
-- DECIMAL(10, 4) so values up to 999999.9999 are accepted.
--
-- Run against [ras-procurement-benchmark].
-- ============================================================

ALTER TABLE [ras_procurement].[benchmark_result]
    ALTER COLUMN [inflation_pct]     DECIMAL(10, 4) NULL;

ALTER TABLE [ras_procurement].[benchmark_result]
    ALTER COLUMN [cpi_inflation_pct] DECIMAL(10, 4) NULL;
