-- ============================================================
-- Migration: replace raw low/last price columns in
--            benchmark_result with FK references to
--            quotation_extracted_items, and add CPI inflation.
--
-- Run this against [ras-procurement-benchmark] BEFORE deploying
-- the updated benchmarking code.
-- ============================================================

-- Step 1: drop the four raw price columns
ALTER TABLE [ras_procurement].[benchmark_result]
    DROP COLUMN [low_hist_unit_price];

ALTER TABLE [ras_procurement].[benchmark_result]
    DROP COLUMN [low_hist_total_price];

ALTER TABLE [ras_procurement].[benchmark_result]
    DROP COLUMN [last_hist_unit_price];

ALTER TABLE [ras_procurement].[benchmark_result]
    DROP COLUMN [last_hist_total_price];

-- Step 2: add FK columns pointing to quotation_extracted_items
--         and the CPI-based inflation column
ALTER TABLE [ras_procurement].[benchmark_result]
    ADD [low_hist_item_fk]  UNIQUEIDENTIFIER NULL,
        [last_hist_item_fk] UNIQUEIDENTIFIER NULL,
        [cpi_inflation_pct] DECIMAL(10, 4)   NULL;

-- Step 3: add FK constraints so every non-NULL low/last reference
--         must exist in quotation_extracted_items
ALTER TABLE [ras_procurement].[benchmark_result]
    ADD CONSTRAINT [FK_benchmark_result_low_hist_item]
        FOREIGN KEY ([low_hist_item_fk])
        REFERENCES [ras_procurement].[quotation_extracted_items] ([extracted_item_uuid_pk]);

ALTER TABLE [ras_procurement].[benchmark_result]
    ADD CONSTRAINT [FK_benchmark_result_last_hist_item]
        FOREIGN KEY ([last_hist_item_fk])
        REFERENCES [ras_procurement].[quotation_extracted_items] ([extracted_item_uuid_pk]);
