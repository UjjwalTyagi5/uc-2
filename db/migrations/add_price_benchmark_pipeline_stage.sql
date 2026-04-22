-- ============================================================
-- Migration: register PRICE_BENCHMARK as stage 7 in
--            pipeline_stages so ras_tracker FK is satisfied
--
-- Run this against [ras-procurement-benchmark] BEFORE running
-- python -m price_benchmark for the first time.
-- ============================================================

INSERT INTO [ras_procurement].[pipeline_stages]
    ([STAGE_ID], [STAGE_NAME], [STAGE_DESC], [STAGE_DOMAIN], [STAGE_SEQUENCE])
VALUES
    (7, 'PRICE_BENCHMARK', 'Price benchmark analysis', 'ATTACHMENT', 7);
