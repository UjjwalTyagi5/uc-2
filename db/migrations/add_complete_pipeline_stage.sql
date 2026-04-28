-- ============================================================
-- Migration: register COMPLETE as stage 8 in pipeline_stages
--
-- Run this against [ras-procurement-benchmark] before deploying
-- the updated pipeline that includes the CompleteStage.
-- ============================================================

INSERT INTO [ras_procurement].[pipeline_stages]
    ([STAGE_ID], [STAGE_NAME], [STAGE_DESC], [STAGE_DOMAIN], [STAGE_SEQUENCE])
VALUES
    (8, 'COMPLETE', 'Pipeline fully completed', 'ATTACHMENT', 8);
