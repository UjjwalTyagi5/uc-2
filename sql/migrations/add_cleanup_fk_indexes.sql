-- ============================================================================
-- Migration: add indexes on FK columns used by the pipeline's cleanup DELETEs
--
-- Without these indexes _cleanup_for_pr's DELETE JOINs fall back to table
-- scans, which time out (HYT00) once the target tables grow past a few
-- thousand rows under a 300s-1200s query budget.  Each index is a covering
-- index for exactly the JOIN / WHERE columns used in cleanup; storage cost
-- is small compared with the time saved on every reprocess.
--
-- Safe to run multiple times — wrapped in IF NOT EXISTS checks.
-- ============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ----------------------------------------------------------------------------
-- attachment_classification: ras_uuid_pk is the FK to ras_tracker used by
-- every cleanup DELETE that joins through this table.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_attachment_classification_ras_uuid_pk'
       AND object_id = OBJECT_ID('[ras_procurement].[attachment_classification]')
)
BEGIN
    CREATE INDEX IX_attachment_classification_ras_uuid_pk
        ON [ras_procurement].[attachment_classification]([ras_uuid_pk])
        INCLUDE ([attachment_classify_uuid_pk], [attachment_id]);
    PRINT 'Created IX_attachment_classification_ras_uuid_pk';
END
GO

-- ----------------------------------------------------------------------------
-- quotation_extracted_items: two FK columns, both used by cleanup DELETEs.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_quotation_extracted_items_attachment_classify_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[quotation_extracted_items]')
)
BEGIN
    CREATE INDEX IX_quotation_extracted_items_attachment_classify_fk
        ON [ras_procurement].[quotation_extracted_items]([attachment_classify_fk])
        INCLUDE ([extracted_item_uuid_pk], [purchase_dtl_id], [is_selected_quote]);
    PRINT 'Created IX_quotation_extracted_items_attachment_classify_fk';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_quotation_extracted_items_embedded_classify_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[quotation_extracted_items]')
)
BEGIN
    CREATE INDEX IX_quotation_extracted_items_embedded_classify_fk
        ON [ras_procurement].[quotation_extracted_items]([embedded_classify_fk]);
    PRINT 'Created IX_quotation_extracted_items_embedded_classify_fk';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_quotation_extracted_items_purchase_dtl_id'
       AND object_id = OBJECT_ID('[ras_procurement].[quotation_extracted_items]')
)
BEGIN
    -- Used by benchmark_result's UQ join + Stage 7 historical lookup.
    CREATE INDEX IX_quotation_extracted_items_purchase_dtl_id
        ON [ras_procurement].[quotation_extracted_items]([purchase_dtl_id])
        INCLUDE ([is_selected_quote], [supplier_name]);
    PRINT 'Created IX_quotation_extracted_items_purchase_dtl_id';
END
GO

-- ----------------------------------------------------------------------------
-- embedded_attachment_classification: FK to attachment_classification used
-- by the cleanup join chain.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_embedded_attachment_classification_attachment_classification_id'
       AND object_id = OBJECT_ID('[ras_procurement].[embedded_attachment_classification]')
)
BEGIN
    CREATE INDEX IX_embedded_attachment_classification_attachment_classification_id
        ON [ras_procurement].[embedded_attachment_classification]([attachment_classification_id]);
    PRINT 'Created IX_embedded_attachment_classification_attachment_classification_id';
END
GO

-- ----------------------------------------------------------------------------
-- benchmark_result: three FK columns all used by cleanup. extracted_item_uuid_fk
-- is the join key; low_hist / last_hist are nulled then deleted.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_benchmark_result_extracted_item_uuid_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[benchmark_result]')
)
BEGIN
    CREATE INDEX IX_benchmark_result_extracted_item_uuid_fk
        ON [ras_procurement].[benchmark_result]([extracted_item_uuid_fk]);
    PRINT 'Created IX_benchmark_result_extracted_item_uuid_fk';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_benchmark_result_low_hist_item_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[benchmark_result]')
)
BEGIN
    CREATE INDEX IX_benchmark_result_low_hist_item_fk
        ON [ras_procurement].[benchmark_result]([low_hist_item_fk])
        WHERE [low_hist_item_fk] IS NOT NULL;
    PRINT 'Created IX_benchmark_result_low_hist_item_fk';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_benchmark_result_last_hist_item_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[benchmark_result]')
)
BEGIN
    CREATE INDEX IX_benchmark_result_last_hist_item_fk
        ON [ras_procurement].[benchmark_result]([last_hist_item_fk])
        WHERE [last_hist_item_fk] IS NOT NULL;
    PRINT 'Created IX_benchmark_result_last_hist_item_fk';
END
GO

-- ----------------------------------------------------------------------------
-- vw_get_ras_data_for_bidashboard: cleanup DELETEs filter on PURCHASE_REQ_NO.
-- Despite the name, this is a TABLE (per schema dump), not a view.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO'
       AND object_id = OBJECT_ID('[ras_procurement].[vw_get_ras_data_for_bidashboard]')
)
BEGIN
    CREATE INDEX IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO
        ON [ras_procurement].[vw_get_ras_data_for_bidashboard]([PURCHASE_REQ_NO]);
    PRINT 'Created IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO';
END
GO

-- ----------------------------------------------------------------------------
-- ras_pipeline_exceptions: cleanup deletes by ras_tracker_id.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_ras_pipeline_exceptions_ras_tracker_id'
       AND object_id = OBJECT_ID('[ras_procurement].[ras_pipeline_exceptions]')
)
BEGIN
    CREATE INDEX IX_ras_pipeline_exceptions_ras_tracker_id
        ON [ras_procurement].[ras_pipeline_exceptions]([ras_tracker_id]);
    PRINT 'Created IX_ras_pipeline_exceptions_ras_tracker_id';
END
GO

-- ----------------------------------------------------------------------------
-- ras_tracker.current_stage_fk: used by _fetch_pending_prs WHERE clauses,
-- skip-if-complete worker check, and re-queue logic.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_ras_tracker_current_stage_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[ras_tracker]')
)
BEGIN
    CREATE INDEX IX_ras_tracker_current_stage_fk
        ON [ras_procurement].[ras_tracker]([current_stage_fk])
        INCLUDE ([purchase_req_no], [ras_uuid_pk], [last_processed_at]);
    PRINT 'Created IX_ras_tracker_current_stage_fk';
END
GO

PRINT '--- migration complete ---';
GO
