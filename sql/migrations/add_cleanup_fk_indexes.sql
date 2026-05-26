-- ============================================================================
-- Migration: add indexes on FK columns used by the pipeline's cleanup DELETEs
--
-- Without these indexes _cleanup_for_pr's DELETE JOINs fall back to table
-- scans, which time out (HYT00 "Query timeout expired") once the target
-- tables grow past a few thousand rows under pyodbc's 300s query budget.
--
-- Each index is sized for exactly the JOIN / WHERE columns used in cleanup;
-- storage cost is small compared with the time saved on every reprocess.
--
-- SAFE TO RE-RUN — every CREATE INDEX is guarded by IF NOT EXISTS so it's a
-- no-op if the index already exists.
--
-- Target DB: ras-procurement-benchmark_2026
-- Run as a user with db_ddladmin or db_owner on that DB.
-- Expected duration: 30s - 3min total depending on current table sizes.
-- ============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '=== Adding cleanup-path indexes on ras_procurement schema ===';
PRINT '';
GO

-- ----------------------------------------------------------------------------
-- 1. attachment_classification.ras_uuid_pk
--    FK to ras_tracker. Used by every cleanup DELETE that joins ac → rt.
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
    PRINT '  Created IX_attachment_classification_ras_uuid_pk';
END
ELSE PRINT '  Skipped IX_attachment_classification_ras_uuid_pk (exists)';
GO

-- ----------------------------------------------------------------------------
-- 2. quotation_extracted_items.attachment_classify_fk
--    FK to attachment_classification. Used by Pinecone-id SELECT and the
--    quotation_extracted_items DELETE in cleanup.
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
    PRINT '  Created IX_quotation_extracted_items_attachment_classify_fk';
END
ELSE PRINT '  Skipped IX_quotation_extracted_items_attachment_classify_fk (exists)';
GO

-- ----------------------------------------------------------------------------
-- 3. quotation_extracted_items.embedded_classify_fk
--    Optional FK; nullable. Used by some Stage 7 lookups.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_quotation_extracted_items_embedded_classify_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[quotation_extracted_items]')
)
BEGIN
    CREATE INDEX IX_quotation_extracted_items_embedded_classify_fk
        ON [ras_procurement].[quotation_extracted_items]([embedded_classify_fk])
        WHERE [embedded_classify_fk] IS NOT NULL;
    PRINT '  Created IX_quotation_extracted_items_embedded_classify_fk';
END
ELSE PRINT '  Skipped IX_quotation_extracted_items_embedded_classify_fk (exists)';
GO

-- ----------------------------------------------------------------------------
-- 4. quotation_extracted_items.purchase_dtl_id
--    Used by benchmark_result UQ, Stage 7 historical lookups, and the
--    vw_pr_document_summary "quotation_file_paths" subquery.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_quotation_extracted_items_purchase_dtl_id'
       AND object_id = OBJECT_ID('[ras_procurement].[quotation_extracted_items]')
)
BEGIN
    CREATE INDEX IX_quotation_extracted_items_purchase_dtl_id
        ON [ras_procurement].[quotation_extracted_items]([purchase_dtl_id])
        INCLUDE ([is_selected_quote], [supplier_name], [attachment_classify_fk], [embedded_classify_fk]);
    PRINT '  Created IX_quotation_extracted_items_purchase_dtl_id';
END
ELSE PRINT '  Skipped IX_quotation_extracted_items_purchase_dtl_id (exists)';
GO

-- ----------------------------------------------------------------------------
-- 5. embedded_attachment_classification.attachment_classification_id
--    FK to attachment_classification. Used by cleanup join chain and the
--    document-summary view.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_embedded_attachment_classification_attachment_classification_id'
       AND object_id = OBJECT_ID('[ras_procurement].[embedded_attachment_classification]')
)
BEGIN
    CREATE INDEX IX_embedded_attachment_classification_attachment_classification_id
        ON [ras_procurement].[embedded_attachment_classification]([attachment_classification_id])
        INCLUDE ([embedded_attachment_classification_id], [doc_type], [classification_conf]);
    PRINT '  Created IX_embedded_attachment_classification_attachment_classification_id';
END
ELSE PRINT '  Skipped IX_embedded_attachment_classification_attachment_classification_id (exists)';
GO

-- ----------------------------------------------------------------------------
-- 6. benchmark_result.extracted_item_uuid_fk
--    Join key for benchmark_result → quotation_extracted_items in cleanup.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_benchmark_result_extracted_item_uuid_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[benchmark_result]')
)
BEGIN
    CREATE INDEX IX_benchmark_result_extracted_item_uuid_fk
        ON [ras_procurement].[benchmark_result]([extracted_item_uuid_fk]);
    PRINT '  Created IX_benchmark_result_extracted_item_uuid_fk';
END
ELSE PRINT '  Skipped IX_benchmark_result_extracted_item_uuid_fk (exists)';
GO

-- ----------------------------------------------------------------------------
-- 7. benchmark_result.low_hist_item_fk
--    Cleanup nulls this FK first, then deletes the row. Filtered to
--    NOT NULL keeps the index small (most rows have low_hist filled in
--    only after Stage 7 completes).
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_benchmark_result_low_hist_item_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[benchmark_result]')
)
BEGIN
    CREATE INDEX IX_benchmark_result_low_hist_item_fk
        ON [ras_procurement].[benchmark_result]([low_hist_item_fk])
        WHERE [low_hist_item_fk] IS NOT NULL;
    PRINT '  Created IX_benchmark_result_low_hist_item_fk';
END
ELSE PRINT '  Skipped IX_benchmark_result_low_hist_item_fk (exists)';
GO

-- ----------------------------------------------------------------------------
-- 8. benchmark_result.last_hist_item_fk  — same reasoning as #7.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_benchmark_result_last_hist_item_fk'
       AND object_id = OBJECT_ID('[ras_procurement].[benchmark_result]')
)
BEGIN
    CREATE INDEX IX_benchmark_result_last_hist_item_fk
        ON [ras_procurement].[benchmark_result]([last_hist_item_fk])
        WHERE [last_hist_item_fk] IS NOT NULL;
    PRINT '  Created IX_benchmark_result_last_hist_item_fk';
END
ELSE PRINT '  Skipped IX_benchmark_result_last_hist_item_fk (exists)';
GO

-- ----------------------------------------------------------------------------
-- 9. vw_get_ras_data_for_bidashboard.PURCHASE_REQ_NO
--    Despite the "vw_" prefix this is a TABLE (per schema dump). Cleanup
--    deletes rows by PURCHASE_REQ_NO; without this index it's a heap scan.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO'
       AND object_id = OBJECT_ID('[ras_procurement].[vw_get_ras_data_for_bidashboard]')
)
BEGIN
    CREATE INDEX IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO
        ON [ras_procurement].[vw_get_ras_data_for_bidashboard]([PURCHASE_REQ_NO]);
    PRINT '  Created IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO';
END
ELSE PRINT '  Skipped IX_vw_get_ras_data_for_bidashboard_PURCHASE_REQ_NO (exists)';
GO

-- ----------------------------------------------------------------------------
-- 10. ras_pipeline_exceptions.ras_tracker_id
--     Cleanup deletes by ras_tracker_id; FK lookups during exception writes.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_ras_pipeline_exceptions_ras_tracker_id'
       AND object_id = OBJECT_ID('[ras_procurement].[ras_pipeline_exceptions]')
)
BEGIN
    CREATE INDEX IX_ras_pipeline_exceptions_ras_tracker_id
        ON [ras_procurement].[ras_pipeline_exceptions]([ras_tracker_id])
        INCLUDE ([stage_id], [created_at]);
    PRINT '  Created IX_ras_pipeline_exceptions_ras_tracker_id';
END
ELSE PRINT '  Skipped IX_ras_pipeline_exceptions_ras_tracker_id (exists)';
GO

-- ----------------------------------------------------------------------------
-- 11. ras_tracker.current_stage_fk
--     Heavily used by _fetch_pending_prs (WHERE current_stage_fk IN (...)),
--     the worker-side skip-if-complete check, and re-queue detection.
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
    PRINT '  Created IX_ras_tracker_current_stage_fk';
END
ELSE PRINT '  Skipped IX_ras_tracker_current_stage_fk (exists)';
GO

PRINT '';
PRINT '=== Migration complete ===';
GO
