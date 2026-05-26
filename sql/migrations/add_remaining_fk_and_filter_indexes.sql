-- ============================================================================
-- Migration: remaining FK / filter indexes derived from script 1.sql schema
--
-- Companion to:
--   • sql/migrations/add_cleanup_fk_indexes.sql   (output-table FK columns)
--   • sql/migrations/add_source_table_indexes.sql (heap source-table columns)
--
-- This file plugs the last gaps the schema dump revealed:
--
--   (1) embedded_attachment_classification.parent_attachment_id is declared
--       as FK_embedded_attachment_attachment_id in the schema but has no
--       supporting index. SQL Server never auto-indexes FK columns.
--       Without this, the FK constraint check on every INSERT does a heap
--       scan of attachment_classification.attachment_id (which IS indexed
--       on the UQ side — so the scan is cheap-ish — but the *reverse*
--       lookup from embedded → parent_attachment_id is unindexed.)
--
--   (2) ras_pipeline_exceptions.stage_id — declared FK to pipeline_stages.
--       Tiny lookup but indexing the child side keeps cascades fast.
--
--   (3) Filter-friendly indexes on doc_type and is_selected_quote, which
--       are the most common WHERE clauses against
--       attachment_classification / embedded_attachment_classification /
--       quotation_extracted_items in the existing views.
--
-- Safe to re-run — IF NOT EXISTS on every CREATE INDEX.
--
-- Target DB: ras-procurement-benchmark_2026
-- ============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '=== Adding remaining FK + filter indexes (gap-fill from schema dump) ===';
PRINT '';
GO

-- ----------------------------------------------------------------------------
-- 1. embedded_attachment_classification.parent_attachment_id
--    FK_embedded_attachment_attachment_id is declared but unindexed.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_embedded_attachment_classification_parent_attachment_id'
       AND object_id = OBJECT_ID('[ras_procurement].[embedded_attachment_classification]')
)
BEGIN
    CREATE INDEX IX_embedded_attachment_classification_parent_attachment_id
        ON [ras_procurement].[embedded_attachment_classification]([parent_attachment_id]);
    PRINT '  Created IX_embedded_attachment_classification_parent_attachment_id';
END
ELSE PRINT '  Skipped IX_embedded_attachment_classification_parent_attachment_id (exists)';
GO

-- ----------------------------------------------------------------------------
-- 2. ras_pipeline_exceptions.stage_id
--    Small FK lookup but worth the few KB.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_ras_pipeline_exceptions_stage_id'
       AND object_id = OBJECT_ID('[ras_procurement].[ras_pipeline_exceptions]')
)
BEGIN
    CREATE INDEX IX_ras_pipeline_exceptions_stage_id
        ON [ras_procurement].[ras_pipeline_exceptions]([stage_id]);
    PRINT '  Created IX_ras_pipeline_exceptions_stage_id';
END
ELSE PRINT '  Skipped IX_ras_pipeline_exceptions_stage_id (exists)';
GO

-- ----------------------------------------------------------------------------
-- 3. attachment_classification.doc_type — filtered (NOT NULL, <> Others)
--    Matches vw_pr_document_summary WHERE clauses.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_attachment_classification_doc_type_not_others'
       AND object_id = OBJECT_ID('[ras_procurement].[attachment_classification]')
)
BEGIN
    CREATE INDEX IX_attachment_classification_doc_type_not_others
        ON [ras_procurement].[attachment_classification]([doc_type])
        INCLUDE ([ras_uuid_pk], [file_path], [classification_conf],
                 [attachment_classify_uuid_pk])
        WHERE [doc_type] IS NOT NULL AND [doc_type] <> 'Others';
    PRINT '  Created IX_attachment_classification_doc_type_not_others';
END
ELSE PRINT '  Skipped IX_attachment_classification_doc_type_not_others (exists)';
GO

-- ----------------------------------------------------------------------------
-- 4. embedded_attachment_classification.doc_type — filtered
--    Same reason — matches vw_pr_document_summary embedded-files leg.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_embedded_attachment_classification_doc_type_not_others'
       AND object_id = OBJECT_ID('[ras_procurement].[embedded_attachment_classification]')
)
BEGIN
    CREATE INDEX IX_embedded_attachment_classification_doc_type_not_others
        ON [ras_procurement].[embedded_attachment_classification]([doc_type])
        INCLUDE ([attachment_classification_id], [file_path],
                 [classification_conf])
        WHERE [doc_type] IS NOT NULL AND [doc_type] <> 'Others';
    PRINT '  Created IX_embedded_attachment_classification_doc_type_not_others';
END
ELSE PRINT '  Skipped IX_embedded_attachment_classification_doc_type_not_others (exists)';
GO

-- ----------------------------------------------------------------------------
-- 5. quotation_extracted_items.is_selected_quote — filtered (=1 only)
--    Used by:
--      • cleanup (collects pinecone ids WHERE is_selected_quote=1)
--      • vw_benchmark_summary's proposals OUTER APPLY (ISNULL(...) = 0 path)
--      • vw_pr_document_summary's quotation paths subquery
--      • Stage 7 historical lookup
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_quotation_extracted_items_is_selected_quote'
       AND object_id = OBJECT_ID('[ras_procurement].[quotation_extracted_items]')
)
BEGIN
    CREATE INDEX IX_quotation_extracted_items_is_selected_quote
        ON [ras_procurement].[quotation_extracted_items]([purchase_dtl_id], [is_selected_quote])
        INCLUDE ([extracted_item_uuid_pk], [supplier_name], [unit_price],
                 [unit_price_eur], [total_price], [total_price_eur],
                 [currency], [quantity], [payment_terms], [supplier_country],
                 [quotation_date]);
    PRINT '  Created IX_quotation_extracted_items_is_selected_quote';
END
ELSE PRINT '  Skipped IX_quotation_extracted_items_is_selected_quote (exists)';
GO

-- ----------------------------------------------------------------------------
-- 6. purchase_req_mst composite — PURCHASEFINALAPPROVALSTATUS + C_DATETIME
--    _fetch_pending_prs filters by status, _sort_excel_pr_list_by_date
--    sorts by C_DATETIME. Single composite index serves both.
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_req_mst_status_date'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_req_mst]')
)
BEGIN
    -- PURCHASEFINALAPPROVALSTATUS is nvarchar(max) so it can't be a key column
    -- directly. Use C_DATETIME as the key + INCLUDE the status text via a
    -- shorter cast for filtering.
    -- Compromise: index just on C_DATETIME, INCLUDE the natural key, status
    -- check then runs as a residual predicate over the small candidate set.
    CREATE INDEX IX_purchase_req_mst_status_date
        ON [ras_procurement].[purchase_req_mst]([C_DATETIME])
        INCLUDE ([PURCHASE_REQ_NO], [PURCHASE_REQ_ID], [PURCHASE_REQ_TYPE_ID]);
    PRINT '  Created IX_purchase_req_mst_status_date';
END
ELSE PRINT '  Skipped IX_purchase_req_mst_status_date (exists)';
GO

-- ----------------------------------------------------------------------------
-- Refresh statistics so the optimizer sees the new indexes immediately
-- ----------------------------------------------------------------------------
PRINT '';
PRINT 'Updating statistics on touched tables...';
GO

UPDATE STATISTICS [ras_procurement].[embedded_attachment_classification] WITH FULLSCAN;
UPDATE STATISTICS [ras_procurement].[attachment_classification]          WITH FULLSCAN;
UPDATE STATISTICS [ras_procurement].[quotation_extracted_items]          WITH FULLSCAN;
UPDATE STATISTICS [ras_procurement].[ras_pipeline_exceptions]            WITH FULLSCAN;
UPDATE STATISTICS [ras_procurement].[purchase_req_mst]                   WITH FULLSCAN;
GO

PRINT '';
PRINT '=== Gap-fill migration complete ===';
GO
