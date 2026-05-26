-- ============================================================================
-- Migration: indexes on the ETL-synced source tables
--   (purchase_req_mst, purchase_req_detail, purchase_attachments,
--    purchase_req_type)
--
-- WHY:
--   The schema dump (script 1.sql) shows these four tables as HEAP tables —
--   no PRIMARY KEY, no indexes on the columns that almost every pipeline
--   query joins on. As a result:
--
--     • vw_benchmark_summary's 10 JOINs through prm/prd become heap scans
--     • _fetch_pending_prs joins prm to ras_tracker by PURCHASE_REQ_NO → scan
--     • _fetch_attachments joins prm to purchase_attachments → scan
--     • _build_ras_context looks up prm by PURCHASE_REQ_NO → scan
--     • _sort_excel_pr_list_by_date sorts prm by PURCHASE_REQ_NO + C_DATETIME → scan + sort
--
--   These scans are tolerable while the tables are small but blow up as the
--   data grows (every additional row makes every subsequent query slower).
--
-- HOW:
--   We add NONCLUSTERED indexes — NOT primary key constraints — so the ETL
--   that loads these tables (etl_sync_control / etl_sync_status) keeps
--   working without uniqueness assumptions we can't enforce on synced data.
--
--   IF NOT EXISTS guards on every index so the migration is idempotent.
--
-- Target DB: ras-procurement-benchmark_2026
-- ============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

PRINT '=== Adding source-table indexes on ras_procurement ===';
PRINT '';
GO

-- ----------------------------------------------------------------------------
-- purchase_req_mst — the master table. Three hot join columns.
-- ----------------------------------------------------------------------------

-- 1. PURCHASE_REQ_NO — the natural key. Used by every "find by PR number"
--    lookup in the pipeline + 8 LEFT JOINs in vw_benchmark_summary
--    (via prm, prm_low, prm_lst). HUGE win.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_req_mst_PURCHASE_REQ_NO'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_req_mst]')
)
BEGIN
    CREATE INDEX IX_purchase_req_mst_PURCHASE_REQ_NO
        ON [ras_procurement].[purchase_req_mst]([PURCHASE_REQ_NO])
        INCLUDE ([PURCHASE_REQ_ID], [PURCHASE_REQ_TYPE_ID], [C_DATETIME],
                 [PURCHASEFINALAPPROVALSTATUS]);
    PRINT '  Created IX_purchase_req_mst_PURCHASE_REQ_NO';
END
ELSE PRINT '  Skipped IX_purchase_req_mst_PURCHASE_REQ_NO (exists)';
GO

-- 2. PURCHASE_REQ_ID — the internal numeric key. Used by every join from
--    purchase_req_detail → purchase_req_mst (in the view + RAS context).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_req_mst_PURCHASE_REQ_ID'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_req_mst]')
)
BEGIN
    CREATE INDEX IX_purchase_req_mst_PURCHASE_REQ_ID
        ON [ras_procurement].[purchase_req_mst]([PURCHASE_REQ_ID])
        INCLUDE ([PURCHASE_REQ_NO], [PURCHASE_REQ_TYPE_ID]);
    PRINT '  Created IX_purchase_req_mst_PURCHASE_REQ_ID';
END
ELSE PRINT '  Skipped IX_purchase_req_mst_PURCHASE_REQ_ID (exists)';
GO

-- 3. PURCHASE_REQ_TYPE_ID — small cardinality, used by the prt join in
--    vw_benchmark_summary. Without this, the type lookup scans prm.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_req_mst_PURCHASE_REQ_TYPE_ID'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_req_mst]')
)
BEGIN
    CREATE INDEX IX_purchase_req_mst_PURCHASE_REQ_TYPE_ID
        ON [ras_procurement].[purchase_req_mst]([PURCHASE_REQ_TYPE_ID]);
    PRINT '  Created IX_purchase_req_mst_PURCHASE_REQ_TYPE_ID';
END
ELSE PRINT '  Skipped IX_purchase_req_mst_PURCHASE_REQ_TYPE_ID (exists)';
GO

-- ----------------------------------------------------------------------------
-- purchase_req_detail — the line-items table. Two hot join columns.
-- ----------------------------------------------------------------------------

-- 4. PURCHASE_DTL_ID — the natural per-line key. Joined by every benchmark
--    + extraction + quotation lookup.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_req_detail_PURCHASE_DTL_ID'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_req_detail]')
)
BEGIN
    CREATE INDEX IX_purchase_req_detail_PURCHASE_DTL_ID
        ON [ras_procurement].[purchase_req_detail]([PURCHASE_DTL_ID])
        INCLUDE ([PURCHASE_REQ_ID], [ITEM_NO], [QUANTITY], [ITEMDESCRIPTION],
                 [Insourcing_flag], [C_DATETIME]);
    PRINT '  Created IX_purchase_req_detail_PURCHASE_DTL_ID';
END
ELSE PRINT '  Skipped IX_purchase_req_detail_PURCHASE_DTL_ID (exists)';
GO

-- 5. PURCHASE_REQ_ID — used to find all detail rows for a given master.
--    Critical for _build_ras_context (one query per PR, fetches all dtl).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_req_detail_PURCHASE_REQ_ID'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_req_detail]')
)
BEGIN
    CREATE INDEX IX_purchase_req_detail_PURCHASE_REQ_ID
        ON [ras_procurement].[purchase_req_detail]([PURCHASE_REQ_ID])
        INCLUDE ([PURCHASE_DTL_ID], [ITEM_NO]);
    PRINT '  Created IX_purchase_req_detail_PURCHASE_REQ_ID';
END
ELSE PRINT '  Skipped IX_purchase_req_detail_PURCHASE_REQ_ID (exists)';
GO

-- ----------------------------------------------------------------------------
-- purchase_attachments — used by _fetch_attachments (Stage 1 ingestion).
-- ----------------------------------------------------------------------------

-- 6. PURCHASE_ID — join key from purchase_req_mst.PURCHASE_REQ_ID. Without
--    this, every Stage 1 fetch scans the whole attachments table.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_attachments_PURCHASE_ID'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_attachments]')
)
BEGIN
    CREATE INDEX IX_purchase_attachments_PURCHASE_ID
        ON [ras_procurement].[purchase_attachments]([PURCHASE_ID])
        INCLUDE ([ATTACHMENT_ID], [FILES_NAME]);
    PRINT '  Created IX_purchase_attachments_PURCHASE_ID';
END
ELSE PRINT '  Skipped IX_purchase_attachments_PURCHASE_ID (exists)';
GO

-- 7. ATTACHMENT_ID — the IN (...) filter in _fetch_attachments' second query.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE name = 'IX_purchase_attachments_ATTACHMENT_ID'
       AND object_id = OBJECT_ID('[ras_procurement].[purchase_attachments]')
)
BEGIN
    CREATE INDEX IX_purchase_attachments_ATTACHMENT_ID
        ON [ras_procurement].[purchase_attachments]([ATTACHMENT_ID])
        INCLUDE ([FILES_NAME], [PURCHASE_ID]);
    PRINT '  Created IX_purchase_attachments_ATTACHMENT_ID';
END
ELSE PRINT '  Skipped IX_purchase_attachments_ATTACHMENT_ID (exists)';
GO

-- ----------------------------------------------------------------------------
-- purchase_req_type — used by the prt join in vw_benchmark_summary.
-- ----------------------------------------------------------------------------
-- 8. purchase_req_type_id already has PK per schema dump. No additional
--    index needed unless join cardinality grows surprising.

-- ----------------------------------------------------------------------------
-- Refresh statistics so the optimizer immediately uses the new indexes
-- ----------------------------------------------------------------------------
PRINT '';
PRINT 'Updating statistics on affected tables...';
GO

UPDATE STATISTICS [ras_procurement].[purchase_req_mst]      WITH FULLSCAN;
UPDATE STATISTICS [ras_procurement].[purchase_req_detail]   WITH FULLSCAN;
UPDATE STATISTICS [ras_procurement].[purchase_attachments]  WITH FULLSCAN;
GO

PRINT '';
PRINT '=== Source-table migration complete ===';
GO
