-- =============================================================================
-- Adds the 4 columns needed by PipelineStage123NodeV2 ("Full Pipeline V2 —
-- LLM Category + Smart Benchmark"). The original PipelineStage123Node ignores
-- these columns, so both Nodes can run side-by-side against the same table.
--
-- Run once on the cloud SQL Server (ras_procurement schema).
-- Idempotent — IF NOT EXISTS guards each ADD / CREATE INDEX.
-- =============================================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
     WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items')
       AND [name] = N'purchase_category_llm'
)
    ALTER TABLE [ras_procurement].[quotation_extracted_items]
        ADD [purchase_category_llm] NVARCHAR(200) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
     WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items')
       AND [name] = N'embed_content'
)
    ALTER TABLE [ras_procurement].[quotation_extracted_items]
        ADD [embed_content] NVARCHAR(MAX) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
     WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items')
       AND [name] = N'critical_attributes'
)
    ALTER TABLE [ras_procurement].[quotation_extracted_items]
        ADD [critical_attributes] NVARCHAR(MAX) NULL;   -- JSON array
GO

-- Stage A (SQL pre-filter) queries:
--   WHERE is_selected_quote=1 AND purchase_category_llm=?  ORDER BY C_DATETIME DESC
-- The composite index supports that lookup.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
     WHERE [name] = N'IX_qei_purchase_category_llm'
       AND [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items')
)
    CREATE INDEX [IX_qei_purchase_category_llm]
        ON [ras_procurement].[quotation_extracted_items]
           ([purchase_category_llm], [is_selected_quote])
        INCLUDE ([purchase_dtl_id]);
GO
