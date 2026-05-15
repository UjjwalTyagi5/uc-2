-- =============================================================================
-- Adds the commercial / financial columns populated by the second LLM call
-- in PipelineStage123NodeV2 ("Full Pipeline V2 — Commercials Extraction").
-- The original extraction prompt is unchanged; these columns are filled by a
-- separate prompt that runs AFTER the line-item extraction succeeds and is
-- gated by the canvas knob `enable_commercials_extraction` (default ON).
--
-- Quote-level columns are repeated on every line item row from the same
-- quotation (same shape as supplier_name / currency / payment_terms today).
-- Line-level columns are per-item; when a charge was named only at the
-- bottom of the quote, the LLM proportionally allocates it across lines and
-- records the audit trail in [line_charges_source] + [line_allocation_method].
--
-- Run once on the cloud SQL Server (ras_procurement schema).
-- Idempotent — IF NOT EXISTS guards each ADD.
-- =============================================================================

-- ── Quote-level columns (20) ────────────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_incoterms')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_incoterms] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_incoterms_named_place')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_incoterms_named_place] NVARCHAR(255) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_subtotal')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_subtotal] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_discount_total')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_discount_total] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_packing_forwarding')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_packing_forwarding] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_freight')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_freight] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_insurance')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_insurance] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_customs_duties')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_customs_duties] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_installation')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_installation] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_other_charges')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_other_charges] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_tax_type')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_tax_type] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_tax_rate_pct')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_tax_rate_pct] DECIMAL(9,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_tax_amount_total')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_tax_amount_total] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_grand_total')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_grand_total] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_grand_total_currency')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_grand_total_currency] NVARCHAR(10) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_country_of_origin')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_country_of_origin] NVARCHAR(100) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_port_of_loading')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_port_of_loading] NVARCHAR(255) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_port_of_discharge')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_port_of_discharge] NVARCHAR(255) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_mode_of_transport')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_mode_of_transport] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_delivery_location')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_delivery_location] NVARCHAR(500) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'quote_charges_breakdown_json')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [quote_charges_breakdown_json] NVARCHAR(MAX) NULL;
GO

-- ── Line-level columns (14) ─────────────────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_incoterms')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_incoterms] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_tax_rate_pct')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_tax_rate_pct] DECIMAL(9,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_tax_amount')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_tax_amount] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_freight')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_freight] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_insurance')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_insurance] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_packing_forwarding')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_packing_forwarding] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_customs_duties')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_customs_duties] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_installation')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_installation] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_other_charges')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_other_charges] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_total_inclusive')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_total_inclusive] DECIMAL(20,4) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_country_of_origin')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_country_of_origin] NVARCHAR(100) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_hsn_sac_code')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_hsn_sac_code] NVARCHAR(50) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_charges_source')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_charges_source] NVARCHAR(30) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE [object_id] = OBJECT_ID(N'ras_procurement.quotation_extracted_items') AND [name] = N'line_allocation_method')
    ALTER TABLE [ras_procurement].[quotation_extracted_items] ADD [line_allocation_method] NVARCHAR(40) NULL;
GO
