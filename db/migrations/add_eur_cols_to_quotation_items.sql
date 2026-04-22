-- ============================================================
-- Migration: add EUR-normalised price columns to
--            quotation_extracted_items
--
-- Run this against [ras_procurement] BEFORE deploying the
-- updated extraction writer that populates these columns.
-- Existing rows keep NULL (benchmark module falls back to
-- raw unit_price when these are NULL).
-- ============================================================

ALTER TABLE [ras_procurement].[quotation_extracted_items]
  ADD [unit_price_eur]  DECIMAL(18,4) NULL,
      [total_price_eur] DECIMAL(18,2) NULL;
