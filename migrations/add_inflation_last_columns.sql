-- Migration: Add inflation_pct_last and cpi_inflation_pct_last columns
-- Purpose: Track inflation calculations for last_hist_item (most recent) in addition to low_hist_item

ALTER TABLE [ras_procurement].[benchmark_result]
ADD [inflation_pct_last] DECIMAL(10,4) NULL,
    [cpi_inflation_pct_last] DECIMAL(10,4) NULL;

-- Add comments for documentation
EXEC sp_addextendedproperty
  @name = N'MS_Description',
  @value = N'Inflation % for last_hist_item (most recent) from PR master date to current PR date',
  @level0type = N'SCHEMA', @level0name = 'ras_procurement',
  @level1type = N'TABLE', @level1name = 'benchmark_result',
  @level2type = N'COLUMN', @level2name = 'inflation_pct_last';

EXEC sp_addextendedproperty
  @name = N'MS_Description',
  @value = N'CPI inflation % for last_hist_item (most recent) from PR master date to current PR date',
  @level0type = N'SCHEMA', @level0name = 'ras_procurement',
  @level1type = N'TABLE', @level1name = 'benchmark_result',
  @level2type = N'COLUMN', @level2name = 'cpi_inflation_pct_last';
