-- ============================================================
-- Migration: create cpi_rates cache table
--
-- Stores World Bank FP.CPI.TOTL.ZG annual inflation rates so
-- the benchmark module can read them without hitting the API at
-- run time (useful when the server has no outbound internet).
--
-- Run this against [ras-procurement-benchmark] once, then
-- populate with: python fetch_cpi_rates.py
-- ============================================================

CREATE TABLE [ras_procurement].[cpi_rates] (
    [country_code] NVARCHAR(2)    NOT NULL,   -- ISO alpha-2
    [year]         SMALLINT       NOT NULL,
    [rate_pct]     DECIMAL(10,4)  NOT NULL,
    [fetched_at]   DATETIME2      NOT NULL
        CONSTRAINT [DF_cpi_rates_fetched_at] DEFAULT SYSUTCDATETIME(),

    CONSTRAINT [PK_cpi_rates] PRIMARY KEY ([country_code], [year])
);
