-- ============================================================
-- Migration: add retry_count to ras_tracker
-- Description:
--   Tracks how many times a PR has been re-queued for re-processing.
--   Starts at 0 for every PR (first-time processing).
--   Incremented by 1 each time SourceChangeDetector re-queues a PR
--   (i.e., when source data changes and the PR is reset to 'INGESTION').
--
-- Useful for:
--   - Monitoring dashboards (which PRs keep getting re-processed?)
--   - Future circuit-breaker logic (stop retrying after N attempts)
-- ============================================================

ALTER TABLE [ras_procurement].[ras_tracker]
  ADD [retry_count] INT NOT NULL DEFAULT 0;
