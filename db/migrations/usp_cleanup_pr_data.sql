-- ============================================================
-- Migration: usp_cleanup_pr_data
-- Description:
--   Stored procedure that wipes all pipeline output rows for a
--   given PR before (re-)processing begins.  Called by the
--   PipelineOrchestrator before running stages for each PR so
--   every run — first-time or retry — starts from a clean slate.
--
-- Tables cleaned (in FK-safe order, single transaction):
--   1. EmbeddedAttachmentClassification  (FK child of AttachmentClassification)
--   2. AttachmentClassification
--   3. vw_get_ras_data_for_bidashboard   (BI dashboard table/view)
--
-- NOT touched:
--   ras_tracker           — pipeline uses this row to track state
--   ras_pipeline_exceptions — kept as audit history across all runs
--
-- Safe for new PRs — all DELETEs are no-ops when no rows exist.
-- ============================================================

IF OBJECT_ID('[ras_procurement].[usp_cleanup_pr_data]', 'P') IS NOT NULL
    DROP PROCEDURE [ras_procurement].[usp_cleanup_pr_data];
GO

CREATE PROCEDURE [ras_procurement].[usp_cleanup_pr_data]
    @purchase_req_no NVARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRANSACTION;
    BEGIN TRY

        -- 1. Embedded files — FK child of AttachmentClassification; must go first
        DELETE eac
        FROM   [ras_procurement].[EmbeddedAttachmentClassification] eac
        JOIN   [ras_procurement].[AttachmentClassification]         ac
          ON   eac.[attachment_classify_uuid_pk] = ac.[attachment_classify_uuid_pk]
        WHERE  ac.[purchase_req_no_fk] = @purchase_req_no;

        -- 2. Parent attachment classification rows
        DELETE FROM [ras_procurement].[AttachmentClassification]
        WHERE  [purchase_req_no_fk] = @purchase_req_no;

        -- 3. BI dashboard row
        DELETE FROM [ras_procurement].[vw_get_ras_data_for_bidashboard]
        WHERE  [PURCHASE_REQ_NO] = @purchase_req_no;

        COMMIT TRANSACTION;

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
