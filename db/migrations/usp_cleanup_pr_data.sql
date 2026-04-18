-- ============================================================
-- Migration: usp_cleanup_pr_data
-- Description:
--   Stored procedure that wipes all pipeline output rows for a
--   given PR before (re-)processing begins.  Called by the
--   PipelineOrchestrator before running stages for each PR so
--   every run — first-time or retry — starts from a clean slate.
--
-- Tables cleaned (in FK-safe order, single transaction):
--   1. quotation_extracted_items          (FK child of both classification tables)
--   2. embedded_attachment_classification (FK child of attachment_classification)
--   3. attachment_classification          (FK child of ras_tracker)
--   4. vw_get_ras_data_for_bidashboard    (BI dashboard table/view)
--
-- NOT touched:
--   ras_tracker             — pipeline uses this row to track state
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

        -- 1. Quotation extracted items
        --    FK to both attachment_classification and embedded_attachment_classification.
        --    Must be deleted first.
        DELETE qi
        FROM   [ras_procurement].[quotation_extracted_items]          qi
        JOIN   [ras_procurement].[attachment_classification]          ac
          ON   qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
        JOIN   [ras_procurement].[ras_tracker]                        rt
          ON   ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
        WHERE  rt.[purchase_req_no] = @purchase_req_no;

        -- 2. Embedded attachment classification
        --    FK child of attachment_classification — must go before parent.
        DELETE eac
        FROM   [ras_procurement].[embedded_attachment_classification] eac
        JOIN   [ras_procurement].[attachment_classification]          ac
          ON   eac.[attachment_classification_id] = ac.[attachment_classify_uuid_pk]
        JOIN   [ras_procurement].[ras_tracker]                        rt
          ON   ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
        WHERE  rt.[purchase_req_no] = @purchase_req_no;

        -- 3. Parent attachment classification rows
        DELETE ac
        FROM   [ras_procurement].[attachment_classification] ac
        JOIN   [ras_procurement].[ras_tracker]               rt
          ON   ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
        WHERE  rt.[purchase_req_no] = @purchase_req_no;

        -- 4. BI dashboard row
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
