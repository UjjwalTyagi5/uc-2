-- =============================================================================
-- CLOUD REPLICA — same triggers as the on-prem version, but targeting the
-- [ras_procurement] schema in the cloud SQL Server. Use this to TEST the
-- trigger logic in the cloud environment before deploying to on-prem.
--
-- Differences from the on-prem version:
--   • schema dbo.*                  →  ras_procurement.*
--   • table names are lowercase     (purchase_req_mst, purchase_req_detail,
--                                    purchase_attachments)
--   • column names are still uppercase (PURCHASE_REQ_ID, U_DATETIME, …) —
--     ETL sync preserves on-prem casing
--
-- IMPORTANT — interaction with AgentCoreETLSync:
--   The ETL sync recreates target tables on every run (atomic swap pattern).
--   That DROP-and-rebuild also drops these triggers. So either:
--     (a) re-run this script after each ETL sync, or
--     (b) treat the cloud copy as test-only — keep the real triggers on-prem
--         and let U_DATETIME flow through the sync.
--
--   Option (b) is the long-term answer. Option (a) is fine for a quick
--   smoke test of the trigger logic against cloud-side data shapes.
--
-- Idempotent: drops each trigger before recreating, so safe to run twice.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. purchase_req_mst self-update trigger
--    Bumps U_DATETIME on any UPDATE that did not already set U_DATETIME.
--    Doubles as the recursion guard for the child triggers below.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'ras_procurement.tr_purchase_req_mst_bump_udatetime', 'TR') IS NOT NULL
    DROP TRIGGER ras_procurement.tr_purchase_req_mst_bump_udatetime;
GO

CREATE TRIGGER ras_procurement.tr_purchase_req_mst_bump_udatetime
ON ras_procurement.purchase_req_mst
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- If the caller (application, ETL sync, or a child trigger) already set
    -- U_DATETIME, do nothing. This is also what blocks recursion when child
    -- triggers update the master's U_DATETIME below.
    IF UPDATE(U_DATETIME) RETURN;

    UPDATE m
       SET m.U_DATETIME = GETDATE()
      FROM ras_procurement.purchase_req_mst AS m
      JOIN inserted                          AS i ON i.PURCHASE_REQ_ID = m.PURCHASE_REQ_ID;
END;
GO


-- -----------------------------------------------------------------------------
-- 2. purchase_req_detail — bump master's U_DATETIME on any line-item change
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'ras_procurement.tr_purchase_req_detail_bump_master', 'TR') IS NOT NULL
    DROP TRIGGER ras_procurement.tr_purchase_req_detail_bump_master;
GO

CREATE TRIGGER ras_procurement.tr_purchase_req_detail_bump_master
ON ras_procurement.purchase_req_detail
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- UNION captures both the new (inserted) and old (deleted) PURCHASE_REQ_ID,
    -- so an UPDATE that re-parents a line item from PR A to PR B bumps both.
    UPDATE m
       SET m.U_DATETIME = GETDATE()
      FROM ras_procurement.purchase_req_mst AS m
     WHERE m.PURCHASE_REQ_ID IN (
         SELECT PURCHASE_REQ_ID FROM inserted WHERE PURCHASE_REQ_ID IS NOT NULL
         UNION
         SELECT PURCHASE_REQ_ID FROM deleted  WHERE PURCHASE_REQ_ID IS NOT NULL
     );
END;
GO


-- -----------------------------------------------------------------------------
-- 3. purchase_attachments — bump master's U_DATETIME on any attachment change
--
--    purchase_attachments has no own U_DATETIME and links to the master via:
--      a) PURCHASE_ID directly (when populated and > 0)
--      b) PURCHASE_DTL_ID → purchase_req_detail.PURCHASE_REQ_ID (fallback)
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'ras_procurement.tr_purchase_attachments_bump_master', 'TR') IS NOT NULL
    DROP TRIGGER ras_procurement.tr_purchase_attachments_bump_master;
GO

CREATE TRIGGER ras_procurement.tr_purchase_attachments_bump_master
ON ras_procurement.purchase_attachments
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE m
       SET m.U_DATETIME = GETDATE()
      FROM ras_procurement.purchase_req_mst AS m
     WHERE m.PURCHASE_REQ_ID IN (
         -- Path A: PURCHASE_ID is the master PR ID directly
         SELECT PURCHASE_ID FROM inserted WHERE PURCHASE_ID IS NOT NULL AND PURCHASE_ID > 0
         UNION
         SELECT PURCHASE_ID FROM deleted  WHERE PURCHASE_ID IS NOT NULL AND PURCHASE_ID > 0

         UNION

         -- Path B: resolve PURCHASE_DTL_ID → purchase_req_detail.PURCHASE_REQ_ID
         SELECT d.PURCHASE_REQ_ID
           FROM ras_procurement.purchase_req_detail d
           JOIN inserted i ON i.PURCHASE_DTL_ID = d.PURCHASE_DTL_ID
          WHERE i.PURCHASE_DTL_ID > 0
            AND d.PURCHASE_REQ_ID IS NOT NULL
         UNION
         SELECT d.PURCHASE_REQ_ID
           FROM ras_procurement.purchase_req_detail d
           JOIN deleted dd ON dd.PURCHASE_DTL_ID = d.PURCHASE_DTL_ID
          WHERE dd.PURCHASE_DTL_ID > 0
            AND d.PURCHASE_REQ_ID IS NOT NULL
     );
END;
GO


-- =============================================================================
-- Smoke test (optional — run after deploying to verify)
-- =============================================================================
--
-- DECLARE @pr INT = (SELECT TOP 1 PURCHASE_REQ_ID FROM ras_procurement.purchase_req_mst);
-- DECLARE @before DATETIME = (SELECT U_DATETIME FROM ras_procurement.purchase_req_mst WHERE PURCHASE_REQ_ID = @pr);
-- WAITFOR DELAY '00:00:01';
-- UPDATE ras_procurement.purchase_req_detail
--    SET ITEMDESCRIPTION = ITEMDESCRIPTION
--  WHERE PURCHASE_REQ_ID = @pr;
-- SELECT @pr AS PURCHASE_REQ_ID,
--        @before AS before_u_datetime,
--        (SELECT U_DATETIME FROM ras_procurement.purchase_req_mst WHERE PURCHASE_REQ_ID = @pr) AS after_u_datetime;
-- -- Expect: after_u_datetime > before_u_datetime
