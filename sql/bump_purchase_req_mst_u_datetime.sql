-- =============================================================================
-- Triggers: keep PURCHASE_REQ_MST.U_DATETIME in sync with ANY change to
--           the master row, its line items, or its attachments.
--
-- Run this on the ON-PREM SQL Server database that owns the dbo.* tables.
--
-- Why:
--   The procurement benchmarking pipeline needs to detect "modified RAS"
--   so it can re-process PRs whose line items, master fields, or supporting
--   attachments have changed after the initial run. Watching three tables
--   independently is fragile — these triggers fold all changes into a
--   single column (PURCHASE_REQ_MST.U_DATETIME) so the pipeline can poll
--   one place.
--
-- Design:
--   1. A change to a child table (PURCHASE_REQ_DETAIL or PURCHASE_ATTACHMENTS)
--      bumps the master's U_DATETIME = GETDATE().
--   2. A change to PURCHASE_REQ_MST itself bumps U_DATETIME only when the
--      caller did not explicitly set it. This catches partial UPDATEs the
--      application may issue without touching U_DATETIME.
--   3. Recursion is prevented because the master trigger checks
--      `IF UPDATE(U_DATETIME) RETURN;` — the child triggers always set
--      U_DATETIME, so the master trigger is a no-op when fired by them.
--
-- Idempotent: the script DROPs each trigger before recreating it, so running
-- it twice is safe.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. PURCHASE_REQ_MST self-update trigger
--    Bumps U_DATETIME on any UPDATE that did not already set U_DATETIME.
--    Doubles as the recursion guard for the child triggers below.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.tr_PURCHASE_REQ_MST_bump_udatetime', 'TR') IS NOT NULL
    DROP TRIGGER dbo.tr_PURCHASE_REQ_MST_bump_udatetime;
GO

CREATE TRIGGER dbo.tr_PURCHASE_REQ_MST_bump_udatetime
ON dbo.PURCHASE_REQ_MST
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    -- If the caller (application or a child trigger) already set U_DATETIME,
    -- do nothing. This is also what blocks recursion when child triggers
    -- update the master's U_DATETIME below.
    IF UPDATE(U_DATETIME) RETURN;

    UPDATE m
       SET m.U_DATETIME = GETDATE()
      FROM dbo.PURCHASE_REQ_MST AS m
      JOIN inserted               AS i  ON i.PURCHASE_REQ_ID = m.PURCHASE_REQ_ID;
END;
GO


-- -----------------------------------------------------------------------------
-- 2. PURCHASE_REQ_DETAIL — bump master's U_DATETIME on any line-item change
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.tr_PURCHASE_REQ_DETAIL_bump_master', 'TR') IS NOT NULL
    DROP TRIGGER dbo.tr_PURCHASE_REQ_DETAIL_bump_master;
GO

CREATE TRIGGER dbo.tr_PURCHASE_REQ_DETAIL_bump_master
ON dbo.PURCHASE_REQ_DETAIL
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    -- UNION captures both the new (inserted) and old (deleted) PURCHASE_REQ_ID,
    -- so an UPDATE that re-parents a line item from PR A to PR B bumps both.
    UPDATE m
       SET m.U_DATETIME = GETDATE()
      FROM dbo.PURCHASE_REQ_MST AS m
     WHERE m.PURCHASE_REQ_ID IN (
         SELECT PURCHASE_REQ_ID FROM inserted WHERE PURCHASE_REQ_ID IS NOT NULL
         UNION
         SELECT PURCHASE_REQ_ID FROM deleted  WHERE PURCHASE_REQ_ID IS NOT NULL
     );
END;
GO


-- -----------------------------------------------------------------------------
-- 3. PURCHASE_ATTACHMENTS — bump master's U_DATETIME on any attachment change
--
--    PURCHASE_ATTACHMENTS does not have its own U_DATETIME column and links
--    to the master via two routes:
--      a) PURCHASE_ID directly (when populated and > 0)
--      b) PURCHASE_DTL_ID → PURCHASE_REQ_DETAIL.PURCHASE_REQ_ID (fallback)
--
--    The trigger resolves both paths and unions them, so an attachment
--    pointing at the master directly OR at a line item still bumps the
--    same master row.
-- -----------------------------------------------------------------------------
IF OBJECT_ID(N'dbo.tr_PURCHASE_ATTACHMENTS_bump_master', 'TR') IS NOT NULL
    DROP TRIGGER dbo.tr_PURCHASE_ATTACHMENTS_bump_master;
GO

CREATE TRIGGER dbo.tr_PURCHASE_ATTACHMENTS_bump_master
ON dbo.PURCHASE_ATTACHMENTS
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE m
       SET m.U_DATETIME = GETDATE()
      FROM dbo.PURCHASE_REQ_MST AS m
     WHERE m.PURCHASE_REQ_ID IN (
         -- Path A: PURCHASE_ID is the master PR ID directly
         SELECT PURCHASE_ID FROM inserted WHERE PURCHASE_ID IS NOT NULL AND PURCHASE_ID > 0
         UNION
         SELECT PURCHASE_ID FROM deleted  WHERE PURCHASE_ID IS NOT NULL AND PURCHASE_ID > 0

         UNION

         -- Path B: resolve PURCHASE_DTL_ID → PURCHASE_REQ_DETAIL.PURCHASE_REQ_ID
         SELECT d.PURCHASE_REQ_ID
           FROM dbo.PURCHASE_REQ_DETAIL d
           JOIN inserted i ON i.PURCHASE_DTL_ID = d.PURCHASE_DTL_ID
          WHERE i.PURCHASE_DTL_ID > 0
            AND d.PURCHASE_REQ_ID IS NOT NULL
         UNION
         SELECT d.PURCHASE_REQ_ID
           FROM dbo.PURCHASE_REQ_DETAIL d
           JOIN deleted dd ON dd.PURCHASE_DTL_ID = d.PURCHASE_DTL_ID
          WHERE dd.PURCHASE_DTL_ID > 0
            AND d.PURCHASE_REQ_ID IS NOT NULL
     );
END;
GO


-- =============================================================================
-- Smoke test (optional — run manually to verify)
-- =============================================================================
--
-- DECLARE @pr INT = (SELECT TOP 1 PURCHASE_REQ_ID FROM dbo.PURCHASE_REQ_MST);
-- DECLARE @before DATETIME = (SELECT U_DATETIME FROM dbo.PURCHASE_REQ_MST WHERE PURCHASE_REQ_ID = @pr);
-- WAITFOR DELAY '00:00:01';
-- UPDATE dbo.PURCHASE_REQ_DETAIL SET ITEMDESCRIPTION = ITEMDESCRIPTION
--  WHERE PURCHASE_REQ_ID = @pr;
-- SELECT @pr AS PURCHASE_REQ_ID, @before AS before_u_datetime,
--        (SELECT U_DATETIME FROM dbo.PURCHASE_REQ_MST WHERE PURCHASE_REQ_ID = @pr) AS after_u_datetime;
-- -- Expect: after_u_datetime > before_u_datetime
