-- ============================================================================
-- CLIENT QUERIES: SUPPLIER HIERARCHY (SELECTED QUOTES ONLY)
-- ============================================================================
-- Based on v3 pipeline implementation in pipeline_stage_123_v3.py
--
-- These 2 queries support client analysis of SELECTED quotes ONLY (is_selected_quote = 1):
-- 1. Supplier hierarchy (Primary, Secondary L1, Secondary L2)
-- 2. Technical & Commercial specifications for SELECTED quotes only
-- ============================================================================

-- USAGE: Replace @purchase_req_no with the actual PR number (e.g., 'R_152105/2021')

-- ============================================================================
-- QUERY 1: FETCH PRIMARY, SECONDARY SUPPLIERS FOR A RAS
-- ============================================================================
-- Returns three result sets:
--   1. PRIMARY: is_selected_quote = 1 (the ONE selected supplier per item)
--   2. SECONDARY L1: Lowest price among ALL suppliers per item
--   3. SECONDARY L2: 2nd lowest price among ALL suppliers per item
--
-- NOTE: L1 and L2 rank ALL suppliers by unit price (lowest and 2nd-lowest).
-- A supplier can be PRIMARY for one item and L1/L2 for another item.
-- A supplier can also be L1/L2 even if it's PRIMARY for that same item.

DECLARE @purchase_req_no NVARCHAR(50) = 'R_152105/2021';

-- ─── PRIMARY SUPPLIER: is_selected_quote = 1 ────────────────────────────
-- Exactly one per item (purchase_dtl_id), the one selected by v3 extraction
SELECT
    'PRIMARY' AS supplier_rank,
    prd.PURCHASE_DTL_ID AS item_id,
    prm.PURCHASE_REQ_NO AS pr_number,
    qi.supplier_name,
    qi.supplier_country,
    qi.supplier_match_conf,
    qi.currency,
    qi.unit_price AS unit_price_source_currency,
    qi.unit_price_eur,
    qi.total_price AS total_price_source_currency,
    qi.total_price_eur,
    qi.quotation_date,
    qi.payment_terms,
    qi.is_selected_quote
FROM ras_procurement.quotation_extracted_items qi
INNER JOIN ras_procurement.purchase_req_detail prd
    ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
INNER JOIN ras_procurement.purchase_req_mst prm
    ON prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
WHERE prm.PURCHASE_REQ_NO = @purchase_req_no
  AND qi.is_selected_quote = 1
ORDER BY prd.PURCHASE_DTL_ID;

-- ─── SECONDARY L1: Lowest price among ALL suppliers ───────────────────────
-- For each item, get the cheapest supplier (can include primary if lowest priced)
-- Uses ROW_NUMBER to rank by unit_price_eur ascending
SELECT
    'SECONDARY_L1' AS supplier_rank,
    subq.PURCHASE_DTL_ID AS item_id,
    subq.PURCHASE_REQ_NO AS pr_number,
    subq.supplier_name,
    subq.supplier_country,
    subq.supplier_match_conf,
    subq.currency,
    subq.unit_price_source_currency,
    subq.unit_price_eur,
    subq.total_price_source_currency,
    subq.total_price_eur,
    subq.quotation_date,
    subq.payment_terms,
    subq.is_selected_quote
FROM (
    SELECT
        qi.extracted_item_uuid_pk,
        qi.supplier_name,
        qi.supplier_country,
        qi.supplier_match_conf,
        qi.currency,
        qi.unit_price AS unit_price_source_currency,
        qi.unit_price_eur,
        qi.total_price AS total_price_source_currency,
        qi.total_price_eur,
        qi.quotation_date,
        qi.payment_terms,
        qi.is_selected_quote,
        qi.purchase_dtl_id AS PURCHASE_DTL_ID,
        prm.PURCHASE_REQ_NO,
        ROW_NUMBER() OVER (
            PARTITION BY prd.PURCHASE_DTL_ID
            ORDER BY COALESCE(qi.unit_price_eur, qi.unit_price) ASC
        ) AS price_rank
    FROM ras_procurement.quotation_extracted_items qi
    INNER JOIN ras_procurement.purchase_req_detail prd
        ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
    INNER JOIN ras_procurement.purchase_req_mst prm
        ON prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
    WHERE prm.PURCHASE_REQ_NO = @purchase_req_no
      AND COALESCE(qi.unit_price_eur, qi.unit_price) IS NOT NULL
) subq
WHERE subq.price_rank = 1
ORDER BY subq.PURCHASE_DTL_ID;

-- ─── SECONDARY L2: 2nd lowest price among ALL suppliers ─────────────────
-- For each item, get the 2nd cheapest supplier (can include primary if 2nd lowest priced)
SELECT
    'SECONDARY_L2' AS supplier_rank,
    subq.PURCHASE_DTL_ID AS item_id,
    subq.PURCHASE_REQ_NO AS pr_number,
    subq.supplier_name,
    subq.supplier_country,
    subq.supplier_match_conf,
    subq.currency,
    subq.unit_price_source_currency,
    subq.unit_price_eur,
    subq.total_price_source_currency,
    subq.total_price_eur,
    subq.quotation_date,
    subq.payment_terms,
    subq.is_selected_quote
FROM (
    SELECT
        qi.extracted_item_uuid_pk,
        qi.supplier_name,
        qi.supplier_country,
        qi.supplier_match_conf,
        qi.currency,
        qi.unit_price AS unit_price_source_currency,
        qi.unit_price_eur,
        qi.total_price AS total_price_source_currency,
        qi.total_price_eur,
        qi.quotation_date,
        qi.payment_terms,
        qi.is_selected_quote,
        qi.purchase_dtl_id AS PURCHASE_DTL_ID,
        prm.PURCHASE_REQ_NO,
        ROW_NUMBER() OVER (
            PARTITION BY prd.PURCHASE_DTL_ID
            ORDER BY COALESCE(qi.unit_price_eur, qi.unit_price) ASC
        ) AS price_rank
    FROM ras_procurement.quotation_extracted_items qi
    INNER JOIN ras_procurement.purchase_req_detail prd
        ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
    INNER JOIN ras_procurement.purchase_req_mst prm
        ON prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
    WHERE prm.PURCHASE_REQ_NO = @purchase_req_no
      AND COALESCE(qi.unit_price_eur, qi.unit_price) IS NOT NULL
) subq
WHERE subq.price_rank = 2
ORDER BY subq.PURCHASE_DTL_ID;

-- ============================================================================
-- QUERY 2: TECHNICAL & COMMERCIAL SPECS FOR RAS > ITEM (SELECTED ONLY)
-- ============================================================================
-- Returns full specification matrix for SELECTED quotes ONLY (is_selected_quote = 1):
--   TECHNICAL: item_name, taxonomy (item_level_1..3), critical attributes, commodity
--   COMMERCIAL: quotation terms, payment, delivery, pricing
-- One row per item (PURCHASE_DTL_ID) — the selected supplier only

DECLARE @purchase_req_no NVARCHAR(50) = 'R_152105/2021';

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prm.PURCHASE_REQ_NO AS pr_number,
    prd.ITEMDESCRIPTION AS ras_item_description,
    qi.supplier_name,
    qi.supplier_country,
    ROUND(qi.supplier_match_conf, 3) AS supplier_match_confidence,

    -- ─── TECHNICAL SPECIFICATIONS (RAW DATA) ────────────────────────────
    qi.item_name,
    qi.item_description,
    qi.item_summary,

    -- 8-level taxonomy (raw, not concatenated)
    qi.item_level_1,
    qi.item_level_2,
    qi.item_level_3,
    qi.item_level_4,
    qi.item_level_5,
    qi.item_level_6,
    qi.item_level_7,
    qi.item_level_8,

    -- Critical and important attributes
    qi.critical_attributes,  -- JSON: [{"name": "...", "value": ..., "importance": "critical/important"}]

    -- Categorization
    qi.commodity_tag,
    qi.purchase_category_llm,

    -- Quantity and unit
    qi.unit,
    qi.quantity,

    -- ─── COMMERCIAL SPECIFICATIONS (RAW DATA) ──────────────────────────
    -- Quotation metadata
    qi.quotation_ref_no,
    qi.quotation_date,
    qi.validity_date,
    qi.validity_days,

    -- Payment and delivery terms
    qi.payment_terms,
    qi.delivery_date,
    qi.delivery_time_days,

    -- Incoterms
    qi.quote_incoterms,
    qi.quote_incoterms_named_place,

    -- Quote totals and charges (quote-level)
    qi.quote_subtotal,
    qi.quote_freight,
    qi.quote_insurance,
    qi.quote_customs_duties,
    qi.quote_packing_forwarding,
    qi.quote_installation,
    qi.quote_other_charges,

    -- Tax information
    qi.quote_tax_type,
    qi.quote_tax_rate_pct,
    qi.quote_tax_amount_total,
    qi.quote_grand_total,

    -- Line-item level commercial details (if provided by supplier)
    qi.line_incoterms,
    qi.line_freight,
    qi.line_insurance,
    qi.line_tax_rate_pct,
    qi.line_tax_amount,
    qi.line_other_charges,
    qi.line_total_inclusive,
    qi.line_allocation_method,
    qi.line_hsn_sac_code,
    qi.line_country_of_origin,

    -- Pricing (source currency + EUR)
    qi.currency,
    qi.unit_price AS unit_price_source_currency,
    qi.unit_price_eur,
    qi.total_price AS total_price_source_currency,
    qi.total_price_eur

FROM ras_procurement.quotation_extracted_items qi
INNER JOIN ras_procurement.purchase_req_detail prd
    ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
INNER JOIN ras_procurement.purchase_req_mst prm
    ON prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
WHERE prm.PURCHASE_REQ_NO = @purchase_req_no
  AND qi.is_selected_quote = 1
ORDER BY prd.PURCHASE_DTL_ID;

-- ============================================================================
-- END OF CLIENT QUERIES
-- ============================================================================
