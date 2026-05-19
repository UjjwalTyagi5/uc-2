-- ============================================================================
-- CLIENT QUERIES: SUPPLIER HIERARCHY & BENCHMARKING (v3)
-- ============================================================================
-- Based on v3 pipeline implementation in pipeline_stage_123_v3.py
--
-- v3 Benchmarking uses:
--   Stage A: SQL category pool
--   Stage B: Pinecone vector search (global)
--   Stage C: LLM ranking (6 rejection rules)
--   Result: similar_dtl_ids populated from Stage C "selected" items
--
-- These 4 queries support client analysis of:
-- 1. Supplier hierarchy (Primary, Secondary, Tertiary)
-- 2. Technical & Commercial specifications per supplier
-- 3. Historical benchmarking (BP & LP with inflation)
-- 4. Currency conversion & inflation rates
-- ============================================================================

-- USAGE: Replace @purchase_req_no with the actual PR number (e.g., 'R_152105/2021')

-- ============================================================================
-- QUERY 1: FETCH PRIMARY, SECONDARY SUPPLIERS FOR A RAS
-- ============================================================================
-- Returns three result sets:
--   1. PRIMARY: is_selected_quote = 1 (the ONE selected supplier per item)
--   2. SECONDARY L1: Lowest price among non-selected (is_selected_quote = 0)
--   3. SECONDARY L2: 2nd lowest price among non-selected
--
-- NOTE: L1 and L2 include ALL suppliers (not filtered by is_selected_quote)

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
    qi.total_price_eur,
    qi.quotation_date,
    qi.payment_terms,
    qi.currency
FROM ras_procurement.quotation_extracted_items qi
INNER JOIN ras_procurement.purchase_req_detail prd
    ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
INNER JOIN ras_procurement.purchase_req_mst prm
    ON prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
WHERE prm.PURCHASE_REQ_NO = @purchase_req_no
  AND qi.is_selected_quote = 1
ORDER BY prd.PURCHASE_DTL_ID;

-- ─── SECONDARY L1: Lowest price among ALL suppliers (non-selected) ──────
-- For each item, get the cheapest alternative (not the selected one)
-- Uses ROW_NUMBER to rank by unit_price_eur ascending
SELECT
    'SECONDARY_L1' AS supplier_rank,
    subq.PURCHASE_DTL_ID AS item_id,
    subq.PURCHASE_REQ_NO AS pr_number,
    subq.supplier_name,
    subq.supplier_country,
    subq.supplier_match_conf,
    subq.total_price_eur,
    subq.unit_price_eur,
    subq.quotation_date,
    subq.payment_terms
FROM (
    SELECT
        qi.extracted_item_uuid_pk,
        qi.supplier_name,
        qi.supplier_country,
        qi.supplier_match_conf,
        qi.total_price_eur,
        qi.unit_price_eur,
        qi.quotation_date,
        qi.payment_terms,
        qi.currency,
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
      AND qi.is_selected_quote = 0
      AND COALESCE(qi.unit_price_eur, qi.unit_price) IS NOT NULL
) subq
WHERE subq.price_rank = 1
ORDER BY subq.PURCHASE_DTL_ID;

-- ─── SECONDARY L2: 2nd lowest price among ALL suppliers (non-selected) ──
-- For each item, get the 2nd cheapest alternative
SELECT
    'SECONDARY_L2' AS supplier_rank,
    subq.PURCHASE_DTL_ID AS item_id,
    subq.PURCHASE_REQ_NO AS pr_number,
    subq.supplier_name,
    subq.supplier_country,
    subq.supplier_match_conf,
    subq.total_price_eur,
    subq.unit_price_eur,
    subq.quotation_date,
    subq.payment_terms
FROM (
    SELECT
        qi.extracted_item_uuid_pk,
        qi.supplier_name,
        qi.supplier_country,
        qi.supplier_match_conf,
        qi.total_price_eur,
        qi.unit_price_eur,
        qi.quotation_date,
        qi.payment_terms,
        qi.currency,
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
      AND qi.is_selected_quote = 0
      AND COALESCE(qi.unit_price_eur, qi.unit_price) IS NOT NULL
) subq
WHERE subq.price_rank = 2
ORDER BY subq.PURCHASE_DTL_ID;

-- ============================================================================
-- QUERY 2: TECHNICAL & COMMERCIAL SPECS FOR RAS > ITEM > PER SUPPLIER
-- ============================================================================
-- Returns full specification matrix:
--   TECHNICAL: item_name, taxonomy (item_level_1..3), critical attributes, commodity
--   COMMERCIAL: quotation terms, payment, delivery, pricing
-- Rows are grouped by item (PURCHASE_DTL_ID) and supplier

DECLARE @purchase_req_no NVARCHAR(50) = 'R_152105/2021';

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prm.PURCHASE_REQ_NO AS pr_number,
    prd.ITEMDESCRIPTION AS ras_item_description,
    qi.supplier_name,
    qi.supplier_country,
    ROUND(qi.supplier_match_conf, 3) AS supplier_match_confidence,

    -- ─── TECHNICAL SPECIFICATIONS ───────────────────────────────────────
    qi.item_name,
    qi.item_description,
    CONCAT(
        ISNULL(qi.item_level_1, ''), ' > ',
        ISNULL(qi.item_level_2, ''), ' > ',
        ISNULL(qi.item_level_3, '')
    ) AS category_chain,
    qi.critical_attributes,  -- JSON: [{"name": "...", "value": ..., "importance": "critical"}]
    qi.commodity_tag,
    qi.purchase_category_llm,
    qi.unit,
    qi.quantity,

    -- ─── COMMERCIAL SPECIFICATIONS ───────────────────────────────────────
    qi.quotation_ref_no,
    qi.quotation_date,
    qi.validity_date,
    qi.validity_days,
    qi.payment_terms,
    qi.delivery_date,
    qi.delivery_time_days,

    -- Quote-level terms
    qi.quote_incoterms,
    qi.quote_incoterms_named_place,
    qi.quote_subtotal,
    qi.quote_freight,
    qi.quote_insurance,
    qi.quote_customs_duties,
    qi.quote_tax_type,
    qi.quote_tax_rate_pct,
    qi.quote_grand_total,

    -- Pricing (source currency + EUR)
    qi.currency,
    qi.unit_price AS unit_price_source_currency,
    qi.unit_price_eur,
    qi.total_price AS total_price_source_currency,
    qi.total_price_eur,

    -- Selection status
    CASE WHEN qi.is_selected_quote = 1 THEN 'PRIMARY SELECTED' ELSE 'ALTERNATIVE' END AS supplier_rank,
    qi.quote_rank AS rank_within_supplier

FROM ras_procurement.quotation_extracted_items qi
INNER JOIN ras_procurement.purchase_req_detail prd
    ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
INNER JOIN ras_procurement.purchase_req_mst prm
    ON prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
WHERE prm.PURCHASE_REQ_NO = @purchase_req_no
ORDER BY prd.PURCHASE_DTL_ID, qi.is_selected_quote DESC, qi.total_price_eur;

-- ============================================================================
-- QUERY 3: SUPPLIER MAPPING FOR HISTORICAL BENCHMARKING (BP / LP)
-- ============================================================================
-- Returns benchmark comparison with BOTH LLM and CPI inflation for BP and LP
--
-- BP (Best Price) = lowest unit_price_eur from similar_dtl_ids (LLM-ranked shortlist)
-- LP (Last Purchase) = most recent by PR date from similar_dtl_ids
--
-- Inflation: FOUR values calculated for each (BP and LP):
--   - inflation_pct: LLM-estimated inflation
--   - cpi_inflation_pct: World Bank CPI-based inflation
--   - inflation_pct_last: LLM-estimated inflation for LP
--   - cpi_inflation_pct_last: World Bank CPI for LP
--
-- Normalization formula (both):
--   bp_normalized = bp_unit_price × (1 + cpi_inflation_pct / 100)
--   lp_normalized = lp_unit_price × (1 + cpi_inflation_pct_last / 100)

DECLARE @purchase_req_no NVARCHAR(50) = 'R_152105/2021';

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prm_current.PURCHASE_REQ_NO AS pr_number,
    prd.ITEMDESCRIPTION AS ras_item_description,

    -- ─── CURRENT SUPPLIER (THIS RAS) ─────────────────────────────────────
    qi_current.supplier_name AS current_supplier_name,
    qi_current.supplier_country AS current_supplier_country,
    qi_current.unit_price_eur AS current_unit_price_eur,
    qi_current.total_price_eur AS current_total_price_eur,
    qi_current.currency AS current_currency,
    qi_current.quotation_date AS current_quotation_date,

    -- ─── BEST PRICE (BP) — CHEAPEST FROM similar_dtl_ids ──────────────────
    -- Selected by _compute_low_last(): min(shortlist_dtl_ids, unit_price_eur)
    -- Does NOT filter for is_selected_quote (can be any quote from shortlist)
    qi_bp.supplier_name AS bp_supplier_name,
    qi_bp.supplier_country AS bp_supplier_country,
    qi_bp.currency AS bp_currency,
    qi_bp.unit_price_eur AS bp_unit_price_eur,
    qi_bp.total_price_eur AS bp_total_price_eur,
    qi_bp.quotation_date AS bp_quotation_date,
    prm_bp.PURCHASE_REQ_NO AS bp_pr_number,
    prm_bp.C_DATETIME AS bp_pr_created_date,
    DATEDIFF(YEAR, YEAR(prm_bp.C_DATETIME), YEAR(GETDATE())) AS bp_years_ago,

    -- ─── LAST PURCHASE (LP) — MOST RECENT FROM similar_dtl_ids ────────────
    -- Selected by _compute_low_last(): max(shortlist_dtl_ids, pr_created_date)
    -- Most recent by purchase_req_mst.C_DATETIME
    qi_lp.supplier_name AS lp_supplier_name,
    qi_lp.supplier_country AS lp_supplier_country,
    qi_lp.currency AS lp_currency,
    qi_lp.unit_price_eur AS lp_unit_price_eur,
    qi_lp.total_price_eur AS lp_total_price_eur,
    qi_lp.quotation_date AS lp_quotation_date,
    prm_lp.PURCHASE_REQ_NO AS lp_pr_number,
    prm_lp.C_DATETIME AS lp_pr_created_date,
    DATEDIFF(YEAR, YEAR(prm_lp.C_DATETIME), YEAR(GETDATE())) AS lp_years_ago,

    -- ─── BP INFLATION (TWO TYPES) ───────────────────────────────────────
    -- Lines 5542-5555 in pipeline_stage_123_v3.py
    ROUND(br.inflation_pct, 4) AS bp_llm_inflation_pct,
    ROUND(br.cpi_inflation_pct, 4) AS bp_cpi_inflation_pct,

    -- ─── LP INFLATION (TWO TYPES) ───────────────────────────────────────
    -- Lines 5567-5581 in pipeline_stage_123_v3.py
    ROUND(br.inflation_pct_last, 4) AS lp_llm_inflation_pct,
    ROUND(br.cpi_inflation_pct_last, 4) AS lp_cpi_inflation_pct,

    -- ─── NORMALIZED PRICING (USING CPI INFLATION) ───────────────────────
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100), 2) AS bp_cpi_normalized_unit_price_eur,
    ROUND(br.bp_total_price * (1 + br.cpi_inflation_pct / 100), 2) AS bp_cpi_normalized_total_price_eur,
    ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100), 2) AS lp_cpi_normalized_unit_price_eur,
    ROUND(qi_lp.total_price_eur * (1 + br.cpi_inflation_pct_last / 100), 2) AS lp_cpi_normalized_total_price_eur,

    -- ─── NORMALIZED PRICING (USING LLM INFLATION) ──────────────────────
    ROUND(br.bp_unit_price * (1 + br.inflation_pct / 100), 2) AS bp_llm_normalized_unit_price_eur,
    ROUND(br.bp_total_price * (1 + br.inflation_pct / 100), 2) AS bp_llm_normalized_total_price_eur,
    ROUND(qi_lp.unit_price_eur * (1 + br.inflation_pct_last / 100), 2) AS lp_llm_normalized_unit_price_eur,
    ROUND(qi_lp.total_price_eur * (1 + br.inflation_pct_last / 100), 2) AS lp_llm_normalized_total_price_eur,

    -- ─── SAVINGS / OVERPAYMENT (CPI-ADJUSTED) ──────────────────────────
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100) - qi_current.unit_price_eur, 2) AS bp_cpi_adjusted_vs_current_unit_diff_eur,
    ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100) - qi_current.unit_price_eur, 2) AS lp_cpi_adjusted_vs_current_unit_diff_eur,

    -- ─── CONTEXT ────────────────────────────────────────────────────────
    -- similar_dtl_ids = from Stage C LLM ranking shortlist (lines 7817 in v3)
    br.similar_dtl_ids,
    br.summary

FROM ras_procurement.benchmark_result br
INNER JOIN ras_procurement.quotation_extracted_items qi_current
    ON br.extracted_item_uuid_fk = qi_current.extracted_item_uuid_pk
INNER JOIN ras_procurement.purchase_req_detail prd
    ON br.purchase_dtl_id = prd.PURCHASE_DTL_ID
INNER JOIN ras_procurement.purchase_req_mst prm_current
    ON prd.PURCHASE_REQ_ID = prm_current.PURCHASE_REQ_ID
LEFT JOIN ras_procurement.quotation_extracted_items qi_bp
    ON br.low_hist_item_fk = qi_bp.extracted_item_uuid_pk
LEFT JOIN ras_procurement.purchase_req_detail prd_bp
    ON qi_bp.purchase_dtl_id = prd_bp.PURCHASE_DTL_ID
LEFT JOIN ras_procurement.purchase_req_mst prm_bp
    ON prd_bp.PURCHASE_REQ_ID = prm_bp.PURCHASE_REQ_ID
LEFT JOIN ras_procurement.quotation_extracted_items qi_lp
    ON br.last_hist_item_fk = qi_lp.extracted_item_uuid_pk
LEFT JOIN ras_procurement.purchase_req_detail prd_lp
    ON qi_lp.purchase_dtl_id = prd_lp.PURCHASE_DTL_ID
LEFT JOIN ras_procurement.purchase_req_mst prm_lp
    ON prd_lp.PURCHASE_REQ_ID = prm_lp.PURCHASE_REQ_ID
WHERE prm_current.PURCHASE_REQ_NO = @purchase_req_no
ORDER BY prd.PURCHASE_DTL_ID;

-- ============================================================================
-- QUERY 4: CURRENCY CONVERSION & INFLATION RATES
-- ============================================================================
-- Returns BOTH LLM and CPI inflation for BOTH BP and LP
--
-- CURRENCY: Source currency → EUR conversion (stored at extraction time)
-- INFLATION BP: inflation_pct (LLM) + cpi_inflation_pct (World Bank CPI)
-- INFLATION LP: inflation_pct_last (LLM) + cpi_inflation_pct_last (World Bank CPI)
--
-- Reference dates:
--   - Current item: prm_current.C_DATETIME (current PR master date)
--   - BP item: prm_bp.C_DATETIME (BP item's PR master date, from line 5533)
--   - LP item: prm_lp.C_DATETIME (LP item's PR master date, from line 5564)

DECLARE @purchase_req_no NVARCHAR(50) = 'R_152105/2021';

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prm_current.PURCHASE_REQ_NO AS pr_number,
    prd.ITEMDESCRIPTION AS ras_item_description,
    qi.supplier_name,

    -- ─── CURRENCY CONVERSION (CURRENT ITEM) ────────────────────────────
    qi.currency AS source_currency,
    qi.unit_price AS unit_price_source_currency,
    qi.unit_price_eur AS unit_price_eur_converted,
    ROUND(qi.unit_price_eur / NULLIF(qi.unit_price, 0), 4) AS implied_exchange_rate,
    qi.total_price AS total_price_source_currency,
    qi.total_price_eur AS total_price_eur_converted,

    -- ─── BP INFLATION CALCULATION (TWO SOURCES) ───────────────────────
    -- Calculated at line 5542-5555 in pipeline_stage_123_v3.py
    -- Uses BP item's supplier_country and PR date range
    br.bp_unit_price AS bp_unit_price_eur,
    ROUND(br.inflation_pct, 4) AS bp_llm_inflation_pct,
    ROUND(br.cpi_inflation_pct, 4) AS bp_cpi_inflation_pct,
    ROUND((br.inflation_pct / 100) * br.bp_unit_price, 4) AS bp_llm_inflation_amount_eur,
    ROUND((br.cpi_inflation_pct / 100) * br.bp_unit_price, 4) AS bp_cpi_inflation_amount_eur,
    ROUND(br.bp_unit_price * (1 + br.inflation_pct / 100), 4) AS bp_llm_adjusted_unit_price,
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100), 4) AS bp_cpi_adjusted_unit_price,
    prm_bp.C_DATETIME AS bp_pr_created_date,
    YEAR(prm_bp.C_DATETIME) AS bp_pr_year,

    -- ─── LP INFLATION CALCULATION (TWO SOURCES) ───────────────────────
    -- Calculated at line 5567-5581 in pipeline_stage_123_v3.py
    -- Uses LP item's supplier_country and PR date range
    qi_lp.unit_price_eur AS lp_unit_price_eur,
    ROUND(br.inflation_pct_last, 4) AS lp_llm_inflation_pct,
    ROUND(br.cpi_inflation_pct_last, 4) AS lp_cpi_inflation_pct,
    ROUND((br.inflation_pct_last / 100) * qi_lp.unit_price_eur, 4) AS lp_llm_inflation_amount_eur,
    ROUND((br.cpi_inflation_pct_last / 100) * qi_lp.unit_price_eur, 4) AS lp_cpi_inflation_amount_eur,
    ROUND(qi_lp.unit_price_eur * (1 + br.inflation_pct_last / 100), 4) AS lp_llm_adjusted_unit_price,
    ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100), 4) AS lp_cpi_adjusted_unit_price,
    prm_lp.C_DATETIME AS lp_pr_created_date,
    YEAR(prm_lp.C_DATETIME) AS lp_pr_year,

    -- ─── REFERENCE DATES ────────────────────────────────────────────────
    prm_current.C_DATETIME AS current_pr_created_date,
    YEAR(prm_current.C_DATETIME) AS current_pr_year,
    qi.item_created_date AS current_quote_created_date

FROM ras_procurement.quotation_extracted_items qi
INNER JOIN ras_procurement.purchase_req_detail prd
    ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
INNER JOIN ras_procurement.purchase_req_mst prm_current
    ON prd.PURCHASE_REQ_ID = prm_current.PURCHASE_REQ_ID
LEFT JOIN ras_procurement.benchmark_result br
    ON qi.extracted_item_uuid_pk = br.extracted_item_uuid_fk
LEFT JOIN ras_procurement.quotation_extracted_items qi_bp
    ON br.low_hist_item_fk = qi_bp.extracted_item_uuid_pk
LEFT JOIN ras_procurement.purchase_req_detail prd_bp
    ON qi_bp.purchase_dtl_id = prd_bp.PURCHASE_DTL_ID
LEFT JOIN ras_procurement.purchase_req_mst prm_bp
    ON prd_bp.PURCHASE_REQ_ID = prm_bp.PURCHASE_REQ_ID
LEFT JOIN ras_procurement.quotation_extracted_items qi_lp
    ON br.last_hist_item_fk = qi_lp.extracted_item_uuid_pk
LEFT JOIN ras_procurement.purchase_req_detail prd_lp
    ON qi_lp.purchase_dtl_id = prd_lp.PURCHASE_DTL_ID
LEFT JOIN ras_procurement.purchase_req_mst prm_lp
    ON prd_lp.PURCHASE_REQ_ID = prm_lp.PURCHASE_REQ_ID
WHERE prm_current.PURCHASE_REQ_NO = @purchase_req_no
ORDER BY prd.PURCHASE_DTL_ID, qi.is_selected_quote DESC;

-- ============================================================================
-- END OF CLIENT QUERIES
-- ============================================================================
