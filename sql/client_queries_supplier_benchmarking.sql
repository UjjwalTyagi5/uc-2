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
    qi.total_price_eur,

    -- Selection status
    qi.is_selected_quote,
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
-- QUERY 3: SUPPLIER MAPPING FOR HISTORICAL BENCHMARKING (LOW / LAST)
-- ============================================================================
-- Returns benchmark comparison with BOTH LLM and CPI inflation for LOW and LAST
--
-- LOW = lowest unit_price_eur from similar_dtl_ids (LLM-ranked shortlist)
-- LAST = most recent by PR date from similar_dtl_ids
--
-- Inflation: FOUR values calculated for each (LOW and LAST):
--   - inflation_pct: LLM-estimated inflation
--   - cpi_inflation_pct: World Bank CPI-based inflation
--   - inflation_pct_last: LLM-estimated inflation for LAST
--   - cpi_inflation_pct_last: World Bank CPI for LAST
--
-- Normalization formula (both):
--   low_normalized = low_unit_price × (1 + cpi_inflation_pct / 100)
--   last_normalized = last_unit_price × (1 + cpi_inflation_pct_last / 100)

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

    -- ─── LOW (LOWEST PRICE) — CHEAPEST FROM similar_dtl_ids ─────────────────
    -- Selected by _compute_low_last(): min(shortlist_dtl_ids, unit_price_eur)
    -- Does NOT filter for is_selected_quote (can be any quote from shortlist)
    qi_bp.purchase_dtl_id AS low_dtl_id,
    qi_bp.supplier_name AS low_supplier_name,
    qi_bp.supplier_country AS low_supplier_country,
    qi_bp.currency AS low_currency,
    qi_bp.unit_price_eur AS low_unit_price_eur,
    qi_bp.total_price_eur AS low_total_price_eur,
    qi_bp.quotation_date AS low_quotation_date,
    prm_bp.PURCHASE_REQ_NO AS low_pr_number,
    prm_bp.C_DATETIME AS low_pr_created_date,
    DATEDIFF(YEAR, prm_bp.C_DATETIME, prm_current.C_DATETIME) AS low_years_ago,

    -- ─── LAST (MOST RECENT) — MOST RECENT FROM similar_dtl_ids ────────────
    -- Selected by _compute_low_last(): max(shortlist_dtl_ids, pr_created_date)
    -- Most recent by purchase_req_mst.C_DATETIME
    qi_lp.purchase_dtl_id AS last_dtl_id,
    qi_lp.supplier_name AS last_supplier_name,
    qi_lp.supplier_country AS last_supplier_country,
    qi_lp.currency AS last_currency,
    qi_lp.unit_price_eur AS last_unit_price_eur,
    qi_lp.total_price_eur AS last_total_price_eur,
    qi_lp.quotation_date AS last_quotation_date,
    prm_lp.PURCHASE_REQ_NO AS last_pr_number,
    prm_lp.C_DATETIME AS last_pr_created_date,
    DATEDIFF(YEAR, prm_lp.C_DATETIME, prm_current.C_DATETIME) AS last_years_ago,

    -- ─── LOW INFLATION (TWO TYPES) ──────────────────────────────────────
    -- Lines 5534-5541 in pipeline_stage_123_v3.py: if ref_year < current_year (year integers)
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.inflation_pct, 4) ELSE 0.0 END AS low_llm_inflation_pct,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.cpi_inflation_pct, 4) ELSE 0.0 END AS low_cpi_inflation_pct,

    -- ─── LAST INFLATION (TWO TYPES) ──────────────────────────────────────
    -- Lines 5565-5566 in pipeline_stage_123_v3.py: if ref_year_last < current_year (year integers)
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.inflation_pct_last, 4) ELSE 0.0 END AS last_llm_inflation_pct,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.cpi_inflation_pct_last, 4) ELSE 0.0 END AS last_cpi_inflation_pct,

    -- ─── NORMALIZED PRICING (USING CPI INFLATION) ───────────────────────
    -- Only calculated if year difference exists (matching YEAR comparison above)
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100), 2) ELSE br.bp_unit_price END AS low_cpi_normalized_unit_price_eur,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.bp_total_price * (1 + br.cpi_inflation_pct / 100), 2) ELSE br.bp_total_price END AS low_cpi_normalized_total_price_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100), 2) ELSE qi_lp.unit_price_eur END AS last_cpi_normalized_unit_price_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(qi_lp.total_price_eur * (1 + br.cpi_inflation_pct_last / 100), 2) ELSE qi_lp.total_price_eur END AS last_cpi_normalized_total_price_eur,

    -- ─── NORMALIZED PRICING (USING LLM INFLATION) ──────────────────────
    -- Only calculated if year difference exists (matching YEAR comparison above)
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.bp_unit_price * (1 + br.inflation_pct / 100), 2) ELSE br.bp_unit_price END AS low_llm_normalized_unit_price_eur,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.bp_total_price * (1 + br.inflation_pct / 100), 2) ELSE br.bp_total_price END AS low_llm_normalized_total_price_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(qi_lp.unit_price_eur * (1 + br.inflation_pct_last / 100), 2) ELSE qi_lp.unit_price_eur END AS last_llm_normalized_unit_price_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(qi_lp.total_price_eur * (1 + br.inflation_pct_last / 100), 2) ELSE qi_lp.total_price_eur END AS last_llm_normalized_total_price_eur,

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
-- Returns BOTH LLM and CPI inflation for BOTH LOW and LAST
--
-- CURRENCY: Source currency → EUR conversion (stored at extraction time)
-- INFLATION LOW: inflation_pct (LLM) + cpi_inflation_pct (World Bank CPI)
-- INFLATION LAST: inflation_pct_last (LLM) + cpi_inflation_pct_last (World Bank CPI)
--
-- Reference dates:
--   - Current item: prm_current.C_DATETIME (current PR master date)
--   - LOW item: prm_bp.C_DATETIME (LOW item's PR master date, from line 5533)
--   - LAST item: prm_lp.C_DATETIME (LAST item's PR master date, from line 5564)

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

    -- ─── LOW INFLATION CALCULATION (TWO SOURCES) ─────────────────────
    -- Lines 5534-5556 in pipeline_stage_123_v3.py: if YEAR(ref_dt) < YEAR(created)
    br.bp_unit_price AS low_unit_price_eur,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.inflation_pct, 4) ELSE 0.0 END AS low_llm_inflation_pct,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.cpi_inflation_pct, 4) ELSE 0.0 END AS low_cpi_inflation_pct,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND((br.inflation_pct / 100) * br.bp_unit_price, 4) ELSE 0.0 END AS low_llm_inflation_amount_eur,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND((br.cpi_inflation_pct / 100) * br.bp_unit_price, 4) ELSE 0.0 END AS low_cpi_inflation_amount_eur,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.bp_unit_price * (1 + br.inflation_pct / 100), 4) ELSE ROUND(br.bp_unit_price, 4) END AS low_llm_adjusted_unit_price,
    CASE WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100), 4) ELSE ROUND(br.bp_unit_price, 4) END AS low_cpi_adjusted_unit_price,
    prm_bp.C_DATETIME AS low_pr_created_date,
    YEAR(prm_bp.C_DATETIME) AS low_pr_year,

    -- ─── LAST INFLATION CALCULATION (TWO SOURCES) ──────────────────
    -- Lines 5565-5581 in pipeline_stage_123_v3.py: if YEAR(ref_dt_last) < YEAR(created)
    qi_lp.unit_price_eur AS last_unit_price_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.inflation_pct_last, 4) ELSE 0.0 END AS last_llm_inflation_pct,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(br.cpi_inflation_pct_last, 4) ELSE 0.0 END AS last_cpi_inflation_pct,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND((br.inflation_pct_last / 100) * qi_lp.unit_price_eur, 4) ELSE 0.0 END AS last_llm_inflation_amount_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND((br.cpi_inflation_pct_last / 100) * qi_lp.unit_price_eur, 4) ELSE 0.0 END AS last_cpi_inflation_amount_eur,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(qi_lp.unit_price_eur * (1 + br.inflation_pct_last / 100), 4) ELSE ROUND(qi_lp.unit_price_eur, 4) END AS last_llm_adjusted_unit_price,
    CASE WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100), 4) ELSE ROUND(qi_lp.unit_price_eur, 4) END AS last_cpi_adjusted_unit_price,
    prm_lp.C_DATETIME AS last_pr_created_date,
    YEAR(prm_lp.C_DATETIME) AS last_pr_year,

    -- ─── REFERENCE DATES ────────────────────────────────────────────────
    prm_current.C_DATETIME AS current_pr_created_date,
    YEAR(prm_current.C_DATETIME) AS current_pr_year

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
-- QUERY 5: VALIDATION — CHECK INFLATION LOGIC MATCHES v3 PIPELINE
-- ============================================================================
-- Validates inflation is applied ONLY when: ref_year < current_year (both exist)
-- Reference: Pipeline lines 5541 (LOW) and 5566 (LAST)
--
-- v3 Logic:
--   if ref_year and current_year and ref_year < current_year:
--       calculate inflation
--   else:
--       inflation = 0 (or stays NULL)
--
-- Flags rows where actual behavior differs from expected

DECLARE @purchase_req_no NVARCHAR(50) = 'R_152105/2021';

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prm_current.PURCHASE_REQ_NO AS pr_number,
    YEAR(prm_current.C_DATETIME) AS current_pr_year,

    -- LOW ITEM: Check if inflation conditions are met
    CASE WHEN prm_bp.C_DATETIME IS NULL THEN NULL ELSE YEAR(prm_bp.C_DATETIME) END AS low_pr_year,
    CASE
        WHEN prm_bp.C_DATETIME IS NULL THEN 'NO_LOW_ITEM'
        WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN 'SHOULD_INFLATE'
        WHEN YEAR(prm_bp.C_DATETIME) = YEAR(prm_current.C_DATETIME) THEN 'SAME_YEAR'
        ELSE 'FUTURE_ITEM'
    END AS low_expected_condition,
    br.cpi_inflation_pct AS low_cpi_stored,
    CASE
        WHEN prm_bp.C_DATETIME IS NULL AND (br.cpi_inflation_pct IS NOT NULL AND br.cpi_inflation_pct > 0)
            THEN 'ERROR: No LOW item but inflation > 0'
        WHEN YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME) AND (br.cpi_inflation_pct IS NULL OR br.cpi_inflation_pct = 0)
            THEN 'ERROR: Should inflate but inflation = 0'
        WHEN YEAR(prm_bp.C_DATETIME) >= YEAR(prm_current.C_DATETIME) AND (br.cpi_inflation_pct IS NOT NULL AND br.cpi_inflation_pct > 0)
            THEN 'ERROR: Should NOT inflate but inflation > 0'
        ELSE 'OK'
    END AS low_validation_status,

    -- LAST ITEM: Check if inflation conditions are met
    CASE WHEN prm_lp.C_DATETIME IS NULL THEN NULL ELSE YEAR(prm_lp.C_DATETIME) END AS last_pr_year,
    CASE
        WHEN prm_lp.C_DATETIME IS NULL THEN 'NO_LAST_ITEM'
        WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) THEN 'SHOULD_INFLATE'
        WHEN YEAR(prm_lp.C_DATETIME) = YEAR(prm_current.C_DATETIME) THEN 'SAME_YEAR'
        ELSE 'FUTURE_ITEM'
    END AS last_expected_condition,
    br.cpi_inflation_pct_last AS last_cpi_stored,
    CASE
        WHEN prm_lp.C_DATETIME IS NULL AND (br.cpi_inflation_pct_last IS NOT NULL AND br.cpi_inflation_pct_last > 0)
            THEN 'ERROR: No LAST item but inflation > 0'
        WHEN YEAR(prm_lp.C_DATETIME) < YEAR(prm_current.C_DATETIME) AND (br.cpi_inflation_pct_last IS NULL OR br.cpi_inflation_pct_last = 0)
            THEN 'ERROR: Should inflate but inflation = 0'
        WHEN YEAR(prm_lp.C_DATETIME) >= YEAR(prm_current.C_DATETIME) AND (br.cpi_inflation_pct_last IS NOT NULL AND br.cpi_inflation_pct_last > 0)
            THEN 'ERROR: Should NOT inflate but inflation > 0'
        ELSE 'OK'
    END AS last_validation_status

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
-- END OF CLIENT QUERIES
-- ============================================================================
