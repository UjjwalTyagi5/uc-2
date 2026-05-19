-- ============================================================================
-- CLIENT QUERIES: SUPPLIER HIERARCHY & BENCHMARKING (v3)
-- ============================================================================
-- These 4 queries support client analysis of:
-- 1. Supplier hierarchy (Primary, Secondary, Tertiary)
-- 2. Technical & Commercial specifications per supplier
-- 3. Historical benchmarking (Best Price & Last Purchase)
-- 4. Currency conversion & inflation rates
-- ============================================================================

-- USAGE: Replace @purchase_req_id with the actual PR ID (e.g., 152105)

-- ============================================================================
-- QUERY 1: FETCH PRIMARY, SECONDARY, TERTIARY SUPPLIERS FOR A RAS
-- ============================================================================
-- Returns three result sets:
--   1. PRIMARY SUPPLIERS: extracted from quotations, ranked by selection status
--   2. SECONDARY SUPPLIER: parent_supplier from RAS master (if exists)
--   3. TERTIARY SUPPLIERS: alternative/unselected suppliers by category

DECLARE @purchase_req_id INT = 152105;

-- ─── PRIMARY SUPPLIERS ───────────────────────────────────────────────────
-- Suppliers from extracted quotations, ranked by is_selected_quote count
-- supplier_match_conf = confidence (0-1) of LLM match to RAS supplier
SELECT
    'PRIMARY' AS supplier_type,
    qi.supplier_name,
    qi.supplier_country,
    ROUND(AVG(qi.supplier_match_conf), 3) AS avg_supplier_match_conf,
    COUNT(qi.extracted_item_uuid_pk) AS total_quotes,
    SUM(CASE WHEN qi.is_selected_quote = 1 THEN 1 ELSE 0 END) AS selected_quotes,
    STRING_AGG(DISTINCT qi.purchase_category_llm, ', ') AS categories_quoted
FROM ras_procurement.quotation_extracted_items qi
WHERE qi.purchase_req_id = @purchase_req_id
  AND qi.supplier_name IS NOT NULL
GROUP BY qi.supplier_name, qi.supplier_country
ORDER BY selected_quotes DESC, total_quotes DESC;

-- ─── SECONDARY SUPPLIER ───────────────────────────────────────────────────
-- Parent supplier (if any) from RAS master
-- Represents parent organization or group company
SELECT
    'SECONDARY' AS supplier_type,
    prm.parent_supplier AS supplier_name,
    prm.supplier_type,
    COUNT(DISTINCT prm.PURCHASE_REQ_ID) AS ras_count
FROM ras_procurement.purchase_req_mst prm
WHERE prm.PURCHASE_REQ_ID = @purchase_req_id
  AND prm.parent_supplier IS NOT NULL
GROUP BY prm.parent_supplier, prm.supplier_type;

-- ─── TERTIARY SUPPLIERS ───────────────────────────────────────────────────
-- Alternative suppliers: those quoted but not selected
-- Ranked by frequency (alternative offers per item category)
SELECT
    'TERTIARY' AS supplier_type,
    qi.supplier_name,
    qi.supplier_country,
    ROUND(AVG(qi.supplier_match_conf), 3) AS avg_supplier_match_conf,
    COUNT(qi.extracted_item_uuid_pk) AS alternative_quotes,
    MIN(qi.total_price_eur) AS lowest_price_eur,
    MAX(qi.total_price_eur) AS highest_price_eur,
    qi.purchase_category_llm AS category
FROM ras_procurement.quotation_extracted_items qi
WHERE qi.purchase_req_id = @purchase_req_id
  AND qi.is_selected_quote = 0
  AND qi.supplier_name IS NOT NULL
GROUP BY qi.supplier_name, qi.supplier_country, qi.purchase_category_llm
ORDER BY alternative_quotes DESC, lowest_price_eur ASC;

-- ============================================================================
-- QUERY 2: TECHNICAL & COMMERCIAL SPECS FOR RAS > ITEM > PER SUPPLIER
-- ============================================================================
-- Returns full specification matrix:
--   TECHNICAL: item_name, taxonomy (item_level_1..3), critical attributes, commodity
--   COMMERCIAL: quotation terms, payment, delivery, pricing
-- Rows are grouped by item (PURCHASE_DTL_ID) and supplier

DECLARE @purchase_req_id INT = 152105;

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
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
WHERE prd.PURCHASE_REQ_ID = @purchase_req_id
ORDER BY prd.PURCHASE_DTL_ID, qi.is_selected_quote DESC, qi.total_price_eur;

-- ============================================================================
-- QUERY 3: SUPPLIER MAPPING FOR HISTORICAL BENCHMARKING (BP / LP)
-- ============================================================================
-- Returns benchmark comparison:
--   CURRENT: what this RAS item was quoted at
--   BP (BEST PRICE): cheapest historical match + inflation adjustment
--   LP (LAST PURCHASE): most recent historical match + inflation adjustment
-- Formulas:
--   bp_normalized_unit_price = bp_unit_price * (1 + bp_cpi_inflation_pct / 100)
--   lp_normalized_unit_price = lp_unit_price * (1 + lp_cpi_inflation_pct / 100)

DECLARE @purchase_req_id INT = 152105;

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prd.ITEMDESCRIPTION AS ras_item_description,

    -- ─── CURRENT SUPPLIER (THIS RAS) ─────────────────────────────────────
    qi_current.supplier_name AS current_supplier_name,
    qi_current.supplier_country AS current_supplier_country,
    qi_current.unit_price_eur AS current_unit_price_eur,
    qi_current.total_price_eur AS current_total_price_eur,
    qi_current.quotation_date AS current_quotation_date,

    -- ─── BEST PRICE (BP) — CHEAPEST HISTORICAL MATCH ──────────────────────
    qi_bp.supplier_name AS bp_supplier_name,
    qi_bp.supplier_country AS bp_supplier_country,
    br.bp_unit_price AS bp_unit_price_eur,
    br.bp_total_price AS bp_total_price_eur,
    qi_bp.quotation_date AS bp_quotation_date,
    DATEDIFF(DAY, qi_bp.item_created_date, GETDATE()) AS bp_days_ago,

    -- ─── LAST PURCHASE (LP) — MOST RECENT HISTORICAL MATCH ────────────────
    qi_lp.supplier_name AS lp_supplier_name,
    qi_lp.supplier_country AS lp_supplier_country,
    qi_lp.unit_price_eur AS lp_unit_price_eur,
    qi_lp.total_price_eur AS lp_total_price_eur,
    qi_lp.quotation_date AS lp_quotation_date,
    DATEDIFF(DAY, qi_lp.item_created_date, GETDATE()) AS lp_days_ago,

    -- ─── INFLATION ADJUSTMENTS ──────────────────────────────────────────
    ROUND(br.inflation_pct, 2) AS bp_general_inflation_pct,
    ROUND(br.cpi_inflation_pct, 2) AS bp_cpi_inflation_pct,
    ROUND(br.inflation_pct_last, 2) AS lp_general_inflation_pct,
    ROUND(br.cpi_inflation_pct_last, 2) AS lp_cpi_inflation_pct,

    -- ─── NORMALIZED PRICING (INFLATION-ADJUSTED) ────────────────────────
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100), 2) AS bp_inflation_adjusted_unit_price_eur,
    ROUND(br.bp_total_price * (1 + br.cpi_inflation_pct / 100), 2) AS bp_inflation_adjusted_total_price_eur,
    ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100), 2) AS lp_inflation_adjusted_unit_price_eur,
    ROUND(qi_lp.total_price_eur * (1 + br.cpi_inflation_pct_last / 100), 2) AS lp_inflation_adjusted_total_price_eur,

    -- ─── SAVINGS / OVERPAYMENT ──────────────────────────────────────────
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100) - qi_current.unit_price_eur, 2) AS bp_adjusted_vs_current_unit_diff_eur,
    ROUND(qi_lp.unit_price_eur * (1 + br.cpi_inflation_pct_last / 100) - qi_current.unit_price_eur, 2) AS lp_adjusted_vs_current_unit_diff_eur,

    -- ─── CONTEXT ────────────────────────────────────────────────────────
    br.similar_dtl_ids,  -- JSON: [375663, 375664, ...] list of other similar historical items
    br.summary  -- LLM-generated benchmark recommendation

FROM ras_procurement.benchmark_result br
INNER JOIN ras_procurement.quotation_extracted_items qi_current
    ON br.extracted_item_uuid_fk = qi_current.extracted_item_uuid_pk
INNER JOIN ras_procurement.purchase_req_detail prd
    ON br.purchase_dtl_id = prd.PURCHASE_DTL_ID
LEFT JOIN ras_procurement.quotation_extracted_items qi_bp
    ON br.low_hist_item_fk = qi_bp.extracted_item_uuid_pk
LEFT JOIN ras_procurement.quotation_extracted_items qi_lp
    ON br.last_hist_item_fk = qi_lp.extracted_item_uuid_pk
WHERE prd.PURCHASE_REQ_ID = @purchase_req_id
ORDER BY prd.PURCHASE_DTL_ID;

-- ============================================================================
-- QUERY 4: CURRENCY CONVERSION & INFLATION RATES
-- ============================================================================
-- Returns:
--   CURRENCY: Source currency, original price, converted price (EUR)
--   INFLATION: BP and LP inflation rates (CPI-based) and adjusted prices
-- Inflation formula: normalized_price = historical_price * (1 + cpi_inflation_pct / 100)

DECLARE @purchase_req_id INT = 152105;

SELECT
    prd.PURCHASE_DTL_ID AS item_id,
    prd.ITEMDESCRIPTION AS ras_item_description,
    qi.supplier_name,

    -- ─── CURRENCY CONVERSION ────────────────────────────────────────────
    qi.currency AS source_currency_iso_code,
    qi.unit_price AS unit_price_source_currency,
    qi.unit_price_eur AS unit_price_eur_converted,
    ROUND(qi.unit_price_eur / NULLIF(qi.unit_price, 0), 4) AS implied_exchange_rate,
    CASE
        WHEN qi.currency = 'EUR' THEN 'No conversion required'
        ELSE 'Converted via v3 LLM pricing logic'
    END AS currency_conversion_method,

    qi.total_price AS total_price_source_currency,
    qi.total_price_eur AS total_price_eur_converted,

    -- ─── BP (BEST PRICE) INFLATION ──────────────────────────────────────
    br.bp_unit_price AS bp_historical_unit_price_eur,
    ROUND(br.inflation_pct, 2) AS bp_general_inflation_pct,
    ROUND(br.cpi_inflation_pct, 2) AS bp_cpi_inflation_pct,
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct / 100), 4) AS bp_inflation_adjusted_unit_price,
    ROUND((br.cpi_inflation_pct / 100) * br.bp_unit_price, 4) AS bp_inflation_amount_eur,

    -- ─── LP (LAST PURCHASE) INFLATION ───────────────────────────────────
    br.bp_unit_price AS lp_historical_unit_price_eur,  -- LP uses same historical item as reference
    ROUND(br.inflation_pct_last, 2) AS lp_general_inflation_pct,
    ROUND(br.cpi_inflation_pct_last, 2) AS lp_cpi_inflation_pct,
    ROUND(br.bp_unit_price * (1 + br.cpi_inflation_pct_last / 100), 4) AS lp_inflation_adjusted_unit_price,
    ROUND((br.cpi_inflation_pct_last / 100) * br.bp_unit_price, 4) AS lp_inflation_amount_eur,

    -- ─── REFERENCE DATES ────────────────────────────────────────────────
    prd.C_DATETIME AS pr_created_date,
    qi.item_created_date AS current_quote_created_date,
    DATEDIFF(MONTH, prd.C_DATETIME, GETDATE()) AS months_since_pr_creation

FROM ras_procurement.quotation_extracted_items qi
INNER JOIN ras_procurement.purchase_req_detail prd
    ON qi.purchase_dtl_id = prd.PURCHASE_DTL_ID
LEFT JOIN ras_procurement.benchmark_result br
    ON qi.extracted_item_uuid_pk = br.extracted_item_uuid_fk
WHERE prd.PURCHASE_REQ_ID = @purchase_req_id
ORDER BY prd.PURCHASE_DTL_ID, qi.is_selected_quote DESC;

-- ============================================================================
-- END OF CLIENT QUERIES
-- ============================================================================
