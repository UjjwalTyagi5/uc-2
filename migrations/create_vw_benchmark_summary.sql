-- ============================================================
-- Migration: create vw_benchmark_summary
--
-- Combines benchmark_result with quotation_extracted_items
-- (current item, low/last hist) and the two cheapest proposals
-- (L1, L2) for each line item.
--
-- NEW: Includes inflation_pct and cpi_inflation_pct for both
-- low_hist_item and last_hist_item with clear naming.
--
-- Run against [ras-procurement-benchmark].
-- ============================================================

CREATE OR ALTER VIEW [ras_procurement].[vw_benchmark_summary] AS
SELECT
    -- ========== IDENTIFIERS & PURCHASE CONTEXT ==========
    br.[extracted_item_uuid_fk],
    br.[purchase_dtl_id],
    prm.[PURCHASE_REQ_NO],
    prd.[ITEMDESCRIPTION]        AS item_description_user,
    prt.[PURCHASE_REQ_TYPE]      AS item_category,
    prd.[Insourcing_flag]        AS insourcing_flag,
    LTRIM(RTRIM(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                REPLACE(prm.[JUSTIFICATION], '&nbsp;', ' '),
                                                '&amp;', '&'),
                                            '<br />', ' '),
                                        '<br/>', ' '),
                                    '<br>', ' '),
                                '<strong>', ''),
                            '</strong>', ''),
                        '<em>', ''),
                    '</em>', ''),
                '<b>', ''),
            '</b>', ''),
        '<i>', ''),
    '</i>', ''
    ))  AS project_justification_user,
    prm.[LAST_APPROVED_COMMENTS] AS last_approver_comments,

    -- ========== CURRENT/PRIMARY QUOTE ==========
    qi.[item_name],
    qi.[commodity_tag],
    qi.[quantity]           AS primary_quantity,
    qi.[currency]           AS primary_currency,
    qi.[unit_price]         AS primary_unit_price,
    qi.[unit_price_eur]     AS primary_unit_price_eur,
    qi.[total_price]        AS primary_total_price,
    qi.[total_price_eur]    AS primary_total_price_eur,
    qi.[payment_terms]      AS primary_payment_terms,
    qi.[supplier_name]      AS primary_supplier_name,
    qi.[supplier_country]   AS primary_supplier_country,
    qi.[quotation_date]     AS primary_quotation_date,

    -- ========== BENCHMARK RECOMMENDATION ==========
    br.[bp_unit_price]      AS recommended_unit_price_eur,
    br.[bp_total_price]     AS recommended_total_price_eur,
    br.[summary]            AS recommendation_summary,

    -- ========== BEST AVAILABLE OPTIONS (L1 = Cheapest, L2 = 2nd Cheapest) ==========
    proposals.l1_supplier_name,
    proposals.l1_supplier_country,
    proposals.l1_unit_price,
    proposals.l1_unit_price_eur,
    proposals.l1_total_price,
    proposals.l1_total_price_eur,
    proposals.l1_currency,
    proposals.l1_quantity,
    proposals.l1_payment_terms,

    proposals.l2_supplier_name,
    proposals.l2_supplier_country,
    proposals.l2_unit_price,
    proposals.l2_unit_price_eur,
    proposals.l2_total_price,
    proposals.l2_total_price_eur,
    proposals.l2_currency,
    proposals.l2_quantity,
    proposals.l2_payment_terms,

    -- ========== HISTORICAL COMPARISON: CHEAPEST MATCH ==========
    low.[purchase_dtl_id]          AS low_hist_dtl_id,
    prm_low.[PURCHASE_REQ_NO]      AS low_hist_purchase_req_no,
    low.[quotation_date]           AS low_hist_quotation_date,
    low.[supplier_name]            AS low_hist_supplier_name,
    low.[supplier_country]         AS low_hist_supplier_country,
    low.[quantity]                 AS low_hist_quantity,
    low.[currency]                 AS low_hist_currency,
    low.[unit_price]               AS low_hist_unit_price,
    low.[unit_price_eur]           AS low_hist_unit_price_eur,
    low.[total_price]              AS low_hist_total_price,
    low.[total_price_eur]          AS low_hist_total_price_eur,
    br.[inflation_pct]             AS low_hist_inflation_pct,
    br.[cpi_inflation_pct]         AS low_hist_cpi_inflation_pct,
    low.[unit_price] * (1 + br.[cpi_inflation_pct] / 100)       AS low_hist_normalized_unit_price,
    low.[total_price] * (1 + br.[cpi_inflation_pct] / 100)      AS low_hist_normalized_total_price,
    low.[unit_price_eur] * (1 + br.[cpi_inflation_pct] / 100)   AS low_hist_normalized_unit_price_eur,
    low.[total_price_eur] * (1 + br.[cpi_inflation_pct] / 100)  AS low_hist_normalized_total_price_eur,

    -- ========== HISTORICAL COMPARISON: MOST RECENT MATCH ==========
    lst.[purchase_dtl_id]          AS last_hist_dtl_id,
    prm_lst.[PURCHASE_REQ_NO]      AS last_hist_purchase_req_no,
    lst.[quotation_date]           AS last_hist_quotation_date,
    lst.[supplier_name]            AS last_hist_supplier_name,
    lst.[supplier_country]         AS last_hist_supplier_country,
    lst.[quantity]                 AS last_hist_quantity,
    lst.[currency]                 AS last_hist_currency,
    lst.[unit_price]               AS last_hist_unit_price,
    lst.[unit_price_eur]           AS last_hist_unit_price_eur,
    lst.[total_price]              AS last_hist_total_price,
    lst.[total_price_eur]          AS last_hist_total_price_eur,
    br.[inflation_pct_last]        AS last_hist_inflation_pct,
    br.[cpi_inflation_pct_last]    AS last_hist_cpi_inflation_pct,
    lst.[unit_price] * (1 + br.[cpi_inflation_pct_last] / 100)       AS last_hist_normalized_unit_price,
    lst.[total_price] * (1 + br.[cpi_inflation_pct_last] / 100)      AS last_hist_normalized_total_price,
    lst.[unit_price_eur] * (1 + br.[cpi_inflation_pct_last] / 100)   AS last_hist_normalized_unit_price_eur,
    lst.[total_price_eur] * (1 + br.[cpi_inflation_pct_last] / 100)  AS last_hist_normalized_total_price_eur,

    -- ========== MARKET CONTEXT & STATISTICS ==========
    quot_stats.[supplier_count],
    quot_stats.[supplier_names],
    br.[similar_dtl_ids],
    sim.[similar_purchase_req_nos]

FROM [ras_procurement].[benchmark_result] br

LEFT JOIN [ras_procurement].[quotation_extracted_items] qi
  ON qi.[extracted_item_uuid_pk] = br.[extracted_item_uuid_fk]

LEFT JOIN [ras_procurement].[purchase_req_detail] prd
  ON prd.[PURCHASE_DTL_ID] = br.[purchase_dtl_id]

LEFT JOIN [ras_procurement].[purchase_req_mst] prm
  ON prm.[PURCHASE_REQ_ID] = prd.[PURCHASE_REQ_ID]

LEFT JOIN [ras_procurement].[purchase_req_type] prt
  ON prt.[PURCHASE_REQ_TYPE_ID] = prm.[PURCHASE_REQ_TYPE_ID]

LEFT JOIN [ras_procurement].[quotation_extracted_items] low
  ON low.[extracted_item_uuid_pk] = br.[low_hist_item_fk]

LEFT JOIN [ras_procurement].[quotation_extracted_items] lst
  ON lst.[extracted_item_uuid_pk] = br.[last_hist_item_fk]

LEFT JOIN [ras_procurement].[purchase_req_detail] prd_low
  ON prd_low.[PURCHASE_DTL_ID] = low.[purchase_dtl_id]
LEFT JOIN [ras_procurement].[purchase_req_mst] prm_low
  ON prm_low.[PURCHASE_REQ_ID] = prd_low.[PURCHASE_REQ_ID]

LEFT JOIN [ras_procurement].[purchase_req_detail] prd_lst
  ON prd_lst.[PURCHASE_DTL_ID] = lst.[purchase_dtl_id]
LEFT JOIN [ras_procurement].[purchase_req_mst] prm_lst
  ON prm_lst.[PURCHASE_REQ_ID] = prd_lst.[PURCHASE_REQ_ID]

OUTER APPLY (
    SELECT
        MAX(CASE WHEN rn = 1 THEN p.quantity        END) AS l1_quantity,
        MAX(CASE WHEN rn = 1 THEN p.currency        END) AS l1_currency,
        MAX(CASE WHEN rn = 1 THEN p.unit_price      END) AS l1_unit_price,
        MAX(CASE WHEN rn = 1 THEN p.unit_price_eur  END) AS l1_unit_price_eur,
        MAX(CASE WHEN rn = 1 THEN p.total_price     END) AS l1_total_price,
        MAX(CASE WHEN rn = 1 THEN p.total_price_eur END) AS l1_total_price_eur,
        MAX(CASE WHEN rn = 1 THEN p.payment_terms   END) AS l1_payment_terms,
        MAX(CASE WHEN rn = 1 THEN p.supplier_name   END) AS l1_supplier_name,
        MAX(CASE WHEN rn = 1 THEN p.supplier_country END) AS l1_supplier_country,
        MAX(CASE WHEN rn = 2 THEN p.quantity        END) AS l2_quantity,
        MAX(CASE WHEN rn = 2 THEN p.currency        END) AS l2_currency,
        MAX(CASE WHEN rn = 2 THEN p.unit_price      END) AS l2_unit_price,
        MAX(CASE WHEN rn = 2 THEN p.unit_price_eur  END) AS l2_unit_price_eur,
        MAX(CASE WHEN rn = 2 THEN p.total_price     END) AS l2_total_price,
        MAX(CASE WHEN rn = 2 THEN p.total_price_eur END) AS l2_total_price_eur,
        MAX(CASE WHEN rn = 2 THEN p.payment_terms   END) AS l2_payment_terms,
        MAX(CASE WHEN rn = 2 THEN p.supplier_name   END) AS l2_supplier_name,
        MAX(CASE WHEN rn = 2 THEN p.supplier_country END) AS l2_supplier_country
    FROM (
        SELECT
            p.[quantity],
            p.[currency],
            p.[unit_price],
            p.[unit_price_eur],
            p.[total_price],
            p.[total_price_eur],
            p.[payment_terms],
            p.[supplier_name],
            p.[supplier_country],
            ROW_NUMBER() OVER (
                ORDER BY COALESCE(p.[unit_price_eur], p.[unit_price]) ASC, p.[supplier_name] ASC
            ) AS rn
        FROM (
            SELECT
                p.[quantity],
                p.[currency],
                p.[unit_price],
                p.[unit_price_eur],
                p.[total_price],
                p.[total_price_eur],
                p.[payment_terms],
                p.[supplier_name],
                p.[supplier_country],
                ROW_NUMBER() OVER (
                    PARTITION BY p.[supplier_name], COALESCE(p.[unit_price_eur], p.[unit_price])
                    ORDER BY p.[supplier_name] ASC
                ) AS duplicate_rn
            FROM [ras_procurement].[quotation_extracted_items] p
            WHERE p.[purchase_dtl_id] = br.[purchase_dtl_id]
              AND COALESCE(p.[unit_price_eur], p.[unit_price]) IS NOT NULL
        ) p
        WHERE p.duplicate_rn = 1
    ) p
) proposals

OUTER APPLY (
    SELECT
        COUNT(*)                                                             AS supplier_count,
        '[' + STRING_AGG('"' + s.[supplier_name] + '"', ',')
              WITHIN GROUP (ORDER BY s.[supplier_name]) + ']'               AS supplier_names
    FROM (
        SELECT DISTINCT [supplier_name]
        FROM [ras_procurement].[quotation_extracted_items]
        WHERE [purchase_dtl_id] = br.[purchase_dtl_id]
          AND [supplier_name]   IS NOT NULL
    ) s
) quot_stats

OUTER APPLY (
    SELECT '[' + STRING_AGG('"' + sub.[PURCHASE_REQ_NO] + '"', ',')
               WITHIN GROUP (ORDER BY sub.[PURCHASE_REQ_NO]) + ']' AS similar_purchase_req_nos
    FROM (
        SELECT DISTINCT prm2.[PURCHASE_REQ_NO]
        FROM OPENJSON(br.[similar_dtl_ids]) j
        JOIN [ras_procurement].[purchase_req_detail] prd2
          ON prd2.[PURCHASE_DTL_ID] = CAST(j.[value] AS INT)
        JOIN [ras_procurement].[purchase_req_mst] prm2
          ON prm2.[PURCHASE_REQ_ID] = prd2.[PURCHASE_REQ_ID]
    ) sub
) sim;
