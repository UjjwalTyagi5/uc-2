-- ============================================================
-- Migration: create vw_benchmark_summary
--
-- Combines benchmark_result with quotation_extracted_items
-- (current item, low/last hist) and the two cheapest proposals
-- (L1, L2) for each line item.
--
-- Run against [ras-procurement-benchmark].
-- ============================================================

CREATE OR ALTER VIEW [ras_procurement].[vw_benchmark_summary] AS
SELECT
    br.[extracted_item_uuid_fk],
    br.[purchase_dtl_id],
    prm.[PURCHASE_REQ_NO],

    -- PR / line-item context
    prd.[ITEMDESCRIPTION]        AS item_description,
    prd.[Insourcing_flag]        AS insourcing_flag,
    prm.[JUSTIFICATION]          AS justification,
    prm.[LAST_APPROVED_COMMENTS] AS last_approved_comments,

    br.[bp_unit_price]    AS recommended_unit_price,
    br.[bp_total_price]   AS recommended_total_price,
    br.[inflation_pct]    AS recommended_inflation_pct,
    br.[cpi_inflation_pct],
    br.[summary]          AS recommendation_summary,

    -- Primary selected quote (is_selected_quote = 1)
    qi.[item_name],
    qi.[commodity_tag],
    qi.[unit_price]         AS primary_unit_price,
    qi.[unit_price_eur]     AS primary_unit_price_eur,
    qi.[total_price]        AS primary_total_price,
    qi.[total_price_eur]    AS primary_total_price_eur,
    qi.[currency]           AS primary_currency,
    qi.[payment_terms]      AS primary_payment_terms,
    qi.[supplier_name]      AS primary_supplier_name,
    qi.[supplier_country]   AS primary_supplier_country,
    qi.[quotation_date]     AS primary_quotation_date,

    -- Low hist (cheapest historical match via Pinecone)
    low.[unit_price]        AS low_hist_unit_price,
    low.[unit_price_eur]    AS low_hist_unit_price_eur,
    low.[currency]          AS low_hist_currency,
    low.[supplier_name]     AS low_hist_supplier_name,
    low.[supplier_country]  AS low_hist_supplier_country,
    low.[quotation_date]    AS low_hist_quotation_date,

    -- Last hist (most recent historical match via Pinecone)
    lst.[unit_price]        AS last_hist_unit_price,
    lst.[unit_price_eur]    AS last_hist_unit_price_eur,
    lst.[currency]          AS last_hist_currency,
    lst.[supplier_name]     AS last_hist_supplier_name,
    lst.[quotation_date]    AS last_hist_quotation_date,

    -- L1 = lowest-priced proposal for this line item (is_selected_quote = 0 or NULL)
    proposals.l1_unit_price,
    proposals.l1_unit_price_eur,
    proposals.l1_total_price,
    proposals.l1_total_price_eur,
    proposals.l1_currency,
    proposals.l1_payment_terms,
    proposals.l1_supplier_name,
    proposals.l1_supplier_country,

    -- L2 = second lowest-priced proposal for this line item
    proposals.l2_unit_price,
    proposals.l2_unit_price_eur,
    proposals.l2_total_price,
    proposals.l2_total_price_eur,
    proposals.l2_currency,
    proposals.l2_payment_terms,
    proposals.l2_supplier_name,
    proposals.l2_supplier_country,

    -- Quotation statistics
    quot_stats.[total_quotations],
    quot_stats.[supplier_names],

    -- Similar items from Pinecone
    br.[similar_dtl_ids],
    sim.[similar_purchase_req_nos]

FROM [ras_procurement].[benchmark_result] br

LEFT JOIN [ras_procurement].[quotation_extracted_items] qi
  ON qi.[extracted_item_uuid_pk] = br.[extracted_item_uuid_fk]

LEFT JOIN [ras_procurement].[purchase_req_detail] prd
  ON prd.[PURCHASE_DTL_ID] = br.[purchase_dtl_id]

LEFT JOIN [ras_procurement].[purchase_req_mst] prm
  ON prm.[PURCHASE_REQ_ID] = prd.[PURCHASE_REQ_ID]

LEFT JOIN [ras_procurement].[quotation_extracted_items] low
  ON low.[extracted_item_uuid_pk] = br.[low_hist_item_fk]

LEFT JOIN [ras_procurement].[quotation_extracted_items] lst
  ON lst.[extracted_item_uuid_pk] = br.[last_hist_item_fk]

OUTER APPLY (
    SELECT
        MAX(CASE WHEN rn = 1 THEN p.unit_price      END) AS l1_unit_price,
        MAX(CASE WHEN rn = 1 THEN p.unit_price_eur  END) AS l1_unit_price_eur,
        MAX(CASE WHEN rn = 1 THEN p.total_price     END) AS l1_total_price,
        MAX(CASE WHEN rn = 1 THEN p.total_price_eur END) AS l1_total_price_eur,
        MAX(CASE WHEN rn = 1 THEN p.currency        END) AS l1_currency,
        MAX(CASE WHEN rn = 1 THEN p.payment_terms   END) AS l1_payment_terms,
        MAX(CASE WHEN rn = 1 THEN p.supplier_name   END) AS l1_supplier_name,
        MAX(CASE WHEN rn = 1 THEN p.supplier_country END) AS l1_supplier_country,
        MAX(CASE WHEN rn = 2 THEN p.unit_price      END) AS l2_unit_price,
        MAX(CASE WHEN rn = 2 THEN p.unit_price_eur  END) AS l2_unit_price_eur,
        MAX(CASE WHEN rn = 2 THEN p.total_price     END) AS l2_total_price,
        MAX(CASE WHEN rn = 2 THEN p.total_price_eur END) AS l2_total_price_eur,
        MAX(CASE WHEN rn = 2 THEN p.currency        END) AS l2_currency,
        MAX(CASE WHEN rn = 2 THEN p.payment_terms   END) AS l2_payment_terms,
        MAX(CASE WHEN rn = 2 THEN p.supplier_name   END) AS l2_supplier_name,
        MAX(CASE WHEN rn = 2 THEN p.supplier_country END) AS l2_supplier_country
    FROM (
        SELECT
            p.[unit_price],
            p.[unit_price_eur],
            p.[total_price],
            p.[total_price_eur],
            p.[currency],
            p.[payment_terms],
            p.[supplier_name],
            p.[supplier_country],
            ROW_NUMBER() OVER (
                ORDER BY COALESCE(p.[unit_price_eur], p.[unit_price]) ASC
            ) AS rn
        FROM [ras_procurement].[quotation_extracted_items] p
        WHERE p.[purchase_dtl_id]   = br.[purchase_dtl_id]
          AND (p.[is_selected_quote] = 0 OR p.[is_selected_quote] IS NULL)
          AND COALESCE(p.[unit_price_eur], p.[unit_price]) IS NOT NULL
    ) p
) proposals

OUTER APPLY (
    SELECT
        COUNT(*)                                                             AS total_quotations,
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
