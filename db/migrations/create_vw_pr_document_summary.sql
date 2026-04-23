-- ============================================================
-- Migration: create vw_pr_document_summary
--
-- One row per PURCHASE_REQ_NO showing the best-confidence
-- classified file for each doc type: MPBC, RFQ, BER,
-- E-Auction Results, Quotation.
--
-- Sources: attachment_classification  (parent files)
--          embedded_attachment_classification (extracted files)
--
-- When more than one file of the same doc type exists for a PR,
-- the one with the highest classification_conf is shown.
--
-- Run against [ras-procurement-benchmark].
-- ============================================================

CREATE OR ALTER VIEW [ras_procurement].[vw_pr_document_summary] AS

WITH all_docs AS (
    -- Parent attachment files
    SELECT
        rt.[purchase_req_no],
        ac.[doc_type],
        ac.[classification_conf],
        ac.[file_path],
        CASE
            WHEN CHARINDEX('/', REVERSE(ac.[file_path])) > 1
            THEN RIGHT(ac.[file_path], CHARINDEX('/', REVERSE(ac.[file_path])) - 1)
            ELSE ac.[file_path]
        END AS file_name
    FROM [ras_procurement].[ras_tracker] rt
    JOIN [ras_procurement].[attachment_classification] ac
      ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
    WHERE ac.[doc_type] IS NOT NULL
      AND ac.[doc_type] <> 'Others'

    UNION ALL

    -- Embedded (extracted) files
    SELECT
        rt.[purchase_req_no],
        eac.[doc_type],
        eac.[classification_conf],
        eac.[file_path],
        CASE
            WHEN CHARINDEX('/', REVERSE(eac.[file_path])) > 1
            THEN RIGHT(eac.[file_path], CHARINDEX('/', REVERSE(eac.[file_path])) - 1)
            ELSE eac.[file_path]
        END AS file_name
    FROM [ras_procurement].[ras_tracker] rt
    JOIN [ras_procurement].[attachment_classification] ac
      ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
    JOIN [ras_procurement].[embedded_attachment_classification] eac
      ON eac.[attachment_classification_id] = ac.[attachment_classify_uuid_pk]
    WHERE eac.[doc_type] IS NOT NULL
      AND eac.[doc_type] <> 'Others'
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY [purchase_req_no], [doc_type]
            ORDER BY [classification_conf] DESC
        ) AS rn
    FROM all_docs
)

SELECT
    [purchase_req_no],

    -- presence flags
    CAST(MAX(CASE WHEN [doc_type] = 'MPBC'              THEN 1 ELSE 0 END) AS BIT) AS is_mpbc,
    CAST(MAX(CASE WHEN [doc_type] = 'RFQ'               THEN 1 ELSE 0 END) AS BIT) AS is_rfq,
    CAST(MAX(CASE WHEN [doc_type] = 'BER'               THEN 1 ELSE 0 END) AS BIT) AS is_ber,
    CAST(MAX(CASE WHEN [doc_type] = 'E-Auction Results' THEN 1 ELSE 0 END) AS BIT) AS is_eauction,
    CAST(MAX(CASE WHEN [doc_type] = 'Quotation'         THEN 1 ELSE 0 END) AS BIT) AS is_quotation,

    -- MPBC
    MAX(CASE WHEN [doc_type] = 'MPBC' AND rn = 1 THEN [file_name]           END) AS mpbc_file_name,
    MAX(CASE WHEN [doc_type] = 'MPBC' AND rn = 1 THEN [classification_conf] END) AS mpbc_conf,
    MAX(CASE WHEN [doc_type] = 'MPBC' AND rn = 1 THEN [file_path]           END) AS mpbc_file_path,

    -- RFQ
    MAX(CASE WHEN [doc_type] = 'RFQ'  AND rn = 1 THEN [file_name]           END) AS rfq_file_name,
    MAX(CASE WHEN [doc_type] = 'RFQ'  AND rn = 1 THEN [classification_conf] END) AS rfq_conf,
    MAX(CASE WHEN [doc_type] = 'RFQ'  AND rn = 1 THEN [file_path]           END) AS rfq_file_path,

    -- BER
    MAX(CASE WHEN [doc_type] = 'BER'  AND rn = 1 THEN [file_name]           END) AS ber_file_name,
    MAX(CASE WHEN [doc_type] = 'BER'  AND rn = 1 THEN [classification_conf] END) AS ber_conf,
    MAX(CASE WHEN [doc_type] = 'BER'  AND rn = 1 THEN [file_path]           END) AS ber_file_path,

    -- E-Auction Results
    MAX(CASE WHEN [doc_type] = 'E-Auction Results' AND rn = 1 THEN [file_name]           END) AS eauction_file_name,
    MAX(CASE WHEN [doc_type] = 'E-Auction Results' AND rn = 1 THEN [classification_conf] END) AS eauction_conf,
    MAX(CASE WHEN [doc_type] = 'E-Auction Results' AND rn = 1 THEN [file_path]           END) AS eauction_file_path,

    -- Quotation
    MAX(CASE WHEN [doc_type] = 'Quotation' AND rn = 1 THEN [file_name]           END) AS quotation_file_name,
    MAX(CASE WHEN [doc_type] = 'Quotation' AND rn = 1 THEN [classification_conf] END) AS quotation_conf,
    MAX(CASE WHEN [doc_type] = 'Quotation' AND rn = 1 THEN [file_path]           END) AS quotation_file_path

FROM ranked
GROUP BY [purchase_req_no];
