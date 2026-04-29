"""
db.tables
~~~~~~~~~
Single source of truth for every table/view name used across this repo.

Usage
-----
    from db.tables import AzureTables, OnPremTables

    cursor.execute(f"SELECT * FROM {AzureTables.RAS_TRACKER} WHERE ...")
    cursor.execute(f"SELECT * FROM {OnPremTables.RAS_ATTACHMENTS} WHERE ...")

All names are fully schema-qualified and bracket-escaped so they can be
dropped directly into SQL strings without further escaping.

Azure SQL (ras_procurement schema)
----------------------------------
These tables live in the Azure SQL database and are written/read by the
pipeline, attachment_blob_sync, and purchase_sync packages.

On-prem SQL Server
------------------
These tables/views live in the on-prem database and are read-only from
our side. The ETL sync (purchase_sync) copies them to Azure SQL.
"""


class AzureTables:
    """Fully qualified table/view names in the Azure SQL ras_procurement schema."""

    SCHEMA = "ras_procurement"

    # ── Pipeline tracking ──────────────────────────────────────────────────
    RAS_TRACKER              = "[ras_procurement].[ras_tracker]"
    RAS_PIPELINE_EXCEPTIONS  = "[ras_procurement].[ras_pipeline_exceptions]"
    PIPELINE_STAGES          = "[ras_procurement].[pipeline_stages]"

    # ── Attachment classification ──────────────────────────────────────────
    ATTACHMENT_CLASSIFICATION          = "[ras_procurement].[attachment_classification]"
    EMBEDDED_ATTACHMENT_CLASSIFICATION = "[ras_procurement].[embedded_attachment_classification]"
    QUOTATION_EXTRACTED_ITEMS          = "[ras_procurement].[quotation_extracted_items]"
    BENCHMARK_RESULT                   = "[ras_procurement].[benchmark_result]"
    CURRENCY_MST                       = "[ras_procurement].[currency_mst]"
    EXCHANGE_RATE                      = "[ras_procurement].[EXCHANGE_RATE]"

    # ── BI dashboard ───────────────────────────────────────────────────────
    BI_DASHBOARD = "[ras_procurement].[vw_get_ras_data_for_bidashboard]"

    # ── Procurement source tables (ETL sync target) ────────────────────────
    PURCHASE_REQ_MST     = "[ras_procurement].[purchase_req_mst]"
    PURCHASE_ATTACHMENTS = "[ras_procurement].[purchase_attachments]"
    PURCHASE_REQ_DETAIL  = "[ras_procurement].[purchase_req_detail]"


class OnPremTables:
    """Fully qualified table/view names in the on-prem SQL Server database."""

    # Binary attachment documents
    RAS_ATTACHMENTS = "[dbo].[ras_attachments]"

    # BI dashboard source view (mirrored to Azure by ETL sync)
    BI_DASHBOARD = "[dbo].[vw_get_ras_data_for_bidashboard]"

    # Procurement source tables (read by ETL sync, written by on-prem systems)
    PURCHASE_REQ_MST     = "[dbo].[purchase_req_mst]"
    PURCHASE_ATTACHMENTS = "[dbo].[purchase_attachments]"
    PURCHASE_REQ_DETAIL  = "[dbo].[purchase_req_detail]"
