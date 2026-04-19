"""Resolve which attachment files are quotations for a given PR.

All DB access goes through BaseRepository so queries get automatic retry
on transient Azure SQL errors and consistent connection management.

Both tables are linked to a purchase_req_no via ras_tracker:
    ras_tracker.purchase_req_no → ras_tracker.ras_uuid_pk
                                → attachment_classification.ras_uuid_pk

When the classification stage has not yet run (doc_type IS NULL), provides
a fallback mode (include_all=True) that returns ALL attachments for testing.
"""

from __future__ import annotations

import uuid

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables

from .config import ExtractionConfig
from .models import QuotationSource

# ── SQL ──────────────────────────────────────────────────────────────────────

# Parent attachments classified as Quotation
_CLASSIFIED_SQL = f"""
SELECT ac.[attachment_classify_uuid_pk],
       ac.[file_path],
       ac.[attachment_id]
  FROM {AzureTables.ATTACHMENT_CLASSIFICATION} ac
  JOIN {AzureTables.RAS_TRACKER} rt
    ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
 WHERE rt.[purchase_req_no] = ?
   AND ac.[doc_type] = 'Quotation'
"""

# Embedded files classified as Quotation.
# Also selects parent attachment_classify_uuid_pk because the DB constraint
# on quotation_extracted_items requires attachment_classify_fk IS NOT NULL
# whenever embedded_classify_fk IS NOT NULL.
_EMBEDDED_CLASSIFIED_SQL = f"""
SELECT ec.[embedded_attachment_classification_id],
       ec.[file_path],
       ec.[parent_attachment_id],
       ac.[attachment_classify_uuid_pk]
  FROM {AzureTables.EMBEDDED_ATTACHMENT_CLASSIFICATION} ec
  JOIN {AzureTables.ATTACHMENT_CLASSIFICATION} ac
    ON ec.[attachment_classification_id] = ac.[attachment_classify_uuid_pk]
  JOIN {AzureTables.RAS_TRACKER} rt
    ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
 WHERE rt.[purchase_req_no] = ?
   AND ec.[doc_type] = 'Quotation'
"""

# Fallback: all parent attachments regardless of doc_type
_ALL_PARENT_SQL = f"""
SELECT ac.[attachment_classify_uuid_pk],
       ac.[file_path],
       ac.[attachment_id]
  FROM {AzureTables.ATTACHMENT_CLASSIFICATION} ac
  JOIN {AzureTables.RAS_TRACKER} rt
    ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
 WHERE rt.[purchase_req_no] = ?
"""

# Fallback: all embedded files regardless of doc_type
_ALL_EMBEDDED_SQL = f"""
SELECT ec.[embedded_attachment_classification_id],
       ec.[file_path],
       ec.[parent_attachment_id],
       ac.[attachment_classify_uuid_pk]
  FROM {AzureTables.EMBEDDED_ATTACHMENT_CLASSIFICATION} ec
  JOIN {AzureTables.ATTACHMENT_CLASSIFICATION} ac
    ON ec.[attachment_classification_id] = ac.[attachment_classify_uuid_pk]
  JOIN {AzureTables.RAS_TRACKER} rt
    ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
 WHERE rt.[purchase_req_no] = ?
"""


# ── Repository ────────────────────────────────────────────────────────────────

class _QuotationSourceRepository(BaseRepository):
    """Internal repository — not part of the public API of this module."""

    def fetch_parent_sources(self, purchase_req_no: str, include_all: bool):
        sql = _ALL_PARENT_SQL if include_all else _CLASSIFIED_SQL
        return self._fetch(sql, purchase_req_no)

    def fetch_embedded_sources(self, purchase_req_no: str, include_all: bool):
        sql = _ALL_EMBEDDED_SQL if include_all else _EMBEDDED_CLASSIFIED_SQL
        return self._fetch(sql, purchase_req_no)


# ── Public API ────────────────────────────────────────────────────────────────

def resolve_quotation_sources(
    config: ExtractionConfig,
    purchase_req_no: str,
    *,
    include_all: bool = False,
) -> list[QuotationSource]:
    """Return quotation file sources for *purchase_req_no*.

    Parameters
    ----------
    include_all:
        When True, returns ALL attachments regardless of doc_type.
        Useful for testing before the classification stage is wired.
    """
    repo = _QuotationSourceRepository(config.get_azure_conn_str())
    sources: list[QuotationSource] = []

    # ── Parent attachments ────────────────────────────────────────────────────
    # row: [attachment_classify_uuid_pk, file_path, attachment_id]
    for row in repo.fetch_parent_sources(purchase_req_no, include_all):
        if not row[1]:
            continue
        sources.append(
            QuotationSource(
                blob_path=row[1],
                attachment_classify_fk=uuid.UUID(str(row[0])),
                attachment_id=str(row[2]),
            )
        )

    # ── Embedded files ────────────────────────────────────────────────────────
    # row: [embedded_attachment_classification_id, file_path,
    #       parent_attachment_id, attachment_classify_uuid_pk]
    #
    # Both embedded_classify_fk AND attachment_classify_fk are set here
    # to satisfy the DB constraint on quotation_extracted_items:
    #   embedded_classify_fk IS NULL OR attachment_classify_fk IS NOT NULL
    for row in repo.fetch_embedded_sources(purchase_req_no, include_all):
        if not row[1]:
            continue
        sources.append(
            QuotationSource(
                blob_path=row[1],
                embedded_classify_fk=uuid.UUID(str(row[0])),
                attachment_classify_fk=uuid.UUID(str(row[3])),
                attachment_id=str(row[2]),
            )
        )

    logger.info(
        "Resolved {} quotation source(s) for {} (include_all={})",
        len(sources), purchase_req_no, include_all,
    )
    return sources
