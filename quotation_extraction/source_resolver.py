"""Resolve which attachment files are quotations for a given PR.

Reads AttachmentClassification and EmbeddedAttachmentClassification tables
to find files classified as 'Quotation' (doc_type column).

When the classification stage has not yet run (doc_type IS NULL), provides
a fallback mode that returns ALL attachments for manual / testing use.
"""

from __future__ import annotations

import uuid

import pyodbc
from loguru import logger

from pipeline.db_utils import connect_with_retry

from .config import ExtractionConfig
from .models import QuotationSource

_SCHEMA = "ras_procurement"

_CLASSIFIED_SQL = f"""
SELECT ac.[attachment_classify_uuid_pk],
       ac.[file_path],
       ac.[attachment_id]
  FROM [{_SCHEMA}].[AttachmentClassification] ac
 WHERE ac.[purchase_req_no_fk] = ?
   AND ac.[doc_type] = 'Quotation'
"""

_EMBEDDED_CLASSIFIED_SQL = f"""
SELECT ec.[embedded_classify_uuid_pk],
       ec.[file_path],
       ec.[parent_attachment_id]
  FROM [{_SCHEMA}].[EmbeddedAttachmentClassification] ec
  JOIN [{_SCHEMA}].[AttachmentClassification] ac
    ON ec.[attachment_classify_uuid_pk] = ac.[attachment_classify_uuid_pk]
 WHERE ac.[purchase_req_no_fk] = ?
   AND ec.[doc_type] = 'Quotation'
"""

_ALL_PARENT_SQL = f"""
SELECT ac.[attachment_classify_uuid_pk],
       ac.[file_path],
       ac.[attachment_id]
  FROM [{_SCHEMA}].[AttachmentClassification] ac
 WHERE ac.[purchase_req_no_fk] = ?
"""

_ALL_EMBEDDED_SQL = f"""
SELECT ec.[embedded_classify_uuid_pk],
       ec.[file_path],
       ec.[parent_attachment_id]
  FROM [{_SCHEMA}].[EmbeddedAttachmentClassification] ec
  JOIN [{_SCHEMA}].[AttachmentClassification] ac
    ON ec.[attachment_classify_uuid_pk] = ac.[attachment_classify_uuid_pk]
 WHERE ac.[purchase_req_no_fk] = ?
"""


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

    conn: pyodbc.Connection = connect_with_retry(
        config.get_azure_conn_str(), autocommit=True
    )
    sources: list[QuotationSource] = []

    try:
        cursor = conn.cursor()

        parent_sql = _ALL_PARENT_SQL if include_all else _CLASSIFIED_SQL
        embedded_sql = _ALL_EMBEDDED_SQL if include_all else _EMBEDDED_CLASSIFIED_SQL

        # parent attachments
        cursor.execute(parent_sql, purchase_req_no)
        for row in cursor.fetchall():
            if row[1]:
                sources.append(
                    QuotationSource(
                        blob_path=row[1],
                        attachment_classify_fk=uuid.UUID(str(row[0])),
                        attachment_id=str(row[2]),
                    )
                )

        # embedded files
        cursor.execute(embedded_sql, purchase_req_no)
        for row in cursor.fetchall():
            if row[1]:
                sources.append(
                    QuotationSource(
                        blob_path=row[1],
                        embedded_classify_fk=uuid.UUID(str(row[0])),
                        attachment_id=str(row[2]),
                    )
                )

        logger.info(
            "Resolved {} quotation source(s) for {} (include_all={})",
            len(sources),
            purchase_req_no,
            include_all,
        )
        return sources

    finally:
        conn.close()
