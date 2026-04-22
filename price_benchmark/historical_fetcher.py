"""Fetch historical pricing for a line item by querying Pinecone + Azure SQL."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Optional

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables
from quotation_embedding.embedding_client import EmbeddingClient
from quotation_embedding.pinecone_writer import PineconeWriter
from utils.config import AppConfig

from .models import HistoricalItem

_FETCH_HISTORICAL_SQL = f"""
    SELECT
        qi.[purchase_dtl_id],
        qi.[unit_price],
        qi.[total_price],
        qi.[quantity],
        qi.[unit],
        qi.[currency],
        qi.[quotation_date],
        qi.[supplier_name]
    FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS} qi
    WHERE qi.[purchase_dtl_id] IN ({{placeholders}})
      AND qi.[is_selected_quote] = 1
"""


class HistoricalFetcher(BaseRepository):
    """Finds similar historical items via Pinecone and fetches their DB data.

    Parameters
    ----------
    config:
        AppConfig — used to initialise the embedding client and Pinecone writer.
    conn_str:
        Azure SQL connection string for fetching historical prices from the DB.
    """

    def __init__(self, config: AppConfig, conn_str: str) -> None:
        super().__init__(conn_str)
        self._emb_client    = EmbeddingClient(config)
        self._pinecone      = PineconeWriter(config)

    def fetch(self, row: dict, exclude_pr: str) -> list[HistoricalItem]:
        """Return historically similar items for one selected line item.

        Steps:
          1. Build embedding text from the item's taxonomy / description fields.
          2. Embed it via Azure OpenAI.
          3. Query Pinecone for the nearest neighbours, excluding the current PR.
          4. Fetch those items' pricing data from quotation_extracted_items.

        Parameters
        ----------
        row:
            One dict from BenchmarkItemReader (contains all embedding + pricing cols).
        exclude_pr:
            The current purchase_req_no — filtered out of Pinecone results so we
            only get historical comparisons, not the current PR's own items.

        Returns
        -------
        list[HistoricalItem]
            Empty if no similar items are found in Pinecone or if none have
            pricing data in the DB.
        """
        text = self._emb_client.build_text(row)
        if not text.strip():
            logger.debug(
                "purchase_dtl_id={} has no embedding text — skipping Pinecone query",
                row.get("purchase_dtl_id"),
            )
            return []

        vectors = self._emb_client.embed([text])
        if not vectors:
            return []

        matches = self._pinecone.query(
            vector=vectors[0],
            filter={"purchase_req_no": {"$ne": exclude_pr}},
        )
        if not matches:
            logger.debug(
                "No Pinecone matches for purchase_dtl_id={}",
                row.get("purchase_dtl_id"),
            )
            return []

        dtl_ids = [
            int(m["metadata"]["purchase_dtl_id"])
            for m in matches
            if m.get("metadata", {}).get("purchase_dtl_id") is not None
        ]
        if not dtl_ids:
            return []

        logger.debug(
            "Pinecone returned {} match(es) for purchase_dtl_id={}: dtl_ids={}",
            len(dtl_ids), row.get("purchase_dtl_id"), dtl_ids,
        )

        return self._fetch_from_db(dtl_ids)

    def _fetch_from_db(self, dtl_ids: list[int]) -> list[HistoricalItem]:
        """Fetch pricing rows from quotation_extracted_items for the given dtl_ids."""
        placeholders = ", ".join(["?"] * len(dtl_ids))
        sql = _FETCH_HISTORICAL_SQL.format(placeholders=placeholders)

        rows, cols = self._fetch_with_columns(sql, *dtl_ids)
        items = []
        for row in rows:
            rec = dict(zip(cols, row))
            items.append(HistoricalItem(
                purchase_dtl_id = int(rec["purchase_dtl_id"]),
                unit_price      = _to_decimal(rec.get("unit_price")),
                total_price     = _to_decimal(rec.get("total_price")),
                quantity        = _to_decimal(rec.get("quantity")),
                unit            = rec.get("unit"),
                currency        = rec.get("currency"),
                quotation_date  = _to_date(rec.get("quotation_date")),
                supplier_name   = rec.get("supplier_name"),
            ))
        return items


def _to_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _to_date(value: object) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        from datetime import datetime
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        return None
