"""Persist extracted items to quotation_extracted_items.

All DB access goes through BaseRepository so writes get automatic retry
on transient Azure SQL errors, consistent connection management, and
bulk-insert via fast_executemany for better performance.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from difflib import SequenceMatcher
from typing import TYPE_CHECKING

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables
from utils.currency_converter import CurrencyConverter

from .config import ExtractionConfig
from .models import ExtractedItem

if TYPE_CHECKING:
    from .models import RASContext

_INSERT_SQL = f"""
INSERT INTO {AzureTables.QUOTATION_EXTRACTED_ITEMS} (
    [attachment_classify_fk],
    [embedded_classify_fk],
    [purchase_dtl_id],
    [is_selected_quote],
    [supplier_match_conf],
    [quote_rank],
    [supplier_name],
    [supplier_address],
    [supplier_country],
    [quotation_ref_no],
    [quotation_date],
    [currency],
    [validity_date],
    [validity_days],
    [payment_terms],
    [item_name],
    [item_description],
    [quantity],
    [unit],
    [unit_price],
    [total_price],
    [discount],
    [taxation_details],
    [delivery_date],
    [delivery_time_days],
    [item_level_1],
    [item_level_2],
    [item_level_3],
    [item_level_4],
    [item_level_5],
    [item_level_6],
    [item_level_7],
    [item_level_8],
    [commodity_tag],
    [item_summary],
    [unit_price_eur],
    [total_price_eur]
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?
)
"""

_DELETE_BY_ATTACHMENT_SQL = f"""
DELETE FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS}
 WHERE [attachment_classify_fk] = ?
   AND [embedded_classify_fk] IS NULL
"""

_DELETE_BY_EMBEDDED_SQL = f"""
DELETE FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS}
 WHERE [embedded_classify_fk] = ?
"""

_FETCH_FOR_CANONICALIZE_SQL = """
    SELECT [extracted_item_uuid_pk], [supplier_name], [supplier_country], [purchase_dtl_id]
    FROM   {table}
    WHERE  [purchase_dtl_id] IN ({{placeholders}})
      AND  [supplier_name]   IS NOT NULL
""".format(table=AzureTables.QUOTATION_EXTRACTED_ITEMS)

_UPDATE_SUPPLIER_NAME_SQL = f"""
    UPDATE {AzureTables.QUOTATION_EXTRACTED_ITEMS}
    SET    [supplier_name] = ?
    WHERE  [extracted_item_uuid_pk] = ?
"""

# Two variants — embedded items matched by embedded FK, others by attachment FK.
# Updates supplier_match_conf + is_selected_quote + quote_rank in one pass.
_UPDATE_SELECTION_EMBEDDED_SQL = f"""
    UPDATE {AzureTables.QUOTATION_EXTRACTED_ITEMS}
    SET    [supplier_match_conf] = ?,
           [is_selected_quote]  = ?,
           [quote_rank]         = ?
    WHERE  [embedded_classify_fk] = ?
      AND  [purchase_dtl_id]      = ?
"""

_UPDATE_SELECTION_ATTACHMENT_SQL = f"""
    UPDATE {AzureTables.QUOTATION_EXTRACTED_ITEMS}
    SET    [supplier_match_conf] = ?,
           [is_selected_quote]  = ?,
           [quote_rank]         = ?
    WHERE  [attachment_classify_fk] = ?
      AND  [embedded_classify_fk]   IS NULL
      AND  [purchase_dtl_id]        = ?
"""


def _p(val: object) -> object:
    """Convert Python types to pyodbc-safe values.

    Decimal → float: pyodbc fast_executemany fails when Decimal objects have
    varying scale across rows (e.g. Decimal("1.00000") vs Decimal("1.5")).
    SQL Server accepts float for all DECIMAL columns and rounds to column scale.
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, Decimal):
        return float(val)
    return val


def _item_to_row(item: ExtractedItem) -> list:
    """Convert one ExtractedItem to the ordered INSERT parameter list."""
    return [
        _p(str(item.attachment_classify_fk) if item.attachment_classify_fk else None),
        _p(str(item.embedded_classify_fk)   if item.embedded_classify_fk   else None),
        _p(item.purchase_dtl_id),
        _p(item.is_selected_quote),
        _p(item.supplier_match_conf),
        _p(item.quote_rank),
        _p(item.supplier_name),
        _p(item.supplier_address),
        _p(item.supplier_country),
        _p(item.quotation_ref_no),
        _p(item.quotation_date),
        _p(item.currency),
        _p(item.validity_date),
        _p(item.validity_days),
        _p(item.payment_terms),
        _p(item.item_name),
        _p(item.item_description),
        _p(item.quantity),
        _p(item.unit),
        _p(item.unit_price),
        _p(item.total_price),
        _p(item.discount),
        _p(item.taxation_details),
        _p(item.delivery_date),
        _p(item.delivery_time_days),
        _p(item.item_level_1),
        _p(item.item_level_2),
        _p(item.item_level_3),
        _p(item.item_level_4),
        _p(item.item_level_5),
        _p(item.item_level_6),
        _p(item.item_level_7),
        _p(item.item_level_8),
        _p(item.commodity_tag),
        _p(item.item_summary),
        _p(item.unit_price_eur),
        _p(item.total_price_eur),
    ]


class ExtractionWriter(BaseRepository):
    """Writes :class:`ExtractedItem` rows to the silver table."""

    def __init__(self, config: ExtractionConfig) -> None:
        super().__init__(config.get_azure_conn_str())

    def write(self, items: list[ExtractedItem], *, replace: bool = True) -> int:
        """Insert items into the silver table.

        If *replace* is True, existing rows for the same attachment /
        embedded FK are deleted first (idempotent re-runs).

        Returns the number of rows written.
        """
        if not items:
            return 0

        if replace:
            self._delete_existing(items)

        converter = CurrencyConverter(self._conn_str)
        for item in items:
            eur_unit  = converter.to_eur(item.unit_price,  item.currency, item.quotation_date)
            eur_total = converter.to_eur(item.total_price, item.currency, item.quotation_date)
            # Quantize to match DB column precision: DECIMAL(18,4) and DECIMAL(18,2)
            item.unit_price_eur  = eur_unit.quantize(Decimal("0.0001"))  if eur_unit  is not None else None
            item.total_price_eur = eur_total.quantize(Decimal("0.01"))   if eur_total is not None else None

        rows = [_item_to_row(item) for item in items]
        self._execute_many(_INSERT_SQL, rows)

        logger.info(
            "Wrote {} row(s) to {}",
            len(rows), AzureTables.QUOTATION_EXTRACTED_ITEMS,
        )
        return len(rows)

    def _delete_existing(self, items: list[ExtractedItem]) -> None:
        """Delete pre-existing rows for the same source FK(s).

        Embedded items carry both FKs — delete by the more-specific key
        (embedded FK) so rows from sibling embedded files are untouched.
        Parent-only items are deleted by their attachment FK.
        """
        deleted_emb: set[str] = set()
        deleted_att: set[str] = set()

        for item in items:
            if item.embedded_classify_fk:
                key = str(item.embedded_classify_fk)
                if key not in deleted_emb:
                    self._execute(_DELETE_BY_EMBEDDED_SQL, key)
                    deleted_emb.add(key)
            elif item.attachment_classify_fk:
                key = str(item.attachment_classify_fk)
                if key not in deleted_att:
                    self._execute(_DELETE_BY_ATTACHMENT_SQL, key)
                    deleted_att.add(key)

    def canonicalize_supplier_names_in_db(
        self,
        items: list[ExtractedItem],
        ras_context: "RASContext",
    ) -> dict[tuple[int, str], str]:
        """Post-INSERT: cluster supplier name variants per DTL_ID and UPDATE non-canonical rows.

        For each purchase_dtl_id in *items*, fetches all rows from
        quotation_extracted_items (including any from previous runs that were
        not cleaned up), clusters supplier name variants, and issues UPDATE
        statements so every row in a cluster carries the canonical name.

        Merge rules (after stripping contact suffixes from names):
          1. Cleaned names are identical (case-insensitive)
          2. One cleaned name is a substring of the other
          3. SequenceMatcher ratio >= 0.82
          4. One name is the uppercase initials of the other (TCS ↔ Tata Consultancy Services)

        Guard: if both rows have a non-empty supplier_country and those countries
        differ, they are NOT merged — different country branches stay separate.

        Canonical name: RAS-known supplier name first, else shortest name in cluster.
        """
        from .extractor import _CANONICALIZE_THRESHOLD, _is_acronym_of, _strip_contact_suffix

        dtl_ids = list({item.purchase_dtl_id for item in items if item.purchase_dtl_id is not None})
        if not dtl_ids:
            return {}

        ras_known: set[str] = set()
        if ras_context.supplier_name:
            ras_known.add(ras_context.supplier_name.strip().lower())
        if ras_context.parent_supplier:
            ras_known.add(ras_context.parent_supplier.strip().lower())
        for li in ras_context.line_items:
            if li.supplier_name:
                ras_known.add(li.supplier_name.strip().lower())

        placeholders = ",".join("?" * len(dtl_ids))
        sql = _FETCH_FOR_CANONICALIZE_SQL.replace("{placeholders}", placeholders)
        db_rows = self._fetch(sql, *dtl_ids)

        # Group by purchase_dtl_id → [(uuid_pk, supplier_name, supplier_country)]
        by_dtl: dict[int, list[tuple]] = defaultdict(list)
        for row in db_rows:
            uuid_pk, name, country, dtl_id = row[0], row[1], row[2], row[3]
            by_dtl[int(dtl_id)].append((uuid_pk, name, (country or "").strip()))

        updates: list[tuple[str, str]] = []  # (canonical_name, uuid_pk)
        # (purchase_dtl_id, original_name) → canonical_name — returned to callers
        name_to_canonical: dict[tuple[int, str], str] = {}

        for dtl_id, dtl_rows in by_dtl.items():
            raw_names = list({r[1] for r in dtl_rows})
            if len(raw_names) < 2:
                continue

            # First country seen per name (used for country-based merge guard)
            name_country: dict[str, str] = {}
            for _, name, country in dtl_rows:
                if name not in name_country:
                    name_country[name] = country.lower()

            # Union-Find
            uf_parent: dict[str, str] = {n: n for n in raw_names}

            def _find(x: str) -> str:
                while uf_parent[x] != x:
                    uf_parent[x] = uf_parent[uf_parent[x]]
                    x = uf_parent[x]
                return x

            def _union(a: str, b: str) -> None:
                uf_parent[_find(a)] = _find(b)

            for i, a in enumerate(raw_names):
                a_clean   = _strip_contact_suffix(a).lower()
                a_country = name_country.get(a, "")
                for b in raw_names[i + 1:]:
                    b_clean   = _strip_contact_suffix(b).lower()
                    b_country = name_country.get(b, "")

                    # Different non-empty supplier_country → separate suppliers
                    if a_country and b_country and a_country != b_country:
                        continue

                    if a_clean == b_clean:
                        _union(a, b)
                    elif a_clean in b_clean or b_clean in a_clean:
                        _union(a, b)
                    elif SequenceMatcher(None, a_clean, b_clean).ratio() >= _CANONICALIZE_THRESHOLD:
                        _union(a, b)
                    elif _is_acronym_of(a.strip(), b) or _is_acronym_of(b.strip(), a):
                        _union(a, b)

            clusters: dict[str, list[str]] = defaultdict(list)
            for n in raw_names:
                clusters[_find(n)].append(n)

            canonical_map: dict[str, str] = {}
            for members in clusters.values():
                ras_match = next((m for m in members if m.lower() in ras_known), None)
                canonical = ras_match if ras_match else min(members, key=len)
                for m in members:
                    canonical_map[m] = canonical

            for uuid_pk, name, _ in dtl_rows:
                canonical = canonical_map.get(name)
                if canonical and canonical != name:
                    updates.append((canonical, str(uuid_pk)))
                    self._log.debug(
                        "DB canonicalize DTL {}: {!r} → {!r}", dtl_id, name, canonical
                    )

            # Record the full mapping for this DTL_ID so callers can sync in-memory items
            for name, canonical in canonical_map.items():
                name_to_canonical[(dtl_id, name)] = canonical

        if updates:
            self._log.info(
                "Canonicalizing {} supplier name row(s) in quotation_extracted_items",
                len(updates),
            )
            self._execute_many(_UPDATE_SUPPLIER_NAME_SQL, updates)
        else:
            self._log.debug("Supplier names already canonical — no DB updates needed")

        return name_to_canonical

    def update_selection_fields(self, items: list[ExtractedItem]) -> None:
        """UPDATE supplier_match_conf, is_selected_quote, and quote_rank for all items.

        Called after canonicalize_supplier_names_in_db + recompute conf +
        run_selection_llm_query so the DB reflects rankings computed on
        canonical supplier names with fresh confidence scores.
        """
        emb_rows: list[tuple] = []
        att_rows: list[tuple] = []

        for item in items:
            if item.purchase_dtl_id is None:
                continue
            conf     = _p(item.supplier_match_conf)
            selected = 1 if item.is_selected_quote else 0
            rank     = item.quote_rank
            dtl_id   = item.purchase_dtl_id

            if item.embedded_classify_fk:
                emb_rows.append((conf, selected, rank, str(item.embedded_classify_fk), dtl_id))
            elif item.attachment_classify_fk:
                att_rows.append((conf, selected, rank, str(item.attachment_classify_fk), dtl_id))

        if emb_rows:
            self._execute_many(_UPDATE_SELECTION_EMBEDDED_SQL, emb_rows)
        if att_rows:
            self._execute_many(_UPDATE_SELECTION_ATTACHMENT_SQL, att_rows)

        self._log.info(
            "Updated selection fields for {} item(s) ({} embedded, {} attachment)",
            len(emb_rows) + len(att_rows), len(emb_rows), len(att_rows),
        )
