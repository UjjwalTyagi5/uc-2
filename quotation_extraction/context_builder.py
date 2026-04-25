"""Build RAS line-item context from Azure SQL for LLM consumption.

All DB access goes through BaseRepository so queries get automatic retry
on transient Azure SQL errors and consistent connection management.

Queries:
  * purchase_req_mst                  – RAS header (supplier, justification, …)
  * purchase_req_detail               – per-item rows (DTL_ID, description, qty, …)
  * vw_get_ras_data_for_bidashboard   – RAS-level enrichment (category, region, …)
    └─ failure / no rows here is non-fatal; enrichment fields are left null
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from loguru import logger

from db.crud import BaseRepository
from db.tables import AzureTables

from .config import ExtractionConfig
from .models import LineItemContext, RASContext

# ── SQL ──────────────────────────────────────────────────────────────────────

_MST_SQL = f"""
SELECT TOP 1
       prm.[PURCHASE_REQ_ID],
       prm.[PURCHASE_REQ_NO],
       prm.[SUPPLIER_NAME],
       prm.[JUSTIFICATION],
       prm.[CURRENCY],
       prm.[ENQUIRY_NO],
       prm.[CLASSIFICATION],
       prm.[Department],
       prm.[NEGOTIATED_BY],
       prm.[ADDRESS],
       prm.[CONTRACT_NO],
       prm.[ORDER_NO],
       prm.[PURCHASE_VALUE]
  FROM {AzureTables.PURCHASE_REQ_MST} prm
 WHERE prm.[PURCHASE_REQ_NO] = ?
"""

_DTL_SQL = f"""
SELECT prd.[PURCHASE_DTL_ID],
       prd.[PURCHASE_REQ_ID],
       prd.[ITEM_NO],
       prd.[QUANTITY],
       prd.[ITEM_TYPE],
       prd.[ITEMDESCRIPTION],
       prd.[PRICE],
       prd.[UOM],
       prd.[DISCOUNT],
       prd.[REQ_VALUE],
       prd.[CURRENCY],
       prd.[DELIVERY_DATE],
       prd.[SUPPLIER_NAME],
       prd.[PAYMENT_DETAILS],
       prd.[ORIGINAL_VALUE],
       prd.[Initial_Offer],
       prd.[Negotiation],
       prd.[CommentsforItem],
       prd.[PREPAYMENT],
       prd.[ITEM_CODE]
  FROM {AzureTables.PURCHASE_REQ_DETAIL} prd
 WHERE prd.[PURCHASE_REQ_ID] = ?
 ORDER BY prd.[ITEM_NO]
"""

_VW_SQL = f"""
SELECT TOP 1
       vw.[L1],               -- 0:  category level 1
       vw.[L2],               -- 1:  category level 2
       vw.[Site_Country],     -- 2
       vw.[Site_Region],      -- 3
       vw.[Division],         -- 4
       vw.[Sub_Category_Type],-- 5
       vw.[L3],               -- 6:  category level 3
       vw.[L4],               -- 7:  category level 4
       vw.[DEPARTMENT],       -- 8   (may duplicate mst.Department)
       vw.[Purchase_Category],-- 9
       vw.[Title],            -- 10: RAS title / short description
       vw.[Site],             -- 11
       vw.[Requisition_Type], -- 12
       vw.[L5],               -- 13
       vw.[L6],               -- 14
       vw.[L7],               -- 15
       vw.[L8],               -- 16
       vw.[Parent_Supplier],  -- 17
       vw.[Supplier_Type],    -- 18
       vw.[Suplier_country],  -- 19 (intentional typo in DB column name)
       vw.[Payment_Days],     -- 20
       vw.[PO_Date],          -- 21
       vw.[Category_Buyer]    -- 22
  FROM {AzureTables.BI_DASHBOARD} vw
 WHERE vw.[PURCHASE_REQ_ID] = ?
"""


# Raw SELECT * queries — capture every column; used as reference context for the LLM.
# No TOP 1 on the VW query so all rows are returned (some PRs have multiple rows).
_MST_RAW_SQL = f"SELECT * FROM {AzureTables.PURCHASE_REQ_MST}    WHERE [PURCHASE_REQ_NO]  = ?"
_DTL_RAW_SQL = f"SELECT * FROM {AzureTables.PURCHASE_REQ_DETAIL}  WHERE [PURCHASE_REQ_ID]  = ? ORDER BY [ITEM_NO]"
_VW_RAW_SQL  = f"SELECT * FROM {AzureTables.BI_DASHBOARD}         WHERE [PURCHASE_REQ_ID]  = ?"

# Audit / system columns that add noise without helping the LLM match items
_NOISE_COLS: frozenset[str] = frozenset({
    "C_DATETIME", "U_DATETIME", "C_USER", "U_USER",
    "C_USER_ID",  "U_USER_ID",
})


def _rows_to_dicts(rows: list, columns: list[str]) -> list[dict]:
    """Convert raw pyodbc rows + column name list into clean dicts.

    Strips noise columns and skips columns whose value is None or blank
    so the LLM prompt isn't cluttered with empty fields.
    """
    result = []
    for row in rows:
        d = {
            col: val
            for col, val in zip(columns, row)
            if col not in _NOISE_COLS
            and val is not None
            and str(val).strip() != ""
        }
        if d:
            result.append(d)
    return result


# ── Repository ────────────────────────────────────────────────────────────────

class _RASContextRepository(BaseRepository):
    """Internal repository — not part of the public API of this module."""

    def fetch_mst(self, purchase_req_no: str):
        return self._fetch_one(_MST_SQL, purchase_req_no)

    def fetch_dtl(self, purchase_req_id: int):
        return self._fetch(_DTL_SQL, purchase_req_id)

    def fetch_vw(self, purchase_req_id: int):
        return self._fetch_one(_VW_SQL, purchase_req_id)

    # ── Raw full-column fetches (reference context for LLM) ──
    def fetch_mst_raw(self, purchase_req_no: str) -> tuple:
        return self._fetch_with_columns(_MST_RAW_SQL, purchase_req_no)

    def fetch_dtl_raw(self, purchase_req_id: int) -> tuple:
        return self._fetch_with_columns(_DTL_RAW_SQL, purchase_req_id)

    def fetch_vw_raw(self, purchase_req_id: int) -> tuple:
        return self._fetch_with_columns(_VW_RAW_SQL, purchase_req_id)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dec(val: object) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def build_ras_context(
    config: ExtractionConfig,
    purchase_req_no: str,
) -> RASContext:
    """Return a fully-populated :class:`RASContext` for the given PR number."""

    repo = _RASContextRepository(config.get_azure_conn_str())

    # ── Master header ─────────────────────────────────────────��───────────────
    mst_row = repo.fetch_mst(purchase_req_no)
    if mst_row is None:
        raise ValueError(
            f"No purchase_req_mst row for PURCHASE_REQ_NO={purchase_req_no!r}"
        )

    purchase_req_id: int          = mst_row[0]
    supplier_name: Optional[str]  = mst_row[2] or None
    justification: Optional[str]  = mst_row[3] or None
    currency: Optional[str]       = mst_row[4] or None
    enquiry_no: Optional[str]     = mst_row[5] or None
    classification: Optional[str] = mst_row[6] or None
    department: Optional[str]     = mst_row[7] or None
    negotiated_by: Optional[str]  = mst_row[8] or None
    address: Optional[str]        = mst_row[9] or None
    contract_no: Optional[str]    = mst_row[10] or None
    order_no: Optional[str]       = mst_row[11] or None
    purchase_value: Optional[Decimal] = _dec(mst_row[12])

    # ── Detail line items ─────────────────────────────────────────────────────
    dtl_rows = repo.fetch_dtl(purchase_req_id)

    line_items: list[LineItemContext] = []
    for r in dtl_rows:
        raw_delivery = r[11]
        if raw_delivery is not None:
            delivery_str = (
                raw_delivery.date().isoformat()
                if hasattr(raw_delivery, "date")
                else str(raw_delivery)
            )
        else:
            delivery_str = None

        line_items.append(
            LineItemContext(
                purchase_dtl_id=r[0],
                purchase_req_id=r[1],
                item_no=r[2] or 0,
                quantity=_dec(r[3]),
                item_type=r[4] or None,
                item_description=r[5] or None,
                unit_price=_dec(r[6]),
                uom=r[7] or None,
                discount=_dec(r[8]),
                req_value=_dec(r[9]),
                currency=r[10] or None,
                delivery_date=delivery_str,
                supplier_name=r[12] or supplier_name or None,
                payment_details=r[13] or None,
                original_value=_dec(r[14]),
                initial_offer=_dec(r[15]),
                negotiation=_dec(r[16]),
                comments=r[17] or None,
                prepayment=r[18] or None,
                item_code=r[19] or None,
            )
        )

    # ── BI-dashboard enrichment (best-effort — null if unavailable) ───────────
    category: Optional[str]          = None
    sub_category: Optional[str]      = None
    site_country: Optional[str]      = None
    site_region: Optional[str]       = None
    division: Optional[str]          = None
    site: Optional[str]              = None
    requisition_type: Optional[str]  = None
    l3: Optional[str]                = None
    l4: Optional[str]                = None
    l5: Optional[str]                = None
    l6: Optional[str]                = None
    l7: Optional[str]                = None
    l8: Optional[str]                = None
    purchase_category: Optional[str] = None
    ras_title: Optional[str]         = None
    parent_supplier: Optional[str]   = None
    supplier_type: Optional[str]     = None
    supplier_country: Optional[str]  = None
    payment_days: Optional[str]      = None
    po_date: Optional[str]           = None
    category_buyer: Optional[str]    = None

    try:
        vw_row = repo.fetch_vw(purchase_req_id)
        if vw_row is None:
            logger.warning(
                "No BI dashboard data found for REQ_ID={} (PR={}) "
                "— enrichment fields will be null",
                purchase_req_id, purchase_req_no,
            )
        else:
            category          = vw_row[0] or None
            sub_category      = vw_row[5] or vw_row[1] or None
            site_country      = vw_row[2] or None
            site_region       = vw_row[3] or None
            division          = vw_row[4] or None
            l3                = vw_row[6] or None
            l4                = vw_row[7] or None
            purchase_category = vw_row[9] or None
            ras_title         = vw_row[10] or None
            site              = vw_row[11] or None
            requisition_type  = vw_row[12] or None
            l5                = vw_row[13] or None
            l6                = vw_row[14] or None
            l7                = vw_row[15] or None
            l8                = vw_row[16] or None
            parent_supplier   = vw_row[17] or None
            supplier_type     = vw_row[18] or None
            supplier_country  = vw_row[19] or None
            payment_days      = vw_row[20] or None
            po_date           = vw_row[21] or None
            category_buyer    = vw_row[22] or None
    except Exception:
        logger.warning(
            "BI-view query failed for REQ_ID={} (PR={}) "
            "— continuing without enrichment",
            purchase_req_id, purchase_req_no,
        )

    # ── Raw full-column context (non-fatal — used only as LLM reference) ─────
    raw_mst: dict = {}
    raw_dtl_rows: list[dict] = []
    raw_vw_rows: list[dict] = []
    try:
        mst_rows, mst_cols = repo.fetch_mst_raw(purchase_req_no)
        raw_mst = _rows_to_dicts(mst_rows, mst_cols)[0] if mst_rows else {}
    except Exception:
        logger.warning("Raw MST fetch failed for PR={} — skipping", purchase_req_no)
    try:
        dtl_rows, dtl_cols = repo.fetch_dtl_raw(purchase_req_id)
        raw_dtl_rows = _rows_to_dicts(dtl_rows, dtl_cols)
    except Exception:
        logger.warning("Raw DTL fetch failed for PR={} — skipping", purchase_req_no)
    try:
        vw_rows, vw_cols = repo.fetch_vw_raw(purchase_req_id)
        raw_vw_rows = _rows_to_dicts(vw_rows, vw_cols)
        if not raw_vw_rows:
            logger.debug("No BI dashboard rows for PR={} — raw_vw_rows empty", purchase_req_no)
    except Exception:
        logger.warning("Raw VW fetch failed for PR={} — skipping", purchase_req_no)

    ctx = RASContext(
        purchase_req_no=purchase_req_no,
        purchase_req_id=purchase_req_id,
        justification=justification,
        supplier_name=supplier_name,
        currency=currency,
        enquiry_no=enquiry_no,
        classification=classification,
        department=department,
        negotiated_by=negotiated_by,
        address=address,
        contract_no=contract_no,
        order_no=order_no,
        purchase_value=purchase_value,
        category=category,
        sub_category=sub_category,
        site_country=site_country,
        site_region=site_region,
        site=site,
        division=division,
        requisition_type=requisition_type,
        parent_supplier=parent_supplier,
        supplier_type=supplier_type,
        supplier_country=supplier_country,
        payment_days=payment_days,
        po_date=po_date,
        category_buyer=category_buyer,
        l3=l3,
        l4=l4,
        l5=l5,
        l6=l6,
        l7=l7,
        l8=l8,
        purchase_category=purchase_category,
        ras_title=ras_title,
        line_items=line_items,
        raw_mst=raw_mst,
        raw_dtl_rows=raw_dtl_rows,
        raw_vw_rows=raw_vw_rows,
    )

    logger.info(
        "Built RAS context: PR={} | {} line item(s) | category={}",
        purchase_req_no, len(line_items), category,
    )
    return ctx
