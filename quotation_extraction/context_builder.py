"""Build RAS line-item context from Azure SQL for LLM consumption.

Queries:
  * purchase_req_mst         – RAS header (supplier, justification, …)
  * purchase_req_detail      – per-item rows  (DTL_ID, description, qty, …)
  * vw_get_ras_data_for_bidashboard – RAS-level enrichment (category, region)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

import pyodbc
from loguru import logger

from pipeline.db_utils import connect_with_retry

from .config import ExtractionConfig
from .models import LineItemContext, RASContext

_SCHEMA = "ras_procurement"


def _dec(val: object) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


# ── SQL ──

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
  FROM [{_SCHEMA}].[purchase_req_mst] prm
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
  FROM [{_SCHEMA}].[purchase_req_detail] prd
 WHERE prd.[PURCHASE_REQ_ID] = ?
 ORDER BY prd.[ITEM_NO]
"""

_VW_SQL = f"""
SELECT TOP 1
       vw.[L1],
       vw.[L2],
       vw.[Site_Country],
       vw.[Site_Region],
       vw.[Division],
       vw.[Sub_Category_Type],
       vw.[L3],
       vw.[L4],
       vw.[DEPARTMENT],
       vw.[Purchase_Category],
       vw.[Title],
       vw.[Site],
       vw.[Requisition_Type],
       vw.[L5],
       vw.[L6],
       vw.[L7],
       vw.[L8],
       vw.[Parent_Supplier],
       vw.[Supplier_Type],
       vw.[Suplier_country],
       vw.[Payment_Days],
       vw.[PO_Date],
       vw.[Category_Buyer]
  FROM [{_SCHEMA}].[vw_get_ras_data_for_bidashboard] vw
 WHERE vw.[PURCHASE_REQ_ID] = ?
"""


def build_ras_context(
    config: ExtractionConfig,
    purchase_req_no: str,
) -> RASContext:
    """Return a fully-populated :class:`RASContext` for the given PR number."""

    conn_str = config.get_azure_conn_str()
    conn: pyodbc.Connection = connect_with_retry(conn_str, autocommit=True)

    try:
        cursor = conn.cursor()

        # ── master ──
        cursor.execute(_MST_SQL, purchase_req_no)
        mst_row = cursor.fetchone()
        if mst_row is None:
            raise ValueError(
                f"No purchase_req_mst row for PURCHASE_REQ_NO={purchase_req_no}"
            )

        purchase_req_id: int = mst_row[0]
        supplier_name: Optional[str] = mst_row[2]
        justification: Optional[str] = mst_row[3]
        currency: Optional[str] = mst_row[4]
        enquiry_no: Optional[str] = mst_row[5]
        classification: Optional[str] = mst_row[6]
        department: Optional[str] = mst_row[7]
        negotiated_by: Optional[str] = mst_row[8]
        address: Optional[str] = mst_row[9]
        contract_no: Optional[str] = mst_row[10]
        order_no: Optional[str] = mst_row[11]
        purchase_value: Optional[Decimal] = _dec(mst_row[12])

        # ── detail lines ──
        cursor.execute(_DTL_SQL, purchase_req_id)
        dtl_rows = cursor.fetchall()

        line_items: list[LineItemContext] = []
        for r in dtl_rows:
            line_items.append(
                LineItemContext(
                    purchase_dtl_id=r[0],
                    purchase_req_id=r[1],
                    item_no=r[2],
                    quantity=_dec(r[3]),
                    item_type=r[4],
                    item_description=r[5],
                    unit_price=_dec(r[6]),
                    uom=r[7],
                    discount=_dec(r[8]),
                    req_value=_dec(r[9]),
                    currency=r[10],
                    delivery_date=r[11],
                    supplier_name=r[12] or supplier_name,
                    payment_details=r[13],
                    original_value=_dec(r[14]),
                    initial_offer=_dec(r[15]),
                    negotiation=_dec(r[16]),
                    comments=r[17] or None,
                    prepayment=r[18] or None,
                    item_code=r[19] or None,
                )
            )

        # ── BI view (RAS-level enrichment, best-effort) ──
        category: Optional[str] = None
        sub_category: Optional[str] = None
        site_country: Optional[str] = None
        site_region: Optional[str] = None
        division: Optional[str] = None
        site: Optional[str] = None
        requisition_type: Optional[str] = None
        l5: Optional[str] = None
        l6: Optional[str] = None
        l7: Optional[str] = None
        l8: Optional[str] = None
        parent_supplier: Optional[str] = None
        supplier_type: Optional[str] = None
        supplier_country: Optional[str] = None
        payment_days: Optional[str] = None
        po_date: Optional[str] = None
        category_buyer: Optional[str] = None

        try:
            cursor.execute(_VW_SQL, purchase_req_id)
            vw_row = cursor.fetchone()
            if vw_row is not None:
                category = vw_row[0]
                sub_category = vw_row[5] or vw_row[1]
                site_country = vw_row[2]
                site_region = vw_row[3]
                division = vw_row[4]
                site = vw_row[11]
                requisition_type = vw_row[12]
                l5 = vw_row[13]
                l6 = vw_row[14]
                l7 = vw_row[15]
                l8 = vw_row[16]
                parent_supplier = vw_row[17]
                supplier_type = vw_row[18]
                supplier_country = vw_row[19]
                payment_days = vw_row[20]
                po_date = vw_row[21]
                category_buyer = vw_row[22]
        except Exception:
            logger.warning(
                "BI-view enrichment failed for REQ_ID={}; continuing without it",
                purchase_req_id,
            )

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
            l5=l5,
            l6=l6,
            l7=l7,
            l8=l8,
            line_items=line_items,
        )

        logger.info(
            "Built RAS context: PR={} | {} line items | category={}",
            purchase_req_no,
            len(line_items),
            category,
        )
        return ctx

    finally:
        conn.close()
