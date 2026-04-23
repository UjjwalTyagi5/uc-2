"""Debug and patch cpi_inflation_pct for a single benchmark_result row.

Usage:
    python debug_cpi_update.py <extracted_item_uuid_fk>

Traces every step of the CPI calculation and updates the record if a value
can be computed.  All intermediate values are printed so you can see exactly
where it breaks.

Example:
    python debug_cpi_update.py 550e8400-e29b-41d4-a716-446655440000
"""

from __future__ import annotations

import sys
from decimal import Decimal
from typing import Optional

import pyodbc
from dotenv import load_dotenv

load_dotenv()

from utils.config import AppConfig
from utils.cpi_inflation import compute_cpi_inflation_pct
from db.tables import AzureTables

# ── SQL ──────────────────────────────────────────────────────────────────────

_FETCH_BENCHMARK_SQL = f"""
    SELECT
        br.[purchase_dtl_id],
        br.[extracted_item_uuid_fk],
        br.[low_hist_item_fk],
        br.[last_hist_item_fk],
        br.[cpi_inflation_pct]
    FROM {AzureTables.BENCHMARK_RESULT} br
    WHERE br.[extracted_item_uuid_fk] = ?
"""

_FETCH_ITEM_INFO_SQL = f"""
    SELECT
        qi.[purchase_dtl_id],
        qi.[supplier_country],
        prd.[PURCHASE_REQ_ID],
        prm.[PURCHASE_REQ_NO],
        prm.[C_DATETIME]          AS pr_c_datetime
    FROM {AzureTables.QUOTATION_EXTRACTED_ITEMS} qi
    LEFT JOIN {AzureTables.PURCHASE_REQ_DETAIL} prd
      ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
    LEFT JOIN {AzureTables.PURCHASE_REQ_MST} prm
      ON prd.[PURCHASE_REQ_ID] = prm.[PURCHASE_REQ_ID]
    WHERE qi.[extracted_item_uuid_pk] = ?
"""

_UPDATE_CPI_SQL = f"""
    UPDATE {AzureTables.BENCHMARK_RESULT}
    SET    [cpi_inflation_pct] = ?,
           [updated_at]        = SYSUTCDATETIME()
    WHERE  [extracted_item_uuid_fk] = ?
"""


def _connect(conn_str: str) -> pyodbc.Connection:
    return pyodbc.connect(conn_str, autocommit=False)


def _fetch_one(conn: pyodbc.Connection, sql: str, *params):
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return cursor.fetchone()


def main(uuid: str) -> None:
    config = AppConfig()
    conn_str = config.get_azure_conn_str()
    conn = _connect(conn_str)

    print(f"\n{'='*60}")
    print(f"  CPI debug for extracted_item_uuid_fk = {uuid}")
    print(f"{'='*60}\n")

    # ── Step 1: fetch benchmark_result row ───────────────────────────────────
    br = _fetch_one(conn, _FETCH_BENCHMARK_SQL, uuid)
    if br is None:
        print(f"[ERROR] No benchmark_result row found for uuid={uuid!r}")
        return

    purchase_dtl_id     = br[0]
    low_hist_item_fk    = str(br[2]) if br[2] is not None else None
    cpi_current         = br[4]

    print(f"[STEP 1] benchmark_result row found:")
    print(f"         purchase_dtl_id   = {purchase_dtl_id}")
    print(f"         low_hist_item_fk  = {low_hist_item_fk}")
    print(f"         cpi_inflation_pct = {cpi_current}  (current value)")

    if low_hist_item_fk is None:
        print("\n[STOP] low_hist_item_fk is NULL — no historical item to compare against.")
        print("       CPI cannot be computed without a low-hist reference.")
        return

    # ── Step 2: current item pr_c_datetime ───────────────────────────────────
    curr = _fetch_one(conn, _FETCH_ITEM_INFO_SQL, uuid)
    print(f"\n[STEP 2] Current item info (uuid={uuid}):")
    if curr is None:
        print(f"[ERROR] No quotation_extracted_items row found for uuid={uuid!r}")
        return
    print(f"         purchase_dtl_id = {curr[0]}")
    print(f"         supplier_country= {curr[1]}")
    print(f"         PURCHASE_REQ_ID = {curr[2]}")
    print(f"         PURCHASE_REQ_NO = {curr[3]}")
    print(f"         pr_c_datetime   = {curr[4]}")

    current_pr_dt = curr[4]
    if current_pr_dt is None:
        print("\n[STOP] Current item pr_c_datetime is NULL.")
        print("       Check purchase_req_detail and purchase_req_mst for this dtl_id.")
        return

    end_year = current_pr_dt.year if hasattr(current_pr_dt, "year") else None
    if end_year is None:
        print(f"\n[STOP] Could not read year from pr_c_datetime={current_pr_dt!r}")
        return

    # ── Step 3: low_hist item pr_c_datetime and supplier_country ─────────────
    hist = _fetch_one(conn, _FETCH_ITEM_INFO_SQL, low_hist_item_fk)
    print(f"\n[STEP 3] Low-hist item info (uuid={low_hist_item_fk}):")
    if hist is None:
        print(f"[ERROR] No quotation_extracted_items row found for low_hist uuid={low_hist_item_fk!r}")
        return
    print(f"         purchase_dtl_id = {hist[0]}")
    print(f"         supplier_country= {hist[1]}")
    print(f"         PURCHASE_REQ_ID = {hist[2]}")
    print(f"         PURCHASE_REQ_NO = {hist[3]}")
    print(f"         pr_c_datetime   = {hist[4]}")

    low_pr_dt       = hist[4]
    supplier_country = hist[1]

    if low_pr_dt is None:
        print("\n[STOP] Low-hist item pr_c_datetime is NULL.")
        print("       Check purchase_req_detail and purchase_req_mst for this dtl_id.")
        return

    start_year = low_pr_dt.year if hasattr(low_pr_dt, "year") else None
    if start_year is None:
        print(f"\n[STOP] Could not read year from low_hist pr_c_datetime={low_pr_dt!r}")
        return

    # ── Step 4: CPI calc ─────────────────────────────────────────────────────
    print(f"\n[STEP 4] CPI inputs:")
    print(f"         country      = {supplier_country!r}")
    print(f"         start_year   = {start_year}  (low_hist PR)")
    print(f"         end_year     = {end_year}    (current PR)")

    if start_year >= end_year:
        print(f"\n[STOP] start_year ({start_year}) >= end_year ({end_year}) — same year, no CPI span.")
        return

    cpi_pct = compute_cpi_inflation_pct(supplier_country, start_year, end_year)
    print(f"\n[STEP 5] compute_cpi_inflation_pct result = {cpi_pct}")

    if cpi_pct is None:
        print("         CPI returned None — see warnings above (missing country / no World Bank data).")
        return

    # ── Step 5: update ───────────────────────────────────────────────────────
    cpi_decimal = Decimal(str(cpi_pct))
    cursor = conn.cursor()
    cursor.execute(_UPDATE_CPI_SQL, float(cpi_decimal), uuid)
    rows_affected = cursor.rowcount
    conn.commit()

    print(f"\n[STEP 6] UPDATE benchmark_result SET cpi_inflation_pct = {cpi_pct}")
    print(f"         Rows affected: {rows_affected}")
    if rows_affected == 1:
        print("         SUCCESS — cpi_inflation_pct updated.")
    else:
        print("         WARNING — unexpected rowcount, check manually.")

    conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <extracted_item_uuid_fk>")
        print("       Pass the extracted_item_uuid_fk from benchmark_result.")
        sys.exit(1)
    main(sys.argv[1].strip())
