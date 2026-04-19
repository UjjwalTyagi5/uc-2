"""Inspect Azure SQL schema + sample data for quotation extraction context.

Usage:
    python inspect_db_schema.py

Reads credentials from .env and writes output to db_schema_info.txt.
"""

import os
import pathlib
import sys

import pyodbc
from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).parent / ".env")

SCHEMA = "ras_procurement"

TABLES = [
    "purchase_req_mst",
    "purchase_req_detail",
]

RASMASTER_TABLES = [
    "vw_get_ras_data_for_bidashboard",
]

# Pick 2-3 RAS records from 2025 that have detail rows
_SAMPLE_RAS_SQL = f"""
SELECT TOP 5 prm.PURCHASE_REQ_ID, prm.PURCHASE_REQ_NO
  FROM [{SCHEMA}].[purchase_req_mst] prm
 WHERE prm.C_DATETIME >= '2026-01-01'
   AND EXISTS (
       SELECT 1 FROM [{SCHEMA}].[purchase_req_detail] prd
        WHERE prd.PURCHASE_REQ_ID = prm.PURCHASE_REQ_ID
   )
 ORDER BY prm.C_DATETIME DESC
"""


RASMASTER_DB = "rasmaster"
RASMASTER_SCHEMA = "dbo"


def _get_conn_str(db_override: str = None) -> str:
    server = os.getenv("AZURE_SERVER", "")
    db = db_override or os.getenv("AZURE_DB", "")
    user = os.getenv("AZURE_USER", "")
    pwd = os.getenv("AZURE_PASS", "")

    drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
    driver = drivers[-1] if drivers else "ODBC Driver 18 for SQL Server"

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={pwd};"
        f"Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )


def _write_section(f, title: str) -> None:
    f.write(f"\n{'=' * 80}\n")
    f.write(f"  {title}\n")
    f.write(f"{'=' * 80}\n\n")


def _dump_columns(cursor, schema: str, name: str, f, obj_type: str = "TABLE") -> None:
    _write_section(f, f"DDL: [{schema}].[{name}]  ({obj_type})")

    cursor.execute(
        """
        SELECT c.COLUMN_NAME,
               c.DATA_TYPE,
               c.CHARACTER_MAXIMUM_LENGTH,
               c.NUMERIC_PRECISION,
               c.NUMERIC_SCALE,
               c.IS_NULLABLE,
               c.COLUMN_DEFAULT
          FROM INFORMATION_SCHEMA.COLUMNS c
         WHERE c.TABLE_SCHEMA = ?
           AND c.TABLE_NAME = ?
         ORDER BY c.ORDINAL_POSITION
        """,
        schema,
        name,
    )
    rows = cursor.fetchall()

    if not rows:
        f.write(f"  (no columns found — check schema/name)\n")
        return

    f.write(f"{'Column':<40} {'Type':<25} {'Nullable':<10} {'Default'}\n")
    f.write(f"{'-'*40} {'-'*25} {'-'*10} {'-'*30}\n")
    for r in rows:
        col_name = r[0]
        dtype = r[1]
        char_len = r[2]
        num_prec = r[3]
        num_scale = r[4]
        nullable = r[5]
        default = r[6] or ""

        if char_len is not None and char_len > 0:
            type_str = f"{dtype}({char_len})"
        elif char_len == -1:
            type_str = f"{dtype}(max)"
        elif num_prec is not None and dtype not in ("int", "bigint", "smallint", "tinyint", "bit", "float", "real"):
            type_str = f"{dtype}({num_prec},{num_scale})"
        else:
            type_str = dtype

        f.write(f"{col_name:<40} {type_str:<25} {nullable:<10} {default}\n")

    # Primary key / unique constraints
    cursor.execute(
        """
        SELECT kcu.COLUMN_NAME, tc.CONSTRAINT_TYPE
          FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
          JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
           AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
         WHERE tc.TABLE_SCHEMA = ?
           AND tc.TABLE_NAME = ?
         ORDER BY tc.CONSTRAINT_TYPE, kcu.ORDINAL_POSITION
        """,
        schema,
        name,
    )
    constraints = cursor.fetchall()
    if constraints:
        f.write(f"\n  Constraints:\n")
        for c in constraints:
            f.write(f"    {c[1]}: {c[0]}\n")


def _dump_sample_rows(cursor, schema: str, table: str, where: str, f, limit: int = 5) -> None:
    sql = f"SELECT TOP {limit} * FROM [{schema}].[{table}] WHERE {where}"
    try:
        cursor.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        if not rows:
            f.write(f"  (no rows matched)\n")
            return

        for idx, row in enumerate(rows):
            f.write(f"\n  --- Row {idx + 1} ---\n")
            for col, val in zip(cols, row):
                val_str = str(val) if val is not None else "NULL"
                if len(val_str) > 200:
                    val_str = val_str[:200] + "... [truncated]"
                f.write(f"    {col:<40} = {val_str}\n")
    except Exception as e:
        f.write(f"  (query failed: {e})\n")


def main() -> None:
    # Main DB (ras-procurement-benchmark) for purchase tables
    conn_str = _get_conn_str()
    print(f"Connecting to Azure SQL ({os.getenv('AZURE_DB')})...")
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    print("Connected to main DB.")

    # rasmaster DB for vw_get_ras_data_for_bidashboard (table, not view)
    rm_conn_str = _get_conn_str(db_override=RASMASTER_DB)
    print(f"Connecting to Azure SQL ({RASMASTER_DB})...")
    rm_conn = pyodbc.connect(rm_conn_str, autocommit=True)
    rm_cursor = rm_conn.cursor()
    print("Connected to rasmaster DB.")

    out_path = pathlib.Path(__file__).parent / "db_schema_info.txt"

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("Database Schema Inspection\n")
        f.write(f"Generated for quotation_extraction context enrichment\n")
        f.write(f"Main DB: {os.getenv('AZURE_SERVER')} / {os.getenv('AZURE_DB')}\n")
        f.write(f"Rasmaster DB: {os.getenv('AZURE_SERVER')} / {RASMASTER_DB}\n\n")

        # 1. DDL for tables (main DB)
        for tbl in TABLES:
            _dump_columns(cursor, SCHEMA, tbl, f, "TABLE")

        # 2. DDL for vw_get table (rasmaster DB, dbo schema)
        for tbl in RASMASTER_TABLES:
            _dump_columns(rm_cursor, RASMASTER_SCHEMA, tbl, f, "TABLE (rasmaster DB)")

        # 3. Sample RAS records
        _write_section(f, "SAMPLE DATA: 5 RAS records from 2026")

        cursor.execute(_SAMPLE_RAS_SQL)
        sample_ras = cursor.fetchall()

        if not sample_ras:
            f.write("  No 2026 RAS records found.\n")
        else:
            for req_id, req_no in sample_ras:
                f.write(f"\n{'~' * 70}\n")
                f.write(f"  RAS: {req_no}  (PURCHASE_REQ_ID = {req_id})\n")
                f.write(f"{'~' * 70}\n")

                f.write(f"\n  >> purchase_req_mst (main DB):\n")
                _dump_sample_rows(
                    cursor, SCHEMA, "purchase_req_mst",
                    f"PURCHASE_REQ_ID = {req_id}", f, limit=1,
                )

                f.write(f"\n  >> purchase_req_detail (main DB):\n")
                _dump_sample_rows(
                    cursor, SCHEMA, "purchase_req_detail",
                    f"PURCHASE_REQ_ID = {req_id}", f, limit=20,
                )

                f.write(f"\n  >> vw_get_ras_data_for_bidashboard (rasmaster DB):\n")
                _dump_sample_rows(
                    rm_cursor, RASMASTER_SCHEMA, "vw_get_ras_data_for_bidashboard",
                    f"PURCHASE_REQ_ID = {req_id}", f, limit=5,
                )

                # Also check if this RAS has any attachments classified
                f.write(f"\n  >> AttachmentClassification (main DB):\n")
                _dump_sample_rows(
                    cursor, SCHEMA, "AttachmentClassification",
                    f"purchase_req_no_fk = '{req_no}'", f, limit=10,
                )

                f.write(f"\n  >> EmbeddedAttachmentClassification (if any):\n")
                try:
                    cursor.execute(f"""
                        SELECT TOP 5 ec.*
                          FROM [{SCHEMA}].[EmbeddedAttachmentClassification] ec
                          JOIN [{SCHEMA}].[AttachmentClassification] ac
                            ON ec.attachment_classify_uuid_pk = ac.attachment_classify_uuid_pk
                         WHERE ac.purchase_req_no_fk = ?
                    """, req_no)
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    if not rows:
                        f.write("  (none)\n")
                    else:
                        for idx, row in enumerate(rows):
                            f.write(f"\n  --- Embedded {idx + 1} ---\n")
                            for col, val in zip(cols, row):
                                val_str = str(val) if val is not None else "NULL"
                                f.write(f"    {col:<40} = {val_str}\n")
                except Exception as e:
                    f.write(f"  (query failed: {e})\n")

    conn.close()
    rm_conn.close()
    print(f"\nDone! Output written to: {out_path}")
    print(f"File size: {out_path.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
