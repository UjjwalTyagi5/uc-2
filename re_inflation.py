"""
re_inflation.py — Backfill inflation_pct and cpi_inflation_pct columns

Use this when:
  • You already have benchmark_result rows calculated, but want to recalculate
    inflation with the new logic (using purchase_req_mst.C_DATETIME instead of quotation_date)
  • You added new columns (inflation_pct_last, cpi_inflation_pct_last) and want to
    populate them on existing rows without re-running full benchmarking

Usage:
    python re_inflation.py                              # all stage-8 PRs with benchmarks
    python re_inflation.py R_153634/2021                # specific PR(s)
    python re_inflation.py --limit 10                   # small test run
    python re_inflation.py --dry-run                    # log only, no UPDATE
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import types
from decimal import Decimal
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# ── DB connection (Azure SQL — target) ───────────────────────────────────────
TGT_CS = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={os.getenv('AZURE_SERVER', '')};"
    f"DATABASE={os.getenv('AZURE_DB', '')};"
    f"UID={os.getenv('AZURE_USER', '')};"
    f"PWD={os.getenv('AZURE_PASS', '')};"
    "TrustServerCertificate=yes;"
)

# ── Azure OpenAI ─────────────────────────────────────────────────────────────
AZURE_ENDPOINT     = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY      = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_API_VERSION  = os.getenv("AZURE_OPENAI_API_VERSION",      "2024-10-21")
AZURE_LLM_DEPLOY   = os.getenv("AZURE_OPENAI_DEPLOYMENT",        "gpt-4o")

# ── LLM timeouts ──────────────────────────────────────────────────────────────
LLM_CALL_TIMEOUT_S = int(os.getenv("LLM_CALL_TIMEOUT_S", "300"))
LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
LLM_RETRY_COOLDOWN = int(os.getenv("LLM_RETRY_COOLDOWN", "180"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("re_inflation")

# ── Stub agentcore.* (same pattern as re_benchmark.py) ───────────────────────

def _stub_agentcore() -> None:
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __init_subclass__(cls, **kw): pass

    mod_names = [
        "agentcore", "agentcore.custom", "agentcore.io",
        "agentcore.schema", "agentcore.schema.data", "agentcore.schema.message",
        "agentcore.services", "agentcore.services.deps",
    ]
    for name in mod_names:
        mod = types.ModuleType(name)
        mod.__path__ = []
        mod.__package__ = name
        sys.modules.setdefault(name, mod)

    sys.modules["agentcore.custom"].Node = _Stub
    for _name in ("BoolInput", "DataInput", "HandleInput", "IntInput", "MessageInput", "MessageTextInput", "MultilineInput", "Output"):
        setattr(sys.modules["agentcore.io"], _name, _Stub)
    sys.modules["agentcore.schema.data"].Data = _Stub
    sys.modules["agentcore.schema.message"].Message = _Stub

_stub_agentcore()

# Load pipeline_stage_123_v3 to reuse inflation functions
import importlib.util as _ilu
_HERE = os.path.dirname(os.path.abspath(__file__))
_PIPELINE_PATH = os.path.join(_HERE, "agentcore_components", "pipeline_stage_123_v3.py")
_spec = _ilu.spec_from_file_location("pipeline_stage_123_v3_standalone", _PIPELINE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load module spec from {_PIPELINE_PATH}")
_P = _ilu.module_from_spec(_spec)
sys.modules[_spec.name] = _P
_spec.loader.exec_module(_P)

# Stop stack dumper thread (not needed for sequential backfill)
import threading as _threading
if hasattr(_P, '_DUMPER_STOP'):
    _P._DUMPER_STOP = True
for _t in _threading.enumerate():
    if _t.name == 'dumper' and _t.is_alive():
        try:
            _P._DUMPER_STOP = True
            _t.join(timeout=1)
        except:
            pass

_connect                   = _P._connect
_compute_cpi_pct           = _P._compute_cpi_pct
_get_pr_master_date_for_dtl_id = _P._get_pr_master_date_for_dtl_id


# ── Helpers ──────────────────────────────────────────────────────────────────

def _estimate_inflation(llm, item_name: str, item_category: str, supplier_country: str, ref_year: int, current_year: int) -> float:
    """Estimate inflation % using LLM."""
    prompt = f"""Estimate the inflation percentage for this item from {ref_year} to {current_year}:
- Item: {item_name}
- Category: {item_category or 'N/A'}
- Supplier Country: {supplier_country}

Return ONLY a number (0-100) representing the estimated inflation percentage. No explanation."""

    try:
        msg = llm.invoke(prompt)
        result = msg.content.strip()
        return float(result)
    except Exception as exc:
        logger.warning(f"LLM inflation estimate failed: {exc}")
        return None


def _compute_cpi_pct_with_retry(country: str, ref_year: int, current_year: int, max_retries: int = 3, cooldown_s: int = 60) -> float:
    """Compute CPI % with retry logic on timeout."""
    import time
    for attempt in range(max_retries):
        try:
            result = _compute_cpi_pct(country, ref_year, current_year)
            if result is not None:
                return result
        except Exception as exc:
            if attempt < max_retries - 1:
                logger.warning(f"CPI API failed for {country} {ref_year}-{current_year} (attempt {attempt + 1}/{max_retries}), retrying in {cooldown_s}s: {exc}")
                time.sleep(cooldown_s)
            else:
                logger.warning(f"CPI API failed for {country} {ref_year}-{current_year} after {max_retries} retries: {exc}")
    return None


def _get_benchmarked_prs(tgt_cs: str) -> list[str]:
    """Get all PRs at stage 8 that have benchmark_result rows."""
    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        cur.execute("""
            WITH filtered AS (
              SELECT DISTINCT rt.[purchase_req_no], rt.[updated_at]
                FROM [ras_procurement].[ras_tracker] rt
               WHERE rt.[current_stage_fk] = 8
                 AND EXISTS (
                     SELECT 1
                       FROM [ras_procurement].[benchmark_result] br
                      WHERE br.[purchase_dtl_id] IN (
                          SELECT qi.[purchase_dtl_id]
                            FROM [ras_procurement].[quotation_extracted_items] qi
                            JOIN [ras_procurement].[attachment_classification] ac
                              ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
                           WHERE ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
                      )
                 )
            )
            SELECT [purchase_req_no]
              FROM filtered
             ORDER BY [updated_at] DESC
        """)
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _get_benchmark_rows(tgt_cs: str, pr_no: str) -> list[dict]:
    """Get all benchmark_result rows for a PR."""
    conn = _connect(tgt_cs)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT
                br.[purchase_dtl_id],
                low_qi.[purchase_dtl_id] AS [low_hist_dtl_id],
                last_qi.[purchase_dtl_id] AS [last_hist_dtl_id],
                qi.[item_name],
                qi.[item_level_1],
                qi.[item_level_2],
                qi.[item_level_3],
                prd.[C_DATETIME],
                qi.[supplier_country],
                qi.[quotation_date]
              FROM [ras_procurement].[benchmark_result] br
              LEFT JOIN [ras_procurement].[quotation_extracted_items] qi
                ON br.[purchase_dtl_id] = qi.[purchase_dtl_id]
              LEFT JOIN [ras_procurement].[purchase_req_detail] prd
                ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
              LEFT JOIN [ras_procurement].[quotation_extracted_items] low_qi
                ON br.[low_hist_item_fk] = low_qi.[extracted_item_uuid_pk]
              LEFT JOIN [ras_procurement].[quotation_extracted_items] last_qi
                ON br.[last_hist_item_fk] = last_qi.[extracted_item_uuid_pk]
             WHERE qi.[purchase_dtl_id] IN (
                SELECT qi2.[purchase_dtl_id]
                FROM [ras_procurement].[quotation_extracted_items] qi2
                JOIN [ras_procurement].[attachment_classification] ac
                  ON qi2.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
                JOIN [ras_procurement].[ras_tracker] rt
                  ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
                WHERE rt.[purchase_req_no] = ?
             )
        """, pr_no)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _recalculate_inflation(llm, tgt_cs: str, row: dict) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Recalculate inflation for low_hist_item and last_hist_item."""
    infl_dec = cpi_dec = None
    infl_dec_last = cpi_dec_last = None

    dtl_id = row.get("purchase_dtl_id")
    low_dtl_id = row.get("low_hist_dtl_id")
    last_dtl_id = row.get("last_hist_dtl_id")
    current_created = row.get("C_DATETIME")
    current_year = current_created.year if current_created and hasattr(current_created, "year") else None
    supplier_country = row.get("supplier_country")

    item_category = " > ".join(str(row[f] or "") for f in ["item_level_1", "item_level_2", "item_level_3"] if row.get(f))
    item_name = row.get("item_name")

    if not current_year or not supplier_country:
        logger.warning(f"dtl_id={dtl_id}: missing current_year or supplier_country, skipping inflation")
        return infl_dec, cpi_dec, infl_dec_last, cpi_dec_last

    # Inflation for low_hist_item
    if low_dtl_id:
        try:
            ref_dt = _get_pr_master_date_for_dtl_id(tgt_cs, low_dtl_id)
            ref_year = ref_dt.year if ref_dt and hasattr(ref_dt, "year") else None

            if ref_year and current_year and ref_year < current_year:
                infl_raw = _estimate_inflation(
                    llm, item_name, item_category or None,
                    supplier_country, ref_year, current_year,
                )
                if infl_raw is not None:
                    infl_dec = Decimal(str(infl_raw))

                cpi_raw = _compute_cpi_pct_with_retry(supplier_country, ref_year, current_year)
                if cpi_raw is not None:
                    cpi_dec = Decimal(str(cpi_raw))

                logger.info(f"dtl_id={dtl_id}: low inflation={infl_dec}, cpi={cpi_dec} ({ref_year}-{current_year})")
        except Exception as exc:
            logger.warning(f"dtl_id={dtl_id}: failed to calculate inflation for low_hist_item: {exc}")

    # Inflation for last_hist_item
    if last_dtl_id:
        try:
            ref_dt_last = _get_pr_master_date_for_dtl_id(tgt_cs, last_dtl_id)
            ref_year_last = ref_dt_last.year if ref_dt_last and hasattr(ref_dt_last, "year") else None

            if ref_year_last and current_year and ref_year_last < current_year:
                infl_raw_last = _estimate_inflation_via_llm(
                    llm, item_name, item_category or None,
                    supplier_country, ref_year_last, current_year,
                )
                if infl_raw_last is not None:
                    infl_dec_last = Decimal(str(infl_raw_last))

                cpi_raw_last = _compute_cpi_pct_with_retry(supplier_country, ref_year_last, current_year)
                if cpi_raw_last is not None:
                    cpi_dec_last = Decimal(str(cpi_raw_last))

                logger.info(f"dtl_id={dtl_id}: last inflation={infl_dec_last}, cpi={cpi_dec_last} ({ref_year_last}-{current_year})")
        except Exception as exc:
            logger.warning(f"dtl_id={dtl_id}: failed to calculate inflation for last_hist_item: {exc}")

    return infl_dec, cpi_dec, infl_dec_last, cpi_dec_last


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill inflation columns on benchmark_result rows")
    parser.add_argument("pr_nos", nargs="*", help="Specific PR(s) to backfill (e.g., R_153634/2021)")
    parser.add_argument("--limit", type=int, help="Limit number of PRs to process")
    parser.add_argument("--dry-run", action="store_true", help="Log only, no UPDATE")
    args = parser.parse_args()

    # Build LLM client
    try:
        from langchain_openai import AzureChatOpenAI
    except ImportError:
        logger.error("langchain-openai not installed")
        sys.exit(1)

    llm = AzureChatOpenAI(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=AZURE_API_KEY,
        api_version=AZURE_API_VERSION,
        azure_deployment=AZURE_LLM_DEPLOY,
        request_timeout=float(os.getenv("REQUEST_TIMEOUT_S", "300")),
    )

    # Get PR list
    if args.pr_nos:
        pr_nos = args.pr_nos
        logger.info(f"Processing {len(pr_nos)} specified PR(s)")
    else:
        pr_nos = _get_benchmarked_prs(TGT_CS)
        logger.info(f"Found {len(pr_nos)} benchmarked PR(s)")

    if args.limit:
        pr_nos = pr_nos[:args.limit]
        logger.info(f"Limited to {len(pr_nos)} PR(s)")

    if not pr_nos:
        logger.info("No PRs to process")
        return

    # Process each PR
    results = []
    for pr_idx, pr_no in enumerate(pr_nos, 1):
        logger.info(f"[{pr_idx}/{len(pr_nos)}] Processing PR={pr_no}")

        try:
            rows = _get_benchmark_rows(TGT_CS, pr_no)
            if not rows:
                logger.info(f"  {pr_no}: no benchmark rows found")
                results.append({"pr_no": pr_no, "status": "skipped", "reason": "no_rows", "timestamp": datetime.now().isoformat()})
                continue

            # Update each benchmark row
            updated_count = 0
            conn = _connect(TGT_CS)
            cur = conn.cursor()

            try:
                for row in rows:
                    dtl_id = row.get("purchase_dtl_id")
                    infl_dec, cpi_dec, infl_dec_last, cpi_dec_last = _recalculate_inflation(llm, TGT_CS, row)

                    if not args.dry_run:
                        cur.execute("""
                            UPDATE [ras_procurement].[benchmark_result]
                            SET inflation_pct = ?,
                                cpi_inflation_pct = ?,
                                inflation_pct_last = ?,
                                cpi_inflation_pct_last = ?,
                                updated_at = SYSUTCDATETIME()
                            WHERE purchase_dtl_id = ?
                        """, (infl_dec, cpi_dec, infl_dec_last, cpi_dec_last, dtl_id))
                        updated_count += 1

                if not args.dry_run:
                    conn.commit()
                    logger.info(f"  {pr_no}: updated {updated_count} row(s)")
                else:
                    logger.info(f"  {pr_no}: would update {len(rows)} row(s) [DRY-RUN]")

                results.append({
                    "pr_no": pr_no,
                    "status": "ok",
                    "rows_updated": len(rows),
                    "timestamp": datetime.now().isoformat()
                })
            finally:
                conn.close()

        except Exception as exc:
            logger.error(f"[{pr_no}] Error: {exc}", exc_info=True)
            results.append({
                "pr_no": pr_no,
                "status": "failed",
                "reason": str(exc),
                "timestamp": datetime.now().isoformat()
            })

    # Summary
    ok_count = sum(1 for r in results if r["status"] == "ok")
    total_rows = sum(r.get("rows_updated", 0) for r in results)
    logger.info(f"Completed: {ok_count}/{len(pr_nos)} PRs, {total_rows} row(s) updated")


if __name__ == "__main__":
    main()
