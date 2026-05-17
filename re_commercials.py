"""
re_commercials.py — Backfill the Stage 5b "commercials" columns on already-extracted
quotation rows without re-running ingestion / classification / extraction.

Use this when:
  • You ran the V2 pipeline on N PRs before the commercials migration shipped, and
  • You applied quotation_extracted_items_add_commercials_cols.sql afterward, and
  • You want to populate the 35 new columns (Incoterms, freight, insurance,
    customs/duties, packing & forwarding, installation, taxes, grand total,
    line_total_inclusive, etc.) on those existing rows.

What it does NOT touch:
  • The 40 columns that already exist on each row (supplier_name, line item
    fields, taxonomy, embed_content, critical_attributes, prices, etc.).
  • The classification, embedding, or benchmark tables.
  • Any row whose group has already been processed by a previous run
    (detected via `line_charges_source IS NOT NULL`). Use --reprocess to force.

Usage:
    python re_commercials.py                              # all stage-8 PRs
    python re_commercials.py R_153634/2021                # specific PR(s)
    python re_commercials.py --limit 10 --workers 2       # small dry test
    python re_commercials.py --dry-run                    # log only, no UPDATE
    python re_commercials.py --reprocess R_153634/2021    # force re-run

All config is read from the same .env keys re_benchmark.py uses, plus
BLOB_CONNECTOR_NAME for the quotation blob container.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import logging
import os
import sys
import time
import types
from typing import Any

from dotenv import load_dotenv

load_dotenv()

# ── DB connection (Azure SQL — target) ───────────────────────────────────────
# Traditional password auth from .env (AZURE_USER and AZURE_PASS)
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

# ── Blob (Azure Storage — use endpoint + container or connector name) ────────
BLOB_CONNECTOR_NAME = os.getenv("BLOB_CONNECTOR_NAME", "")
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING", "")  # Full connection string
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "quotations")

# ── Tuning knobs passed straight through to the commercials helpers ──────────
COMMERCIALS_PROMPTS: dict = {
    # mirrors the V2 pipeline defaults
    "ext_max_chars":  int(os.getenv("EXT_MAX_CHARS",  "50000")),
    "ext_max_pages":  int(os.getenv("EXT_MAX_PAGES",  "20")),
    "ext_max_images": int(os.getenv("EXT_MAX_IMAGES", "50")),
    # LLM retry knobs reuse the pipeline defaults
    "llm_max_retries":    int(os.getenv("LLM_MAX_RETRIES",    "3")),
    "llm_retry_cooldown": int(os.getenv("LLM_RETRY_COOLDOWN", "60")),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("re_commercials")

# ── Stub agentcore.* (same pattern as re_benchmark.py) ───────────────────────

def _stub_agentcore() -> None:
    class _Stub:
        def __init__(self, *a, **kw): pass
        def __init_subclass__(cls, **kw): pass

    mod_names = [
        "agentcore", "agentcore.custom", "agentcore.io",
        "agentcore.schema", "agentcore.schema.data", "agentcore.schema.message",
        "agentcore.services", "agentcore.services.deps",
        "agentcore.services.database",
        "agentcore.services.database.models",
        "agentcore.services.database.models.connector_catalogue",
        "agentcore.services.database.models.connector_catalogue.model",
        "agentcore.services.pinecone_service_client",
    ]
    for name in mod_names:
        mod = types.ModuleType(name)
        mod.__path__ = []
        mod.__package__ = name
        sys.modules.setdefault(name, mod)

    sys.modules["agentcore.custom"].Node            = _Stub
    # Cover every Input/Output class any sibling module might reference, so
    # `import agentcore_components.pipeline_stage_123_v2` doesn't blow up on
    # an unrelated peer file (etl_sync, pipeline_stage_58, …).
    for _name in (
        "BoolInput", "DataInput", "HandleInput", "IntInput",
        "MessageInput", "MessageTextInput", "MultilineInput", "Output",
    ):
        setattr(sys.modules["agentcore.io"], _name, _Stub)
    sys.modules["agentcore.schema.data"].Data       = _Stub
    sys.modules["agentcore.schema.message"].Message = _Stub

_stub_agentcore()

# Load pipeline_stage_123_v3 directly by file path so we don't trigger
# agentcore_components/__init__.py which pulls in sibling modules
# (etl_sync, pipeline_stage_58) that reference names not used here.
# V3 includes the commercials extraction (Stage 5b) with the required constants.
import importlib.util as _ilu  # noqa: E402
_HERE = os.path.dirname(os.path.abspath(__file__))
_PIPELINE_PATH = os.path.join(_HERE, "agentcore_components", "pipeline_stage_123_v3.py")
_spec = _ilu.spec_from_file_location("pipeline_stage_123_v3_standalone", _PIPELINE_PATH)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Could not load module spec from {_PIPELINE_PATH}")
_P = _ilu.module_from_spec(_spec)
sys.modules[_spec.name] = _P
_spec.loader.exec_module(_P)

_COMMERCIALS_QUOTE_FIELDS  = _P._COMMERCIALS_QUOTE_FIELDS
_COMMERCIALS_LINE_FIELDS   = _P._COMMERCIALS_LINE_FIELDS
_build_ras_context         = _P._build_ras_context
_connect                   = _P._connect
_download_blob             = _P._download_blob
_enrich_with_commercials   = _P._enrich_with_commercials
_get_blob_config_by_name   = _P._get_blob_config_by_name
_load_document             = _P._load_document
_resolve_quotation_sources = _P._resolve_quotation_sources
DocumentContent            = _P.DocumentContent
RASContext                 = _P.RASContext


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_stage8_prs(tgt_cs: str) -> list[str]:
    """All PRs at stage 8 (pipeline complete). Mirrors re_benchmark.py."""
    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT rt.[purchase_req_no]
              FROM [ras_procurement].[ras_tracker] rt
             WHERE rt.[current_stage_fk] = 8
             ORDER BY rt.[updated_at] DESC
        """)
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _fetch_existing_rows(tgt_cs: str, pr_no: str) -> dict[tuple, list[dict]]:
    """Pull every quotation_extracted_items row for this PR, grouped by
    (attachment_classify_fk, embedded_classify_fk).

    Returns {(att_fk, emb_fk): [row_dict, ...]} — each row_dict has the
    columns needed to feed _enrich_with_commercials and the audit flag we
    use to decide whether to skip the group.
    """
    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT
                qi.[extracted_item_uuid_pk],
                qi.[attachment_classify_fk],
                qi.[embedded_classify_fk],
                qi.[purchase_dtl_id],
                qi.[item_name],
                qi.[quantity],
                qi.[unit],
                qi.[unit_price],
                qi.[total_price],
                qi.[currency],
                qi.[line_charges_source]
              FROM [ras_procurement].[quotation_extracted_items] qi
              JOIN [ras_procurement].[attachment_classification] ac
                ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
              JOIN [ras_procurement].[ras_tracker] rt
                ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
             WHERE rt.[purchase_req_no] = ?
        """, pr_no)
        cols = [c[0] for c in cur.description]
        groups: dict[str, list[dict]] = {}
        for row in cur.fetchall():
            rd = dict(zip(cols, row))
            att = rd["attachment_classify_fk"]
            emb = rd["embedded_classify_fk"]
            # Priority: if embedded_classify_fk exists, use it; otherwise use attachment_classify_fk
            source_id = str(emb) if emb else str(att)
            groups.setdefault(source_id, []).append(rd)
        return groups
    finally:
        conn.close()


def _build_minimal_context(pr_no: str, rows: list[dict], tgt_cs: str) -> RASContext:
    """Build the bare minimum RASContext the commercials prompt needs.

    Prefers the full _build_ras_context (which reads on-prem mst/dtl tables),
    but falls back to a stub built from the existing extracted rows so the
    backfill keeps working even when on-prem data has rotated. The commercials
    prompt only consumes purchase_req_no, currency, site_country from the
    context — no line_items field is referenced.
    """
    try:
        ctx = _build_ras_context(tgt_cs, pr_no)
        if ctx is not None:
            return ctx
    except Exception as exc:
        logger.warning("[%s] _build_ras_context failed (%s); using stub context", pr_no, exc)

    # Stub fallback. currency = whatever the existing rows say.
    currencies = {r["currency"] for r in rows if r.get("currency")}
    currency = next(iter(currencies)) if currencies else None
    return RASContext(
        purchase_req_no=pr_no, purchase_req_id=0,
        supplier_name=None, justification=None, currency=currency,
        site_country=None, line_items=[],
    )


# ── 35 columns we own — built from the constants exposed by the pipeline file
_NEW_COLS: tuple = tuple(_COMMERCIALS_QUOTE_FIELDS) + tuple(_COMMERCIALS_LINE_FIELDS)
assert len(_NEW_COLS) == 35, f"Expected 35 new columns, found {len(_NEW_COLS)}"
_SET_CLAUSE   = ",\n       ".join(f"[{c}] = ?" for c in _NEW_COLS)
_UPDATE_SQL   = (
    f"UPDATE [ras_procurement].[quotation_extracted_items]\n"
    f"   SET {_SET_CLAUSE}\n"
    f" WHERE [extracted_item_uuid_pk] = ?"
)


def _stamp_default_charges_source_if_empty(items: list[dict]) -> None:
    """If the LLM call succeeded but emitted no commercial info at all, stamp
    line_charges_source = 'none' so the next backfill run can skip this group
    instead of re-paying LLM cost on a quote that genuinely has no charges.

    Only stamps when EVERY new column on the row is None — preserves any
    intentional partial result the LLM produced.
    """
    for it in items:
        if it.get("line_charges_source"):
            continue
        all_null = all(it.get(c) is None for c in _NEW_COLS)
        if all_null:
            it["line_charges_source"] = "none"


def _update_rows(tgt_cs: str, items: list[dict], *, dry_run: bool) -> int:
    """UPDATE only the 35 new columns on each row, keyed by
    extracted_item_uuid_pk. Returns rows written (or pretended-to-write under
    --dry-run)."""
    rows = [it for it in items if it.get("extracted_item_uuid_pk")]
    if not rows:
        return 0
    params: list[tuple] = []
    for it in rows:
        values = tuple(it.get(c) for c in _NEW_COLS) + (str(it["extracted_item_uuid_pk"]),)
        params.append(values)
    if dry_run:
        return len(params)

    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        try:
            cur.fast_executemany = True   # pyodbc speedup; harmless if unsupported
        except Exception:
            pass
        cur.executemany(_UPDATE_SQL, params)
        conn.commit()
    except Exception:
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        conn.close()
    return len(params)


# ── Per-quotation worker ─────────────────────────────────────────────────────

def _process_quotation_group(
    *, llm, tgt_cs: str, blob_cfg: dict, pr_no: str,
    source_id: str, rows: list[dict], src_blob_path: str,
    reprocess: bool, dry_run: bool,
) -> dict:
    """Backfill one quotation source (one blob → one LLM call → UPDATE N rows).

    Returns a result dict for the summary table.
    """
    result = {
        "pr_no": pr_no, "source_id": source_id,
        "rows": len(rows), "updated": 0, "status": "ok", "reason": "",
    }

    # 1. Skip-if-already-processed
    if not reprocess:
        if all(r.get("line_charges_source") for r in rows):
            result.update(status="skipped", reason="already_processed")
            return result

    # 2. Build items list the prompt + allocator expect
    ctx = _build_minimal_context(pr_no, rows, tgt_cs)
    items: list[dict] = []
    for r in rows:
        items.append({
            "extracted_item_uuid_pk": r["extracted_item_uuid_pk"],
            "purchase_dtl_id":        r["purchase_dtl_id"],
            "item_name":              r.get("item_name"),
            "quantity":               r.get("quantity"),
            "unit":                   r.get("unit"),
            "unit_price":             r.get("unit_price"),
            "total_price":            r.get("total_price"),
            "currency":               r.get("currency"),
        })

    # 3. Download + load the quotation document
    try:
        file_bytes = _download_blob(src_blob_path, blob_cfg)
    except Exception as exc:
        result.update(status="failed", reason=f"blob_download: {exc}")
        return result

    import os as _os
    filename = _os.path.basename(src_blob_path)
    try:
        doc: DocumentContent = _load_document(
            file_bytes, filename,
            max_pages=int(COMMERCIALS_PROMPTS.get("ext_max_pages", 20)),
        )
    except Exception as exc:
        result.update(status="failed", reason=f"load_document: {exc}")
        return result

    # 4. Fire the commercials LLM call — fail-soft inside _enrich_with_commercials,
    #    so an exception here is unexpected; let it propagate to the caller.
    _enrich_with_commercials(llm, ctx, doc, items, prompts=COMMERCIALS_PROMPTS)

    # 5. Ensure the audit flag is set so re-runs can skip this group
    _stamp_default_charges_source_if_empty(items)

    # 6. UPDATE
    try:
        updated = _update_rows(tgt_cs, items, dry_run=dry_run)
        result["updated"] = updated
        if dry_run:
            result["status"] = "dry_run"
    except Exception as exc:
        result.update(status="failed", reason=f"db_update: {exc}")
    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def _format_summary(results: list[dict]) -> str:
    by_status: dict = {}
    for r in results:
        by_status.setdefault(r["status"], 0)
        by_status[r["status"]] += 1
    parts = [f"{k}={v}" for k, v in sorted(by_status.items())]
    rows_updated = sum(r["updated"] for r in results)
    return f"groups: {len(results)} ({', '.join(parts)}) — rows updated: {rows_updated}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "pr_nos", nargs="*",
        help="Specific PR numbers. Omit to process all stage-8 PRs.",
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Parallel quotation groups (default 1 for sequential processing; set >1 for parallel).",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Process only the first N quotation groups (across all PRs). 0 = no limit.",
    )
    parser.add_argument(
        "--reprocess", action="store_true",
        help="Re-run the commercials LLM even on rows whose line_charges_source is already set.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run everything (DB read + blob download + LLM call) but skip the UPDATE.",
    )
    args = parser.parse_args()

    # ── Build the LLM client ─────────────────────────────────────────────────
    try:
        from langchain_openai import AzureChatOpenAI
    except ImportError:
        logger.error("langchain-openai not installed — run: pip install langchain-openai")
        sys.exit(1)
    llm = AzureChatOpenAI(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=AZURE_API_KEY,
        api_version=AZURE_API_VERSION,
        azure_deployment=AZURE_LLM_DEPLOY,
    )

    # ── Blob config ──────────────────────────────────────────────────────────
    # Two options:
    # 1. Direct endpoint + container (recommended with Azure CLI)
    # 2. Connector name (legacy, from AgentCore)

    blob_cfg = None
    if BLOB_CONNECTION_STRING:
        # Direct connection string auth
        logger.info("Using blob storage connection string")
        try:
            from azure.storage.blob import BlobServiceClient
            blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
            blob_cfg = {
                "container_name": BLOB_CONTAINER,
                "blob_client": blob_client,
            }
            logger.info("Connected to blob storage: %s", BLOB_CONTAINER)
        except Exception as exc:
            logger.error("Failed to initialize blob client: %s", exc)
            sys.exit(1)
    elif BLOB_CONNECTOR_NAME:
        # Legacy: connector name lookup
        logger.info("Using blob connector: %s", BLOB_CONNECTOR_NAME)
        try:
            blob_cfg = _get_blob_config_by_name(BLOB_CONNECTOR_NAME)
        except Exception as exc:
            logger.error("Could not resolve blob connector %r: %s", BLOB_CONNECTOR_NAME, exc)
            sys.exit(1)
    else:
        logger.error("Either BLOB_ENDPOINT or BLOB_CONNECTOR_NAME must be set")
        sys.exit(1)

    # ── PR list & Excel tracking ────────────────────────────────────────────────
    try:
        import pandas as pd
        from datetime import datetime
        excel_file = "re_commercials_progress.xlsx"
    except ImportError:
        logger.error("pandas required for progress tracking — pip install pandas openpyxl")
        sys.exit(1)

    if args.pr_nos:
        pr_nos = args.pr_nos
        logger.info("Processing %d PR(s) supplied on the command line", len(pr_nos))
    else:
        pr_nos = _get_stage8_prs(TGT_CS)
        logger.info("Found %d stage-8 PR(s) in DB", len(pr_nos))

    if not pr_nos:
        logger.info("Nothing to do.")
        return

    # Write PR list to Excel (for tracking) if not already there
    try:
        existing_df = pd.read_excel(excel_file)
        processed_prs = set(existing_df[existing_df["status"].notna()]["pr_no"])
    except FileNotFoundError:
        processed_prs = set()
        existing_df = pd.DataFrame()

    # Filter out already-processed PRs
    pending_prs = [p for p in pr_nos if p not in processed_prs]
    if not pending_prs and existing_df.empty:
        pending_prs = pr_nos  # Process all if no prior runs
    elif not pending_prs:
        logger.info("All PRs already processed. Use --reprocess to retry.")
        return

    logger.info("Pending: %d/%d PR(s)", len(pending_prs), len(pr_nos))

    # ── Process one PR at a time, updating Excel ────────────────────────────────
    results: list[dict] = []
    for pr_idx, pr_no in enumerate(pending_prs, 1):
        logger.info("[%d/%d] Processing PR=%s", pr_idx, len(pending_prs), pr_no)

        # Fetch & resolve for this PR only
        try:
            row_groups = _fetch_existing_rows(TGT_CS, pr_no)
            if not row_groups:
                logger.info("[%s] no quotation_extracted_items rows — skipping", pr_no)
                results.append({"pr_no": pr_no, "status": "skipped", "reason": "no_rows", "timestamp": datetime.now().isoformat()})
                continue

            sources = _resolve_quotation_sources(TGT_CS, pr_no)
            blob_by_key = {str(s.get("embedded_classify_fk") or s.get("attachment_classify_fk")): s["blob_path"] for s in sources if s.get("blob_path")}

            # Process all quotation groups for this PR
            pr_results = []
            for source_id, rows in row_groups.items():
                blob_path = blob_by_key.get(source_id)
                if not blob_path:
                    continue

                try:
                    result = _process_quotation_group(
                        llm=llm, tgt_cs=TGT_CS, blob_cfg=blob_cfg,
                        pr_no=pr_no, source_id=source_id,
                        rows=rows, src_blob_path=blob_path,
                        reprocess=args.reprocess, dry_run=args.dry_run,
                    )
                    pr_results.append(result)
                except Exception as exc:
                    pr_results.append({"pr_no": pr_no, "source_id": source_id, "status": "failed", "reason": str(exc)})

            # Aggregate results for this PR
            if pr_results:
                ok_count = sum(1 for r in pr_results if r["status"] == "ok")
                total_updated = sum(r.get("updated", 0) for r in pr_results)
                results.append({
                    "pr_no": pr_no,
                    "status": "ok" if ok_count > 0 else "failed",
                    "groups": len(pr_results),
                    "rows_updated": total_updated,
                    "timestamp": datetime.now().isoformat()
                })
                logger.info("[%s] completed: %d group(s), %d row(s) updated", pr_no, len(pr_results), total_updated)
        except Exception as exc:
            results.append({"pr_no": pr_no, "status": "failed", "reason": str(exc), "timestamp": datetime.now().isoformat()})
            logger.warning("[%s] error: %s", pr_no, exc)

    # ── Save results to Excel ────────────────────────────────────────────────────
    t0 = time.time()
    dt = time.time() - t0
    logger.info("Done in %.1fs", dt)

    # Write results to Excel tracking file
    try:
        import pandas as pd
        from datetime import datetime

        excel_file = "re_commercials_progress.xlsx"
        df = pd.DataFrame(results)
        df["timestamp"] = datetime.now().isoformat()

        # Append to existing file if it exists
        try:
            existing = pd.read_excel(excel_file)
            df = pd.concat([existing, df], ignore_index=True)
        except FileNotFoundError:
            pass

        df.to_excel(excel_file, index=False)
        logger.info("Progress saved to %s", excel_file)
    except Exception as exc:
        logger.warning("Could not save Excel file: %s", exc)

    # Non-zero exit code if anything genuinely failed (skips & dry-runs are OK)
    failed = sum(1 for r in results if r["status"] == "failed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
