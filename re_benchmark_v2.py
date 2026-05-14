"""
re_benchmark_v2.py — Re-run V2 Stage 7 (3-stage benchmarking) for PRs.

Use this when you have updated the V2 benchmarking logic (Stage C rank
prompt, category aliases, supplier canonicalization, thresholds, etc.)
and want to refresh benchmark_result rows without re-running the full
ingestion / classification / extraction pipeline from scratch.

Differs from re_benchmark.py:
  - Imports from pipeline_stage_123_v2 instead of pipeline_stage_123
  - Calls _run_benchmark_v2 (3-stage SQL + Pinecone + LLM rank)
  - Exposes all V2-specific tuning knobs as env vars

Usage:
    python re_benchmark_v2.py                          # ALL stage-8 PRs
    python re_benchmark_v2.py R_153634/2021            # specific PR(s)
    python re_benchmark_v2.py R_153634/2021 R_151787/2021

All config is read from a .env file or environment variables.
"""

import os
import sys
import logging
import argparse

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

# ── Pinecone ─────────────────────────────────────────────────────────────────
PINECONE_INDEX      = os.getenv("PINECONE_INDEX_NAME",  "")
PINECONE_NAMESPACE  = os.getenv("PINECONE_NAMESPACE",   "")
# V2 calls this `top_k_final` — the K that ultimately reaches the pricing LLM.
PINECONE_TOP_K      = int(os.getenv("PINECONE_TOP_K",   "5"))

# ── Azure OpenAI ─────────────────────────────────────────────────────────────
AZURE_ENDPOINT      = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY       = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_API_VERSION   = os.getenv("AZURE_OPENAI_API_VERSION",          "2024-10-21")
AZURE_LLM_DEPLOY    = os.getenv("AZURE_OPENAI_DEPLOYMENT",           "gpt-4o")
AZURE_EMBED_DEPLOY  = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-large")

# ── V2 Benchmark tuning — exact defaults from pipeline_stage_123_v2.py ───────
# All knobs are overridable via env vars so you can tweak per-run without
# touching the file. Matches the IntInput / FloatInput defaults exposed
# on the MiCore component.
BENCH_PARAMS = {
    # Stage A — SQL category pool
    "bench_sql_pool_size":           int(os.getenv("BENCH_SQL_POOL_SIZE",           "100")),
    "bench_widen_l1l2_when_sparse":  os.getenv("BENCH_WIDEN_L1L2", "true").lower() in ("1", "true", "yes"),

    # Stage B — Pinecone within-pool
    "bench_pinecone_top_k":          int(os.getenv("BENCH_PINECONE_TOP_K",          "10")),
    "bench_min_similarity":          float(os.getenv("BENCH_MIN_SIMILARITY",        "0.80")),  # tightened from 0.70

    # Stage C — LLM relevance ranking
    "bench_llm_shortlist_size":      int(os.getenv("BENCH_LLM_SHORTLIST_SIZE",      "5")),
    "bench_critical_threshold_pct":  int(os.getenv("BENCH_CRITICAL_THRESHOLD_PCT",  "10")),  # tightened from 25
    "bench_important_threshold_pct": int(os.getenv("BENCH_IMPORTANT_THRESHOLD_PCT", "20")),
    "bench_ratio_band_pct":          int(os.getenv("BENCH_RATIO_BAND_PCT",          "50")),

    # Post-fetch historical filters (used during pricing)
    "bench_outlier_factor":          float(os.getenv("BENCH_OUTLIER_FACTOR",        "3.0")),
    "bench_max_age_months":          int(os.getenv("BENCH_MAX_AGE_MONTHS",          "0")),
    "bench_uom_strict":              os.getenv("BENCH_UOM_STRICT", "false").lower() in ("1", "true", "yes"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# pipeline_stage_123_v2.py imports agentcore framework classes at module
# level. Stub them out so the file can be imported standalone — the
# benchmark function itself doesn't use any of these classes at runtime.
import types as _types

def _stub_agentcore() -> None:
    class _Stub:
        def __init__(self, *args, **kwargs): pass  # noqa: ANN
        def __init_subclass__(cls, **kwargs): pass  # noqa: ANN

    mod_names = [
        "agentcore",
        "agentcore.custom",
        "agentcore.io",
        "agentcore.schema",
        "agentcore.schema.data",
        "agentcore.schema.message",
        "agentcore.services",
        "agentcore.services.deps",
        "agentcore.services.database",
        "agentcore.services.database.models",
        "agentcore.services.database.models.connector_catalogue",
        "agentcore.services.database.models.connector_catalogue.model",
        "agentcore.services.pinecone_service_client",
    ]
    for name in mod_names:
        mod = _types.ModuleType(name)
        mod.__path__ = []       # mark as package so sub-imports work
        mod.__package__ = name
        sys.modules.setdefault(name, mod)

    # Module-level class stubs — cover every name pipeline_stage_123_v2.py
    # (and its sibling components in __init__.py) may try to import.
    io_stubs = [
        "HandleInput", "IntInput", "MessageTextInput", "MultilineInput",
        "BoolInput", "DropdownInput", "FileInput", "StrInput", "FloatInput",
        "MessageInput", "DataInput", "Output",
    ]
    for name in io_stubs:
        setattr(sys.modules["agentcore.io"], name, _Stub)
    sys.modules["agentcore.custom"].Node             = _Stub
    sys.modules["agentcore.schema.data"].Data        = _Stub
    sys.modules["agentcore.schema.message"].Message  = _Stub

    # Pinecone — V2 uses both ingest_via_service (during embedding stage,
    # not needed for re-benchmarking) and search_via_service (during
    # Stage B Pinecone within-pool retrieval).
    def search_via_service(
        index_name: str,
        namespace: str,
        text_key: str,         # unused here; kept for signature compatibility
        query: str,            # V2 requires non-empty string
        query_embedding: list,
        number_of_results: int,
        filter: dict = None,   # noqa: A002 — V2 doesn't actually pass this
    ) -> list:
        from pinecone import Pinecone
        pc    = Pinecone(api_key=os.getenv("PINECONE_API_KEY", ""))
        index = pc.Index(index_name)
        kwargs = dict(
            namespace=namespace or "",
            vector=query_embedding,
            top_k=number_of_results,
            include_metadata=True,
        )
        if filter:
            kwargs["filter"] = filter
        result = index.query(**kwargs)
        return [
            {"score": float(m["score"]), "metadata": m.get("metadata") or {}}
            for m in (result.get("matches") or [])
        ]

    def ingest_via_service(*args, **kwargs):
        # Re-benchmark does NOT re-ingest vectors; Stage 6 embeddings are
        # assumed to already exist in Pinecone for the PR.
        raise RuntimeError("ingest_via_service called during re-benchmark — Stage 6 should already be done")

    sys.modules["agentcore.services.pinecone_service_client"].search_via_service = search_via_service
    sys.modules["agentcore.services.pinecone_service_client"].ingest_via_service = ingest_via_service

_stub_agentcore()

# Load pipeline_stage_123_v2.py directly via importlib so we DON'T trigger
# agentcore_components/__init__.py (which imports siblings like etl_sync.py
# that need additional stubs we don't care about for re-benchmarking).
import importlib.util as _ilu  # noqa: E402

_v2_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "agentcore_components", "pipeline_stage_123_v2.py",
)
_spec = _ilu.spec_from_file_location("pipeline_stage_123_v2", _v2_path)
_v2 = _ilu.module_from_spec(_spec)
sys.modules["pipeline_stage_123_v2"] = _v2
_spec.loader.exec_module(_v2)

_run_benchmark_v2 = _v2._run_benchmark_v2
_connect          = _v2._connect


def _get_stage8_prs(tgt_cs: str) -> list[str]:
    """Return all purchase_req_no values where current_stage_fk = 8 (pipeline complete)."""
    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT rt.[purchase_req_no]
            FROM   [ras_procurement].[ras_tracker] rt
            WHERE  rt.[current_stage_fk] = 8
            ORDER  BY rt.[updated_at] DESC
        """)
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "pr_nos", nargs="*",
        help="Purchase request numbers to re-benchmark. "
             "Omit to re-benchmark all stage-8 PRs.",
    )
    args = parser.parse_args()

    try:
        from langchain_openai import AzureChatOpenAI, AzureOpenAIEmbeddings
    except ImportError:
        logger.error("langchain-openai not installed — run: pip install langchain-openai")
        sys.exit(1)

    llm = AzureChatOpenAI(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=AZURE_API_KEY,
        api_version=AZURE_API_VERSION,
        azure_deployment=AZURE_LLM_DEPLOY,
    )
    embed_model = AzureOpenAIEmbeddings(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=AZURE_API_KEY,
        api_version=AZURE_API_VERSION,
        azure_deployment=AZURE_EMBED_DEPLOY,
    )

    if args.pr_nos:
        pr_nos = args.pr_nos
        logger.info("Re-benchmarking %d PR(s) supplied on command line", len(pr_nos))
    else:
        pr_nos = _get_stage8_prs(TGT_CS)
        logger.info("Found %d PR(s) at stage 8 in DB", len(pr_nos))

    if not pr_nos:
        logger.info("Nothing to do.")
        return

    logger.info("V2 benchmark params: %s", BENCH_PARAMS)

    ok = err = 0
    for i, pr_no in enumerate(pr_nos, 1):
        logger.info("[%d/%d] Re-benchmarking %s ...", i, len(pr_nos), pr_no)
        try:
            _run_benchmark_v2(
                llm, TGT_CS, pr_no, embed_model,
                PINECONE_INDEX, PINECONE_NAMESPACE, PINECONE_TOP_K,
                prompts=BENCH_PARAMS,
            )
            ok += 1
            logger.info("[%d/%d] OK   %s complete", i, len(pr_nos), pr_no)
        except Exception as exc:
            err += 1
            logger.exception("[%d/%d] FAIL %s: %s", i, len(pr_nos), pr_no, exc)

    logger.info("Done — %d succeeded, %d failed", ok, err)


if __name__ == "__main__":
    main()
