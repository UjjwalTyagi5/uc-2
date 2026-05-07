"""
re_benchmark.py — Re-run Stage 7 (benchmarking) for PRs already at stage 8.

Use this when you have updated the benchmarking logic (e.g. new LLM context,
filter changes) and want to refresh benchmark_result rows without re-running
the full pipeline from ingestion.

Usage:
    python re_benchmark.py                        # re-benchmark ALL stage-8 PRs
    python re_benchmark.py R_153634/2021          # specific PR(s)
    python re_benchmark.py R_153634/2021 R_151787/2021

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
PINECONE_TOP_K      = int(os.getenv("PINECONE_TOP_K",   "5"))

# ── Azure OpenAI ─────────────────────────────────────────────────────────────
AZURE_ENDPOINT      = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY       = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_API_VERSION   = os.getenv("AZURE_OPENAI_API_VERSION",          "2024-10-21")
AZURE_LLM_DEPLOY    = os.getenv("AZURE_OPENAI_DEPLOYMENT",           "gpt-4o")
AZURE_EMBED_DEPLOY  = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", "text-embedding-3-large")

# ── Benchmark tuning — exact defaults from pipeline_stage_123.py ─────────────
BENCH_PARAMS = {
    "bench_min_similarity": float(os.getenv("PINECONE_THRESHOLD", "0.70")),
    "bench_outlier_factor": 3.0,
    "bench_max_age_months": 0,
    "bench_uom_strict":     False,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from agentcore_components.pipeline_stage_123 import _run_benchmark, _connect  # noqa: E402


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
        temperature=0,
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

    ok = err = 0
    for i, pr_no in enumerate(pr_nos, 1):
        logger.info("[%d/%d] Re-benchmarking %s ...", i, len(pr_nos), pr_no)
        try:
            _run_benchmark(
                llm, TGT_CS, pr_no, embed_model,
                PINECONE_INDEX, PINECONE_NAMESPACE, PINECONE_TOP_K,
                prompts=BENCH_PARAMS,
            )
            ok += 1
            logger.info("[%d/%d] ✓ %s complete", i, len(pr_nos), pr_no)
        except Exception as exc:
            err += 1
            logger.error("[%d/%d] ✗ %s failed: %s", i, len(pr_nos), pr_no, exc)

    logger.info("Done — %d succeeded, %d failed", ok, err)


if __name__ == "__main__":
    main()
