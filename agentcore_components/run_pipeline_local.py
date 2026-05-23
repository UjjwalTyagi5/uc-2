"""Standalone local runner for `pipeline_stage_123_v3_new (5).py`.

Bypasses the MiCore / AgentCore canvas entirely: builds the LLM, embedding
model, blob client, Pinecone client, and SQL connections directly from a
`.env` file in this same folder, instantiates `PipelineStage123NodeV2`,
wires the attributes the canvas would normally set, and calls the pipeline
synchronously so logs stream to stdout and the script exits when done.

Architecture:
    * `agentcore` package must be importable (you already have it because
      MiCore runs on this machine) — the pipeline module imports `Node`,
      `HandleInput`, `Data`, etc. from it at module load time.
    * The AgentCore Pinecone HTTP service is NOT required. This runner
      installs a thin in-process shim over the official `pinecone` SDK and
      patches it into `agentcore.services.pinecone_service_client` before
      any Stage 6/7 import fires.
    * The AgentCore connector_catalogue DB lookup is NOT required either —
      `_blob_cfg` is monkey-patched on the instance to return a direct
      `BlobServiceClient`.

Usage:
    1. cp .env.example .env  (then fill in every blank)
    2. pip install python-dotenv langchain-openai azure-storage-blob \\
                   azure-identity pinecone openpyxl pyodbc loguru
    3. python run_pipeline_local.py

Single-PR smoke test before the full Excel:
    set PR_FILTER in .env to one known-good PR, leave EXCEL_PATH blank.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import threading
import time
import uuid
from pathlib import Path
from types import ModuleType, SimpleNamespace


# ─── Fake `agentcore` package so the pipeline file imports without MiCore ────
#
# The pipeline does `from agentcore.custom import Node` and friends at module
# load time. On a machine without MiCore installed we manufacture the minimum
# surface those imports need — empty subpackages + tiny shim classes — and
# inject them into sys.modules BEFORE loading the pipeline file. The class
# bodies just need to exist; we instantiate the pipeline class via
# object.__new__ so __init__ is never called, and we never use the Input
# classes for anything except their presence in the `inputs = [...]` list.

def _install_agentcore_stubs() -> bool:
    """Return True if we installed stubs, False if the real `agentcore`
    package was already importable (we leave it alone in that case)."""
    if "agentcore" in sys.modules:
        return False
    try:
        import agentcore   # noqa: F401
        return False
    except ImportError:
        pass

    def _mod(name: str) -> ModuleType:
        m = ModuleType(name)
        sys.modules[name] = m
        return m

    _mod("agentcore")

    # agentcore.custom — Node base class. We never construct it; we use
    # object.__new__(PipelineStage123NodeV2). `log` is provided as a no-op
    # fallback in case anything reaches for it; _safe_log on the pipeline
    # routes to loguru when there's no canvas context anyway.
    ac_custom = _mod("agentcore.custom")
    class Node:
        def log(self, msg, *a, **k):
            print(msg)
    ac_custom.Node = Node

    # agentcore.io — Input classes are referenced in the `inputs = [...]`
    # list on the class body, so they must exist and be call-able. We never
    # read their attributes; their only job is "don't crash at class definition".
    ac_io = _mod("agentcore.io")
    def _input_stub(cls_name: str):
        class _Stub:
            def __init__(self, **kw):
                self.__dict__.update(kw)
            def __repr__(self):
                return f"<{cls_name} {getattr(self, 'name', '?')}>"
        _Stub.__name__ = cls_name
        return _Stub
    for _n in ("BoolInput", "HandleInput", "IntInput", "MessageTextInput",
               "MultilineInput", "FileInput", "Output"):
        setattr(ac_io, _n, _input_stub(_n))

    # agentcore.schema + Data + Message — the pipeline uses Data.data and
    # Message(text=...). Both are tiny.
    _mod("agentcore.schema")
    ac_data = _mod("agentcore.schema.data")
    class Data:
        def __init__(self, data=None, **kw):
            self.data = data if data is not None else dict(kw)
    ac_data.Data = Data

    ac_msg = _mod("agentcore.schema.message")
    class Message:
        def __init__(self, text="", **kw):
            self.text = text
        def __repr__(self):
            return f"<Message text={self.text!r}>"
    ac_msg.Message = Message

    # agentcore.services + the submodules the pipeline imports lazily inside
    # functions we either bypass (connector-catalogue lookup is replaced via
    # the _blob_cfg monkey-patch on the instance) or shim (pinecone client).
    _mod("agentcore.services")
    _mod("agentcore.services.deps")
    _mod("agentcore.services.database")
    _mod("agentcore.services.database.models")
    _mod("agentcore.services.database.models.connector_catalogue")
    _mod("agentcore.services.database.models.connector_catalogue.model")
    _mod("agentcore.services.pinecone_service_client")

    return True


# ─── locate this folder, load .env from it ────────────────────────────────────

HERE          = Path(__file__).resolve().parent
PIPELINE_FILE = HERE / "pipeline_stage_123_v3_new (5).py"

if not PIPELINE_FILE.exists():
    sys.exit(f"ERROR: pipeline file not found: {PIPELINE_FILE}")

try:
    from dotenv import load_dotenv
except ImportError:
    sys.exit("ERROR: install python-dotenv first → pip install python-dotenv")

load_dotenv(HERE / ".env")


def env(key: str, default: str | None = None, required: bool = False) -> str:
    val = (os.environ.get(key) or "").strip()
    if not val:
        if required:
            sys.exit(f"ERROR: required env var {key} is blank or missing in .env")
        val = default or ""
    return val


def env_int(key: str, default: int) -> int:
    raw = (os.environ.get(key) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        sys.exit(f"ERROR: env var {key}={raw!r} is not a valid int")


# ─── build Azure OpenAI LLM + embeddings ──────────────────────────────────────

def build_llm():
    from langchain_openai import AzureChatOpenAI
    return AzureChatOpenAI(
        azure_endpoint   = env("AZURE_OPENAI_ENDPOINT",      required=True),
        api_key          = env("AZURE_OPENAI_API_KEY",       required=True),
        api_version      = env("AZURE_OPENAI_API_VERSION",   default="2024-08-01-preview"),
        azure_deployment = env("AZURE_OPENAI_LLM_DEPLOYMENT", required=True),
        temperature      = 0.0,
        timeout          = 120,
        max_retries      = 0,   # pipeline does its own retry
    )


def build_embed_model():
    from langchain_openai import AzureOpenAIEmbeddings
    return AzureOpenAIEmbeddings(
        azure_endpoint   = env("AZURE_OPENAI_ENDPOINT",        required=True),
        api_key          = env("AZURE_OPENAI_API_KEY",         required=True),
        api_version      = env("AZURE_OPENAI_API_VERSION",     default="2024-08-01-preview"),
        azure_deployment = env("AZURE_OPENAI_EMBED_DEPLOYMENT", required=True),
        timeout          = 60,
        max_retries      = 0,
    )


# ─── build Azure Blob client (3 auth modes, first non-empty wins) ─────────────

def build_blob_service_client():
    from azure.storage.blob import BlobServiceClient

    conn_str = env("AZURE_BLOB_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    account_url = env("AZURE_BLOB_ACCOUNT_URL", required=True)
    account_key = env("AZURE_BLOB_ACCOUNT_KEY")
    if account_key:
        return BlobServiceClient(account_url=account_url, credential=account_key)

    # Fallback — DefaultAzureCredential (az login / managed identity / SP env)
    from azure.identity import DefaultAzureCredential
    return BlobServiceClient(account_url=account_url, credential=DefaultAzureCredential())


# ─── Pinecone shim ────────────────────────────────────────────────────────────
#
# The pipeline does `from agentcore.services.pinecone_service_client
# import ensure_index_via_service, ingest_via_service, search_via_service`
# inside Stage 6/7 functions. We patch the symbols on the real module so
# those imports get our shim functions instead of the HTTP-based ones.

def install_pinecone_shim() -> None:
    api_key  = env("PINECONE_API_KEY", required=True)
    cloud    = env("PINECONE_CLOUD",  default="aws")
    region   = env("PINECONE_REGION", default="us-east-1")
    metric   = env("PINECONE_METRIC", default="dotproduct")

    from pinecone import Pinecone, ServerlessSpec
    pc = Pinecone(api_key=api_key)

    def _list_existing_names() -> set:
        try:
            return {idx["name"] if isinstance(idx, dict) else idx.name
                    for idx in pc.list_indexes()}
        except Exception:
            # Older SDK: list_indexes returns a names list
            try:
                return set(pc.list_indexes().names())
            except Exception:
                return set()

    def ensure_index_via_service(index_name: str, embedding_dimension: int, **kw):
        if index_name in _list_existing_names():
            return {"exists": True, "created": False, "index_name": index_name}
        pc.create_index(
            name=index_name,
            dimension=int(embedding_dimension),
            metric=metric,
            spec=ServerlessSpec(cloud=cloud, region=region),
        )
        return {"exists": True, "created": True, "index_name": index_name}

    def ingest_via_service(
        index_name: str,
        namespace: str,
        documents: list,
        embedding_vectors: list,
        vector_ids: list,
        embedding_dimension: int | None = None,
        text_key: str = "page_content",
        **kw,
    ):
        idx = pc.Index(index_name)
        vectors = []
        for doc, emb, vid in zip(documents, embedding_vectors, vector_ids):
            metadata = dict((doc or {}).get("metadata") or {})
            page_content = (doc or {}).get(text_key) or (doc or {}).get("page_content")
            if page_content:
                metadata[text_key] = page_content
            vectors.append({
                "id":       str(vid),
                "values":   list(emb),
                "metadata": metadata,
            })
        idx.upsert(vectors=vectors, namespace=namespace)
        return {"upserted_count": len(vectors)}

    def search_via_service(
        index_name: str,
        namespace: str,
        query_embedding: list,
        number_of_results: int,
        query: str | None = None,
        text_key: str = "page_content",
        **kw,
    ):
        idx = pc.Index(index_name)
        res = idx.query(
            vector=list(query_embedding),
            top_k=int(number_of_results),
            namespace=namespace,
            include_metadata=True,
        )
        raw_matches = []
        if isinstance(res, dict):
            raw_matches = res.get("matches") or []
        else:
            raw_matches = getattr(res, "matches", []) or []
        out = []
        for m in raw_matches:
            if isinstance(m, dict):
                mid    = m.get("id")
                score  = m.get("score", 0.0)
                meta   = m.get("metadata") or {}
            else:
                mid    = getattr(m, "id", None)
                score  = getattr(m, "score", 0.0)
                meta   = getattr(m, "metadata", {}) or {}
            out.append({
                "id":       mid,
                "score":    float(score),
                "metadata": dict(meta),
            })
        return out

    def delete_vectors_via_service(
        index_name: str,
        namespace: str,
        vector_ids: list | None = None,
        ids: list | None = None,
        **kw,
    ):
        # Pipeline calls this with `vector_ids=...`; the AgentCore service
        # historically accepted `ids=...` too. Accept either.
        target = vector_ids if vector_ids is not None else ids
        if not target:
            return {"deleted_count": 0}
        idx = pc.Index(index_name)
        idx.delete(ids=[str(i) for i in target], namespace=namespace)
        return {"deleted_count": len(target)}

    # Patch the agentcore.services.pinecone_service_client module — this works
    # whether it's the real one (installed agentcore) or the stub created by
    # _install_agentcore_stubs(). Either way `from ... import name` resolves
    # to our shim functions.
    try:
        import agentcore.services.pinecone_service_client as ps
    except Exception as exc:
        sys.exit(f"ERROR: agentcore.services.pinecone_service_client unavailable "
                 f"(stubs should have created it — internal bug): {exc}")
    ps.ensure_index_via_service   = ensure_index_via_service
    ps.ingest_via_service         = ingest_via_service
    ps.search_via_service         = search_via_service
    ps.delete_vectors_via_service = delete_vectors_via_service
    print(f"[init] Pinecone shim installed (cloud={cloud}, region={region}, metric={metric})")


# ─── load the pipeline module (filename has a space → importlib) ──────────────

def load_pipeline_module():
    spec = importlib.util.spec_from_file_location("pipeline_v3_new5", str(PIPELINE_FILE))
    mod  = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# ─── build PR list (Excel preferred, PR_FILTER overrides) ─────────────────────

def parse_pr_list_from_excel(path: Path, sheet: str | None, column: str | None) -> list[str]:
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb[sheet] if sheet else wb.worksheets[0]
        prs: list[str] = []
        first = True
        for row in ws.iter_rows(values_only=True):
            if not row:
                continue
            v = row[0]
            if v is None:
                continue
            s = str(v).strip()
            if not s:
                continue
            if first:
                first = False
                if any(tok in s.lower()
                       for tok in ("pr_no", "pr number", "purchase_req", "purchase req")):
                    continue
            prs.append(s)
        # dedupe preserving order
        seen, out = set(), []
        for p in prs:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out
    finally:
        try: wb.close()
        except Exception: pass


# ─── helper Data-shaped wrapper for source/target connection inputs ───────────

class _ConnData:
    """Mimics agentcore.io.Data — _conn_str only reads .data."""
    def __init__(self, **kw):
        self.data = dict(kw)


# ─── attach the pipeline instance, wire every attribute, run ──────────────────

def build_and_run() -> int:
    # Order matters: agentcore stubs must be in sys.modules BEFORE the
    # pipeline file is loaded (its top-level `from agentcore.* import …`
    # statements resolve at module-load time). The Pinecone shim then patches
    # symbols onto whichever module — real or stubbed — is already in place.
    used_stubs = _install_agentcore_stubs()
    if used_stubs:
        print("[init] agentcore stubs installed (real package not found — using minimal shims)")
    else:
        print("[init] real agentcore package detected — using it as-is")
    install_pinecone_shim()
    pmod = load_pipeline_module()
    Cls  = pmod.PipelineStage123NodeV2

    # Bypass Node.__init__ — we set attributes manually.
    inst = object.__new__(Cls)

    # ── core dependencies ────────────────────────────────────────────────────
    inst.llm         = build_llm()
    inst.embed_model = build_embed_model()
    print("[init] LLM + embedding model built")

    blob_svc  = build_blob_service_client()
    container = env("AZURE_BLOB_CONTAINER", required=True)
    # Both _blob_cfg() (returns dict with blob_client) and _container_client()
    # (returns ContainerClient) are consulted by the pipeline. We override
    # _blob_cfg so _download_blob takes the `blob_client` direct path. We also
    # preinstall the cached container client so _upload_blob / _delete_blob_folder
    # skip the connector-catalogue lookup.
    inst._blob_config_cache    = {"blob_client": blob_svc, "container_name": container}
    inst._blob_cfg             = lambda: inst._blob_config_cache
    inst._container_client_cache = blob_svc.get_container_client(container)
    print(f"[init] Blob client ready → container {container!r}")

    # ── connection inputs ────────────────────────────────────────────────────
    inst.source_connection = _ConnData(
        driver        = env("SRC_DB_DRIVER",   default="ODBC Driver 17 for SQL Server"),
        host          = env("SRC_DB_HOST",     required=True),
        port          = env_int("SRC_DB_PORT", 1433),
        database_name = env("SRC_DB_DATABASE", required=True),
        username      = env("SRC_DB_USERNAME", required=True),
        password      = env("SRC_DB_PASSWORD", required=True),
    )
    inst.target_connection = _ConnData(
        driver        = env("TGT_DB_DRIVER",   default="ODBC Driver 18 for SQL Server"),
        host          = env("TGT_DB_HOST",     required=True),
        port          = env_int("TGT_DB_PORT", 1433),
        database_name = env("TGT_DB_DATABASE", required=True),
        username      = env("TGT_DB_USERNAME", required=True),
        password      = env("TGT_DB_PASSWORD", required=True),
    )

    # ── Pinecone config the pipeline reads off self ──────────────────────────
    inst.pinecone_index     = env("PINECONE_INDEX_NAME", default="ras-quotations")
    inst.pinecone_namespace = env("PINECONE_NAMESPACE",  default="procurement")
    inst.pinecone_top_k     = env_int("PINECONE_TOP_K",  10)

    # ── pipeline knobs (defaults match the IntInput defaults on the class) ───
    inst.parallel_workers       = env_int("PARALLEL_WORKERS",       2)
    inst.cls_parallel_sources   = env_int("CLS_PARALLEL_SOURCES",   3)
    inst.ext_parallel_sources   = env_int("EXT_PARALLEL_SOURCES",   1)
    inst.max_db_connections     = env_int("MAX_DB_CONNECTIONS",     30)
    inst.batch_limit            = env_int("BATCH_LIMIT",            0)
    inst.max_content_chars      = env_int("MAX_CONTENT_CHARS",      80000)
    inst.cls_max_chars          = env_int("CLS_MAX_CHARS",          24000)
    inst.cls_max_pages_vision   = env_int("CLS_MAX_PAGES_VISION",   5)
    inst.ext_max_chars          = env_int("EXT_MAX_CHARS",          50000)
    inst.ext_max_pages          = env_int("EXT_MAX_PAGES",          20)
    inst.ext_max_images         = env_int("EXT_MAX_IMAGES",         50)
    inst.llm_max_retries        = env_int("LLM_MAX_RETRIES",        3)
    inst.llm_retry_cooldown     = env_int("LLM_RETRY_COOLDOWN",     60)
    inst.db_deadlock_max_retries = env_int("DB_DEADLOCK_MAX_RETRIES", 3)
    inst.db_deadlock_base_delay = env_int("DB_DEADLOCK_BASE_DELAY", 2)

    inst.selected_threshold     = env("SELECTED_THRESHOLD",      default="0.70")
    inst.price_max_boost        = env("PRICE_MAX_BOOST",         default="0.10")
    inst.canonicalize_threshold = env("CANONICALIZE_THRESHOLD",  default="0.82")
    inst.bench_min_similarity   = env("BENCH_MIN_SIMILARITY",    default="0.80")
    inst.bench_outlier_factor   = env("BENCH_OUTLIER_FACTOR",    default="3.0")
    inst.bench_max_age_months   = env_int("BENCH_MAX_AGE_MONTHS", 0)
    inst.bench_uom_strict       = env_int("BENCH_UOM_STRICT",     1)

    inst.enable_commercials_extraction = True

    inst.bench_sql_pool_size           = env_int("BENCH_SQL_POOL_SIZE",           100)
    inst.bench_pinecone_top_k          = env_int("BENCH_PINECONE_TOP_K",          10)
    inst.bench_llm_shortlist_size      = env_int("BENCH_LLM_SHORTLIST_SIZE",      10)
    inst.bench_widen_l1l2_when_sparse  = env_int("BENCH_WIDEN_L1L2_WHEN_SPARSE",  1)
    inst.bench_critical_threshold_pct  = env("BENCH_CRITICAL_THRESHOLD_PCT",     default="")
    inst.bench_important_threshold_pct = env("BENCH_IMPORTANT_THRESHOLD_PCT",    default="")
    inst.bench_ratio_band_pct          = env("BENCH_RATIO_BAND_PCT",             default="")
    inst.bench_rank_debug_dump_dir     = env("BENCH_RANK_DEBUG_DUMP_DIR",        default="")
    inst.bench_rank_prompt_system_v2   = ""
    inst.bench_rank_prompt_user_v2     = ""

    # ── prompt overrides (kept blank; pipeline falls back to baked-in prompts)
    for attr in (
        "cls_system_prompt", "cls_user_text_prompt", "cls_user_image_prompt",
        "ext_system_prompt", "ext_user_template", "ext_item_taxonomy",
    ):
        setattr(inst, attr, None)

    # ── PR list ──────────────────────────────────────────────────────────────
    pr_filter  = env("PR_FILTER", default="")
    excel_path = env("EXCEL_PATH", default="")

    inst.pr_no_filter        = pr_filter
    inst.pr_excel_blob_path  = ""                       # not used in local mode
    inst.pr_excel_sheet      = env("EXCEL_SHEET", default="") or None
    inst.pr_excel_column     = env("EXCEL_COLUMN", default="") or None

    inst.pr_list_data = None
    if not pr_filter and excel_path:
        p = Path(excel_path)
        if not p.exists():
            sys.exit(f"ERROR: EXCEL_PATH does not exist: {p}")
        pr_nos = parse_pr_list_from_excel(p, inst.pr_excel_sheet, inst.pr_excel_column)
        if not pr_nos:
            sys.exit(f"ERROR: no PR numbers found in {p}")
        # Feed via pr_list_data with already-parsed pr_numbers — pipeline
        # accepts this shape and skips Excel-bytes parsing.
        inst.pr_list_data = SimpleNamespace(data={"pr_numbers": pr_nos})
        print(f"[init] Excel loaded → {len(pr_nos)} PR(s) (first={pr_nos[0]!r}, last={pr_nos[-1]!r})")

    if not pr_filter and inst.pr_list_data is None:
        sys.exit("ERROR: set either EXCEL_PATH (with PR numbers) or PR_FILTER in .env")

    # Pipeline expects this attribute even when only one of the two paths is used.
    inst.pinecone_namespace = inst.pinecone_namespace or "procurement"

    # ── connection pool resize (mirrors build_file_batch's setup) ────────────
    try:
        pool_max = int(inst.max_db_connections or pmod._DEFAULT_POOL_SIZE)
    except Exception:
        pool_max = pmod._DEFAULT_POOL_SIZE
    if pool_max != pmod._CONN_POOL.max_size:
        pmod._CONN_POOL.resize(pool_max)
    print(f"[init] DB connection pool sized to {pmod._CONN_POOL.max_size}")

    # Resolve connection strings.
    src_cs = inst._conn_str(inst.source_connection)
    tgt_cs = inst._conn_str(inst.target_connection)

    # Quick DB smoke test so credential errors fail fast with a clear message.
    print("[init] Smoke-testing DB connections…")
    import pyodbc
    for label, cs in (("source", src_cs), ("target", tgt_cs)):
        t0 = time.time()
        try:
            c = pyodbc.connect(cs, timeout=15)
            c.close()
            print(f"[init]   {label}: OK ({time.time()-t0:.2f}s)")
        except Exception as exc:
            sys.exit(f"ERROR: {label} DB connection failed: {exc}")

    # ── run the pipeline synchronously ───────────────────────────────────────
    job_id = str(uuid.uuid4())[:8]
    print(f"\n[run] job_id={job_id} | workers={inst.parallel_workers} "
          f"| cls={inst.cls_parallel_sources} | ext={inst.ext_parallel_sources}")
    print(f"[run] starting full pipeline — this blocks until every PR is done\n")

    t_start = time.time()
    try:
        inst._run_full_pipeline(src_cs, tgt_cs, job_id)
    except KeyboardInterrupt:
        print("\n[run] interrupted by user — partial progress is committed in DB")
        return 130
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print(f"\n[run] pipeline raised: {exc}")
        return 1

    elapsed = time.time() - t_start
    print(f"\n[run] DONE in {elapsed:.1f}s — job_id={job_id}")
    print(f"[run] Check ras_procurement.ras_tracker for per-PR stage state")
    print(f"[run] Check ras_procurement.ras_pipeline_exceptions for any failures")
    return 0


if __name__ == "__main__":
    sys.exit(build_and_run())
