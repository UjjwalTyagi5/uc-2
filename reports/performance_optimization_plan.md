# Performance Optimization Plan: Classification Pipeline at Scale

**Date:** 19 April 2026  
**Current baseline:** 726 PRs, 19,561 files, ~15 hours with 4 parallel workers  
**Target:** Process millions of files efficiently for client confidential data  

---

## Current Performance Profile

| Metric | Value |
|--------|-------|
| PRs processed | 726 |
| Total files classified | 19,561 |
| Wall-clock time | ~15 hours |
| Parallel workers | 4 (PR-level parallelism only) |
| Avg time per PR | 293 seconds (~5 minutes) |
| Avg time per file | ~10.8 seconds |
| Total worker-time (sum across workers) | 59.1 hours |
| LLM escalations (mini → full) | 0 (zero — mini model handled everything) |
| LLM rate limit errors | 0 |

### Where Time Is Spent

```
67.2% — Image files (13,148 PNGs/JPGs) sent to LLM for classification
        → 7,849 of these (59.7%) classified as "Others" — wasted LLM calls
        → These are logos, signatures, watermarks from embedded docs

32.8% — Actual document files (Excel, PDF, Word, PPTX) — legitimate work

~90%  — LLM API latency per file (~9-10s waiting for Azure OpenAI response)
~8%   — Content extraction (PDF parsing, Excel reading, image encoding)
~2%   — Token counting, DB writes, file I/O
```

### Critical Finding

**67% of all classification time is spent sending embedded images (logos, signatures) to the LLM, which classifies them as "Others".** This is the single largest waste. Eliminating even half of these calls would cut total time by ~33%.

---

## Optimization Plan

### Tier 1: High Impact (implement first)

---

#### Optimization 1: Skip Trivial Embedded Images Without LLM Call

**Impact: Saves ~6-8 hours (40-53% reduction)**  
**Effort: Low**  
**Risk: None — these files are already classified as Others**

**Problem:** 13,148 image files (67% of all files) are sent to the LLM. 7,849 (60%) are logos, signatures, and watermarks that are always "Others". Each costs ~10.8s of LLM time.

**Solution:** Add a pre-classification check that skips trivial images:

```python
# In classification.py, before calling classify_file:

def _should_skip_as_trivial(self, file_path: Path) -> bool:
    """Return True if file is a trivial image (logo/signature/watermark)."""
    ext = file_path.suffix.lower()
    if ext not in (".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"):
        return False
    
    file_size = file_path.stat().st_size
    
    # Skip very small images (< 10KB — logos, icons, signatures)
    if file_size < 10_000:
        return True
    
    # Skip medium images (< 50KB) with small dimensions
    if file_size < 50_000:
        try:
            from PIL import Image
            import io
            img = Image.open(io.BytesIO(file_path.read_bytes()))
            # Images smaller than 300x300 are unlikely to be documents
            if max(img.size) < 300:
                return True
        except Exception:
            return True  # Can't even open it — skip
    
    return False
```

**When skipped:** Set `doc_type = "Others"`, `classification_conf = 0.0` directly, log as "Skipped trivial image", no LLM call.

**Estimated savings:** ~7,000-8,000 files skipped × 10.8s = ~75,000-86,000s worker-time = **~5-6 hours wall-clock savings**

---

#### Optimization 2: Intra-PR File Parallelism

**Impact: Saves ~5-7 hours (33-47% reduction on remaining work)**  
**Effort: Medium**  
**Risk: Low — LLM calls are I/O-bound, thread-safe with connection pooling**

**Problem:** Files within a single PR are classified sequentially. The slowest PR (R_261115/2026) had 189 files and took 39.5 minutes — one LLM call blocking the next.

**Solution:** Add a `ThreadPoolExecutor` inside `ClassificationStage.execute()` to classify files within a PR in parallel:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def execute(self, purchase_req_no: str) -> None:
    # ... setup code ...
    
    # Collect all files to classify
    file_tasks = []
    for att_dir in sorted(pr_work_dir.iterdir()):
        if not att_dir.is_dir():
            continue
        att_id = att_dir.name
        
        # Parent files
        for file_path in sorted(att_dir.iterdir()):
            if file_path.is_file():
                file_tasks.append(("parent", file_path, att_id, None))
        
        # Embedded files
        extracted_dir = att_dir / "extracted"
        if extracted_dir.exists():
            parent_pk = self._att_repo.get_parent_pk(att_id)
            if parent_pk:
                for emb_file in sorted(extracted_dir.iterdir()):
                    if emb_file.is_file():
                        file_tasks.append(("embedded", emb_file, att_id, parent_pk))
    
    # Classify in parallel (4-8 concurrent LLM calls per PR)
    max_file_workers = min(8, len(file_tasks))
    with ThreadPoolExecutor(max_workers=max_file_workers) as pool:
        futures = {
            pool.submit(self._classify_and_store, task, classify_file): task
            for task in file_tasks
        }
        for future in as_completed(futures):
            # ... collect results, count successes/errors ...
```

**Why this is safe:**
- The OpenAI Python SDK uses `httpx` connection pooling — thread-safe
- Each DB update is a separate connection (already per-call in the repository)
- File reads are independent — no shared state

**Configuration:** Add `CLASSIFICATION_WORKERS` env var (default: 4, max: 8). Combined with `PIPELINE_WORKERS=4`, gives 4 PRs × 4-8 files = 16-32 concurrent LLM calls. Azure OpenAI typically supports 48-300 RPM depending on deployment tier.

**Estimated savings:** PR with 189 files: 189 × 10.8s = 2040s sequential → 189/8 × 10.8s = ~255s parallel = **8x speedup for large PRs**

---

#### Optimization 3: Cache tiktoken Encoding

**Impact: Saves ~0.5-1 hour**  
**Effort: Trivial (5 lines)**  
**Risk: None**

**Problem:** `tiktoken.encoding_for_model("gpt-4o")` is called for every single file classification. This loads and initializes the tokenizer each time.

**Fix in `file_classifier/utils/token_utils.py`:**

```python
import tiktoken

_encoding_cache = {}

def _get_encoding(model: str = "gpt-4o"):
    if model not in _encoding_cache:
        try:
            _encoding_cache[model] = tiktoken.encoding_for_model(model)
        except KeyError:
            _encoding_cache[model] = tiktoken.get_encoding("cl100k_base")
    return _encoding_cache[model]

def truncate_to_token_limit(text: str, max_tokens: int = 3500, model: str = "gpt-4o") -> str:
    encoding = _get_encoding(model)
    tokens = encoding.encode(text)
    if len(tokens) <= max_tokens:
        return text
    return encoding.decode(tokens[:max_tokens]) + "\n\n[... content truncated ...]"
```

---

### Tier 2: Medium Impact (implement second)

---

#### Optimization 4: Batch Database Updates

**Impact: Saves ~10-15 minutes**  
**Effort: Medium**  
**Risk: Low**

**Problem:** 19,561 individual `UPDATE` statements, each opening and closing a DB connection.

**Solution:** Collect classifications in batches of 50-100 and execute a single parameterized UPDATE with `executemany()`:

```python
def bulk_update_parent_classifications(self, updates: list[dict]) -> None:
    """Batch update doc_type and classification_conf for multiple attachments."""
    conn = self._connect()
    try:
        cursor = conn.cursor()
        cursor.executemany(
            self._UPDATE_PARENT_CLASSIFICATION_SQL,
            [(u["doc_type"], u["classification_conf"], u["attachment_id"]) for u in updates],
        )
        conn.commit()
    except pyodbc.Error as exc:
        conn.rollback()
        raise
    finally:
        conn.close()
```

---

#### Optimization 5: Reduce System Prompt Size

**Impact: Saves ~5-10% LLM latency per call**  
**Effort: Low**  
**Risk: Low — field definitions are preserved, only verbose explanations trimmed**

**Current system prompt:** ~2,400 tokens (253 lines)  
**Target:** ~1,600-1,800 tokens

**What to trim:**
- Language translation examples (lines 7-13): Reduce from 8 examples to 3 key ones
- Disambiguation sections: Currently repeated per category, could be consolidated
- Output format instructions: Already well understood by GPT-4o-mini, can be shortened

**Keep intact:** Category definitions, mandatory field lists, decision process steps

---

#### Optimization 6: Reduce max_content_tokens

**Impact: ~5% faster LLM responses**  
**Effort: Trivial (1 line config change)**  
**Risk: Low — most documents are well under 4000 tokens**

Change in `.env` or config:
```
MAX_CONTENT_TOKENS=4000   # down from 6000
```

Most procurement documents (quotations, BERs, approval notes) are < 2000 tokens. Only large RFQ spec sheets approach 4000. Setting 4000 vs 6000 reduces average input size and speeds up LLM inference.

---

### Tier 3: Architecture for Millions of Files

---

#### Optimization 7: Async LLM Calls with `httpx`/`asyncio`

**Impact: 2-3x throughput increase over threads**  
**Effort: High (rewrite LLM client to async)**  
**Risk: Medium — requires async-compatible pipeline**

For millions of files, switch from `ThreadPoolExecutor` to `asyncio` with the async OpenAI client:

```python
from openai import AsyncAzureOpenAI

async def classify_batch(files: list[tuple[bytes, str]]) -> list[dict]:
    client = AsyncAzureOpenAI(...)
    semaphore = asyncio.Semaphore(20)  # limit concurrent requests
    
    async def classify_one(file_bytes, filename):
        async with semaphore:
            response = await client.chat.completions.create(...)
            return json.loads(response.choices[0].message.content)
    
    tasks = [classify_one(fb, fn) for fb, fn in files]
    return await asyncio.gather(*tasks, return_exceptions=True)
```

**Why:** Threads are limited by GIL overhead and OS thread limits. Async I/O can handle 50-100+ concurrent LLM requests with a single thread, limited only by API rate limits.

---

#### Optimization 8: Pre-Filter with File Heuristics (No LLM)

**Impact: Skip 40-60% of files entirely**  
**Effort: Medium**  
**Risk: Low if heuristics are conservative**

For millions of files, add rule-based pre-classification that avoids LLM entirely:

```python
def heuristic_classify(file_bytes: bytes, filename: str) -> dict | None:
    """Return classification if confident without LLM, else None."""
    ext = Path(filename).suffix.lower()
    size = len(file_bytes)
    
    # Images under 100KB → always Others (logos, signatures, watermarks)
    if ext in (".png", ".jpg", ".jpeg", ".bmp", ".gif", ".tif") and size < 100_000:
        return {"classification": "Other", "confidence": 0.95, "reason": "Small image file"}
    
    # Excel files with "MPBC" in filename → likely MPBC (still verify with LLM)
    # But images with no text content → skip
    
    # XML, .rels, .emz → always Others
    if ext in (".xml", ".rels", ".emz", ".vcf", ".eml"):
        return {"classification": "Other", "confidence": 1.0, "reason": "Non-document metadata file"}
    
    return None  # Needs LLM
```

---

#### Optimization 9: Horizontal Scaling with Multiple VMs

**Impact: Linear speedup**  
**Effort: Medium-High**  
**Risk: Low with proper work partitioning**

For truly large-scale (millions of files):
1. Partition PRs into batches (e.g., 1000 per VM)
2. Run pipeline instances on multiple Azure VMs
3. Use Azure Service Bus or Redis queue for work distribution
4. Each VM runs independently with its own `--limit` and `--offset`

**Simple approach (no queue needed):**
```bash
# VM 1: python run_pipeline.py --offset 0 --limit 5000
# VM 2: python run_pipeline.py --offset 5000 --limit 5000
# VM 3: python run_pipeline.py --offset 10000 --limit 5000
```

---

## Projected Timeline

### Current: 726 PRs → 15 hours

### After Tier 1 optimizations (for same 726 PRs):

| Optimization | Time Saved | New Total |
|---|---|---|
| Baseline | — | 15.0 hours |
| Skip trivial images | -6.0 hours | 9.0 hours |
| Intra-PR parallelism (8 workers) | -4.5 hours | 4.5 hours |
| Cache tiktoken | -0.5 hours | 4.0 hours |
| **Total Tier 1** | **-11.0 hours** | **~4 hours** |

### Scaling projections (after Tier 1):

| Scale | PRs | Est. Files | Time (1 VM, 4 workers) | Time (4 VMs) |
|---|---|---|---|---|
| Current run | 726 | 19,561 | ~4 hours | — |
| 5,000 PRs | 5,000 | ~135,000 | ~28 hours | ~7 hours |
| 10,000 PRs | 10,000 | ~270,000 | ~55 hours | ~14 hours |
| 50,000 PRs | 50,000 | ~1.35M | ~275 hours | ~69 hours (3 days) |
| 100,000 PRs | 100,000 | ~2.7M | — | ~6 days (8 VMs) |

### After Tier 1 + 2 + async (Tier 3):

With async LLM calls (Optimization 7) + heuristic pre-filter (Optimization 8):
- **~2-3x additional speedup** over thread-based
- 50,000 PRs on 4 VMs: ~1-2 days instead of 3 days

---

## Implementation Priority

### Phase 1 — Quick Wins (1-2 days of development)
1. Skip trivial images (Optimization 1) — **biggest single win**
2. Cache tiktoken encoding (Optimization 3) — **5 lines, instant**
3. Reduce max_content_tokens to 4000 (Optimization 6) — **1 line config**

### Phase 2 — Parallelism (2-3 days of development)
4. Intra-PR file parallelism (Optimization 2) — **major architectural improvement**
5. Batch DB updates (Optimization 4)

### Phase 3 — Scale Architecture (1-2 weeks)
6. Async LLM calls (Optimization 7)
7. Heuristic pre-filter (Optimization 8)
8. Multi-VM scaling (Optimization 9)

---

## Security & Robustness Considerations for Client Confidential Data

1. **Data never leaves Azure:** All LLM calls go to your Azure OpenAI deployment — data stays in your Azure tenant
2. **No data logging to LLM provider:** Azure OpenAI with data processing opt-out ensures no training on your data
3. **Thread safety:** OpenAI SDK uses httpx connection pooling — safe for concurrent use
4. **Retry resilience:** Exponential back-off with Retry-After header respect already implemented
5. **Error isolation:** Per-file failures never crash the pipeline — already proven with 105 errors in 19,561 files
6. **Idempotent operations:** Pipeline can be safely re-run; MERGE operations prevent duplicates
7. **Change detection:** Source changes auto-trigger re-processing — no stale classifications
