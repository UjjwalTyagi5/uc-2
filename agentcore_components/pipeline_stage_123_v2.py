"""Full Pipeline V2 — LLM Category + Smart 3-Stage Benchmark.

Subclass of PipelineStage123Node that swaps Stage 5 / 6 / 7 logic for a
metadata-rich extraction + filtered retrieval + LLM relevance ranking
pipeline. Stages 1-4 (file ingestion / classification / blob upload), the
fire-and-forget plumbing, structured logging, ThreadPoolExecutor
parallelism, and _safe_log are all inherited from V1 unchanged.

Why this exists — the V1 benchmark uses pure cosine similarity over a
12-field embedding text and misfires whenever a single numeric attribute
drives pricing (1800T IMM matched with 1000T, 1TB laptop matched with
512GB, 1.5T AC matched with 2T because brand similarity dominates the
vector). V2 instead asks the extraction LLM to (a) classify each item
into a canonical purchase_category, (b) produce a brand/supplier-free
embedding text, (c) emit structured critical_attributes. The benchmark
then runs 3-stage retrieval:

    Stage A — SQL pre-filter: TOP N same-category historical dtl_ids
    Stage B — Pinecone narrows to top K within that SQL pool
    Stage C — Relevance LLM picks the final shortlist using critical-
              attribute weights + spec-to-price ratio sanity checks

V1 stays as-is in pipeline_stage_123.py. Both Nodes register
independently and can run side-by-side against the same DB / Pinecone
index — V2's writes simply produce richer metadata than V1's.

DB prerequisite: run sql/migrations/quotation_extracted_items_add_classification_cols.sql once.
"""
from __future__ import annotations

import json
import os
import re
import time
from decimal import Decimal
from typing import Any, Optional

from loguru import logger

from agentcore.io import BoolInput, IntInput, MultilineInput

from agentcore_components.pipeline_stage_123 import (
    PipelineStage123Node,
    EXTRACTION_SYSTEM_PROMPT,
    EXTRACTION_USER_TEMPLATE,
    ITEM_TAXONOMY,
    RASContext,
    DocumentContent,
    _connect,
    _build_embed_text,
    _build_ras_context,
    _build_line_items_table,
    _build_raw_context,
    _resolve_quotation_sources,
    _download_blob,
    _load_document,
    _call_llm_with_retry,
    _align_to_ras_line_items,
    _apply_price_alignment_boost,
    _canonicalize_supplier_names,
    _compute_quote_ranks,
    _select_best_quotes,
    _convert_to_eur,
    _is_pinecone_server_error,
    _write_benchmark_stub,
    _set_last_processed_at,
    _run_classification,
    _STAGE_CLASSIFICATION,
    _STAGE_EXTRACTION,
    _STAGE_EMBEDDINGS,
    _STAGE_PRICE_BENCHMARK,
    _STAGE_COMPLETE,
    ExtractionAbortError,
    NoQuotationFoundError,
    NoLineItemsError,
    NoRASContextError,
    AllExtractionsFailedError,
)


# ── Canonical category seed list (the user's 52-item catalogue) ─────────────
PURCHASE_CATEGORIES_SEED: list[str] = [
    "Compressor", "TBE Wiring Harness", "Others", "Tooling Rubber",
    "Central Portal Purchase", "IT Hardware", "Oil and Lubricants",
    "Measuring Instruments", "HVAC Air Conditioner", "Vehicle Car",
    "Group Company", "TBE Fee and Charges", "Tooling Plastic", "VMC",
    "Storage Equipment Trolley", "Robot and Gripper", "Wiring Harness",
    "IMM", "Pump and Motor", "TBE Negotiated by Unit and Overseas Unit",
    "Gensets", "Environmental Chamber", "Software", "Transformer",
    "Tooling Die Casting", "Testing", "Electrical work",
    "Vehicle Motorcycle", "Fabrication", "CCTV", "Chillers",
    "Service and Repair", "Tool Room Machines", "Storage Equipment",
    "Consumable", "certification", "UPS", "Spares",
    "Storage Equipment Racks", "Post Facto",
    "TBE Customer and JV Partner directed", "Piping", "Electrical Panel",
    "IMM Spares", "IMM Auxiliary", "Jigs and Fixtures", "MHE",
    "Wire Extruder and Auxiliary Equip", "SPM", "AMC",
    "Storage Equipment Bin", "Calibration",
    "Fire Safety and Alarm System", "TBE Negotiated by MCO Tooling",
    "Treatment Plant",
]

_CATEGORY_CACHE: dict = {}            # {tgt_cs: (timestamp, list[str])}
_CATEGORY_TTL_SECONDS = 300           # 5 min — extractions don't need fresher


def _resolve_category_list(tgt_cs: str) -> list[str]:
    """Returns the seed list UNION every distinct purchase_category_llm
    previously persisted by V2. Cached in-process for 5 min so the LLM call
    doesn't hammer the DB on every PR."""
    now = time.time()
    cached = _CATEGORY_CACHE.get(tgt_cs)
    if cached and now - cached[0] < _CATEGORY_TTL_SECONDS:
        return cached[1]
    db_categories: list[str] = []
    try:
        conn = _connect(tgt_cs)
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT [purchase_category_llm]
                  FROM [ras_procurement].[quotation_extracted_items]
                 WHERE [purchase_category_llm] IS NOT NULL
                   AND LTRIM(RTRIM([purchase_category_llm])) <> ''
            """)
            db_categories = [r[0] for r in cur.fetchall() if r and r[0]]
        finally:
            conn.close()
    except Exception as exc:
        logger.warning(f"V2 _resolve_category_list — DB lookup failed: {exc}")
    seed = {c.strip() for c in PURCHASE_CATEGORIES_SEED if c and c.strip()}
    dbset = {str(c).strip() for c in db_categories if c and str(c).strip()}
    combined = sorted(seed | dbset, key=str.lower)
    _CATEGORY_CACHE[tgt_cs] = (now, combined)
    return combined


def _normalise_category(value: Any, allowed: list[str]) -> str:
    """Snap an LLM-emitted category to the canonical case from `allowed`
    when a case-insensitive match exists; otherwise return the stripped
    raw value (treated as a newly-proposed category)."""
    if not value:
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    lookup = {c.lower(): c for c in allowed}
    return lookup.get(raw.lower(), raw)


# ── V2 extraction prompts ───────────────────────────────────────────────────

EXTRACTION_SYSTEM_PROMPT_V2 = (
    EXTRACTION_SYSTEM_PROMPT
    + """

V2 — additional REQUIREMENTS for this extraction (override anything above
that conflicts):

1. purchase_category_llm — pick exactly one category from the canonical list
   provided in the user message. If NO category on the list reasonably fits
   the item, propose a new short Title-Case noun-phrase category. Minor
   wording differences are NOT a reason to invent (e.g. "IMM Machine" → use
   the existing "IMM"). Required, never null.

2. embed_content — the ONLY text downstream embeds for similarity search.
   Two different supplier quotations of the same product MUST produce
   identical embed_content (modulo trivial whitespace) so they collide in
   the vector index. RULES:
     • NEVER include supplier / vendor / distributor name
     • NEVER include brand or manufacturer name (brand stays in item_level_4)
     • NEVER include quotation reference / supplier code / document id
     • DO include: product category, every numeric spec with canonical
       units (T for tonnage, KVA for capacity, TR for cooling tons, KW for
       power, GB for storage, V for voltage, KG for weight, MM/M for
       length, MONTHS for term, HOURS for hour-based services), configuration
       variants (3-phase, vertical/horizontal, IP rating), grade/standard
       codes (IS, ASTM, ISO).
     • One run-on noun phrase. 30-80 words. No marketing language.

3. critical_attributes — 1 to 5 entries. The price-driving attributes for
   THIS item, each as {name, value, unit, importance}. Importance levels:
     • "critical"      — single biggest price driver (tonnage for IMM,
                          cooling_capacity_tr for AC, storage_gb for laptop)
     • "important"     — secondary spec that shifts price
     • "informational" — diagnostic but not directly priced
   Always extract numbers — never "1000T", always {"value": 1000, "unit": "T"}.
   Use canonical lowercase snake_case names so identical specs across PRs
   collide on the same key (tonnage, capacity_kva, cooling_capacity_tr,
   power_kw, voltage_v, storage_gb, ram_gb, cpu_cores, area_sqft,
   contract_months, purity_pct, pack_size_kg, ...).
"""
)

# The V2 user template appends a Canonical Categories block + the 4 new
# JSON fields to the original. We keep the original placeholders intact so
# downstream code that already passes those keys doesn't break.
EXTRACTION_USER_TEMPLATE_V2 = EXTRACTION_USER_TEMPLATE + """

---

## Canonical Purchase Categories (V2)

Pick the single best `purchase_category_llm` for EACH line item from the
list below. Emit the value verbatim from this list when chosen (case and
spacing preserved). If NO category from the list reasonably fits, propose
a new short Title-Case noun phrase.

{category_list}

---

## Additional V2 fields — append to EACH item in the JSON output

In addition to every field already specified in the schema above, every
item MUST include the three V2 fields below. Required. Never null.

  "purchase_category_llm": "<one of the canonical categories, or a new short noun phrase>",

  "embed_content": "<30-80 word brand-free / supplier-free canonical
                     description focused only on price-driving attributes
                     and specs — see system prompt rules>",

  "critical_attributes": [
      {{"name": "<canonical_snake_case>", "value": <number>, "unit": "<canonical>", "importance": "critical|important|informational"}}
  ]

Return the SAME single JSON object specified in the original Required Output
section — just with these 3 extra fields on each item.
"""


# ── V2 rank prompts (Stage C) ───────────────────────────────────────────────

RANK_PROMPT_SYSTEM_V2 = """You are a procurement-benchmark relevance ranker. You receive one source item (a line from a new purchase requisition being benchmarked) and N candidate items (historical purchases retrieved by SQL + vector search). Your job: pick the K candidates most price-relevant to the source.

Apply these rules in order:

1. CRITICAL ATTRIBUTES ARE BARRIERS.
   For every attribute on the source with importance="critical", compare the
   candidate's same-name attribute. If the numeric value differs by more than
   {critical_pct}% (or a string value is not equal), REJECT the candidate
   regardless of vector similarity. List the rejection under "rejected".

2. IMPORTANT ATTRIBUTES ARE PENALTIES.
   For every "important" attribute mismatch of more than {important_pct}%,
   lower the candidate's rank. Two important mismatches start to be
   disqualifying.

3. SPEC-TO-PRICE RATIO SANITY.
   When the source has at least one critical numeric attribute, compute
   ratio = candidate.unit_price_eur / candidate.<that_attribute>. If a
   candidate's ratio is outside the band [median_ratio * (1 - {ratio_pct}/100),
   median_ratio * (1 + {ratio_pct}/100)] of the surviving pool, downrank
   (likely unit confusion, typo, or category drift).

4. BRAND SIMILARITY IS NOT RELEVANCE.
   Same brand alone is not a relevance boost. Specs first.

5. RECENCY TIE-BREAK.
   Among candidates that survive rules 1-3, prefer more recently created PRs.

Return exactly K survivors as `selected`, with a short `reason` per row.
Every rejected candidate goes under `rejected` with the rejecting rule.

Output strict JSON, no fences, no prose:

{{
  "selected": [
    {{"purchase_dtl_id": <int>, "rank": 1, "reason": "<short>"}},
    ...
  ],
  "rejected": [
    {{"purchase_dtl_id": <int>, "reason": "<short>"}},
    ...
  ]
}}
"""


RANK_PROMPT_USER_V2 = """Pick the top {top_k} most benchmark-relevant candidates for the SOURCE item below.

## SOURCE ITEM

```json
{source_json}
```

## CANDIDATES ({n_candidates} retrieved by SQL+Pinecone)

```json
{candidates_json}
```

Return strict JSON as specified in the system prompt. critical_pct={critical_pct}, important_pct={important_pct}, ratio_pct={ratio_pct}.
"""


# ── V2 extraction pipeline (Stage 5) ────────────────────────────────────────

def _build_extraction_user_prompt_v2(
    ctx: RASContext,
    doc: DocumentContent,
    prompts: dict | None = None,
    category_list: list[str] | None = None,
) -> str:
    """V2 version that injects {category_list} into the V2 template.

    Mirrors V1's _build_extraction_user_prompt but uses EXTRACTION_USER_TEMPLATE_V2
    (or an override from prompts["ext_user_v2"]).
    """
    def _f(v): return str(v) if v is not None else "N/A"

    if doc.ocr_source and doc.text:
        doc_content_str = f"[OCR markdown from Azure Document Intelligence]\n\n{doc.text}"
    elif doc.images and doc.text:
        doc_content_str = f"[Extracted text — page images attached below]\n\n{doc.text}"
    elif doc.images:
        doc_content_str = "[Scanned document — page image(s) attached]"
    else:
        doc_content_str = doc.text or "[No content extracted]"

    ext_max = (prompts or {}).get("ext_max_chars")
    if ext_max and doc_content_str and len(doc_content_str) > ext_max:
        doc_content_str = doc_content_str[:ext_max]

    cat_list = category_list or PURCHASE_CATEGORIES_SEED
    cat_bullet = "\n".join(f"  - {c}" for c in cat_list)

    user_tmpl = (prompts or {}).get("ext_user_v2", EXTRACTION_USER_TEMPLATE_V2)
    return user_tmpl.format(
        purchase_req_no=_f(ctx.purchase_req_no),
        purchase_req_id=_f(ctx.purchase_req_id),
        justification=_f(ctx.justification),
        supplier_name=_f(ctx.supplier_name),
        currency=_f(ctx.currency),
        enquiry_no=_f(ctx.enquiry_no),
        classification=_f(ctx.classification),
        department=_f(ctx.department),
        negotiated_by=_f(ctx.negotiated_by),
        address=_f(ctx.address),
        contract_no=_f(ctx.contract_no),
        order_no=_f(ctx.order_no),
        purchase_value=_f(ctx.purchase_value),
        category=_f(ctx.category),
        sub_category=_f(ctx.sub_category),
        l3=_f(ctx.l3),
        l4=_f(ctx.l4),
        purchase_category=_f(ctx.purchase_category),
        ras_title=_f(ctx.ras_title),
        site_region=_f(ctx.site_region),
        site_country=_f(ctx.site_country),
        site=_f(ctx.site),
        division=_f(ctx.division),
        requisition_type=_f(ctx.requisition_type),
        parent_supplier=_f(ctx.parent_supplier),
        supplier_type=_f(ctx.supplier_type),
        supplier_country=_f(ctx.supplier_country),
        payment_days=_f(ctx.payment_days),
        po_date=_f(ctx.po_date),
        category_buyer=_f(ctx.category_buyer),
        line_items_table=_build_line_items_table(ctx),
        item_taxonomy=(prompts or {}).get("ext_taxonomy", ITEM_TAXONOMY),
        document_content=doc_content_str,
        raw_ras_context=_build_raw_context(ctx),
        category_list=cat_bullet,
    )


def _call_extraction_llm_v2(
    llm,
    ctx: RASContext,
    doc: DocumentContent,
    prompts: dict | None,
    category_list: list[str],
) -> str:
    """Mirrors V1 _call_extraction_llm but uses the V2 system + user prompts."""
    from langchain_core.messages import HumanMessage, SystemMessage
    user_prompt = _build_extraction_user_prompt_v2(ctx, doc, prompts, category_list)
    sys_prompt  = (prompts or {}).get("ext_system_v2", EXTRACTION_SYSTEM_PROMPT_V2)
    max_images  = max(1, int((prompts or {}).get("ext_max_images", 50)))

    messages: list = [SystemMessage(content=sys_prompt)]
    img_count, img_detail = 0, "n/a"
    if doc.is_image_based and doc.images:
        if len(doc.images) > max_images:
            logger.warning(
                "[V2 PR={}] Quotation has {} image(s) but max_images={} — truncating to first {}",
                ctx.purchase_req_no, len(doc.images), max_images, max_images,
            )
        images = doc.images[:max_images]
        img_count  = len(images)
        img_detail = "low" if doc.text else "high"
        content_parts: list = [{"type": "text", "text": user_prompt}]
        for b64 in images:
            content_parts.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}", "detail": img_detail},
            })
        messages.append(HumanMessage(content=content_parts))
    else:
        messages.append(HumanMessage(content=user_prompt))

    logger.info(
        "[V2 PR={}] Calling extraction LLM — {} image(s) detail={}, ~{:.0f} kB prompt",
        ctx.purchase_req_no, img_count, img_detail, len(user_prompt) / 1024,
    )
    response = _call_llm_with_retry(llm, messages, prompts, force_json=True)
    return (getattr(response, "content", None) or str(response)).strip()


# ── V2 response parsing (handles 4 new fields + brand-leak check) ───────────

def _strip_brand_from_embed_content(text: str, brand_tokens: list[str]) -> tuple[str, list[str]]:
    """Best-effort regex strip of brand tokens from embed_content. Returns
    (cleaned_text, removed_tokens). Case-insensitive, whole-word match."""
    if not text:
        return text, []
    removed: list[str] = []
    out = text
    for tok in brand_tokens:
        if not tok or len(tok.strip()) < 2:
            continue
        pat = re.compile(rf"\b{re.escape(tok.strip())}\b", re.IGNORECASE)
        if pat.search(out):
            out = pat.sub("", out)
            removed.append(tok.strip())
    if removed:
        out = re.sub(r"\s+", " ", out).strip(" ,.")
    return out, removed


def _coerce_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = re.search(r"-?\d+(?:\.\d+)?", v)
        if m:
            try:
                return float(m.group(0))
            except Exception:
                return None
    return None


def _validate_critical_attributes(raw: Any) -> list[dict]:
    """Coerce LLM-emitted critical_attributes into the canonical schema.
    Drops malformed entries; never raises."""
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip().lower()
        if not name:
            continue
        val = _coerce_number(entry.get("value"))
        if val is None:
            # allow string-valued informational attrs (e.g. edition="Pro")
            sv = entry.get("value")
            if sv is None:
                continue
            value: Any = str(sv).strip()
        else:
            value = val
        unit = str(entry.get("unit") or "").strip()
        importance = str(entry.get("importance") or "important").strip().lower()
        if importance not in ("critical", "important", "informational"):
            importance = "important"
        out.append({"name": name, "value": value, "unit": unit, "importance": importance})
    return out


def _parse_extraction_response_v2(
    raw: str,
    source: dict,
    ctx: RASContext,
    allowed_categories: list[str],
) -> list:
    """V1's parser handles the standard fields; this wrapper adds the 4 V2
    fields with validation + brand-leak scrubbing. We delegate the heavy
    parsing to V1's _parse_extraction_response (single source of truth for
    the base schema) and then enrich each item."""
    # Lazy import to avoid a circular at module-load time.
    from agentcore_components.pipeline_stage_123 import _parse_extraction_response
    base_items = _parse_extraction_response(raw, source, ctx)
    if not base_items:
        return base_items

    # The original raw is JSON — re-parse to pick up V2-only fields per item.
    raw_clean = raw.strip()
    raw_clean = re.sub(r"^```(?:json)?\s*", "", raw_clean)
    raw_clean = re.sub(r"\s*```$", "", raw_clean)
    try:
        envelope = json.loads(raw_clean)
        raw_items = envelope.get("items", []) if isinstance(envelope, dict) else []
    except Exception as exc:
        logger.warning(f"[V2] Could not re-parse raw JSON for V2 fields: {exc}")
        raw_items = []

    raw_by_dtl: dict = {}
    for r in raw_items:
        if isinstance(r, dict):
            dtl = r.get("purchase_dtl_id")
            if dtl is not None:
                try:
                    raw_by_dtl[int(dtl)] = r
                except Exception:
                    pass

    for item in base_items:
        try:
            dtl = int(item.get("purchase_dtl_id") or 0)
        except Exception:
            dtl = 0
        v2_src = raw_by_dtl.get(dtl, {}) if dtl else {}

        # 1. purchase_category_llm — snap to canonical when case-insensitive match
        cat_raw = v2_src.get("purchase_category_llm")
        cat_canon = _normalise_category(cat_raw, allowed_categories)
        if not cat_canon:
            # Defensive fallback — use raw RAS purchase_category, then "Others"
            cat_canon = (str(ctx.purchase_category).strip()
                         if getattr(ctx, "purchase_category", None) else "")
            cat_canon = cat_canon or "Others"
            logger.warning(
                f"[V2 PR={ctx.purchase_req_no}] dtl_id={dtl} missing purchase_category_llm — "
                f"defaulting to {cat_canon!r}"
            )
        item["purchase_category_llm"] = cat_canon

        # 2. embed_content — brand-leak scrub
        embed_text = (v2_src.get("embed_content") or "").strip()
        brand_tokens = []
        for f in ("item_level_4", "item_level_5", "supplier_name"):
            t = item.get(f) or v2_src.get(f)
            if t and isinstance(t, str):
                brand_tokens.extend(re.findall(r"[A-Za-z][A-Za-z0-9\-]+", t))
        if embed_text:
            cleaned, removed = _strip_brand_from_embed_content(embed_text, brand_tokens)
            if removed:
                logger.warning(
                    f"[V2 PR={ctx.purchase_req_no}] dtl_id={dtl} embed_content brand-leak scrubbed: "
                    f"removed {removed}"
                )
            embed_text = cleaned
        if not embed_text:
            # Fallback so embeddings still happen on partially-filled rows.
            embed_text = _build_embed_text({**item, **v2_src})
            logger.warning(
                f"[V2 PR={ctx.purchase_req_no}] dtl_id={dtl} no embed_content emitted — "
                f"falling back to 12-field concat"
            )
        item["embed_content"] = embed_text

        # 3. critical_attributes — coerce/validate
        item["critical_attributes"] = _validate_critical_attributes(v2_src.get("critical_attributes"))

    return base_items


# ── V2 persist (adds 4 new columns to the INSERT) ───────────────────────────

def _save_extracted_items_v2(tgt_cs: str, items: list, fallback_date=None) -> int:
    """V2's writer — same shape as V1 but adds 4 columns."""
    if not items:
        return 0
    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    saved = 0
    _COL_LIMITS = {
        "item_description": 4000, "item_summary": 4000,
        "taxation_details": 2000, "payment_terms": 2000,
        "supplier_address": 500,  "item_name": 500,
        "supplier_name": 255,     "supplier_country": 100,
        "quotation_ref_no": 100,  "commodity_tag": 255,
        "unit": 50,               "purchase_category_llm": 200,
    }
    try:
        for item in items:
            def _v(k, cast=None):
                v = item.get(k)
                if v is None: return None
                try:
                    v = cast(v) if cast else v
                except Exception:
                    return None
                if isinstance(v, str) and k in _COL_LIMITS:
                    limit = _COL_LIMITS[k]
                    if len(v) > limit:
                        logger.warning("V2 truncating '{}': {} → {} chars", k, len(v), limit)
                        v = v[:limit]
                return v
            def _d(k):
                v = item.get(k)
                if v is None: return None
                try: return Decimal(str(v))
                except Exception: return None
            def _date(k):
                v = item.get(k)
                if not v: return None
                try:
                    from datetime import date as date_cls
                    if isinstance(v, date_cls): return v
                    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(v))
                    if m: return date_cls(int(m[1]), int(m[2]), int(m[3]))
                except Exception: pass
                return None

            unit_price_eur  = _convert_to_eur(tgt_cs, item.get("unit_price"),  item.get("currency"), fallback_date)
            total_price_eur = _convert_to_eur(tgt_cs, item.get("total_price"), item.get("currency"), fallback_date)

            crit_attrs = item.get("critical_attributes") or []
            critical_attributes_json = json.dumps(crit_attrs, ensure_ascii=False) if crit_attrs else None

            cur.execute("""
                INSERT INTO [ras_procurement].[quotation_extracted_items] (
                    [attachment_classify_fk],[embedded_classify_fk],[purchase_dtl_id],
                    [is_selected_quote],[supplier_match_conf],[quote_rank],
                    [supplier_name],[supplier_address],[supplier_country],
                    [quotation_ref_no],[quotation_date],[currency],
                    [validity_date],[validity_days],[payment_terms],
                    [item_name],[item_description],[quantity],[unit],
                    [unit_price],[total_price],[discount],[taxation_details],
                    [delivery_date],[delivery_time_days],
                    [item_level_1],[item_level_2],[item_level_3],[item_level_4],
                    [item_level_5],[item_level_6],[item_level_7],[item_level_8],
                    [commodity_tag],[item_summary],
                    [unit_price_eur],[total_price_eur],
                    [purchase_category_llm],[embed_content],[critical_attributes]
                ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
            """,
                _v("attachment_classify_fk"), _v("embedded_classify_fk"),
                _v("purchase_dtl_id", int), int(item.get("is_selected_quote") or 0),
                _d("supplier_match_conf"), _v("quote_rank", int),
                _v("supplier_name"), _v("supplier_address"), _v("supplier_country"),
                _v("quotation_ref_no"), _date("quotation_date"), _v("currency"),
                _date("validity_date"), _v("validity_days", int), _v("payment_terms"),
                _v("item_name"), _v("item_description"), _d("quantity"), _v("unit"),
                _d("unit_price"), _d("total_price"), _d("discount"), _v("taxation_details"),
                _date("delivery_date"), _v("delivery_time_days", int),
                _v("item_level_1"), _v("item_level_2"), _v("item_level_3"), _v("item_level_4"),
                _v("item_level_5"), _v("item_level_6"), _v("item_level_7"), _v("item_level_8"),
                _v("commodity_tag"), _v("item_summary"),
                unit_price_eur, total_price_eur,
                _v("purchase_category_llm"), _v("embed_content"),
                critical_attributes_json,
            )
            saved += 1
        conn.commit()
    except Exception as exc:
        logger.opt(exception=True).error(
            "V2 DB INSERT into quotation_extracted_items failed after {} row(s): {}",
            saved, exc,
        )
        try: conn.rollback()
        except Exception as rb_exc: logger.warning("V2 rollback failed: {}", rb_exc)
        raise
    finally:
        conn.close()
    return saved


# ── V2 Stage 5 orchestrator ─────────────────────────────────────────────────

def _run_extraction_v2(llm, tgt_cs: str, blob_cfg: dict, pr_no: str, prompts: dict | None = None) -> int:
    """V2 extraction: identical structure to V1 _run_extraction but uses the
    V2 prompt, parser, and saver. Same error-raising contract (ExtractionAbortError
    subclasses) so the orchestrator's exception handling is unchanged."""
    ctx = _build_ras_context(tgt_cs, pr_no)
    if ctx is None:
        raise NoRASContextError(
            f"No purchase_req_mst row for PR={pr_no!r} — cannot extract."
        )
    if not ctx.line_items:
        raise NoLineItemsError(
            f"PR={pr_no!r} has no rows in purchase_req_detail — nothing to extract against."
        )
    sources = _resolve_quotation_sources(tgt_cs, pr_no)
    if not sources:
        raise NoQuotationFoundError(
            f"No quotation documents found for PR={pr_no!r}."
        )

    category_list = _resolve_category_list(tgt_cs)
    n_workers = max(1, int((prompts or {}).get("ext_parallel_sources", 1)))
    max_pages = max(1, int((prompts or {}).get("ext_max_pages", 20)))

    def _extract_one(src: dict) -> list[dict]:
        blob_path = src["blob_path"]
        filename  = os.path.basename(blob_path)
        try:
            file_bytes = _download_blob(blob_path, blob_cfg)
            doc        = _load_document(file_bytes, filename, max_pages=max_pages)
            raw        = _call_extraction_llm_v2(llm, ctx, doc, prompts, category_list)
            items      = _parse_extraction_response_v2(raw, src, ctx, category_list)
            items      = _align_to_ras_line_items(items, ctx, src)
            logger.info(
                "[V2 {}] Extracted {} item(s) from {!r}", pr_no, len(items), filename
            )
            return items
        except Exception as exc:
            logger.opt(exception=True).warning(
                "[V2 {}] Extraction failed for {!r}: {}", pr_no, filename, exc
            )
            return []

    if n_workers == 1:
        all_items: list[dict] = []
        for src in sources:
            all_items.extend(_extract_one(src))
    else:
        import concurrent.futures
        logger.info(f"[V2 {pr_no}] Extracting {len(sources)} source(s) with {n_workers} parallel worker(s)")
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as pool:
            results = list(pool.map(_extract_one, sources))
        all_items = [item for batch in results for item in batch]

    if not all_items:
        raise AllExtractionsFailedError(
            f"All {len(sources)} quotation source(s) for PR={pr_no!r} failed extraction."
        )

    _apply_price_alignment_boost(all_items, ctx, prompts)
    _canonicalize_supplier_names(all_items, ctx, prompts)
    _compute_quote_ranks(all_items)
    _select_best_quotes(all_items, ctx)
    fallback_date = ctx.req_start_date or ctx.c_datetime
    saved = _save_extracted_items_v2(tgt_cs, all_items, fallback_date=fallback_date)
    logger.info(f"[V2 {pr_no}] {saved} item(s) written to quotation_extracted_items")
    return saved


# ── V2 Stage 6: embeddings using embed_content ───────────────────────────────

def _run_embeddings_v2(tgt_cs: str, pr_no: str, embed_model, pinecone_index: str, pinecone_ns: str) -> None:
    """Same shape as V1 _run_embeddings, but uses embed_content (LLM-crafted)
    instead of the 12-field concat, and writes richer metadata so the V2
    benchmark can filter by purchase_category_llm at search time."""
    from agentcore.services.pinecone_service_client import ensure_index_via_service, ingest_via_service
    ensure_index_via_service(index_name=pinecone_index, embedding_dimension=3072)

    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT qi.[extracted_item_uuid_pk], qi.[purchase_dtl_id],
                   qi.[item_name], qi.[item_description], qi.[item_summary],
                   qi.[item_level_1], qi.[item_level_2], qi.[item_level_3],
                   qi.[item_level_4], qi.[item_level_5], qi.[item_level_6],
                   qi.[item_level_7], qi.[item_level_8], qi.[commodity_tag],
                   qi.[purchase_category_llm], qi.[embed_content],
                   prd.[C_DATETIME] AS [item_created_date]
              FROM [ras_procurement].[quotation_extracted_items] qi
              JOIN [ras_procurement].[attachment_classification] ac
                ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
              JOIN [ras_procurement].[ras_tracker] rt
                ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
              LEFT JOIN [ras_procurement].[purchase_req_detail] prd
                ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
             WHERE rt.[purchase_req_no] = ?
               AND qi.[is_selected_quote] = 1
               AND qi.[purchase_dtl_id] IS NOT NULL
        """, pr_no)
        all_rows = cur.fetchall()
    finally:
        conn.close()

    cols = [
        "extracted_item_uuid_pk", "purchase_dtl_id",
        "item_name", "item_description", "item_summary",
        "item_level_1", "item_level_2", "item_level_3", "item_level_4",
        "item_level_5", "item_level_6", "item_level_7", "item_level_8",
        "commodity_tag", "purchase_category_llm", "embed_content",
        "item_created_date",
    ]
    seen: dict = {}
    for row in all_rows:
        rd = dict(zip(cols, row))
        dtl_id = rd["purchase_dtl_id"]
        if dtl_id not in seen:
            seen[dtl_id] = rd
    rows = list(seen.values())
    logger.info(
        f"[V2 {pr_no}] Embedding {len(rows)} selected item(s) (deduped from {len(all_rows)})"
    )

    for rd in rows:
        dtl_id    = rd["purchase_dtl_id"]
        item_uuid = rd["extracted_item_uuid_pk"]
        created   = rd.get("item_created_date")
        created_iso = (
            created.isoformat() if created and hasattr(created, "isoformat")
            else str(created or "")
        )
        # Prefer LLM-crafted embed_content; fall back to V1 12-field concat
        # only when the row predates the schema change.
        content = (rd.get("embed_content") or "").strip()
        if not content:
            content = _build_embed_text(rd)
            logger.warning(
                f"[V2 {pr_no}] dtl_id={dtl_id}: no embed_content — fallback to 12-field concat"
            )
        if not content:
            logger.warning(f"[V2 {pr_no}] dtl_id={dtl_id}: no text to embed, skipping")
            continue
        try:
            embedding = embed_model.embed_query(content)
            ingest_via_service(
                index_name=pinecone_index,
                namespace=pinecone_ns,
                text_key="page_content",
                documents=[{
                    "page_content": content,
                    "metadata": {
                        "purchase_req_no":        pr_no,
                        "purchase_dtl_id":        int(dtl_id),
                        "extracted_item_uuid_pk": str(item_uuid or ""),
                        "commodity_tag":          str(rd.get("commodity_tag") or ""),
                        "item_created_date":      created_iso,
                        "purchase_category_llm":  str(rd.get("purchase_category_llm") or ""),
                        "item_level_1":           str(rd.get("item_level_1") or ""),
                        "item_level_2":           str(rd.get("item_level_2") or ""),
                    },
                }],
                embedding_vectors=[embedding],
                vector_ids=[f"dtl_{dtl_id}"],
                embedding_dimension=3072,
            )
            logger.info(f"[V2 {pr_no}] Upserted vector dtl_{dtl_id} (item_created={created_iso})")
        except Exception as exc:
            logger.warning(f"[V2 {pr_no}] Embedding failed for dtl_id {dtl_id}: {exc}")


# ── V2 Stage 7: 3-stage benchmark ───────────────────────────────────────────

def _sql_pool_by_category(
    tgt_cs: str,
    src_category: str,
    src_created_date: Any,
    pool_size: int,
) -> list[int]:
    """Stage A — Top N historical dtl_ids in the same V2 category, oldest-than-source,
    ordered by C_DATETIME DESC (newest first). Empty if no peers."""
    if not src_category:
        return []
    conn = _connect(tgt_cs)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP (?) qi.[purchase_dtl_id]
              FROM [ras_procurement].[quotation_extracted_items] qi
              LEFT JOIN [ras_procurement].[purchase_req_detail] prd
                ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
             WHERE qi.[is_selected_quote] = 1
               AND qi.[purchase_category_llm] = ?
               AND (prd.[C_DATETIME] IS NULL OR prd.[C_DATETIME] < ?)
               AND qi.[unit_price_eur] IS NOT NULL
             ORDER BY prd.[C_DATETIME] DESC
        """, pool_size, src_category, src_created_date)
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _widen_pool_by_levels(
    tgt_cs: str,
    l1: str,
    l2: str,
    src_created_date: Any,
    pool_size: int,
) -> list[int]:
    """Sparse-pool fallback — same as above but matches on item_level_1 + 2
    instead of LLM category. Used when the strict category filter yields < 20."""
    if not (l1 and l2):
        return []
    conn = _connect(tgt_cs)
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP (?) qi.[purchase_dtl_id]
              FROM [ras_procurement].[quotation_extracted_items] qi
              LEFT JOIN [ras_procurement].[purchase_req_detail] prd
                ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
             WHERE qi.[is_selected_quote] = 1
               AND qi.[item_level_1] = ?
               AND qi.[item_level_2] = ?
               AND (prd.[C_DATETIME] IS NULL OR prd.[C_DATETIME] < ?)
               AND qi.[unit_price_eur] IS NOT NULL
             ORDER BY prd.[C_DATETIME] DESC
        """, pool_size, l1, l2, src_created_date)
        return [int(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def _pinecone_narrow_within_pool(
    embedding: list[float],
    candidate_dtl_ids: list[int],
    top_k: int,
    pinecone_index: str,
    pinecone_ns: str,
) -> list[dict]:
    """Stage B — Pinecone similarity restricted to the SQL pool by dtl_id.
    Empty pool → empty result; caller falls back to passing the raw SQL pool
    to Stage C."""
    if not candidate_dtl_ids:
        return []
    from agentcore.services.pinecone_service_client import search_via_service
    try:
        raw = search_via_service(
            index_name=pinecone_index,
            namespace=pinecone_ns,
            text_key="page_content",
            query="",
            query_embedding=embedding,
            number_of_results=max(1, min(top_k, len(candidate_dtl_ids))),
            filter={"purchase_dtl_id": {"$in": candidate_dtl_ids}},
        )
    except Exception as exc:
        if _is_pinecone_server_error(exc):
            raise
        logger.warning(f"V2 Pinecone within-pool search failed (non-server): {exc}")
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return raw.get("matches", []) or raw.get("results", []) or []
    return []


def _fetch_candidate_snapshots(tgt_cs: str, dtl_ids: list[int]) -> list[dict]:
    """Pull a compact snapshot of each candidate row for the rank LLM."""
    if not dtl_ids:
        return []
    placeholders = ",".join("?" * len(dtl_ids))
    conn = _connect(tgt_cs)
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT qi.[purchase_dtl_id], qi.[item_name], qi.[item_description],
                   qi.[purchase_category_llm], qi.[commodity_tag],
                   qi.[critical_attributes],
                   qi.[unit_price_eur], qi.[currency], qi.[unit_price],
                   qi.[unit], qi.[quantity],
                   prd.[C_DATETIME] AS [item_created_date]
              FROM [ras_procurement].[quotation_extracted_items] qi
              LEFT JOIN [ras_procurement].[purchase_req_detail] prd
                ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
             WHERE qi.[purchase_dtl_id] IN ({placeholders})
        """, *dtl_ids)
        cols = ["purchase_dtl_id","item_name","item_description","purchase_category_llm",
                "commodity_tag","critical_attributes","unit_price_eur","currency",
                "unit_price","unit","quantity","item_created_date"]
        rows = []
        for r in cur.fetchall():
            rd = dict(zip(cols, r))
            ca = rd.get("critical_attributes")
            if isinstance(ca, str) and ca.strip():
                try: rd["critical_attributes"] = json.loads(ca)
                except Exception: rd["critical_attributes"] = []
            else:
                rd["critical_attributes"] = []
            for k in ("unit_price_eur","unit_price","quantity"):
                v = rd.get(k)
                if v is not None:
                    try: rd[k] = float(v)
                    except Exception: rd[k] = None
            d = rd.get("item_created_date")
            if d and hasattr(d, "isoformat"):
                rd["item_created_date"] = d.isoformat()
            rows.append(rd)
        return rows
    finally:
        conn.close()


def _llm_rank_candidates(
    llm,
    source_item: dict,
    candidates: list[dict],
    top_k: int,
    prompts: dict | None,
) -> tuple[list[dict], list[dict]]:
    """Stage C — Ask the LLM to rank candidates by relevance. Returns
    (selected, rejected) lists. On any failure returns ([], []) so the
    caller can fall back to vector-rank order."""
    if not candidates:
        return [], []
    from langchain_core.messages import HumanMessage, SystemMessage
    p = prompts or {}
    sys_tmpl  = p.get("bench_rank_prompt_system_v2", RANK_PROMPT_SYSTEM_V2)
    user_tmpl = p.get("bench_rank_prompt_user_v2",   RANK_PROMPT_USER_V2)
    crit_pct  = int(p.get("bench_critical_threshold_pct",  25))
    imp_pct   = int(p.get("bench_important_threshold_pct", 20))
    ratio_pct = int(p.get("bench_ratio_band_pct",          50))
    sys_prompt  = sys_tmpl.format(
        critical_pct=crit_pct, important_pct=imp_pct, ratio_pct=ratio_pct
    )
    user_prompt = user_tmpl.format(
        top_k=top_k,
        n_candidates=len(candidates),
        source_json=json.dumps(source_item, ensure_ascii=False, default=str),
        candidates_json=json.dumps(candidates, ensure_ascii=False, default=str),
        critical_pct=crit_pct, important_pct=imp_pct, ratio_pct=ratio_pct,
    )
    try:
        response = _call_llm_with_retry(
            llm, [SystemMessage(content=sys_prompt), HumanMessage(content=user_prompt)],
            prompts, force_json=True,
        )
        raw = (getattr(response, "content", None) or str(response)).strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)
        selected = parsed.get("selected", []) if isinstance(parsed, dict) else []
        rejected = parsed.get("rejected", []) if isinstance(parsed, dict) else []
        # Normalise ranks
        for i, s in enumerate(selected):
            if isinstance(s, dict):
                s["rank"] = int(s.get("rank", i + 1))
        return selected, rejected
    except Exception as exc:
        logger.warning(f"V2 rank LLM failed: {exc}")
        return [], []


def _build_source_snapshot_for_rank(rd: dict) -> dict:
    """Compact dict the rank LLM receives as the SOURCE."""
    crit = rd.get("critical_attributes")
    if isinstance(crit, str):
        try: crit = json.loads(crit)
        except Exception: crit = []
    return {
        "purchase_dtl_id":       int(rd.get("purchase_dtl_id") or 0),
        "purchase_category_llm": rd.get("purchase_category_llm"),
        "item_name":             rd.get("item_name"),
        "item_description":      rd.get("item_description"),
        "commodity_tag":         rd.get("commodity_tag"),
        "critical_attributes":   crit or [],
        "unit_price":            float(rd["unit_price"]) if rd.get("unit_price") is not None else None,
        "unit_price_eur":        float(rd["unit_price_eur"]) if rd.get("unit_price_eur") is not None else None,
        "unit":                  rd.get("unit"),
        "quantity":              float(rd["quantity"]) if rd.get("quantity") is not None else None,
        "currency":              rd.get("currency"),
    }


def _run_benchmark_v2(
    llm,
    tgt_cs: str,
    pr_no: str,
    embed_model,
    pinecone_index: str,
    pinecone_ns: str,
    top_k_final: int,
    prompts: dict | None = None,
) -> None:
    """Stage 7 V2 — 3-stage retrieval (SQL → Pinecone → LLM rank). Each row
    in benchmark_result records the filter chain used and the LLM's
    rejection reasons (for QA)."""
    from agentcore.services.pinecone_service_client import search_via_service  # noqa: F401  (forces import early)

    p = prompts or {}
    sql_pool_size   = int(p.get("bench_sql_pool_size",      100))
    pinecone_top_k  = int(p.get("bench_pinecone_top_k",     10))
    llm_shortlist   = int(p.get("bench_llm_shortlist_size", max(5, top_k_final)))
    widen_when_sparse = bool(p.get("bench_widen_l1l2_when_sparse", True))
    min_score       = float(p.get("bench_min_similarity",   0.70))   # kept for future use

    # Fetch the source PR's rows
    conn = _connect(tgt_cs)
    cur  = conn.cursor()
    try:
        cur.execute("""
            SELECT qi.[extracted_item_uuid_pk], qi.[purchase_dtl_id],
                   qi.[item_name], qi.[item_description],
                   qi.[purchase_category_llm], qi.[embed_content],
                   qi.[critical_attributes], qi.[commodity_tag],
                   qi.[item_level_1], qi.[item_level_2],
                   qi.[unit_price], qi.[total_price], qi.[quantity], qi.[unit],
                   qi.[currency], qi.[supplier_name],
                   qi.[unit_price_eur], qi.[total_price_eur],
                   prd.[C_DATETIME] AS [item_created_date]
              FROM [ras_procurement].[quotation_extracted_items] qi
              JOIN [ras_procurement].[attachment_classification] ac
                ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
              JOIN [ras_procurement].[ras_tracker] rt
                ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
              LEFT JOIN [ras_procurement].[purchase_req_detail] prd
                ON qi.[purchase_dtl_id] = prd.[PURCHASE_DTL_ID]
             WHERE rt.[purchase_req_no] = ?
               AND qi.[is_selected_quote] = 1
               AND qi.[purchase_dtl_id] IS NOT NULL
        """, pr_no)
        rows = cur.fetchall()
    finally:
        conn.close()

    src_cols = [
        "extracted_item_uuid_pk","purchase_dtl_id","item_name","item_description",
        "purchase_category_llm","embed_content","critical_attributes","commodity_tag",
        "item_level_1","item_level_2",
        "unit_price","total_price","quantity","unit","currency","supplier_name",
        "unit_price_eur","total_price_eur","item_created_date",
    ]

    conn2 = _connect(tgt_cs)
    cur2  = conn2.cursor()
    try:
        for raw_row in rows:
            rd = dict(zip(src_cols, raw_row))
            dtl_id    = int(rd["purchase_dtl_id"])
            item_uuid = str(rd["extracted_item_uuid_pk"] or "")
            src_cat   = (rd.get("purchase_category_llm") or "").strip()
            src_date  = rd.get("item_created_date")

            embed_text = (rd.get("embed_content") or "").strip() or _build_embed_text(rd)
            if not embed_text:
                _write_benchmark_stub(cur2, dtl_id, item_uuid,
                                      "V2 — no embed_content / no descriptor fields")
                continue

            # ── Stage A: SQL pre-filter by category ──────────────────────
            pool = _sql_pool_by_category(tgt_cs, src_cat, src_date, sql_pool_size)
            filter_chain = "sql_category"
            if len(pool) < 20 and widen_when_sparse:
                widened = _widen_pool_by_levels(
                    tgt_cs, rd.get("item_level_1") or "", rd.get("item_level_2") or "",
                    src_date, sql_pool_size,
                )
                # union, preserving original order
                seen_ids = set(pool)
                for did in widened:
                    if did not in seen_ids and len(pool) < sql_pool_size:
                        pool.append(did); seen_ids.add(did)
                if widened:
                    filter_chain += "+widen_l1l2"
                    logger.info(
                        f"[V2 {pr_no}] dtl_id={dtl_id} sparse category pool — "
                        f"widened to L1+L2 added {len(widened)} dtl_ids"
                    )

            if not pool:
                _write_benchmark_stub(cur2, dtl_id, item_uuid,
                                      f"V2 — no peer pool for category {src_cat!r}")
                continue

            # ── Stage B: Pinecone within the SQL pool ────────────────────
            try:
                embedding = embed_model.embed_query(embed_text)
                narrowed = _pinecone_narrow_within_pool(
                    embedding, pool, pinecone_top_k, pinecone_index, pinecone_ns,
                )
                filter_chain += "+pinecone"
            except Exception as exc:
                if _is_pinecone_server_error(exc):
                    raise RuntimeError(
                        f"Pinecone server error during V2 benchmark for PR={pr_no}: {exc}"
                    ) from exc
                logger.warning(
                    f"[V2 {pr_no}] dtl_id={dtl_id} Pinecone within-pool failed: {exc}"
                )
                narrowed = []

            # If Pinecone failed or returned nothing, send the head of the SQL pool
            # straight to Stage C so we still benchmark.
            if narrowed:
                stage_b_dtl_ids = []
                for m in narrowed:
                    meta = (m.get("metadata") if isinstance(m, dict) else {}) or {}
                    did = meta.get("purchase_dtl_id")
                    if did is not None:
                        try: stage_b_dtl_ids.append(int(did))
                        except Exception: continue
            else:
                stage_b_dtl_ids = pool[:pinecone_top_k]
                filter_chain += "_skipped"

            # ── Stage C: LLM relevance rank ──────────────────────────────
            candidates = _fetch_candidate_snapshots(tgt_cs, stage_b_dtl_ids)
            source_snapshot = _build_source_snapshot_for_rank(rd)
            selected, rejected = _llm_rank_candidates(
                llm, source_snapshot, candidates, llm_shortlist, prompts,
            )
            if selected:
                filter_chain += "+llm_rank"
                shortlist_dtl_ids = []
                for s in selected:
                    if not isinstance(s, dict):
                        continue
                    sid = s.get("purchase_dtl_id")
                    if sid is not None:
                        try: shortlist_dtl_ids.append(int(sid))
                        except Exception: continue
            else:
                # Fall back to Pinecone order (or SQL pool head)
                shortlist_dtl_ids = stage_b_dtl_ids[:llm_shortlist]
                filter_chain += "+rank_skipped"

            if not shortlist_dtl_ids:
                _write_benchmark_stub(
                    cur2, dtl_id, item_uuid,
                    f"V2 chain={filter_chain} — no candidates survived ranking. "
                    f"rejected={json.dumps(rejected, ensure_ascii=False)[:1500]}",
                )
                continue

            # ── Stage D: write benchmark_result with the ranked shortlist ─
            similar_dtl_ids_csv = ",".join(str(x) for x in shortlist_dtl_ids)
            top_reason = ""
            if selected and isinstance(selected[0], dict):
                top_reason = str(selected[0].get("reason") or "")[:500]
            summary = (
                f"V2 chain={filter_chain} | shortlist={len(shortlist_dtl_ids)} "
                f"| top: {top_reason}"
            )
            try:
                cur2.execute("""
                    MERGE [ras_procurement].[benchmark_result] WITH (HOLDLOCK) AS target
                    USING (SELECT ? AS purchase_dtl_id) AS src
                       ON target.purchase_dtl_id = src.purchase_dtl_id
                    WHEN MATCHED THEN
                        UPDATE SET extracted_item_uuid_fk=?, similar_dtl_ids=?, summary=?,
                                   updated_at=SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                        INSERT (purchase_dtl_id, extracted_item_uuid_fk,
                                similar_dtl_ids, summary)
                        VALUES (?, ?, ?, ?);
                """,
                    dtl_id, item_uuid, similar_dtl_ids_csv, summary,
                    dtl_id, item_uuid, similar_dtl_ids_csv, summary,
                )
                conn2.commit()
            except Exception as exc:
                logger.warning(f"[V2 {pr_no}] benchmark_result MERGE failed dtl_id={dtl_id}: {exc}")
    finally:
        conn2.close()


# ── V2 Node ─────────────────────────────────────────────────────────────────

class PipelineStage123NodeV2(PipelineStage123Node):
    display_name = "Full Pipeline V2 (LLM Category + Smart Benchmark)"
    name         = "PipelineStage123NodeV2"
    icon         = "Database"
    description  = (
        "V2 of the full procurement pipeline. Stages 1-4 are inherited from V1. "
        "Stage 5 (Extraction) additionally produces purchase_category_llm, "
        "embed_content (brand/supplier-free), and critical_attributes. "
        "Stage 6 (Embeddings) uses embed_content for Pinecone. "
        "Stage 7 (Benchmark) uses 3-stage retrieval: SQL category filter → "
        "Pinecone within pool → LLM relevance rank. Requires the V2 DDL "
        "migration on ras_procurement.quotation_extracted_items."
    )

    inputs = PipelineStage123Node.inputs + [
        # ── V2 benchmark tuning ────────────────────────────────────────────
        IntInput(
            name="bench_sql_pool_size",
            display_name="V2 — SQL Pool Size (Stage A)",
            value=100,
            advanced=True,
            info=(
                "TOP N historical dtl_ids pulled by category match in Stage A. "
                "Default 100 — increase for highly populated categories, lower "
                "for sparse ones."
            ),
        ),
        IntInput(
            name="bench_pinecone_top_k",
            display_name="V2 — Pinecone Top-K (Stage B)",
            value=10,
            advanced=True,
            info=(
                "Top K returned from Pinecone within-pool search. These are "
                "then passed to the LLM rank step."
            ),
        ),
        IntInput(
            name="bench_llm_shortlist_size",
            display_name="V2 — LLM Shortlist Size (Stage C)",
            value=5,
            advanced=True,
            info=(
                "How many candidates the rank LLM keeps after applying "
                "critical-attribute barriers and spec-to-price sanity checks."
            ),
        ),
        BoolInput(
            name="bench_widen_l1l2_when_sparse",
            display_name="V2 — Widen to L1+L2 When Sparse",
            value=True,
            advanced=True,
            info=(
                "True: if the category pool has <20 rows, ALSO accept rows "
                "matching item_level_1 AND item_level_2 (unioned). False: "
                "only use the strict category filter."
            ),
        ),
        IntInput(
            name="bench_critical_threshold_pct",
            display_name="V2 — Critical Attribute Threshold (%)",
            value=25,
            advanced=True,
            info=(
                "A candidate is rejected by Stage C if any \"critical\" attribute "
                "differs from the source by more than this %. Lower = stricter."
            ),
        ),
        IntInput(
            name="bench_important_threshold_pct",
            display_name="V2 — Important Attribute Threshold (%)",
            value=20,
            advanced=True,
            info=(
                "Penalty band for \"important\" attribute mismatches in Stage C "
                "(does not auto-reject)."
            ),
        ),
        IntInput(
            name="bench_ratio_band_pct",
            display_name="V2 — Spec-to-Price Ratio Band (%)",
            value=50,
            advanced=True,
            info=(
                "Stage C downranks candidates whose unit_price / critical_attribute "
                "ratio is outside ±this% of the surviving-pool median."
            ),
        ),
        MultilineInput(
            name="bench_rank_prompt_system_v2",
            display_name="V2 — Stage C Rank System Prompt",
            value="",
            advanced=True,
            info=(
                "Override the system prompt used by the relevance-ranking LLM. "
                "Leave blank to use the built-in default."
            ),
        ),
        MultilineInput(
            name="bench_rank_prompt_user_v2",
            display_name="V2 — Stage C Rank User Prompt Template",
            value="",
            advanced=True,
            info=(
                "Override the user prompt template (with {source_json}, "
                "{candidates_json}, {top_k}, {n_candidates}, {critical_pct}, "
                "{important_pct}, {ratio_pct} placeholders) for the rank LLM."
            ),
        ),
    ]

    def _build_prompts(self) -> dict:
        """Extends V1's prompts dict with V2-specific overrides."""
        prompts = super()._build_prompts()

        # Pass the V2 knobs through so the V2 helpers pick them up.
        for src_attr, key in (
            ("bench_sql_pool_size",            "bench_sql_pool_size"),
            ("bench_pinecone_top_k",           "bench_pinecone_top_k"),
            ("bench_llm_shortlist_size",       "bench_llm_shortlist_size"),
            ("bench_widen_l1l2_when_sparse",   "bench_widen_l1l2_when_sparse"),
            ("bench_critical_threshold_pct",   "bench_critical_threshold_pct"),
            ("bench_important_threshold_pct",  "bench_important_threshold_pct"),
            ("bench_ratio_band_pct",           "bench_ratio_band_pct"),
        ):
            v = getattr(self, src_attr, None)
            if v is not None and v != "":
                prompts[key] = v

        # Multiline overrides — only set when non-empty so defaults still apply.
        sys_override  = (getattr(self, "bench_rank_prompt_system_v2", "") or "").strip()
        user_override = (getattr(self, "bench_rank_prompt_user_v2",   "") or "").strip()
        if sys_override:
            prompts["bench_rank_prompt_system_v2"] = sys_override
        if user_override:
            prompts["bench_rank_prompt_user_v2"] = user_override
        return prompts

    def _run_stages_48(self, pr_no: str, tgt_cs: str, result: dict, prompts: dict) -> None:
        """V2 override of stages 4-8 — Stage 4 inherited unchanged from V1;
        Stages 5/6/7 swapped for the V2 helpers; Stage 8 unchanged."""
        blob_cfg      = self._blob_cfg()
        current_stage = _STAGE_CLASSIFICATION
        top_k         = int(getattr(self, "pinecone_top_k", 5))

        try:
            # Stage 4 — Classification (inherited V1 helper)
            self._safe_log(f"[V2 {pr_no}] Stage 4 — classifying attachments…")
            _run_classification(self.llm, tgt_cs, blob_cfg, pr_no, prompts)
            self._advance_tracker(tgt_cs, pr_no, _STAGE_CLASSIFICATION)
            self._safe_log(f"[V2 {pr_no}] Stage 4 — classification complete")

            # Stage 5 — Extraction (V2)
            current_stage = _STAGE_EXTRACTION
            self._safe_log(f"[V2 {pr_no}] Stage 5 — extracting quotation items (V2)…")
            n_items = _run_extraction_v2(self.llm, tgt_cs, blob_cfg, pr_no, prompts)
            self._advance_tracker(tgt_cs, pr_no, _STAGE_EXTRACTION)
            self._safe_log(f"[V2 {pr_no}] Stage 5 — {n_items} item(s) extracted (V2)")

            # Stage 6 — Embeddings (V2)
            current_stage = _STAGE_EMBEDDINGS
            _run_embeddings_v2(
                tgt_cs, pr_no, self.embed_model,
                self.pinecone_index, self.pinecone_namespace,
            )
            self._advance_tracker(tgt_cs, pr_no, _STAGE_EMBEDDINGS)
            self._safe_log(f"[V2 {pr_no}] Stage 6 — embeddings done (V2)")

            # Stage 7 — Benchmark (V2 3-stage retrieval)
            current_stage = _STAGE_PRICE_BENCHMARK
            _run_benchmark_v2(
                self.llm, tgt_cs, pr_no, self.embed_model,
                self.pinecone_index, self.pinecone_namespace, top_k,
                prompts=prompts,
            )
            self._advance_tracker(tgt_cs, pr_no, _STAGE_PRICE_BENCHMARK)
            self._safe_log(f"[V2 {pr_no}] Stage 7 — benchmark done (V2)")

            # Stage 8 — Complete (inherited deadlock-retry plumbing)
            self._advance_tracker(tgt_cs, pr_no, _STAGE_COMPLETE)
            self._run_with_deadlock_retry(
                lambda: _set_last_processed_at(tgt_cs, pr_no),
                op_name="set_last_processed_at",
                pr_no=pr_no,
            )
            self._safe_log(f"[V2 {pr_no}] Stage 8 — pipeline complete (V2)")
            result["status"] = "complete"

        except ExtractionAbortError as exc:
            # Mirror V1's contract — status format failed_<reason> so downstream
            # telemetry stays consistent across V1 and V2 runs.
            reason = type(exc).__name__
            self._safe_log(f"[V2 {pr_no}] Stage 5 (Extraction) — {reason}: {exc}")
            logger.warning("[V2 {}] Stage 5 (Extraction) — {}: {}", pr_no, reason, exc)
            result["status"] = f"failed_{reason}"
            result["error"]  = f"Stage 5 (Extraction) — {reason}: {exc}"
            self._record_exception(
                tgt_cs, pr_no, _STAGE_EXTRACTION, f"{reason}: {exc}",
            )
        except Exception as exc:
            stage_name = {
                _STAGE_CLASSIFICATION:  "Stage 4 (Classification)",
                _STAGE_EXTRACTION:      "Stage 5 (Extraction)",
                _STAGE_EMBEDDINGS:      "Stage 6 (Embeddings)",
                _STAGE_PRICE_BENCHMARK: "Stage 7 (Benchmark)",
                _STAGE_COMPLETE:        "Stage 8 (Complete)",
            }.get(current_stage, f"Stage {current_stage}")
            logger.opt(exception=True).error(
                "[V2 {}] {} failed: {}", pr_no, stage_name, exc
            )
            result["status"] = "failed_stages_48"
            result["error"]  = f"{stage_name}: {exc}"
            self._record_exception(tgt_cs, pr_no, current_stage, f"{stage_name}: {exc}")
