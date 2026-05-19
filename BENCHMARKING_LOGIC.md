# RAS Procurement Benchmarking System v3 — Complete Logic Documentation

**Version:** v3 (Uses `_run_benchmark_v2` only, from pipeline_stage_123_v3.py)  
**Last Updated:** 2026-05-20

---

## ⚠️ Key Design Decision: PR-Level Exchange Rates

**All quotations within a single PR use the same exchange rate** — determined by the PR's start/creation date, NOT individual quotation dates. This ensures fair price comparison across all suppliers within a PR, even if quotes were issued on different dates.

**Implementation:**
- v3 line 4486: `fallback_date = ctx.req_start_date or ctx.c_datetime`
- Used for all `_convert_to_eur()` calls during extraction (line 4356-4357)
- Query 4 performs lookup using PR date to match this logic

**Why:** Standardization. A PR evaluated on 2022-09-10 should use 2022-09-10 exchange rates for all quotes, regardless of whether quotes were issued on 2022-06-02 or 2022-09-15.

---

## Executive Summary

The v3 benchmarking system provides:
1. **Supplier Hierarchy** — Primary (is_selected_quote=1), Secondary L1 (lowest price), Secondary L2 (2nd lowest)
2. **Specification Capture** — Technical (8-level taxonomy) and Commercial (quote & line-item details)
3. **Historical Benchmarking** — BP (Best Price) and LP (Last Purchase) with dual inflation calculation
4. **Currency & Inflation** — EUR conversion + LLM + CPI inflation for both BP and LP

---

## 1. Supplier Hierarchy

### Primary Supplier (L0)

**Definition:** The ONE supplier selected for each RAS item by the v3 extraction pipeline

**Selection Criteria:** `is_selected_quote = 1`

**Logic:**  
Function `_select_best_quotes()` (line 3905) ranks all suppliers per item by:
1. `supplier_match_conf` — LLM confidence that extracted supplier matches RAS supplier (0-1)
2. `price_fit` — proximity of unit/total price to RAS requisition price
3. `has_price` — whether unit_price is not null

The item with the highest score gets `is_selected_quote = 1`

**Count:** Exactly one per `purchase_dtl_id` (or none if extraction failed)

### Secondary Supplier L1 (Lowest Price Alternative)

**Definition:** Cheapest supplier among all non-selected quotes for the same item

**Selection Criteria:**  
- `is_selected_quote = 0` (NOT the selected one)
- Minimum `unit_price_eur` (or `unit_price` if EUR not available)
- Within same `purchase_dtl_id`

**Logic:** Rank all non-selected items for an item by unit price ascending, pick rank=1

**Availability:** May be NULL if no alternatives exist

### Secondary Supplier L2 (2nd Lowest Price)

**Definition:** Second-cheapest supplier among all non-selected quotes for the same item

**Selection Criteria:**  
- `is_selected_quote = 0`
- Second-minimum `unit_price_eur`
- Within same `purchase_dtl_id`

**Logic:** Rank all non-selected items by unit price ascending, pick rank=2

**Availability:** May be NULL if fewer than 3 suppliers quoted

---

## 2. Technical Specifications

### Taxonomy Structure (item_level_1 through item_level_8)

8-level hierarchical classification assigned by the LLM extraction:

| Level | Meaning | Example |
|-------|---------|---------|
| 1 | Broad Category | Mechanical, Electrical, Software |
| 2 | Product Type | Industrial Equipment, Motor, Cloud Service |
| 3 | Product Subtype | Mould Tooling, Servo Motor, SaaS |
| 4–8 | Specific Features | Cavity count, machine model, version, options |

### Critical Attributes (JSON)

Each item has `critical_attributes` JSON array with structure:

```json
[
  {
    "name": "cavity_count",
    "value": 2.0,
    "unit": "cavity",
    "importance": "critical"
  },
  {
    "name": "machine_model",
    "value": 65.0,
    "unit": "series",
    "importance": "important"
  }
]
```

**Importance Levels:**
- `critical` — Must match (±10% tolerance); mismatch = REJECT in LLM ranking
- `important` — Should match (±20% tolerance); mismatch = downrank
- `informational` — No penalty if different

### Fields in quotation_extracted_items

**Technical Specs:**
- `item_name`, `item_description`, `item_summary`
- `item_level_1` through `item_level_8`
- `critical_attributes` (JSON)
- `commodity_tag`, `purchase_category_llm`

**Quantity & Unit:**
- `unit`, `quantity`

---

## 3. Commercial Specifications

### Quote-Level Details (Applied to Entire Quotation)

| Field | Type | Purpose |
|-------|------|---------|
| `quotation_ref_no` | NVARCHAR | Supplier's quote reference |
| `quotation_date` | DATETIME | Date quotation was issued |
| `validity_date` | DATETIME | Quote expiry |
| `validity_days` | INT | Validity period (days) |
| `payment_terms` | NVARCHAR | Payment conditions (e.g., "Net 30") |
| `delivery_date` | DATETIME | Promised delivery |
| `delivery_time_days` | INT | Lead time (days) |
| `quote_incoterms` | NVARCHAR | Incoterms (FOB, CIF, DDP, etc.) |
| `quote_incoterms_named_place` | NVARCHAR | Port/location for Incoterms |

**Charges & Totals:**
- `quote_subtotal`, `quote_freight`, `quote_insurance`, `quote_customs_duties`, `quote_packing_forwarding`, `quote_installation`, `quote_other_charges`
- `quote_tax_type`, `quote_tax_rate_pct`, `quote_tax_amount_total`
- `quote_grand_total`, `quote_grand_total_currency`

### Line-Item Commercial Details (New in v3)

When supplier provides line-level breakdown:
- `line_incoterms`, `line_freight`, `line_insurance`, `line_tax_rate_pct`, `line_tax_amount`, `line_other_charges`
- `line_total_inclusive`
- `line_allocation_method` — how charges were split
- `line_hsn_sac_code` — tariff code
- `line_country_of_origin`

---

## 4. Historical Benchmarking (v3 3-Stage Pipeline)

### Overview

For each selected quote item, the pipeline finds:
- **BP (Best Price):** Lowest-priced historical item matching the specs
- **LP (Last Purchase):** Most-recently purchased historical item matching the specs

Both are extracted from `similar_dtl_ids` (populated by Stage C LLM ranking).

### How similar_dtl_ids Are Populated

**Stage A — SQL Category Pool**
- Query `quotation_extracted_items` for historical items in same `purchase_category_llm`
- Older than current PR (by `item_created_date`)
- With known `unit_price_eur`
- Default: up to 100 items
- If < 20 items, widen to `item_level_1 + item_level_2` matching

**Stage B — Pinecone Vector Search (Global)**
- Embed current item using `embed_content`
- Search Pinecone globally (empty pool)
- Min similarity: 0.80 (default)
- Fetch top-k results

**Stage C — LLM Relevance Ranking**
- Combine Stage A + Stage B pools
- Apply hard date guard (drop items newer than current PR)
- Pre-filter for extreme price outliers (`bench_max_price_ratio`)
- Call LLM with `RANK_PROMPT_SYSTEM_V2` (6 rejection rules)
- LLM returns `selected` and `rejected` arrays

**Result:** `shortlist_dtl_ids` = extract `purchase_dtl_id` from LLM "selected" items

**Storage:** Convert to JSON, store in `benchmark_result.similar_dtl_ids`

### LLM Rejection Rules (Stage C)

1. **Critical Attribute Barriers** — critical attrs must match (±10%)
2. **Product Type Consistency** — item_level chain compatible
3. **Spec-to-Price Ratio Sanity** — price/spec ratio not > 5× off pool median
4. **Important Attribute Penalties** — mismatches downrank (±20%)
5. **Brand Similarity is Not Relevance** — no boost for same brand
6. **Recency Tiebreak** — prefer more recent among survivors

### BP & LP Extraction from similar_dtl_ids

**Function:** `_compute_low_last()` (line 4746)

```python
# From shortlist_dtl_ids, fetch full historical data
historical = _fetch_historical_for_dtl_ids(shortlist_dtl_ids)

# Filter by: UOM (optional), age (max_age_months), outliers (3×IQR)
historical = _filter_historical_*()

# Extract low and last
low_item  = min(historical, key=unit_price_eur)   # BP = cheapest
last_item = max(historical, key=pr_created_date)  # LP = most recent
```

**BP (Best Price):**
- Source: `similar_dtl_ids` (LLM-ranked shortlist)
- Selection: minimum `unit_price_eur`
- No filter for `is_selected_quote` (can be ANY quote)
- Stored in: `benchmark_result.low_hist_item_fk`

**LP (Last Purchase):**
- Source: same `similar_dtl_ids`
- Selection: maximum `pr_created_date` (from `purchase_req_mst.C_DATETIME`)
- No filter for `is_selected_quote`
- Stored in: `benchmark_result.last_hist_item_fk`

---

## 5. Currency Conversion

### Source and Target

**Source:** Supplier's quote currency (INR, USD, EUR, GBP, ZAR, AED, RMB, JPY, etc.)  
**Target:** EUR (for all comparisons and storage)

### Conversion Method (v3)

Prices are converted at **extraction time** (not dynamically updated):

**Function:** `_convert_to_eur()` (line 3945)

**Process:**
1. Extract both `unit_price` (source currency) and `currency` code from quotation
2. Lookup source currency in `currency_mst` table → get CUR_ID
3. Query `EXCHANGE_RATE` table for active rate:
   ```sql
   WHERE CUR_ID = source_currency_id
     AND BASE_CUR_ID = 3 (EUR)
     AND STATUS_ID = 10 (active)
     AND FROM_DATE <= pr_date
     AND TO_DATE >= pr_date
   ```
4. Apply conversion: `unit_price_eur = unit_price × CONVERSION_RATE`
5. Storage: Both `unit_price` (source) and `unit_price_eur` (EUR) stored in DB

**Exchange Rate Tables:**
- `currency_mst` — Maps ISO currency codes (USD, INR, EUR, etc.) → CUR_ID
- `EXCHANGE_RATE` — Contains:
  - CUR_ID: source currency
  - BASE_CUR_ID: target currency (3 = EUR)
  - CONVERSION_RATE: rate to apply (source → EUR)
  - FROM_DATE, TO_DATE: validity period
  - STATUS_ID: 10 = active rate

**Exchange Rate Date (PR-Level):**  
✓ Uses **PR-level date** (not quotation_date) to standardize all quotes in a PR to same exchange rate:
```python
fallback_date = ctx.req_start_date or ctx.c_datetime  # Line 4486
```

**Reason:** All quotations within a PR use the same exchange rate (from PR creation/start date) for consistency, regardless of when individual quotes were issued. This ensures fair price comparison within the PR.

### In Benchmarking

**For Current Item:**
- `unit_price_eur` from `quotation_extracted_items` (pre-converted at extraction)
- Exchange rate determined at quotation date

**For BP & LP:**
- Each uses its own PR's exchange rate (via `unit_price_eur` stored in DB)
- No re-conversion; historical price already in EUR with rate from its own date

---

## 6. Inflation Calculation (BOTH LLM + CPI for BOTH BP & LP)

### Overview

v3 calculates **four inflation values**:
- `inflation_pct` — LLM estimate for BP (line 5542-5552)
- `cpi_inflation_pct` — World Bank CPI for BP (line 5549)
- `inflation_pct_last` — LLM estimate for LP (line 5567-5577)
- `cpi_inflation_pct_last` — World Bank CPI for LP (line 5574)

### Reference Dates & Year Comparison

Each historical item uses its own PR date (from `purchase_req_mst.C_DATETIME`), compared against the **CURRENT PR's year** (not today's date):

**Current PR Year (for both BP and LP):**
```python
created = src_date  # From purchase_req_detail.C_DATETIME (line 7611)
current_year = created.year  # Extract the year (line 7614 & 7824)
```

**For BP (Best Price):**
```python
ref_dt = _get_pr_master_date_for_dtl_id(tgt_cs, low_item.purchase_dtl_id)  # Line 5533
ref_year = ref_dt.year  # Historical BP item's PR year
# Inflation only applies if: ref_year < current_year (line 5541)
```

**For LP (Last Purchase):**
```python
ref_dt_last = _get_pr_master_date_for_dtl_id(tgt_cs, last_item.purchase_dtl_id)  # Line 5564
ref_year_last = ref_dt_last.year  # Historical LP item's PR year
# Inflation only applies if: ref_year_last < current_year (line 5566)
```

### Year Comparison Logic

**Inflation is calculated ONLY when:**
```python
if ref_year and current_year and ref_year < current_year:
    # Calculate inflation
else:
    # No inflation (set to 0 or NULL)
```

**Example:**
- Historical BP item: 2021 PR
- Current PR: 2022 PR
- Result: Apply 1 year of inflation ✓

**NOT:**
- Historical BP item: 2021 PR
- Today's date: 2026
- Result: Apply 5 years of inflation ✗ (INCORRECT)

### LLM Inflation Estimation

**Function:** `_estimate_inflation_via_llm()` (line 5266)

Calls LLM with prompt asking for estimated cumulative inflation % for:
- Item name/category
- Supplier country
- Year range (ref_year → current_year)

**Formula:** Macroeconomic inflation for that item category in that country

### World Bank CPI Inflation

**Function:** `_compute_cpi_pct()` (line 5216)

```python
# Fetch CPI data from World Bank API
url = "https://api.worldbank.org/v2/country/{alpha2}/indicator/FP.CPI.TOTL.ZG"

# Resolve country name → ISO alpha-2 code
alpha2 = resolve_country(supplier_country)

# Retrieve annual CPI rates for year range
rates = {year: cpi_annual_rate for year in range(ref_year, current_year)}

# Cumulative inflation
factor = 1.0
for yr in range(ref_year + 1, current_year + 1):
    if yr in rates:
        factor *= (1 + rates[yr] / 100)

inflation_pct = (factor - 1) * 100
```

**Endpoint:** https://api.worldbank.org/v2/country/{country_code}/indicator/FP.CPI.TOTL.ZG

### Normalization Formula

**Both BP and LP:**

```
normalized_bp = bp_unit_price × (1 + cpi_inflation_pct / 100)
normalized_lp = lp_unit_price × (1 + cpi_inflation_pct_last / 100)
```

Used in view (vw_benchmark_summary.sql, lines 89-92 and 110).

### Storage in benchmark_result

| Column | Source | For Item |
|--------|--------|----------|
| `inflation_pct` | LLM estimate | BP |
| `cpi_inflation_pct` | World Bank CPI | BP |
| `inflation_pct_last` | LLM estimate | LP |
| `cpi_inflation_pct_last` | World Bank CPI | LP |

---

## 7. Implementation References (v3)

### Core Pipeline

`agentcore_components/pipeline_stage_123_v3.py`
- `_run_benchmark_v2()` line 7537 — Main orchestration
  - Line 7614: `created = src_date` (Current PR's C_DATETIME)
  - Line 7824: `current_year = created.year` (Current PR's year)
- `_sql_pool_by_category()` line 7149 — Stage A (SQL category pool)
- `_pinecone_narrow_within_pool()` line 7225 — Stage B (vector search)
- `_llm_rank_candidates()` line 7355 — Stage C (LLM ranking)
- `_pre_filter_candidates()` line 7367 — Price ratio pre-filter
- `_compute_low_last()` line 4746 — BP/LP extraction
- `_estimate_inflation_via_llm()` line 5266 — LLM inflation estimate
  - Condition: `if ref_year and current_year and current_year > ref_year` (line 5273)
- `_compute_cpi_pct()` line 5216 — World Bank CPI inflation
  - Condition: `if start_year >= end_year: return None` (line 5222)
  - Loop: `for yr in range(start_year + 1, end_year + 1)` (line 5257)
- `_get_pr_master_date_for_dtl_id()` line 5300 — PR master date lookup

**Inflation Application (lines 5541 & 5566):**
```python
if ref_year and current_year and ref_year < current_year:
    infl_dec = _estimate_inflation_via_llm(...)
    cpi_dec = _compute_cpi_pct(...)
else:
    infl_dec = cpi_dec = Decimal("0")
```

### Database Schema

`migrations/create_vw_benchmark_summary.sql` — Benchmark summary view with normalized prices:
- Lines 89-92: `low_hist_normalized_unit_price_eur = low_unit_price_eur × (1 + cpi_inflation_pct / 100)`
- Lines 110: `last_hist_normalized_unit_price_eur = last_unit_price_eur × (1 + cpi_inflation_pct_last / 100)`

### Client Queries

`sql/client_queries_supplier_benchmarking.sql` — **5 queries for client analysis:**

1. **Query 1: Supplier Hierarchy** — PRIMARY, SECONDARY L1, SECONDARY L2 suppliers
2. **Query 2: Technical & Commercial Specs** — Item specs and quotation details per supplier
3. **Query 3: Historical Benchmarking (BP & LP)** — Best Price and Last Purchase with inflation-adjusted pricing
   - Year comparison: `YEAR(prm_bp.C_DATETIME) < YEAR(prm_current.C_DATETIME)`
   - Normalized pricing: `bp_unit_price × (1 + cpi_inflation_pct / 100)`
4. **Query 4: Currency Conversion & Inflation Rates (SELECTED QUOTES ONLY)** — Source→EUR conversion and both inflation types
   - Exchange rate lookup: `PR_DATE` (req_start_date or c_datetime), NOT quotation_date
   - Shows: quotation_date, pr_created_date, pr_req_start_date for transparency
   - Exchange rate: Lookup based on PR-level date to match v3 standardization logic
5. **Query 5: Validation** — Checks if stored inflation values match year comparison logic
   - Flags: "Should inflate but inflation=0" or "Should NOT inflate but inflation>0"

---

## 8. Glossary

| Term | Definition |
|------|-----------|
| **BP** | Best Price — lowest-cost historical item from similar_dtl_ids |
| **LP** | Last Purchase — most-recent historical item from similar_dtl_ids |
| **RAS** | Requisition Approval System — source of purchase requirements |
| **PURCHASE_DTL_ID** | Line item ID within a RAS |
| **PURCHASE_REQ_ID** | RAS header ID (PR number) |
| **extracted_item_uuid_pk** | Unique ID for each extracted quotation row |
| **is_selected_quote** | Binary flag (0/1) indicating if item was selected as primary supplier |
| **supplier_match_conf** | Confidence (0-1) that extracted supplier matches RAS supplier |
| **item_level_1..8** | 8-level hierarchical taxonomy (Mechanical > Industrial Equipment > Mould Tooling > ...) |
| **critical_attributes** | JSON array of must-match specs (importance="critical") |
| **similar_dtl_ids** | JSON array of historical items ranked by LLM Stage C |
| **cpi_inflation_pct** | World Bank CPI-based inflation % (BP) — Only applied when ref_year < current_year |
| **cpi_inflation_pct_last** | World Bank CPI-based inflation % (LP) — Only applied when ref_year_last < current_year |
| **Incoterms** | International Commercial Terms (FOB, CIF, DDP, etc.) |
| **purchase_category_llm** | LLM-assigned category (used for Stage A SQL filtering) |
| **ref_year** | Year extracted from historical item's PR date (`purchase_req_mst.C_DATETIME`) |
| **ref_year_last** | Year extracted from LAST item's PR date (`purchase_req_mst.C_DATETIME`) |
| **current_year** | Year of the CURRENT PR being benchmarked (from `purchase_req_mst.C_DATETIME`), NOT today's date |
| **C_DATETIME** | Purchase requisition creation date in `purchase_req_mst` table — source for all year comparisons |
| **REQ_START_DATE** | Purchase requisition start date in `purchase_req_mst` table — used as PR-level date for currency conversion (fallback: C_DATETIME if NULL) |
| **PR-level date** | Either req_start_date or c_datetime (whichever is not NULL); used for all quotations within a PR for exchange rate lookup |
