# RAS Procurement Benchmarking System v3 — Logic Documentation

## Executive Summary

The v3 benchmarking system provides:
1. **Supplier Hierarchy** — Primary (extracted), Secondary (parent from RAS master), Tertiary (alternatives)
2. **Specification Capture** — Technical (taxonomy) and Commercial (terms, payment, delivery) details per supplier
3. **Historical Benchmarking** — Comparison against cheapest (BP) and most recent (LP) historical purchases, with inflation adjustment
4. **Currency & Inflation** — Automatic EUR conversion and CPI-based price normalization

---

## 1. Supplier Hierarchy

### Definitions

**Primary Supplier**
- Source: `quotation_extracted_items.supplier_name` (extracted via LLM from quotation documents)
- Confidence: `supplier_match_conf` (0-1 scale) indicates how strongly the extracted name matches the RAS master supplier
- Ranking: Suppliers are ranked by `is_selected_quote` (boolean) — the selected supplier is the primary
- RAS Context: `purchase_req_mst.supplier_name` (the original RAS requisition supplier; used to match extracted names)

**Secondary Supplier**
- Source: `purchase_req_mst.parent_supplier` (from on-prem RAS master)
- Definition: Parent organization, group company, or parent vendor entity
- Availability: Not all RAS records have a secondary supplier; may be NULL
- Use Case: Group purchasing, parent company invoicing, multi-location supplier networks

**Tertiary Supplier**
- Source: Alternative suppliers quoted but not selected (`is_selected_quote = 0`)
- Definition: All other suppliers who provided quotes for the same RAS
- Ranking: Ranked by quote frequency (how many items were quoted) and price (lowest/highest per item)
- Use Case: Supplier performance comparison, market competition analysis, alternative sourcing

### Query References
- **Query 1** — `client_queries_supplier_benchmarking.sql` — Fetch Primary, Secondary, Tertiary suppliers

---

## 2. Technical Specifications

### Taxonomy Structure (item_level_1 through item_level_8)

The extraction LLM assigns an 8-level hierarchical taxonomy to every item:

| Level | Meaning | Example |
|-------|---------|---------|
| 1 | **Broad Category** | Mechanical, Electrical, Software, Services |
| 2 | **Product Type** | Industrial Equipment, Power Supply, Cloud Service |
| 3 | **Product Subtype** | Mould Tooling, Servo Motor, Data Analytics |
| 4 | **Specific Feature** | Cavity Count, Frame Size, Licensing Model |
| 5–8 | **Detailed Specs** | Manufacturer, Version, Options, etc. |

**Example Chain:**
```
Mechanical > Industrial Equipment > Mould Tooling > Double Cavity > F65 Series > DN13+15 > Air Forming > Supplier Code S001
```

### Critical Attributes (JSON Structure)

Each item has a `critical_attributes` JSON array:

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
  },
  {
    "name": "tube_diameter_nominal",
    "value": 13.0,
    "unit": "DN",
    "importance": "important"
  }
]
```

**Importance Levels:**
- `critical` — Must match exactly (or within 10% tolerance) between source and historical candidates; mismatch → REJECT
- `important` — Should match (within 20% tolerance); mismatch → downrank
- `informational` — Nice to match; no penalty if different

### Fields in quotation_extracted_items

| Field | Type | Purpose |
|-------|------|---------|
| `item_name` | NVARCHAR | Brief item description |
| `item_description` | NVARCHAR | Detailed specification text |
| `item_summary` | NVARCHAR | Condensed LLM summary |
| `item_level_1` through `item_level_8` | NVARCHAR | Hierarchical taxonomy |
| `critical_attributes` | NVARCHAR(MAX) JSON | Spec attributes with importance |
| `commodity_tag` | NVARCHAR | Commodity/family tag (e.g., "Moulding", "Fasteners") |
| `purchase_category_llm` | NVARCHAR(200) | LLM-derived category (v2+, used for SQL pool filtering) |

### Query References
- **Query 2** — `client_queries_supplier_benchmarking.sql` — Technical specs per supplier

---

## 3. Commercial Specifications

### Quote-Level Commercial Details (Standard Terms)

Applied to the entire quotation:

| Field | Purpose |
|-------|---------|
| `quotation_ref_no` | Supplier's quote reference number |
| `quotation_date` | Date quotation was issued |
| `validity_date` | Quote expiry date |
| `validity_days` | Quote validity period in days |
| `payment_terms` | Payment conditions (e.g., "Net 30", "2/10 Net 30", "Bank Transfer On Sight") |
| `delivery_date` | Promised delivery date |
| `delivery_time_days` | Lead time in days from order to delivery |
| `quote_incoterms` | Incoterms code (FOB, CIF, DDP, FCA, etc.) |
| `quote_incoterms_named_place` | Specific port/location for Incoterms (e.g., "Port of Shanghai") |

### Quote-Level Financial Details (Charges & Totals)

| Field | Purpose |
|-------|---------|
| `quote_subtotal` | Subtotal before any charges |
| `quote_freight` | Freight/shipping charges |
| `quote_insurance` | Insurance charges |
| `quote_customs_duties` | Customs/duty charges |
| `quote_packing_forwarding` | Packing & forwarding charges |
| `quote_installation` | Installation charges (if applicable) |
| `quote_other_charges` | Any other charges |
| `quote_tax_type` | Tax type (VAT, GST, etc.) |
| `quote_tax_rate_pct` | Tax rate as percentage |
| `quote_tax_amount_total` | Total tax amount |
| `quote_grand_total` | Final total after all charges & tax |
| `quote_grand_total_currency` | Currency of grand total |

### Line-Item Commercial Details (New in v3)

Applied per line item (when supplier provides line-level breakdown):

| Field | Purpose |
|-------|---------|
| `line_incoterms` | Incoterms if specified per line |
| `line_freight` | Freight allocated to this line |
| `line_insurance` | Insurance allocated to this line |
| `line_tax_rate_pct` | Tax rate for this line |
| `line_tax_amount` | Tax amount for this line |
| `line_other_charges` | Other charges for this line |
| `line_total_inclusive` | Line total including all allocated charges |
| `line_allocation_method` | How charges were split (e.g., "proportional by value", "equal", "weight-based") |
| `line_hsn_sac_code` | Harmonized System/SAC tariff code (used for import duty classification) |
| `line_country_of_origin` | Country where item is manufactured |

### Query References
- **Query 2** — `client_queries_supplier_benchmarking.sql` — Commercial specs per supplier

---

## 4. Historical Benchmarking Logic

### Overview

The system maintains a `benchmark_result` table that links each extracted quotation item to:
1. **BP (Best Price)** — the cheapest historical item for similar specs
2. **LP (Last Purchase)** — the most recently purchased item for similar specs

Both are inflation-adjusted for fair comparison.

### How BP & LP Are Computed

#### Stage A: SQL Pool (Category-Based)
- Query all historical items in the same `purchase_category_llm`
- Older than current PR (by `item_created_date`)
- With known unit_price_eur

#### Stage B: Pinecone Vector Search (Similarity-Based)
- Embed the current item using its `embed_content` (or fallback to `_build_embed_text`)
- Search Pinecone for items with cosine similarity ≥ 0.80
- Fetch top-k results

#### Stage C: LLM Relevance Ranking
- Combine A + B pools
- Filter by date (no future items)
- Pre-filter for extreme price outliers
- **Rank candidates using LLM** with rules:
  1. **Critical attribute barriers** — critical attrs must match (±10%)
  2. **Product type consistency** — item_level chain must be compatible
  3. **Spec-to-price ratio sanity** — price/spec ratio must not diverge > 5×
  4. **Important attribute penalties** — mismatches downrank
  5. **Brand & recency** — recency is a tiebreaker

Result: LLM returns top-K ranked candidates

#### Extraction of BP & LP
- **BP (cheapest):** Among surviving candidates, pick the one with lowest `unit_price_eur`
- **LP (most recent):** Among surviving candidates, pick the one with latest `item_created_date`

### Storage in benchmark_result

| Column | Type | Purpose |
|--------|------|---------|
| `purchase_dtl_id` | INT | Current RAS item ID |
| `extracted_item_uuid_fk` | UUID | FK to current quotation_extracted_items row |
| `low_hist_item_fk` | UUID | FK to BP historical item in quotation_extracted_items |
| `last_hist_item_fk` | UUID | FK to LP historical item in quotation_extracted_items |
| `bp_unit_price` | DECIMAL(20,4) | Best price per unit (EUR) |
| `bp_total_price` | DECIMAL(20,4) | Best price total line value (EUR) |
| `similar_dtl_ids` | NVARCHAR(MAX) JSON | Array of similar historical item IDs for context |
| `summary` | NVARCHAR(MAX) | LLM benchmark recommendation text |
| `inflation_pct` | DECIMAL(10,4) | General inflation % (BP → current) |
| `cpi_inflation_pct` | DECIMAL(10,4) | CPI-based inflation % (BP → current) |
| `inflation_pct_last` | DECIMAL(10,4) | General inflation % (LP → current) [v3 new] |
| `cpi_inflation_pct_last` | DECIMAL(10,4) | CPI-based inflation % (LP → current) [v3 new] |

### Normalization Formula

Once historical items are selected, their prices are normalized (adjusted for inflation):

```
normalized_bp_unit_price = bp_unit_price × (1 + cpi_inflation_pct / 100)
normalized_lp_unit_price = lp_unit_price × (1 + cpi_inflation_pct_last / 100)
```

This allows fair comparison: "If the historical purchase had happened today, what would it cost?"

### Query References
- **Query 3** — `client_queries_supplier_benchmarking.sql` — BP/LP with inflation

---

## 5. Currency Conversion

### Source and Target

- **Source Currency:** Varies by supplier (`quotation_extracted_items.currency`)
  - Common: INR, USD, EUR, GBP, ZAR, AED, RMB, JPY, etc.
- **Target Currency:** EUR (for all comparisons)
  - Stored as `unit_price_eur`, `total_price_eur`

### Conversion Method (v3)

The v3 pipeline converts currencies within the `_run_pricing_llm()` function:

1. **Extraction**: LLM extracts both `unit_price` (source currency) and `unit_price_eur` (if available in document)
2. **Fallback Conversion**: If `unit_price_eur` is missing:
   - Call LLM pricing function with historical exchange rate lookup
   - Apply conversion: `unit_price_eur = unit_price × exchange_rate`
3. **Storage**: Both source and EUR prices are stored

### Exchange Rates

- **Source:** Historical exchange rates (inferred from market data or LLM-retrieved rates)
- **Application:** Applied at extraction time (not dynamically updated)
- **Impact:** A 10-year-old quote in INR is converted using the exchange rate from that date

### Query References
- **Query 4** — `client_queries_supplier_benchmarking.sql` — Currency conversion details
- **Query 4 Implied Exchange Rate** — `unit_price_eur / unit_price` (for verification)

---

## 6. Inflation Calculation

### CPI-Based Inflation (v3)

The system uses **Consumer Price Index (CPI)** to adjust historical prices to current levels.

### Calculation Flow

#### Step 1: Identify Reference Dates
- **Current PR date:** `purchase_req_mst.C_DATETIME` (when the current RAS was created)
- **Historical item date:** `quotation_extracted_items.item_created_date` (when the BP/LP was quoted)

#### Step 2: Retrieve CPI Data
The function `_compute_cpi_pct()` in `pipeline_stage_123_v3.py`:
- Takes country (supplier country), reference year, current year
- Calls LLM (Azure OpenAI) to fetch CPI data
- Returns CPI inflation percentage

#### Step 3: Apply Formula

```
inflation_pct = (CPI_current_year / CPI_historical_year - 1) × 100

normalized_price = historical_price × (1 + inflation_pct / 100)
```

**Example:**
- Historical item: quoted 2021, price 100 EUR
- Current PR: 2026
- CPI inflation from 2021 to 2026: 15%
- Normalized price: 100 × (1 + 15/100) = 115 EUR

### Two Inflation Figures (v3 New)

| Column | Source | Purpose |
|--------|--------|---------|
| `inflation_pct` | `_compute_cpi_pct()` general | Standard inflation % |
| `cpi_inflation_pct` | `_compute_cpi_pct()` CPI | CPI-specific inflation % (more accurate) |
| `inflation_pct_last` | Same logic, LP date | Inflation from LP to current (v3 new) |
| `cpi_inflation_pct_last` | Same logic, LP date | CPI inflation from LP to current (v3 new) |

Usually `cpi_inflation_pct ≥ inflation_pct` (CPI is typically more conservative).

### Limitations

- **Data Freshness:** CPI lookup happens at extraction time; not updated if inflation rates change
- **Country Specificity:** Only supplier country is used; item-specific inflation (e.g., semiconductor prices vs general inflation) is not captured
- **Manual Entry:** Not all inflations can be automatically retrieved; fallback to general country inflation

### Query References
- **Query 4** — `client_queries_supplier_benchmarking.sql` — Inflation amounts & adjusted prices

---

## 7. Key Data Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────┐
│ INPUT: New RAS (purchase_req_id = 152105)                           │
└───────────────────────┬──────────────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │ QUOTATION EXTRACTION (LLM)    │
        │ Extracts from PDF documents   │
        └───────┬───────────────────────┘
                │
                ▼
    ┌───────────────────────────────────────────┐
    │ quotation_extracted_items                 │
    │ ├─ supplier_name (PRIMARY SUPPLIER)      │
    │ ├─ item_name, item_level_1..8 (TECH)    │
    │ ├─ critical_attributes (SPECS)          │
    │ ├─ quotation_date, payment_terms (COM)  │
    │ ├─ currency, unit_price, unit_price_eur│
    │ └─ is_selected_quote (RANKING)          │
    └────────────────┬────────────────────────┘
                     │
         ┌───────────┴────────────┐
         │                        │
         ▼                        ▼
    ┌─────────────┐       ┌──────────────┐
    │ purchase_   │       │ SECONDARY &  │
    │ req_mst     │       │ TERTIARY     │
    │ supplier_   │       │ SUPPLIERS    │
    │ name        │       └──────────────┘
    │ parent_     │
    │ supplier    │
    └─────────────┘
         │
         ▼
    ┌────────────────────────────────┐
    │ BENCHMARKING STAGE (V2 3-STAGE)│
    │ ┌─────────────┐                │
    │ │ SQL POOL    │                │
    │ │ by category │                │
    │ └──────┬──────┘                │
    │        │                       │
    │   ┌────┴────┐                  │
    │   │ combined │                 │
    │   │ pool     │                 │
    │   └────┬────┘                  │
    │        │                       │
    │   ┌────┴──────────┐            │
    │   │ LLM RANKING   │            │
    │   │ (6 rules)     │            │
    │   └────┬──────────┘            │
    │        │                       │
    │   ┌────┴──────┐                │
    │   │ BP & LP   │                │
    │   │ selected  │                │
    │   └────┬──────┘                │
    └────────┼────────────────────────┘
             │
             ▼
    ┌─────────────────────────────────────┐
    │ benchmark_result                    │
    │ ├─ purchase_dtl_id (reference)     │
    │ ├─ low_hist_item_fk (BP)           │
    │ ├─ last_hist_item_fk (LP)          │
    │ ├─ bp_unit_price (cheapest)        │
    │ ├─ inflation_pct (BP inflation)    │
    │ ├─ inflation_pct_last (LP infla.)  │
    │ ├─ similar_dtl_ids (context)       │
    │ └─ summary (LLM recommendation)    │
    └────────────┬──────────────────────┘
                 │
                 ▼
    ┌─────────────────────────────────┐
    │ NORMALIZED PRICING              │
    │ ├─ bp_normalized = bp × (1 +   │
    │ │   cpi_inflation_pct / 100)    │
    │ ├─ lp_normalized = lp × (1 +   │
    │ │   cpi_inflation_pct_last/100) │
    │ └─ READY FOR REPORTING          │
    └─────────────────────────────────┘
```

---

## 8. Configuration & Tuning (v3)

### Benchmark Configuration Keys

These can be passed as `prompts` dict to `_run_benchmark_v2()`:

| Key | Default | Purpose |
|-----|---------|---------|
| `bench_sql_pool_size` | 100 | Max historical items to fetch in SQL pool (Stage A) |
| `bench_pinecone_top_k` | 10 | Max results from Pinecone (Stage B) |
| `bench_min_similarity` | 0.80 | Minimum cosine similarity threshold for Pinecone |
| `bench_llm_shortlist_size` | max(5, top_k_final) | How many candidates LLM ranks |
| `bench_widen_l1l2_when_sparse` | True | If SQL pool < 20, widen to item_level_1+2 |
| `bench_max_price_ratio` | 10.0 | Pre-filter: drop candidates with unit_price > 10× source |
| `bench_critical_threshold_pct` | 10 | Critical attribute tolerance |
| `bench_important_threshold_pct` | 20 | Important attribute tolerance |
| `bench_ratio_band_pct` | 50 | Spec-to-price ratio band (±50% of median) |
| `bench_max_age_months` | 0 | Max age of historical items (0 = no limit) |
| `bench_uom_strict` | False | Require exact unit match for comparison |

---

## 9. Implementation References

### Core Files (v3)

- **Pipeline Logic:** `agentcore_components/pipeline_stage_123_v3.py`
  - `_run_benchmark_v2()` (line ~7470) — Main benchmarking orchestration
  - `_sql_pool_by_category()` (line ~7149) — Stage A SQL pool
  - `_pinecone_narrow_within_pool()` (line ~7225) — Stage B search
  - `_llm_rank_candidates()` (line ~7355) — Stage C LLM ranking
  - `_pre_filter_candidates()` (line ~7367) — Price ratio pre-filter
  - `_compute_cpi_pct()` — CPI inflation lookup

- **Schema Definitions:**
  - `sql/quotation_extracted_items_add_commercials_cols.sql` — Commercial spec columns
  - `migrations/add_inflation_last_columns.sql` — v3 LP inflation support
  - `migrations/create_vw_benchmark_summary.sql` — Combined benchmark view

- **Client Queries:**
  - `sql/client_queries_supplier_benchmarking.sql` — All 4 queries (this document)

---

## 10. Glossary

| Term | Definition |
|------|-----------|
| **BP** | Best Price — cheapest historical item matching current specs |
| **LP** | Last Purchase — most recently purchased historical item |
| **RAS** | Requisition Approval System — source of purchase requirements |
| **PURCHASE_DTL_ID** | Line item ID within a RAS |
| **PURCHASE_REQ_ID** | RAS header ID (PR number) |
| **extracted_item_uuid_pk** | Unique identifier for each extracted quotation row |
| **supplier_match_conf** | Confidence (0-1) that extracted supplier matches RAS supplier |
| **item_level_1..8** | 8-level hierarchical taxonomy for categorizing items |
| **critical_attributes** | JSON array of must-match specifications |
| **CPI** | Consumer Price Index — measure of inflation |
| **Incoterms** | International Commercial Terms (FOB, CIF, DDP, etc.) |

---

## Document History

- **v3 (2026-05-18):** Initial documentation for v3 benchmarking with LP inflation, improved pre-filtering
- **Reference:** Based on pipeline_stage_123_v3.py and schema migrations
