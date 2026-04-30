# AgentCore Canvas — Procurement Pipeline Setup

## What this replicates

`run_pipeline.py` (8-stage procurement attachment pipeline) as an AgentCore/MiCore flow.

**Stages:**
| # | Name | Where |
|---|------|--------|
| 1 | INGESTION (MERGE ras_tracker) | Stage 1-3 component |
| 2 | EMBED_DOC_EXTRACTION (text extract) | Stage 1-3 component |
| 3 | BLOB_UPLOAD | Stage 1-3 component |
| 4 | CLASSIFICATION (save to DB) | Stage 5-8 component |
| 5 | METADATA_EXTRACTION (LLM quotation extraction) | Stage 5-8 component |
| 6 | EMBEDDINGS (Pinecone) | Stage 5-8 component |
| 7 | PRICE_BENCHMARK (Pinecone + LLM) | Stage 5-8 component |
| 8 | COMPLETE | Stage 5-8 component |

---

## 10-Node Canvas Layout

```
[On-Prem DB Connector]  ──► ─┐
                              ├──► [Stage 1-3] ──► File Batch ──► [Classify User PT] ──► [Worker Node] ──►─┐
[Azure SQL Connector]   ──► ─┘              └──► File Batch (fan-out)                                      │
                                                                                                            │
[Azure SQL Connector]   ──►─────────────────────────────────────────────────────────────────────────────►─┤
                                                                                                            │
[LLM (GPT-4o)]          ──►──────────────────────────────── Worker Node ──► (also)                        │
[LLM (GPT-4o)]          ──►──────────────────────────────────────────────────────────────── Stage 5-8 ◄──►─┤
[Embeddings Model]      ──►──────────────────────────────────────────────────────────────── Stage 5-8      │
[Extract System PT]     ──►──────────────────────────────────────────────────────────────── Stage 5-8      │
                                                                                                            │
Worker Node Response    ──►──────────────────────────────────────────────────────────────── Stage 5-8 ◄───┘
Stage 5-8 Sync Status   ──►──────────────────────────────────────────────────────────────── [Chat Output]
```

---

## Step-by-step setup

### Node 1 — On-Prem DB Connector
- From: **Tools → Database Connector**
- Connector: select your on-prem SQL Server entry (e.g. `SpendDB-OnPrem`)
- This outputs: **Connection Config** (Data)

### Node 2 — Azure SQL Connector
- From: **Tools → Database Connector**
- Connector: select your Azure SQL entry
- This outputs: **Connection Config** (Data)
- **Note:** Connect this to BOTH Stage 1-3 and Stage 5-8

### Node 3 — LLM (GPT-4o)
- From: **Models → Model**
- Select GPT-4o (or your configured model)
- Connect to both Worker Node and Stage 5-8

### Node 4 — Embeddings Model
- From: **Models → Embeddings**
- Select your text-embedding model (e.g. text-embedding-3-small, 1536 dims)
- Connect to Stage 5-8

### Node 5 — Stage 1-3 (Custom Code)
- From: **Tools → Custom Code**
- Click **Code** tab → paste contents of `agentcore_components/pipeline_stage_123.py`
- Click **Save**
- Fill in UI fields:
  - **Azure Storage Account URL**: `https://<your-account>.blob.core.windows.net`
  - **Blob Container Name**: `agentcore-knowledge-container`
  - **Max PRs per Run**: `50` (or as needed)
- Connect:
  - On-Prem DB Connector → `Source DB`
  - Azure SQL Connector  → `Target DB`

### Node 6 — Classify User Prompt Template
- From: **Prompts → Prompt Template**
- Template text:
  ```
  Classify each of the following procurement attachments.

  {file_batch}

  Return a JSON array ONLY — no markdown, no explanation:
  [
    {"filename": "<exact filename>", "classification": "Quotation|MPBC|RFQ|BER|E-Auction|Other"},
    ...
  ]
  ```
- Connect:
  - Stage 1-3 File Batch output → `{file_batch}` input port

### Node 7 — Worker Node (Classifier)
- From: **Agents → Worker Node** (or Agents → Agent)
- **Agent Instructions** field — paste the full `SYSTEM_PROMPT` from
  `file_classifier/classifier/prompt_templates.py` (the 258-line prompt)
- Connect:
  - LLM → Agent LLM input
  - Classify User Prompt Template output → Input / Message input

### Node 8 — Extract System Prompt Template
- From: **Prompts → Prompt Template**
- Template text — paste the full contents of
  `quotation_extraction/prompts/system.txt` (no variables, it's static)
- Connect to Stage 5-8 `Extraction System Prompt` input

### Node 9 — Stage 5-8 (Custom Code)
- From: **Tools → Custom Code**
- Click **Code** tab → paste contents of `agentcore_components/pipeline_stage_58.py`
- Click **Save**
- Fill in UI fields:
  - **Extraction User Template**: paste full content of
    `quotation_extraction/prompts/extraction.txt`
  - **Pinecone Index Name**: `ras-quotations`
  - **Pinecone Namespace**: `procurement`
  - **Embedding Dimensions**: `1536`
- Connect:
  - Worker Node Response → `Classification (Worker Node Response)`
  - Stage 1-3 File Batch → `File Batch` (fan-out second connection)
  - Extract System Prompt Template → `Extraction System Prompt`
  - Azure SQL Connector → `Target DB`
  - LLM → `LLM (GPT-4o)`
  - Embeddings Model → `Embeddings Model`

### Node 10 — Chat Output
- Connect Stage 5-8 `Sync Status` output → Chat Output

---

## Connection table (complete)

| From | Output port | To | Input port |
|------|-------------|-----|------------|
| On-Prem DB Connector | Connection Config | Stage 1-3 | Source DB |
| Azure SQL Connector | Connection Config | Stage 1-3 | Target DB |
| Azure SQL Connector | Connection Config | Stage 5-8 | Target DB |
| LLM | LanguageModel | Worker Node | Agent LLM |
| LLM | LanguageModel | Stage 5-8 | LLM |
| Embeddings Model | Embeddings | Stage 5-8 | Embeddings Model |
| Stage 1-3 | File Batch | Classify User PT | `{file_batch}` |
| Stage 1-3 | File Batch | Stage 5-8 | File Batch |
| Classify User PT | Message | Worker Node | Input |
| Extract System PT | Message | Stage 5-8 | Extraction System Prompt |
| Worker Node | Response | Stage 5-8 | Classification |
| Stage 5-8 | Sync Status | Chat Output | Text |

---

## What to paste where

| Location | Content source |
|----------|---------------|
| Worker Node Agent Instructions | Full `SYSTEM_PROMPT` from `file_classifier/classifier/prompt_templates.py` |
| Classify User PT template | `{file_batch}` with JSON-only instruction (see Step 6 above) |
| Extract System PT template | Full content of `quotation_extraction/prompts/system.txt` |
| Stage 5-8 → Extraction User Template field | Full content of `quotation_extraction/prompts/extraction.txt` |

---

## Required dependencies

Add to your environment / `pyproject.toml`:

```
pyodbc
azure-identity
azure-storage-blob
pymupdf          # fitz — PDF extraction
openpyxl         # Excel extraction
python-docx      # Word extraction
python-pptx      # PowerPoint extraction (optional)
langchain-core
```

---

## Azure SQL tables required

| Table | Purpose |
|-------|---------|
| `purchase_req_mst` | PR header |
| `purchase_req_detail` | PR line items |
| `vw_get_ras_data_for_bidashboard` | BI enrichment view |
| `ras_tracker` | Pipeline stage tracking |
| `purchase_req_attachments` | On-prem attachment blobs |
| `attachment_classification` | Stage 4 output |
| `quotation_extracted_items` | Stage 5 output |
| `benchmark_result` | Stage 7 output |
