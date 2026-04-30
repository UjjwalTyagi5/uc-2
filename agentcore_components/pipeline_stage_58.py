"""Pipeline Stage 5-8: Classification Save → Extraction → Embeddings → Price Benchmark.

Canvas wiring
─────────────
  Worker Node response    ──► worker_result   (classification JSON)
  Stage 1-3 File Batch    ──► file_batch      (raw text of every attachment)
  Extract Sys Prompt node ──► ext_system_prompt (static Prompt Template)
  Azure SQL Connector     ──► target_connection
  LLM node                ──► llm
  Embeddings Model node   ──► embed_model
  ext_user_template       — paste extraction.txt content into the UI field

Worker Node is configured with the full SYSTEM_PROMPT from
  file_classifier/classifier/prompt_templates.py
and is asked to return a JSON array like:
  [{"filename": "quote.pdf", "classification": "Quotation"}, ...]

This component:
  Stage 4 — saves classification to attachment_classification
  Stage 5 — calls LLM to extract quotation data (only for Quotation files)
  Stage 6 — embeds extracted text into Pinecone via AgentCore Pinecone service
  Stage 7 — price benchmark (Pinecone similarity + LLM)
  Stage 8 — marks PR complete in ras_tracker
"""

from __future__ import annotations

import json
import random
import re
import time
from decimal import Decimal
from typing import Optional

import pyodbc
from loguru import logger

from agentcore.custom import Node
from agentcore.io import HandleInput, IntInput, MessageTextInput, Output
from agentcore.schema.data import Data
from agentcore.schema.message import Message

# ── constants ──────────────────────────────────────────────────────────────────

_MAX_RETRIES = 3
_BASE_DELAY  = 2.0

_STAGE_CLASSIFICATION     = 4
_STAGE_EXTRACTION         = 5
_STAGE_EMBEDDINGS         = 6
_STAGE_PRICE_BENCHMARK    = 7
_STAGE_COMPLETE           = 8


# ── component ──────────────────────────────────────────────────────────────────

class PipelineStage58Node(Node):
    display_name = "Pipeline Stage 5-8"
    description = (
        "Stage 4: Save document classifications to DB. "
        "Stage 5: LLM extraction for Quotation files (RAS context from Azure SQL). "
        "Stage 6: Embed extracted text into Pinecone via AgentCore service. "
        "Stage 7: Price benchmark (Pinecone similarity + LLM). "
        "Stage 8: Mark PR complete in ras_tracker."
    )
    icon = "Cpu"
    name = "PipelineStage58Node"

    inputs = [
        HandleInput(
            name="worker_result",
            display_name="Classification (Worker Node Response)",
            input_types=["Message"],
            info="The Worker Node must be instructed to return a JSON array: "
                 "[{\"filename\": \"...\", \"classification\": \"Quotation|MPBC|...\"}]",
        ),
        HandleInput(
            name="file_batch",
            display_name="File Batch (from Stage 1-3)",
            input_types=["Message"],
            info="The file-batch Message produced by Pipeline Stage 1-3.",
        ),
        HandleInput(
            name="ext_system_prompt",
            display_name="Extraction System Prompt",
            input_types=["Message"],
            info="Connect the Extract System Prompt Template node here. "
                 "Paste the content of quotation_extraction/prompts/system.txt into that node.",
        ),
        MessageTextInput(
            name="ext_user_template",
            display_name="Extraction User Template",
            value="",
            info="Paste the full content of quotation_extraction/prompts/extraction.txt here. "
                 "Python {variable} placeholders are filled at runtime from Azure SQL RAS context.",
        ),
        HandleInput(
            name="target_connection",
            display_name="Target DB — Azure SQL",
            input_types=["Data"],
            info="Connection Config output from the Azure SQL Database Connector node.",
        ),
        HandleInput(
            name="llm",
            display_name="LLM (GPT-4o)",
            input_types=["LanguageModel"],
        ),
        HandleInput(
            name="embed_model",
            display_name="Embeddings Model",
            input_types=["Embeddings"],
        ),
        MessageTextInput(
            name="pr_no_filter",
            display_name="PR Number Filter (optional)",
            value="",
            advanced=True,
            info="Leave blank to process all files in the batch. "
                 "Enter a PURCHASE_REQ_NO to restrict to one PR.",
        ),
        MessageTextInput(
            name="pinecone_index",
            display_name="Pinecone Index Name",
            value="ras-quotations",
            advanced=True,
        ),
        MessageTextInput(
            name="pinecone_namespace",
            display_name="Pinecone Namespace",
            value="procurement",
            advanced=True,
        ),
        IntInput(
            name="pinecone_top_k",
            display_name="Benchmark Similarity Top-K",
            value=5,
            advanced=True,
        ),
        IntInput(
            name="embed_dimension",
            display_name="Embedding Dimensions",
            value=1536,
            advanced=True,
        ),
        MessageTextInput(
            name="az_schema",
            display_name="Azure SQL Schema prefix",
            value="dbo",
            advanced=True,
            info="Leave as 'dbo' unless your tables are in a different schema.",
        ),
    ]

    outputs = [
        Output(
            display_name="Sync Status",
            name="sync_status",
            method="run_pipeline",
            types=["Message"],
        ),
    ]

    # ── pyodbc helpers ────────────────────────────────────────────────────────

    def _conn_str(self, conn_data: Data) -> str:
        d = conn_data.data or {}
        driver = d.get("driver", "ODBC Driver 18 for SQL Server")
        server = d.get("host", d.get("server", ""))
        port   = d.get("port", 1433)
        db     = d.get("database_name", d.get("database", ""))
        user   = d.get("username", "")
        pwd    = d.get("password", "")
        return (
            f"DRIVER={{{driver}}};SERVER={server},{port};"
            f"DATABASE={db};UID={user};PWD={pwd};TrustServerCertificate=yes;"
        )

    @staticmethod
    def _is_transient(exc: Exception) -> bool:
        kw = ["connection reset", "timeout", "throttl", "resource limit",
              "broken pipe", "transport-level", "login failed"]
        return any(k in str(exc).lower() for k in kw)

    def _connect(self, conn_str: str) -> pyodbc.Connection:
        for attempt in range(_MAX_RETRIES + 1):
            try:
                return pyodbc.connect(conn_str, timeout=30)
            except Exception as exc:
                if not self._is_transient(exc) or attempt == _MAX_RETRIES:
                    raise
                time.sleep(_BASE_DELAY * (2 ** attempt) * (1 + random.random() * 0.2))
        raise RuntimeError("unreachable")

    # ── parsing ───────────────────────────────────────────────────────────────

    def _parse_classifications(self) -> dict[str, str]:
        """Parse Worker Node output → {filename: doc_type}."""
        raw = _msg_text(self.worker_result).strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return {
                    item.get("filename", ""): item.get("classification", "Other")
                    for item in data
                    if isinstance(item, dict)
                }
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            logger.warning("Could not parse classification JSON: {}", raw[:300])
        return {}

    def _parse_file_batch(self) -> dict[str, dict]:
        """Parse file-batch Message → {filename: {content, pr_no}}."""
        text    = _msg_text(self.file_batch)
        pattern = re.compile(
            r"=== FILE: (.+?) \(PR: (.+?)\) ===\n(.*?)(?==== FILE:|$)",
            re.DOTALL,
        )
        result: dict[str, dict] = {}
        for m in pattern.finditer(text):
            result[m.group(1).strip()] = {
                "content": m.group(3).strip(),
                "pr_no":   m.group(2).strip(),
            }
        return result

    # ── RAS context ───────────────────────────────────────────────────────────

    def _fetch_ras_context(self, tgt_cs: str, pr_no: str) -> dict:
        """Fetch purchase_req_mst, purchase_req_detail, vw_get_ras_data_for_bidashboard."""
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        ctx: dict = {"purchase_req_no": pr_no, "line_items": []}

        try:
            cur.execute(
                """
                SELECT TOP 1
                       prm.PURCHASE_REQ_ID, prm.SUPPLIER_NAME, prm.JUSTIFICATION,
                       prm.CURRENCY,        prm.ENQUIRY_NO,    prm.CLASSIFICATION,
                       prm.Department,      prm.NEGOTIATED_BY, prm.ADDRESS,
                       prm.CONTRACT_NO,     prm.ORDER_NO,      prm.PURCHASE_VALUE
                  FROM purchase_req_mst prm
                 WHERE prm.PURCHASE_REQ_NO = ?
                """,
                pr_no,
            )
            mst = cur.fetchone()
            if mst:
                (ctx["purchase_req_id"], ctx["supplier_name"], ctx["justification"],
                 ctx["currency"],        ctx["enquiry_no"],    ctx["classification"],
                 ctx["department"],      ctx["negotiated_by"], ctx["address"],
                 ctx["contract_no"],     ctx["order_no"],      ctx["purchase_value"]) = mst

            req_id = ctx.get("purchase_req_id")
            if req_id:
                cur.execute(
                    """
                    SELECT prd.PURCHASE_DTL_ID, prd.ITEM_NO,       prd.QUANTITY,
                           prd.ITEM_TYPE,       prd.ITEMDESCRIPTION, prd.PRICE,
                           prd.UOM,             prd.DISCOUNT,      prd.REQ_VALUE,
                           prd.CURRENCY,        prd.DELIVERY_DATE, prd.SUPPLIER_NAME,
                           prd.PAYMENT_DETAILS, prd.ORIGINAL_VALUE, prd.Initial_Offer,
                           prd.Negotiation,     prd.CommentsforItem, prd.PREPAYMENT,
                           prd.ITEM_CODE
                      FROM purchase_req_detail prd
                     WHERE prd.PURCHASE_REQ_ID = ?
                     ORDER BY prd.ITEM_NO
                    """,
                    req_id,
                )
                for r in cur.fetchall():
                    deliv = r[10]
                    ctx["line_items"].append({
                        "purchase_dtl_id": r[0],  "item_no":         r[1],
                        "quantity":        r[2],   "item_type":       r[3],
                        "item_description": r[4],  "unit_price":      r[5],
                        "uom":             r[6],   "discount":        r[7],
                        "req_value":       r[8],   "currency":        r[9],
                        "delivery_date":   str(deliv.date()) if deliv else None,
                        "supplier_name":   r[11],  "payment_details": r[12],
                        "original_value":  r[13],  "initial_offer":   r[14],
                        "negotiation":     r[15],  "comments":        r[16],
                        "prepayment":      r[17],  "item_code":       r[18],
                    })

                # BI dashboard enrichment (best-effort)
                try:
                    cur.execute(
                        """
                        SELECT TOP 1
                               vw.L1,               vw.Sub_Category_Type, vw.Site_Country,
                               vw.Site_Region,      vw.Division,          vw.L3,
                               vw.L4,               vw.Purchase_Category, vw.Title,
                               vw.Site,             vw.Requisition_Type,  vw.Parent_Supplier,
                               vw.Supplier_Type,    vw.Suplier_country,   vw.Payment_Days,
                               vw.PO_Date,          vw.Category_Buyer,    vw.L2
                          FROM vw_get_ras_data_for_bidashboard vw
                         WHERE vw.PURCHASE_REQ_ID = ?
                        """,
                        req_id,
                    )
                    vw = cur.fetchone()
                    if vw:
                        ctx["category"]          = vw[0]
                        ctx["sub_category"]       = vw[1] or vw[17]
                        ctx["site_country"]       = vw[2]
                        ctx["site_region"]        = vw[3]
                        ctx["division"]           = vw[4]
                        ctx["l3"]                 = vw[5]
                        ctx["l4"]                 = vw[6]
                        ctx["purchase_category"]  = vw[7]
                        ctx["ras_title"]          = vw[8]
                        ctx["site"]               = vw[9]
                        ctx["requisition_type"]   = vw[10]
                        ctx["parent_supplier"]    = vw[11]
                        ctx["supplier_type"]      = vw[12]
                        ctx["supplier_country"]   = vw[13]
                        ctx["payment_days"]       = vw[14]
                        ctx["po_date"]            = vw[15]
                        ctx["category_buyer"]     = vw[16]
                except Exception as exc:
                    logger.warning("BI view fetch failed for PR={}: {}", pr_no, exc)

        except Exception as exc:
            logger.error("RAS context fetch failed for PR={}: {}", pr_no, exc)
        finally:
            conn.close()

        return ctx

    # ── prompt helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _na(val: object) -> str:
        if val is None or str(val).strip() == "":
            return "N/A"
        return str(val)

    def _build_line_items_table(self, line_items: list[dict]) -> str:
        if not line_items:
            return (
                "| DTL_ID | Description | Qty | UOM | Currency |\n"
                "|--------|-------------|-----|-----|----------|\n"
            )
        header = (
            "| DTL_ID | Item No | Item Code | Description | Qty | UOM | Type "
            "| Unit Price | Req Value | Currency | Supplier | Delivery Date | Payment Terms |"
        )
        sep = (
            "|--------|---------|-----------|-------------|-----|-----|------|"
            "------------|-----------|----------|----------|---------------|---------------|"
        )
        rows = [header, sep]
        na = self._na
        for li in line_items:
            rows.append(
                f"| {na(li.get('purchase_dtl_id'))} "
                f"| {na(li.get('item_no'))} "
                f"| {na(li.get('item_code'))} "
                f"| {na(li.get('item_description'))} "
                f"| {na(li.get('quantity'))} "
                f"| {na(li.get('uom'))} "
                f"| {na(li.get('item_type'))} "
                f"| {na(li.get('unit_price'))} "
                f"| {na(li.get('req_value'))} "
                f"| {na(li.get('currency'))} "
                f"| {na(li.get('supplier_name'))} "
                f"| {na(li.get('delivery_date'))} "
                f"| {na(li.get('payment_details'))} |"
            )
        return "\n".join(rows)

    # ── extraction ────────────────────────────────────────────────────────────

    def _extract_quotation(self, filename: str, content: str, ctx: dict) -> dict:
        """Build prompt from RAS context + extraction.txt template, call LLM."""
        from langchain_core.messages import HumanMessage, SystemMessage

        ext_sys       = _msg_text(self.ext_system_prompt)
        user_template = self.ext_user_template or ""
        na            = self._na

        # Build line-items table for the template
        line_items_table = self._build_line_items_table(ctx.get("line_items", []))

        # Truncate document content to avoid token overrun
        doc_content = content[:14000]

        try:
            user_msg = user_template.format(
                purchase_req_no  = na(ctx.get("purchase_req_no")),
                purchase_req_id  = na(ctx.get("purchase_req_id")),
                justification    = na(ctx.get("justification")),
                supplier_name    = na(ctx.get("supplier_name")),
                currency         = na(ctx.get("currency")),
                enquiry_no       = na(ctx.get("enquiry_no")),
                classification   = na(ctx.get("classification")),
                department       = na(ctx.get("department")),
                negotiated_by    = na(ctx.get("negotiated_by")),
                address          = na(ctx.get("address")),
                contract_no      = na(ctx.get("contract_no")),
                order_no         = na(ctx.get("order_no")),
                purchase_value   = na(ctx.get("purchase_value")),
                category         = na(ctx.get("category")),
                sub_category     = na(ctx.get("sub_category")),
                l3               = na(ctx.get("l3")),
                l4               = na(ctx.get("l4")),
                purchase_category= na(ctx.get("purchase_category")),
                ras_title        = na(ctx.get("ras_title")),
                site_region      = na(ctx.get("site_region")),
                site_country     = na(ctx.get("site_country")),
                site             = na(ctx.get("site")),
                division         = na(ctx.get("division")),
                requisition_type = na(ctx.get("requisition_type")),
                parent_supplier  = na(ctx.get("parent_supplier")),
                supplier_type    = na(ctx.get("supplier_type")),
                supplier_country = na(ctx.get("supplier_country")),
                payment_days     = na(ctx.get("payment_days")),
                po_date          = na(ctx.get("po_date")),
                category_buyer   = na(ctx.get("category_buyer")),
                line_items_table = line_items_table,
                item_taxonomy    = "",
                document_content = doc_content,
                raw_ras_context  = "",
            )
        except KeyError as exc:
            logger.warning("Template key error for {}: {} — using fallback prompt", filename, exc)
            user_msg = (
                f"Purchase Requisition: {ctx.get('purchase_req_no')}\n"
                f"Supplier: {na(ctx.get('supplier_name'))}\n\n"
                f"Extract all quotation data from the document below:\n\n{doc_content}"
            )

        response = self.llm.invoke([
            SystemMessage(content=ext_sys),
            HumanMessage(content=user_msg),
        ])
        raw = getattr(response, "content", str(response)).strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.error("LLM returned invalid JSON for {} — raw start: {}", filename, raw[:200])
            return {}

    # ── DB writes ─────────────────────────────────────────────────────────────

    def _save_classification(
        self, tgt_cs: str, filename: str, pr_no: str, doc_type: str, blob_path: str,
    ) -> Optional[int]:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                MERGE attachment_classification WITH (HOLDLOCK) AS target
                USING (SELECT ? AS purchase_req_no, ? AS filename) AS src
                   ON target.purchase_req_no = src.purchase_req_no
                  AND target.filename        = src.filename
                WHEN MATCHED THEN
                    UPDATE SET doc_type    = ?,
                               blob_path   = ?,
                               updated_at  = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (purchase_req_no, filename, doc_type, blob_path)
                    VALUES (?, ?, ?, ?);
                """,
                pr_no, filename,
                doc_type, blob_path,
                pr_no, filename, doc_type, blob_path,
            )
            cur.execute(
                "SELECT attachment_classify_pk "
                "FROM   attachment_classification "
                "WHERE  purchase_req_no = ? AND filename = ?",
                pr_no, filename,
            )
            row = cur.fetchone()
            conn.commit()
            return int(row[0]) if row else None
        except Exception as exc:
            logger.error("Classification save failed for {}: {}", filename, exc)
            return None
        finally:
            conn.close()

    def _save_items(
        self, tgt_cs: str, classify_fk: int, extracted: dict,
    ) -> int:
        items = extracted.get("items", [])
        if not items:
            return 0

        header = {
            "supplier_name":    extracted.get("supplier_name"),
            "supplier_address": extracted.get("supplier_address"),
            "supplier_country": extracted.get("supplier_country"),
            "quotation_ref_no": extracted.get("quotation_ref_no"),
            "quotation_date":   extracted.get("quotation_date"),
            "currency":         extracted.get("currency"),
            "validity_date":    extracted.get("validity_date"),
            "validity_days":    extracted.get("validity_days"),
            "payment_terms":    extracted.get("payment_terms"),
        }

        conn     = self._connect(tgt_cs)
        cur      = conn.cursor()
        inserted = 0
        try:
            for item in items:
                cur.execute(
                    """
                    INSERT INTO quotation_extracted_items (
                        attachment_classify_fk,
                        purchase_dtl_id,
                        supplier_name, supplier_address, supplier_country,
                        quotation_ref_no, quotation_date, currency,
                        validity_date, validity_days, payment_terms,
                        item_name, item_description, quantity, unit,
                        unit_price, total_price, discount, taxation_details,
                        delivery_date, delivery_time_days,
                        item_level_1, item_level_2, item_level_3,
                        item_level_4, item_level_5, item_level_6,
                        item_level_7, item_level_8,
                        commodity_tag, item_summary, supplier_match_conf
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                    """,
                    classify_fk,
                    item.get("purchase_dtl_id"),
                    header["supplier_name"],  header["supplier_address"],
                    header["supplier_country"],
                    header["quotation_ref_no"], header["quotation_date"],
                    header["currency"],        header["validity_date"],
                    header["validity_days"],   header["payment_terms"],
                    item.get("item_name"),       item.get("item_description"),
                    item.get("quantity"),         item.get("unit"),
                    item.get("unit_price"),       item.get("total_price"),
                    item.get("discount"),         item.get("taxation_details"),
                    item.get("delivery_date"),    item.get("delivery_time_days"),
                    item.get("item_level_1"),     item.get("item_level_2"),
                    item.get("item_level_3"),     item.get("item_level_4"),
                    item.get("item_level_5"),     item.get("item_level_6"),
                    item.get("item_level_7"),     item.get("item_level_8"),
                    item.get("commodity_tag"),    item.get("item_summary"),
                    item.get("supplier_match_conf"),
                )
                inserted += 1
            conn.commit()
        except Exception as exc:
            logger.error("Item insert failed: {}", exc)
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            conn.close()

        return inserted

    # ── embeddings ────────────────────────────────────────────────────────────

    def _run_embeddings(
        self, classify_fk: int, filename: str, content: str, pr_no: str,
    ) -> None:
        try:
            from agentcore.services.pinecone_service_client import (
                ensure_index_via_service,
                ingest_via_service,
            )

            ensure_index_via_service(
                index_name=self.pinecone_index,
                dimension=int(self.embed_dimension),
                metric="cosine",
            )
            embedding = self.embed_model.embed_query(content[:8000])
            ingest_via_service(
                index_name=self.pinecone_index,
                namespace=self.pinecone_namespace,
                vectors=[{
                    "id": f"{pr_no}_{classify_fk}",
                    "values": embedding,
                    "metadata": {
                        "purchase_req_no": pr_no,
                        "filename":        filename,
                        "classify_fk":     str(classify_fk),
                    },
                }],
            )
            self.log(f"[{pr_no}] Embedded {filename}")
        except Exception as exc:
            logger.warning("Embeddings failed for {} ({}): {}", filename, pr_no, exc)

    # ── benchmark ─────────────────────────────────────────────────────────────

    def _run_benchmark(
        self, tgt_cs: str, pr_no: str, items: list[dict],
    ) -> None:
        try:
            from agentcore.services.pinecone_service_client import search_via_service
            from langchain_core.messages import HumanMessage

            conn = self._connect(tgt_cs)
            cur  = conn.cursor()

            for item in items:
                dtl_id = item.get("purchase_dtl_id")
                if not dtl_id:
                    continue

                item_text = (
                    f"{item.get('item_name', '')} {item.get('item_description', '')}"
                ).strip()
                if not item_text:
                    continue

                embedding = self.embed_model.embed_query(item_text[:500])
                similar   = search_via_service(
                    index_name=self.pinecone_index,
                    namespace=self.pinecone_namespace,
                    vector=embedding,
                    top_k=int(self.pinecone_top_k),
                    filter={"purchase_req_no": {"$ne": pr_no}},
                )

                if not similar:
                    continue

                similar_ids = json.dumps([r.get("id") for r in similar if r.get("id")])
                unit_price  = item.get("unit_price")
                quantity    = item.get("quantity") or 1

                bench_prompt = (
                    f"You are a procurement analyst. Recommend a benchmark unit price "
                    f"for this item based on historical similar purchases.\n\n"
                    f"Item: {item_text}\n"
                    f"Current quoted unit price: {unit_price}\n"
                    f"Quantity: {quantity}\n"
                    f"Historical similar items (top {len(similar[:3])}):\n"
                    f"{json.dumps(similar[:3], indent=2)}\n\n"
                    f"Return ONLY JSON: "
                    f'{{\"bp_unit_price\": <number or null>, \"summary\": \"<2-3 sentences>\"}}'
                )

                try:
                    resp    = self.llm.invoke([HumanMessage(content=bench_prompt)])
                    raw     = getattr(resp, "content", str(resp)).strip()
                    raw     = re.sub(r"^```(?:json)?\s*", "", raw)
                    raw     = re.sub(r"\s*```$", "", raw)
                    bout    = json.loads(raw)
                except Exception:
                    bout = {}

                bp_unit = bout.get("bp_unit_price")
                summary = bout.get("summary", "")

                try:
                    bp_dec   = Decimal(str(bp_unit)) if bp_unit is not None else None
                    bp_total = (
                        round(float(bp_dec) * float(quantity), 2)
                        if bp_dec is not None else None
                    )
                except Exception:
                    bp_dec = bp_total = None

                try:
                    cur.execute(
                        """
                        MERGE benchmark_result WITH (HOLDLOCK) AS target
                        USING (SELECT ? AS purchase_dtl_id) AS src
                           ON target.purchase_dtl_id = src.purchase_dtl_id
                        WHEN MATCHED THEN
                            UPDATE SET bp_unit_price   = ?,
                                       bp_total_price  = ?,
                                       similar_dtl_ids = ?,
                                       summary         = ?,
                                       updated_at      = SYSUTCDATETIME()
                        WHEN NOT MATCHED THEN
                            INSERT (purchase_dtl_id, bp_unit_price,
                                    bp_total_price, similar_dtl_ids, summary)
                            VALUES (?, ?, ?, ?, ?);
                        """,
                        dtl_id,
                        bp_dec, bp_total, similar_ids, summary,
                        dtl_id, bp_dec, bp_total, similar_ids, summary,
                    )
                except Exception as exc:
                    logger.warning("Benchmark write failed dtl_id={}: {}", dtl_id, exc)

            conn.commit()
            conn.close()

        except Exception as exc:
            logger.warning("[{}] Benchmark stage failed: {}", pr_no, exc)

    # ── tracker ───────────────────────────────────────────────────────────────

    def _advance_tracker(self, tgt_cs: str, pr_no: str, stage_id: int) -> None:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE ras_tracker
                   SET current_stage_fk = ?,
                       updated_at       = SYSUTCDATETIME()
                 WHERE purchase_req_no  = ?
                """,
                stage_id, pr_no,
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Tracker advance failed PR={} stage={}: {}", pr_no, stage_id, exc)
        finally:
            conn.close()

    # ── main entry ────────────────────────────────────────────────────────────

    def run_pipeline(self) -> Message:
        tgt_cs          = self._conn_str(self.target_connection)
        classifications = self._parse_classifications()
        file_batch      = self._parse_file_batch()

        if not file_batch:
            return Message(text="ERROR: File batch is empty — check Stage 1-3 output.")
        if not classifications:
            return Message(text="ERROR: No classifications received from Worker Node.")

        ok_files  = 0
        err_files = 0
        log_lines: list[str] = []

        pr_filter = (self.pr_no_filter or "").strip()

        for filename, info in file_batch.items():
            pr_no    = info["pr_no"]
            content  = info["content"]

            if pr_filter and pr_no != pr_filter:
                continue

            doc_type  = classifications.get(filename, "Other")
            blob_path = f"{pr_no}/{filename}"

            # Stage 4 — save classification
            classify_fk = self._save_classification(tgt_cs, filename, pr_no, doc_type, blob_path)
            self._advance_tracker(tgt_cs, pr_no, _STAGE_CLASSIFICATION)
            self.log(f"[{pr_no}] {filename} → {doc_type}")

            if doc_type != "Quotation":
                log_lines.append(f"  SKIP  {filename} ({doc_type})")
                continue

            try:
                ctx = self._fetch_ras_context(tgt_cs, pr_no)

                # Stage 5 — LLM extraction
                extracted = self._extract_quotation(filename, content, ctx)
                self._advance_tracker(tgt_cs, pr_no, _STAGE_EXTRACTION)

                n_items = len(extracted.get("items", []))
                self.log(f"[{pr_no}] {filename} — {n_items} item(s) extracted")

                if classify_fk and extracted:
                    n_saved = self._save_items(tgt_cs, classify_fk, extracted)
                    self.log(f"[{pr_no}] {filename} — {n_saved} row(s) saved")

                # Stage 6 — embeddings
                if classify_fk:
                    self._run_embeddings(classify_fk, filename, content, pr_no)
                    self._advance_tracker(tgt_cs, pr_no, _STAGE_EMBEDDINGS)

                # Stage 7 — benchmark
                items_list = extracted.get("items", [])
                if items_list:
                    self._run_benchmark(tgt_cs, pr_no, items_list)
                    self._advance_tracker(tgt_cs, pr_no, _STAGE_PRICE_BENCHMARK)

                # Stage 8 — complete
                self._advance_tracker(tgt_cs, pr_no, _STAGE_COMPLETE)

                log_lines.append(f"  OK    {filename} → {n_items} item(s)")
                ok_files += 1

            except Exception as exc:
                logger.opt(exception=True).error(
                    "[{}] Stage 5-8 failed for {}: {}", pr_no, filename, exc,
                )
                log_lines.append(f"  ERR   {filename}: {exc}")
                err_files += 1

        summary = (
            f"Pipeline complete — "
            f"{ok_files} file(s) OK, {err_files} error(s), "
            f"{len(file_batch) - ok_files - err_files} skipped (non-Quotation)\n"
        )
        return Message(text=summary + "\n".join(log_lines))


# ── tiny helpers ───────────────────────────────────────────────────────────────

def _msg_text(msg: object) -> str:
    if msg is None:
        return ""
    if hasattr(msg, "text"):
        return msg.text or ""
    return str(msg)
