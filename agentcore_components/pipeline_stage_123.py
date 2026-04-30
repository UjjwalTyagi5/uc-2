from __future__ import annotations

import concurrent.futures
import io
import os
import random
import time
from typing import Optional

import pyodbc
from loguru import logger

from agentcore.custom import Node
from agentcore.io import HandleInput, IntInput, MessageTextInput, Output
from agentcore.schema.data import Data
from agentcore.schema.message import Message

_MAX_RETRIES = 3
_BASE_DELAY  = 2.0

_STAGE_INGESTION = 1
_STAGE_EMBED_DOC = 2
_STAGE_BLOB_UPLOAD = 3

_TEXT_EXT  = {".txt", ".csv", ".xml", ".json", ".html", ".htm", ".msg", ".eml"}
_PDF_EXT   = {".pdf"}
_EXCEL_EXT = {".xlsx", ".xls", ".xlsm", ".xlsb"}
_WORD_EXT  = {".doc", ".docx"}
_PPTX_EXT  = {".ppt", ".pptx"}


def _get_blob_config_by_name(connector_name: str) -> dict:
    name = (connector_name or "").strip()
    if not name:
        raise ValueError("blob_connector_name is empty. Enter the connector name from Settings → Connectors.")
    try:
        import asyncio
        import concurrent.futures as _cf
        from sqlalchemy import select

        async def _fetch():
            from agentcore.services.deps import get_db_service
            from agentcore.services.database.models.connector_catalogue.model import ConnectorCatalogue
            db_service = get_db_service()
            async with db_service.with_session() as session:
                stmt = (
                    select(ConnectorCatalogue)
                    .where(ConnectorCatalogue.name == name)
                    .where(ConnectorCatalogue.provider == "azure_blob")
                )
                result = await session.execute(stmt)
                row = result.scalars().first()
                if row is None:
                    raise ValueError(f"No azure_blob connector named {name!r} found. Check Settings → Connectors.")
                return {
                    "account_url":    row.host or "",
                    "container_name": row.database_name or "",
                }

        try:
            asyncio.get_running_loop()
            with _cf.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, _fetch()).result(timeout=10)
        except RuntimeError:
            return asyncio.run(_fetch())
    except Exception as exc:
        raise RuntimeError(f"Blob connector lookup failed: {exc}") from exc


class PipelineStage123Node(Node):
    display_name = "Pipeline Stage 1-3"
    description = "Stage 1: Ingestion. Stage 2: Text extraction. Stage 3: Azure Blob upload."
    icon = "Database"
    name = "PipelineStage123Node"

    inputs = [
        HandleInput(
            name="source_connection",
            display_name="Source DB — On-Prem SQL Server",
            input_types=["Data"],
            info="Connection Config from the on-prem Database Connector node.",
        ),
        HandleInput(
            name="target_connection",
            display_name="Target DB — Azure SQL",
            input_types=["Data"],
            info="Connection Config from the Azure SQL Database Connector node.",
        ),
        MessageTextInput(
            name="blob_connector_name",
            display_name="Azure Blob Connector Name",
            value="",
            info="Exact name of your Azure Blob connector in Settings → Connectors (e.g. agentcore-blob).",
        ),
        MessageTextInput(
            name="pr_no_filter",
            display_name="PR Number Filter (optional)",
            value="",
            advanced=True,
            info="Leave blank to process all pending PRs. Enter one PURCHASE_REQ_NO to test a single PR.",
        ),
        IntInput(
            name="batch_limit",
            display_name="Max PRs per Run",
            value=50,
            advanced=True,
        ),
        IntInput(
            name="parallel_workers",
            display_name="Parallel Workers",
            value=4,
            advanced=True,
        ),
        IntInput(
            name="max_content_chars",
            display_name="Max Characters per File",
            value=80000,
            advanced=True,
        ),
    ]

    outputs = [
        Output(
            display_name="File Batch",
            name="file_batch",
            method="build_file_batch",
            types=["Message"],
        ),
    ]

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
        kw = ["connection reset", "timeout", "throttl", "resource limit", "broken pipe", "transport-level", "login failed"]
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

    def _blob_cfg(self) -> dict:
        if not hasattr(self, "_blob_config_cache"):
            self._blob_config_cache = _get_blob_config_by_name(self.blob_connector_name)
        return self._blob_config_cache

    def _fetch_pending_prs(self, tgt_cs: str) -> list[str]:
        pr_filter = (self.pr_no_filter or "").strip()
        if pr_filter:
            return [pr_filter]
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                SELECT TOP (?) prm.PURCHASE_REQ_NO
                  FROM purchase_req_mst prm
                  LEFT JOIN ras_tracker rt ON rt.purchase_req_no = prm.PURCHASE_REQ_NO
                 WHERE (rt.current_stage_fk IS NULL OR rt.current_stage_fk < 1)
                 ORDER BY prm.C_DATETIME ASC
                """,
                int(self.batch_limit),
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def _fetch_attachments(self, src_cs: str, pr_no: str) -> list[dict]:
        conn = self._connect(src_cs)
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                SELECT a.ATTACHMENT_ID, a.FILENAME, a.FILECONTENT, a.FILE_TYPE, a.PURCHASE_REQ_NO
                  FROM purchase_req_attachments a
                 WHERE a.PURCHASE_REQ_NO = ? AND a.FILECONTENT IS NOT NULL
                """,
                pr_no,
            )
            return [
                {
                    "attachment_id": r[0],
                    "filename":      r[1] or f"attachment_{r[0]}",
                    "content":       bytes(r[2]),
                    "file_type":     r[3],
                    "pr_no":         r[4],
                }
                for r in cur.fetchall()
            ]
        finally:
            conn.close()

    def _extract_text(self, filename: str, raw: bytes) -> str:
        ext = os.path.splitext(filename.lower())[1]
        try:
            if ext in _TEXT_EXT:  return raw.decode("utf-8", errors="replace")
            if ext in _PDF_EXT:   return _extract_pdf(raw)
            if ext in _EXCEL_EXT: return _extract_excel(raw)
            if ext in _WORD_EXT:  return _extract_word(raw)
            if ext in _PPTX_EXT:  return _extract_pptx(raw)
            return f"[Binary — {ext} not text-extractable]"
        except Exception as exc:
            return f"[Extraction error: {exc}]"

    def _upload_blob(self, filename: str, raw: bytes, pr_no: str) -> str:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
        cfg       = self._blob_cfg()
        blob_name = f"{pr_no}/{filename}"
        BlobServiceClient(
            account_url=cfg["account_url"],
            credential=DefaultAzureCredential(),
        ).get_blob_client(container=cfg["container_name"], blob=blob_name).upload_blob(raw, overwrite=True)
        return blob_name

    def _advance_tracker(self, tgt_cs: str, pr_no: str, stage_id: int) -> None:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            if stage_id == _STAGE_INGESTION:
                cur.execute(
                    """
                    MERGE ras_tracker WITH (HOLDLOCK) AS target
                    USING (
                        SELECT PURCHASE_REQ_NO, PURCHASEFINALAPPROVALSTATUS
                          FROM purchase_req_mst WHERE PURCHASE_REQ_NO = ?
                    ) AS src ON target.purchase_req_no = src.PURCHASE_REQ_NO
                    WHEN MATCHED THEN
                        UPDATE SET current_stage_fk = ?, updated_at = SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN
                        INSERT (purchase_req_no, ras_status, current_stage_fk)
                        VALUES (src.PURCHASE_REQ_NO, src.PURCHASEFINALAPPROVALSTATUS, ?);
                    """,
                    pr_no, stage_id, stage_id,
                )
            else:
                cur.execute(
                    "UPDATE ras_tracker SET current_stage_fk=?, updated_at=SYSUTCDATETIME() WHERE purchase_req_no=?",
                    stage_id, pr_no,
                )
            conn.commit()
        finally:
            conn.close()

    def _process_pr(self, pr_no: str, src_cs: str, tgt_cs: str) -> dict:
        result: dict = {"pr_no": pr_no, "files": [], "status": "failed", "error": ""}
        try:
            self._advance_tracker(tgt_cs, pr_no, _STAGE_INGESTION)
            self.log(f"[{pr_no}] Stage 1 — ingested")

            attachments = self._fetch_attachments(src_cs, pr_no)
            if not attachments:
                self.log(f"[{pr_no}] No attachments — skipping")
                result["status"] = "skipped"
                return result

            file_data = []
            for att in attachments:
                text      = self._extract_text(att["filename"], att["content"])
                file_type = _detect_file_type(att["filename"])
                extra     = _build_extra_metadata(att["filename"], att["content"], text)
                if self.max_content_chars and len(text) > int(self.max_content_chars):
                    text = text[: int(self.max_content_chars)]
                file_data.append({
                    "filename":  att["filename"],
                    "pr_no":     pr_no,
                    "file_type": file_type,
                    "extra":     extra,
                    "text":      text,
                    "raw":       att["content"],
                })

            self._advance_tracker(tgt_cs, pr_no, _STAGE_EMBED_DOC)
            self.log(f"[{pr_no}] Stage 2 — {len(file_data)} file(s) extracted")

            for fd in file_data:
                fd["blob"] = self._upload_blob(fd["filename"], fd["raw"], pr_no)

            self._advance_tracker(tgt_cs, pr_no, _STAGE_BLOB_UPLOAD)
            self.log(f"[{pr_no}] Stage 3 — blobs uploaded")

            result["files"]  = file_data
            result["status"] = "success"
        except Exception as exc:
            logger.opt(exception=True).error("[{}] Stage 1-3 failed: {}", pr_no, exc)
            result["error"] = str(exc)
        return result

    def build_file_batch(self) -> Message:
        if hasattr(self, "_cached_result"):
            return self._cached_result

        src_cs = self._conn_str(self.source_connection)
        tgt_cs = self._conn_str(self.target_connection)

        pr_list = self._fetch_pending_prs(tgt_cs)
        if not pr_list:
            msg = Message(text="[No pending PRs to process]")
            self._cached_result = msg
            return msg

        self.log(f"Processing {len(pr_list)} PR(s)…")

        results: list[dict] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(self.parallel_workers)) as pool:
            futures = {pool.submit(self._process_pr, pr, src_cs, tgt_cs): pr for pr in pr_list}
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())

        parts: list[str] = []
        for r in results:
            for fd in r.get("files", []):
                parts.append(
                    f"=== FILE: {fd['filename']} (PR: {fd['pr_no']}) ===\n"
                    f"============================================================\n"
                    f"FILE METADATA\n"
                    f"============================================================\n"
                    f"Filename: {fd['filename']}\n"
                    f"File Type: {fd['file_type']}\n"
                    f"{fd['extra']}"
                    f"============================================================\n"
                    f"EXTRACTED CONTENT\n"
                    f"============================================================\n"
                    f"{fd['text']}\n"
                    f"============================================================\n\n"
                )

        batch_text = "".join(parts) if parts else "[No files extracted]"
        msg = Message(text=batch_text)
        self._cached_result = msg
        return msg


def _extract_pdf(raw: bytes) -> str:
    import fitz
    parts: list[str] = []
    with fitz.open(stream=raw, filetype="pdf") as doc:
        for page in doc:
            parts.append(page.get_text())
    return "\n".join(parts)


def _extract_excel(raw: bytes) -> str:
    import openpyxl
    wb    = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    parts: list[str] = []
    for ws in wb.worksheets:
        parts.append(f"[Sheet: {ws.title}]")
        for row in ws.iter_rows(values_only=True):
            cells = [str(c) if c is not None else "" for c in row]
            if any(cells):
                parts.append("\t".join(cells))
    return "\n".join(parts)


def _extract_word(raw: bytes) -> str:
    import docx
    doc = docx.Document(io.BytesIO(raw))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def _extract_pptx(raw: bytes) -> str:
    from pptx import Presentation
    prs   = Presentation(io.BytesIO(raw))
    parts: list[str] = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if hasattr(shape, "text") and shape.text.strip():
                parts.append(shape.text)
    return "\n".join(parts)


def _detect_file_type(filename: str) -> str:
    ext = os.path.splitext(filename.lower())[1]
    return {
        ".pdf": "PDF",
        ".xlsx": "Excel", ".xls": "Excel", ".xlsm": "Excel", ".xlsb": "Excel",
        ".docx": "Word",  ".doc": "Word",
        ".pptx": "PowerPoint", ".ppt": "PowerPoint",
        ".txt": "Text", ".csv": "CSV", ".xml": "XML", ".json": "JSON",
        ".html": "HTML", ".htm": "HTML", ".msg": "Email", ".eml": "Email",
        ".png": "Image", ".jpg": "Image", ".jpeg": "Image",
    }.get(ext, f"Unknown ({ext})")


def _build_extra_metadata(filename: str, raw: bytes, text: str) -> str:
    ext   = os.path.splitext(filename.lower())[1]
    lines: list[str] = []
    if ext == ".pdf":
        try:
            import fitz
            with fitz.open(stream=raw, filetype="pdf") as doc:
                lines.append(f"- page_count: {doc.page_count}")
        except Exception:
            pass
    if ext in {".xlsx", ".xls", ".xlsm", ".xlsb"}:
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
            lines.append(f"- sheet_count: {len(wb.sheetnames)}")
            lines.append(f"- sheet_names: {', '.join(wb.sheetnames[:10])}")
        except Exception:
            pass
    if ext in {".pptx", ".ppt"}:
        try:
            from pptx import Presentation
            lines.append(f"- slide_count: {len(Presentation(io.BytesIO(raw)).slides)}")
        except Exception:
            pass
    lines.append(f"- char_count: {len(text)}")
    lines.append(f"- size_bytes: {len(raw)}")
    return ("\n".join(lines) + "\n") if lines else ""
