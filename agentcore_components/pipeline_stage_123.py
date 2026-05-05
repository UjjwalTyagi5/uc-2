from __future__ import annotations

from loguru import logger

from agentcore.custom import Node
from agentcore.io import HandleInput, IntInput, MessageTextInput, Output
from agentcore.schema.data import Data
from agentcore.schema.message import Message

_MAX_RETRIES   = 3
_BASE_DELAY    = 2.0
_STAGE_INGESTION  = 1
_STAGE_EMBED_DOC  = 2
_STAGE_BLOB_UPLOAD = 3
_STAGE_EXCEPTION  = 99

_TEXT_EXT  = {".txt", ".csv", ".xml", ".json", ".html", ".htm", ".eml"}
_PDF_EXT   = {".pdf"}
_EXCEL_EXT = {".xlsx", ".xls", ".xlsm", ".xlsb"}
_WORD_EXT  = {".doc", ".docx"}
_PPTX_EXT  = {".ppt", ".pptx"}
_MSG_EXT   = {".msg"}

SUPPORTED_PARENTS   = (".xlsx", ".xls", ".docx", ".doc", ".pptx", ".ppt", ".pdf", ".msg")
ARCHIVE_EXTENSIONS  = (".zip", ".7z", ".rar", ".tar", ".tar.gz", ".tgz", ".tar.bz2")
EXTRACT_EXTENSIONS  = (
    ".pdf", ".xls", ".xlsx", ".doc", ".docx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".zip", ".7z", ".rar", ".tar", ".tar.gz", ".tgz", ".tar.bz2",
    ".txt", ".msg",
)
SKIP_EXTENSIONS = (".emf", ".bin")
FILE_SIGNATURES = [
    (b"%PDF",               ".pdf"),
    (b"PK\x03\x04",         ".zip"),
    (b"7z\xBC\xAF\x27\x1C", ".7z"),
    (b"Rar!\x1A\x07",       ".rar"),
    (b"\xFF\xD8\xFF",       ".jpg"),
    (b"\x89PNG\r\n\x1A\n",  ".png"),
]


# ── Embedded-doc extractor (inlined from embed_doc_extraction.extractor) ──

class FileExtractor:
    def __init__(self) -> None:
        self.extracted_count: int = 0
        self.parent_prefix:   str = ""

    def should_extract(self, filename: str) -> bool:
        fl = filename.lower()
        if any(fl.endswith(s) for s in SKIP_EXTENSIONS):
            return False
        return any(fl.endswith(e) for e in EXTRACT_EXTENSIONS)

    def sanitize(self, name: str) -> str:
        import os
        name = str(name).replace("\x00", "").replace("\x01", "").strip()
        name = os.path.basename(name)
        for ch in '<>:"/\\|?*':
            name = name.replace(ch, "_")
        name = "".join(ch for ch in name if ch.isprintable() and ord(ch) > 31)
        return name.strip(" .") or "file"

    def with_prefix(self, filename: str) -> str:
        filename = self.sanitize(filename)
        prefix   = self.sanitize(self.parent_prefix).strip()
        return f"{prefix}__{filename}" if prefix else filename

    def unique_path(self, directory: str, filename: str) -> str:
        import os
        fp = os.path.join(directory, filename)
        if not os.path.exists(fp):
            return fp
        base, ext = os.path.splitext(filename)
        c = 1
        while os.path.exists(os.path.join(directory, f"{base}_{c}{ext}")):
            c += 1
        return os.path.join(directory, f"{base}_{c}{ext}")

    def _find_payload(self, data: bytes):
        best = None
        for sig, ext in FILE_SIGNATURES:
            pos = data.find(sig)
            if pos != -1 and (best is None or pos < best[0]):
                best = (pos, ext)
        return best if best else (None, None)

    def _save_file(self, data: bytes, filename: str, out: str) -> str:
        import os, shutil
        target = self.unique_path(out, self.with_prefix(filename))
        with open(target, "wb") as f:
            f.write(data)
        self.extracted_count += 1
        if target.lower().endswith(ARCHIVE_EXTENSIONS):
            self.extract_archive(target, out)
        return target

    def extract_archive(self, path: str, out: str) -> None:
        import os, shutil, tarfile, tempfile, zipfile
        if not os.path.exists(path):
            return
        fl = path.lower()
        try:
            if fl.endswith(".zip"):
                with zipfile.ZipFile(path, "r") as z:
                    for m in z.infolist():
                        if m.is_dir():
                            continue
                        fn = os.path.basename(m.filename.replace("\\", "/").rstrip("/"))
                        if fn:
                            t = self.unique_path(out, self.with_prefix(fn))
                            with z.open(m) as s, open(t, "wb") as d:
                                shutil.copyfileobj(s, d)
                            self.extracted_count += 1
            elif fl.endswith(".7z"):
                import py7zr
                with tempfile.TemporaryDirectory() as tmp:
                    with py7zr.SevenZipFile(path, "r") as ar:
                        ar.extractall(path=tmp)
                    for root, _, files in os.walk(tmp):
                        for f in files:
                            t = self.unique_path(out, self.with_prefix(f))
                            shutil.copy2(os.path.join(root, f), t)
                            self.extracted_count += 1
            elif fl.endswith(".rar"):
                import rarfile
                with tempfile.TemporaryDirectory() as tmp:
                    with rarfile.RarFile(path, "r") as rar:
                        rar.extractall(tmp)
                    for root, _, files in os.walk(tmp):
                        for f in files:
                            t = self.unique_path(out, self.with_prefix(f))
                            shutil.copy2(os.path.join(root, f), t)
                            self.extracted_count += 1
            elif fl.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2")):
                with tarfile.open(path, "r:*") as tar:
                    for m in tar.getmembers():
                        if not m.isfile():
                            continue
                        fn = os.path.basename(m.name)
                        if fn:
                            t  = self.unique_path(out, self.with_prefix(fn))
                            src = tar.extractfile(m)
                            if src:
                                with src as s, open(t, "wb") as d:
                                    shutil.copyfileobj(s, d)
                                self.extracted_count += 1
        except Exception as exc:
            logger.warning(f"Archive extraction error ({os.path.basename(path)}): {exc}")
        try:
            os.remove(path)
        except Exception:
            pass

    def extract_from_ooxml(self, file_path: str, out: str) -> bool:
        import os, shutil, tempfile, zipfile
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with zipfile.ZipFile(file_path, "r") as zr:
                    zr.extractall(tmp)
                for prefix in ("xl", "word", "ppt"):
                    for sub in ("media", "embeddings"):
                        folder = os.path.join(tmp, prefix, sub)
                        if not os.path.exists(folder):
                            continue
                        for fn in os.listdir(folder):
                            src = os.path.join(folder, fn)
                            if not os.path.isfile(src):
                                continue
                            if fn.lower().endswith(".bin"):
                                self._extract_from_bin(src, out)
                            elif self.should_extract(fn):
                                dst = self.unique_path(out, self.with_prefix(fn))
                                shutil.copy2(src, dst)
                                self.extracted_count += 1
                                if dst.lower().endswith(ARCHIVE_EXTENSIONS):
                                    self.extract_archive(dst, out)
            return True
        except Exception as exc:
            logger.error(f"OOXML error ({os.path.basename(file_path)}): {exc}")
            return False

    def _extract_from_bin(self, bin_path: str, out: str) -> None:
        try:
            import olefile
            if not olefile.isOleFile(bin_path):
                return
            ole = olefile.OleFileIO(bin_path)
            for entry in ole.listdir():
                try:
                    sname = entry[-1] if entry else ""
                    data  = ole.openstream(entry).read()
                    if not data or len(data) < 16:
                        continue
                    offset, ext = self._find_payload(data)
                    if offset is None:
                        continue
                    payload   = data[offset:]
                    suggested = None
                    if sname.lower() in ("\x01ole10native", "ole10native"):
                        try:
                            end = data.find(b"\x00", 4)
                            if end != -1 and end - 4 < 260:
                                suggested = data[4:end].decode(errors="ignore")
                        except Exception:
                            pass
                    suggested = suggested or f"embedded_file{ext}"
                    target = self.unique_path(out, self.with_prefix(self.sanitize(suggested)))
                    with open(target, "wb") as f:
                        f.write(payload)
                    self.extracted_count += 1
                except Exception:
                    pass
            ole.close()
        except Exception as exc:
            logger.warning(f"OLE bin error: {exc}")

    def extract_from_ole(self, file_path: str, out: str) -> bool:
        try:
            import olefile
            if not olefile.isOleFile(file_path):
                return False
            ole = olefile.OleFileIO(file_path)
            for entry in ole.listdir():
                try:
                    sname = entry[-1] if entry else ""
                    data  = ole.openstream(entry).read()
                    if not data or len(data) < 16:
                        continue
                    offset, ext = self._find_payload(data)
                    if offset is None:
                        continue
                    payload   = data[offset:]
                    suggested = None
                    if sname.lower() in ("\x01ole10native", "ole10native"):
                        try:
                            end = data.find(b"\x00", 4)
                            if end != -1 and end - 4 < 260:
                                suggested = data[4:end].decode(errors="ignore")
                        except Exception:
                            pass
                    suggested = suggested or f"embedded_file{ext}"
                    target = self.unique_path(out, self.with_prefix(self.sanitize(suggested)))
                    with open(target, "wb") as f:
                        f.write(payload)
                    self.extracted_count += 1
                except Exception:
                    pass
            ole.close()
            return True
        except Exception as exc:
            logger.error(f"OLE error ({os.path.basename(file_path)}): {exc}")
            return False

    def extract_from_pdf(self, file_path: str, out: str) -> bool:
        try:
            import fitz
            doc = fitz.open(file_path)
            if doc.embfile_count() > 0:
                for i in range(doc.embfile_count()):
                    info = doc.embfile_info(i)
                    name = self.sanitize(info.get("filename", f"pdf_attachment_{i}"))
                    data = doc.embfile_get(i)
                    if data:
                        self._save_file(data, name, out)
            for pn, page in enumerate(doc, 1):
                annots = page.annots()
                if not annots:
                    continue
                for annot in annots:
                    if annot.type[0] == 17:
                        try:
                            fd      = annot.get_file()
                            name    = self.sanitize(fd.get("filename", f"page{pn}_attachment"))
                            content = fd.get("content", b"")
                            if content:
                                self._save_file(content, name, out)
                        except Exception:
                            pass
            doc.close()
            return True
        except Exception as exc:
            logger.error(f"PDF error ({os.path.basename(file_path)}): {exc}")
            return False

    def extract_from_msg(self, file_path: str, out: str) -> bool:
        try:
            import tempfile, os
            import extract_msg
            msg = extract_msg.Message(file_path)
            if not msg.attachments:
                msg.close()
                return True
            for att in msg.attachments:
                name = "attachment"
                try:
                    name = (
                        getattr(att, "longFilename", None)
                        or getattr(att, "shortFilename", None)
                        or getattr(att, "filename", None)
                        or "attachment"
                    )
                    name = self.sanitize(name)
                    if getattr(att, "type", None) == "msg" and not name.lower().endswith(".msg"):
                        name += ".msg"
                    data = getattr(att, "data", None) or b""
                    if not data:
                        with tempfile.TemporaryDirectory() as tmp:
                            att.save(customPath=tmp)
                            for fn in os.listdir(tmp):
                                src = os.path.join(tmp, fn)
                                if os.path.isfile(src):
                                    with open(src, "rb") as f:
                                        data = f.read()
                                    if name == "attachment":
                                        name = fn
                                    break
                    if data:
                        self._save_file(data, name, out)
                except Exception as exc:
                    logger.warning(f"MSG attachment '{name}' failed: {exc}")
            msg.close()
            return True
        except Exception as exc:
            logger.error(f"MSG error ({os.path.basename(file_path)}): {exc}")
            return False

    def detect_type(self, path: str):
        lower = path.lower()
        if lower.endswith((".xlsx", ".docx", ".pptx")): return "ooxml"
        if lower.endswith((".xls",  ".doc",  ".ppt")):  return "ole"
        if lower.endswith(".pdf"):                       return "pdf"
        if lower.endswith(".msg"):                       return "msg"
        try:
            with open(path, "rb") as f:
                h = f.read(8)
            if h.startswith(b"PK\x03\x04"):                        return "ooxml"
            if h.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"): return "ole"
            if h.startswith(b"%PDF"):                               return "pdf"
        except Exception:
            pass
        return None

    def process_file(self, file_path: str, out: str) -> bool:
        import os
        ft = self.detect_type(file_path)
        if not ft:
            return False
        os.makedirs(out, exist_ok=True)
        if ft == "ooxml": return self.extract_from_ooxml(file_path, out)
        if ft == "ole":   return self.extract_from_ole(file_path, out)
        if ft == "pdf":   return self.extract_from_pdf(file_path, out)
        if ft == "msg":   return self.extract_from_msg(file_path, out)
        return False


# ── Blob connector lookup ─────────────────────────────────────────────────

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
                cfg            = row.provider_config or {}
                account_url    = (cfg.get("account_url") or row.host or "").strip()
                container_name = (cfg.get("container_name") or row.database_name or "").strip()
                from urllib.parse import urlparse
                if not urlparse(account_url).netloc:
                    raise ValueError(
                        f"Azure Blob connector {name!r} has an invalid Storage Account URL: {account_url!r}. "
                        f"Set the full URL in Settings → Connectors, e.g. https://youraccount.blob.core.windows.net"
                    )
                if not container_name:
                    raise ValueError(f"Azure Blob connector {name!r} has no container_name.")
                return {"account_url": account_url, "container_name": container_name}

        try:
            asyncio.get_running_loop()
            with _cf.ThreadPoolExecutor() as pool:
                return pool.submit(asyncio.run, _fetch()).result(timeout=10)
        except RuntimeError:
            return asyncio.run(_fetch())
    except Exception as exc:
        raise RuntimeError(f"Blob connector lookup failed: {exc}") from exc


# ── Main component ────────────────────────────────────────────────────────

class PipelineStage123Node(Node):
    display_name = "Pipeline Stage 1-3"
    description  = "Stage 1: Ingestion. Stage 2: Embedded doc extraction + text extract. Stage 3: Azure Blob upload."
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
            info="Exact name of your Azure Blob connector in Settings → Connectors.",
        ),
        MessageTextInput(
            name="pr_no_filter",
            display_name="PR Number Filter (optional)",
            value="",
            advanced=True,
            info=(
                "Leave blank to process all pending approved PRs. "
                "Enter one PURCHASE_REQ_NO to force a full reprocess from scratch."
            ),
        ),
        # ── Full-pipeline inputs (wire these to run stages 4-8 in the same worker) ──
        HandleInput(
            name="llm",
            display_name="LLM — for Stages 4-8 (optional)",
            input_types=["LanguageModel"],
            required=False,
            info=(
                "Connect an LLM node to run the full pipeline (stages 1-8) per PR "
                "within the same parallel worker. When wired, each worker processes "
                "one PR end-to-end without waiting for other PRs to finish Stage 1-3. "
                "Leave disconnected to run stages 1-3 only."
            ),
        ),
        HandleInput(
            name="embed_model",
            display_name="Embeddings Model — for Stage 6 (optional)",
            input_types=["Embeddings"],
            required=False,
            info="Required together with LLM to run stages 4-8 inline.",
        ),
        MessageTextInput(
            name="pinecone_index",
            display_name="Pinecone Index Name",
            value="ras-quotations",
            advanced=True,
            info="Pinecone index used for embeddings (Stage 6) and retry cleanup.",
        ),
        MessageTextInput(
            name="pinecone_namespace",
            display_name="Pinecone Namespace",
            value="procurement",
            advanced=True,
            info="Pinecone namespace used for embeddings (Stage 6) and retry cleanup.",
        ),
        IntInput(name="pinecone_top_k",    display_name="Benchmark Top-K (Stage 7)", value=5,     advanced=True),
        IntInput(name="batch_limit",       display_name="Max PRs per Run (0 = all)", value=0,     advanced=True,
                 info="0 = process all pending PRs. Set to N (e.g. 50) to cap at N PRs per run. Ignored when PR Number Filter is set."),
        IntInput(name="parallel_workers",  display_name="Parallel Workers",          value=4,     advanced=True),
        IntInput(name="max_content_chars", display_name="Max Characters per File",   value=80000, advanced=True),
    ]

    outputs = [
        Output(display_name="File Batch", name="file_batch", method="build_file_batch", types=["Message"]),
        Output(display_name="Processed PRs", name="processed_prs", method="get_processed_prs", types=["Data"]),
    ]

    # ── Connection helpers ────────────────────────────────────────────────

    def _conn_str(self, conn_data: Data) -> str:
        d      = conn_data.data or {}
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

    def _connect(self, conn_str: str):
        import pyodbc, random, time
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

    # ── Fetch pending PRs ─────────────────────────────────────────────────

    def _fetch_pending_prs(self, tgt_cs: str) -> list[str]:
        pr_filter = (self.pr_no_filter or "").strip()
        if pr_filter:
            return [pr_filter]
        limit = int(self.batch_limit)
        conn  = self._connect(tgt_cs)
        cur   = conn.cursor()
        # Stage 1-3 only owns stages 1 and 2.  PRs already at stage 3+ have
        # completed Stage 1-3 and belong to Stage 4-8 — do NOT re-pick them.
        # Only pick: new PRs (no tracker row), reset PRs (NULL stage), or PRs
        # stuck mid-Stage-1-3 (stages 1 or 2).
        _WHERE = """
            FROM [ras_procurement].[purchase_req_mst] prm
            LEFT JOIN [ras_procurement].[ras_tracker] rt
              ON prm.PURCHASE_REQ_NO = rt.purchase_req_no
           WHERE (rt.purchase_req_no IS NULL
                  OR rt.current_stage_fk IS NULL
                  OR rt.current_stage_fk IN (1, 2))
             AND UPPER(prm.PURCHASEFINALAPPROVALSTATUS)
                     IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
           ORDER BY prm.C_DATETIME ASC
        """
        try:
            if limit > 0:
                cur.execute(f"SELECT TOP (?) prm.PURCHASE_REQ_NO {_WHERE}", limit)
            else:
                cur.execute(f"SELECT prm.PURCHASE_REQ_NO {_WHERE}")
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    # ── Pre-run cleanup ───────────────────────────────────────────────────

    def _cleanup_for_pr(self, tgt_cs: str, pr_no: str) -> None:
        """Delete all prior pipeline output for a PR — runs only if the PR already
        exists in ras_tracker (i.e. it has been processed before). New PRs are
        skipped entirely so their data is never touched."""
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        cur.execute("SELECT ras_uuid_pk FROM [ras_procurement].[ras_tracker] WHERE purchase_req_no = ?", pr_no)
        existing = cur.fetchone()
        conn.close()
        if existing is None:
            # Brand-new PR — nothing to clean, proceed straight to Stage 1.
            self.log(f"[{pr_no}] New PR — skipping cleanup")
            return
        self.log(f"[{pr_no}] Existing PR detected — cleaning prior data before reprocessing")

        # Single connection for all DB cleanup work (collect IDs + delete tables)
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        pinecone_ids: list[str] = []
        try:
            # Collect Pinecone vector IDs FIRST, before deleting quotation_extracted_items.
            # We build BOTH formats so stale vectors from any run are cleaned:
            #   • old format: extracted_item_uuid_pk (used by pre-dtl_ code)
            #   • new format: dtl_{purchase_dtl_id}   (current code, stable per line item)
            cur.execute("""
                SELECT qi.[extracted_item_uuid_pk], qi.[purchase_dtl_id], qi.[is_selected_quote]
                  FROM [ras_procurement].[quotation_extracted_items] qi
                  JOIN [ras_procurement].[attachment_classification] ac
                    ON qi.[attachment_classify_fk] = ac.[attachment_classify_uuid_pk]
                  JOIN [ras_procurement].[ras_tracker] rt
                    ON ac.[ras_uuid_pk] = rt.[ras_uuid_pk]
                 WHERE rt.[purchase_req_no] = ?
            """, pr_no)
            rows = cur.fetchall()
            selected_count = sum(1 for r in rows if r[2])
            # New format: only is_selected_quote=1 rows (current code only embeds these)
            new_ids = list({f"dtl_{r[1]}" for r in rows if r[1] and r[2]})
            # Old format: only is_selected_quote=1 UUIDs (legacy vectors only existed for selected rows too)
            old_ids = [str(r[0]) for r in rows if r[0] and r[2]]
            pinecone_ids = list(set(old_ids + new_ids))
            self.log(
                f"[{pr_no}] Pinecone cleanup: {len(rows)} extracted rows, "
                f"{selected_count} is_selected_quote=1 — "
                f"deleting {len(pinecone_ids)} vector(s): "
                f"{len(new_ids)} dtl_ ID(s) + {len(old_ids)} legacy UUID(s)"
            )

            # 1. Null FK back-references in benchmark_result
            cur.execute("""
                UPDATE br SET br.low_hist_item_fk = NULL
                  FROM [ras_procurement].[benchmark_result] br
                  JOIN [ras_procurement].[quotation_extracted_items] qi ON br.low_hist_item_fk = qi.extracted_item_uuid_pk
                  JOIN [ras_procurement].[attachment_classification] ac ON qi.attachment_classify_fk = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            cur.execute("""
                UPDATE br SET br.last_hist_item_fk = NULL
                  FROM [ras_procurement].[benchmark_result] br
                  JOIN [ras_procurement].[quotation_extracted_items] qi ON br.last_hist_item_fk = qi.extracted_item_uuid_pk
                  JOIN [ras_procurement].[attachment_classification] ac ON qi.attachment_classify_fk = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            # 2. benchmark_result
            cur.execute("""
                DELETE br FROM [ras_procurement].[benchmark_result] br
                  JOIN [ras_procurement].[quotation_extracted_items] qi ON br.extracted_item_uuid_fk = qi.extracted_item_uuid_pk
                  JOIN [ras_procurement].[attachment_classification] ac ON qi.attachment_classify_fk = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            # 3. quotation_extracted_items
            cur.execute("""
                DELETE qi FROM [ras_procurement].[quotation_extracted_items] qi
                  JOIN [ras_procurement].[attachment_classification] ac ON qi.attachment_classify_fk = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            # 4. embedded_attachment_classification
            cur.execute("""
                DELETE ec FROM [ras_procurement].[embedded_attachment_classification] ec
                  JOIN [ras_procurement].[attachment_classification] ac ON ec.attachment_classification_id = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            # 5. attachment_classification
            cur.execute("""
                DELETE ac FROM [ras_procurement].[attachment_classification] ac
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            # 6. BI dashboard rows
            cur.execute("""
                DELETE FROM [ras_procurement].[vw_get_ras_data_for_bidashboard]
                 WHERE [PURCHASE_REQ_NO] = ?""", pr_no)
            # 7. Exception records
            cur.execute("""
                DELETE FROM [ras_procurement].[ras_pipeline_exceptions]
                 WHERE ras_tracker_id = (
                     SELECT ras_uuid_pk FROM [ras_procurement].[ras_tracker]
                      WHERE purchase_req_no = ?)""", pr_no)
            # 8. Any remaining data via SP
            cur.execute("EXEC [ras_procurement].[usp_cleanup_pr_data] ?", pr_no)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            raise RuntimeError(f"Pre-run cleanup failed for {pr_no}: {exc}") from exc
        finally:
            conn.close()

        # Delete stale Pinecone vectors (non-fatal)
        if pinecone_ids:
            pinecone_index = (getattr(self, "pinecone_index", None) or "").strip()
            pinecone_ns    = (getattr(self, "pinecone_namespace", None) or "").strip()
            if pinecone_index and pinecone_ns:
                try:
                    from agentcore.services.pinecone_service_client import delete_vectors_via_service
                    preview = pinecone_ids[:5]
                    more    = len(pinecone_ids) - 5
                    self.log(
                        f"[{pr_no}] Sending {len(pinecone_ids)} ID(s) to Pinecone delete "
                        f"(index={pinecone_index!r}, namespace={pinecone_ns!r}) — "
                        f"first 5: {preview}" + (f" … +{more} more" if more > 0 else "")
                    )
                    delete_vectors_via_service(
                        index_name=pinecone_index, namespace=pinecone_ns,
                        vector_ids=pinecone_ids,
                    )
                    self.log(f"[{pr_no}] Pinecone delete completed — {len(pinecone_ids)} ID(s) sent for removal")
                except Exception as exc:
                    self.log(f"[{pr_no}] Warning — Pinecone cleanup failed (non-fatal): {exc}")
            else:
                self.log(
                    f"[{pr_no}] Warning — pinecone_index or pinecone_namespace not set; "
                    f"{len(pinecone_ids)} vector(s) were NOT deleted from Pinecone. "
                    f"Check the 'Pinecone Index' and 'Pinecone Namespace' inputs on the component."
                )
        else:
            self.log(f"[{pr_no}] No Pinecone vectors found in DB for this PR — skipping Pinecone delete")

        # Delete Azure Blob folder (non-fatal)
        try:
            self._delete_blob_folder(pr_no)
        except Exception as exc:
            self.log(f"[{pr_no}] Warning — Blob folder cleanup failed (non-fatal): {exc}")

        self.log(f"[{pr_no}] Cleanup complete — all pipeline tables cleared")

    def _reset_for_reprocess(self, tgt_cs: str, pr_no: str) -> None:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute("""
                DELETE FROM [ras_procurement].[ras_pipeline_exceptions]
                 WHERE ras_tracker_id = (SELECT ras_uuid_pk FROM [ras_procurement].[ras_tracker] WHERE purchase_req_no = ?)
            """, pr_no)
            cur.execute("""
                UPDATE [ras_procurement].[ras_tracker]
                   SET current_stage_fk = NULL, retry_count = COALESCE(retry_count,0)+1, updated_at = SYSUTCDATETIME()
                 WHERE purchase_req_no = ?
            """, pr_no)
            conn.commit()
            self.log(f"[{pr_no}] Tracker reset for reprocess")
        finally:
            conn.close()

    # ── Fetch attachments (Azure SQL for IDs+names, on-prem for binary) ───

    def _fetch_attachments(self, src_cs: str, tgt_cs: str, pr_no: str) -> list[dict]:
        tgt = self._connect(tgt_cs)
        cur = tgt.cursor()
        try:
            cur.execute("""
                SELECT pa.[ATTACHMENT_ID], pa.[FILES_NAME]
                  FROM [ras_procurement].[purchase_req_mst] prm
                  JOIN [ras_procurement].[purchase_attachments] pa ON prm.[PURCHASE_REQ_ID] = pa.[PURCHASE_ID]
                 WHERE prm.[PURCHASE_REQ_NO] = ?
                   AND pa.[ATTACHMENT_ID] IS NOT NULL AND pa.[FILES_NAME] IS NOT NULL
            """, pr_no)
            rows = cur.fetchall()
        finally:
            tgt.close()
        if not rows:
            return []
        id_to_filename = {str(r[0]): r[1] for r in rows}
        att_ids        = list(id_to_filename.keys())
        placeholders   = ",".join(["?"] * len(att_ids))
        src = self._connect(src_cs)
        cur = src.cursor()
        try:
            cur.execute(
                f"SELECT [attachment_id], [doc] FROM [dbo].[ras_attachments]"
                f" WHERE [attachment_id] IN ({placeholders}) AND [doc] IS NOT NULL",
                att_ids,
            )
            return [
                {
                    "attachment_id": str(r[0]),
                    "filename":      id_to_filename.get(str(r[0]), f"attachment_{r[0]}"),
                    "content":       bytes(r[1]),
                    "pr_no":         pr_no,
                }
                for r in cur.fetchall()
            ]
        finally:
            src.close()

    # ── Tracker helpers ───────────────────────────────────────────────────

    def _get_tracker_uuid(self, tgt_cs: str, pr_no: str) -> str:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute("SELECT ras_uuid_pk FROM [ras_procurement].[ras_tracker] WHERE purchase_req_no = ?", pr_no)
            row = cur.fetchone()
            return str(row[0]) if row else ""
        finally:
            conn.close()

    def _advance_tracker(self, tgt_cs: str, pr_no: str, stage_id: int) -> None:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            if stage_id == _STAGE_INGESTION:
                cur.execute("""
                    MERGE [ras_procurement].[ras_tracker] WITH (HOLDLOCK) AS target
                    USING (
                        SELECT PURCHASE_REQ_NO, PURCHASEFINALAPPROVALSTATUS
                          FROM [ras_procurement].[purchase_req_mst] WHERE PURCHASE_REQ_NO = ?
                    ) AS src ON target.purchase_req_no = src.PURCHASE_REQ_NO
                    WHEN MATCHED THEN UPDATE SET current_stage_fk=?, updated_at=SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN INSERT (purchase_req_no, ras_status, current_stage_fk)
                        VALUES (src.PURCHASE_REQ_NO, src.PURCHASEFINALAPPROVALSTATUS, ?);
                """, pr_no, stage_id, stage_id)
            else:
                cur.execute("""
                    UPDATE [ras_procurement].[ras_tracker]
                       SET current_stage_fk=?, updated_at=SYSUTCDATETIME()
                     WHERE purchase_req_no=?
                """, stage_id, pr_no)
            conn.commit()
        finally:
            conn.close()

    def _record_exception(self, tgt_cs: str, pr_no: str, stage_id: int, error_msg: str) -> None:
        try:
            conn = self._connect(tgt_cs)
            cur  = conn.cursor()
            try:
                cur.execute("""
                    MERGE [ras_procurement].[ras_tracker] WITH (HOLDLOCK) AS target
                    USING (SELECT PURCHASE_REQ_NO, PURCHASEFINALAPPROVALSTATUS
                             FROM [ras_procurement].[purchase_req_mst] WHERE PURCHASE_REQ_NO = ?
                    ) AS src ON target.purchase_req_no = src.PURCHASE_REQ_NO
                    WHEN MATCHED THEN UPDATE SET current_stage_fk=99, updated_at=SYSUTCDATETIME()
                    WHEN NOT MATCHED THEN INSERT (purchase_req_no, ras_status, current_stage_fk)
                        VALUES (src.PURCHASE_REQ_NO, src.PURCHASEFINALAPPROVALSTATUS, 99);
                """, pr_no)
                cur.execute("SELECT ras_uuid_pk FROM [ras_procurement].[ras_tracker] WHERE purchase_req_no=?", pr_no)
                row = cur.fetchone()
                if row:
                    cur.execute("""
                        INSERT INTO [ras_procurement].[ras_pipeline_exceptions] (ras_tracker_id, stage_id, exception_message)
                        VALUES (?, ?, ?)
                    """, row[0], stage_id, error_msg[:4000])
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning(f"[{pr_no}] Could not write exception record: {exc}")

    # ── attachment_classification upserts ─────────────────────────────────

    def _upsert_parent_classification(self, tgt_cs, pr_no, ras_uuid_pk, att_id, file_path, embedded_flag, embedded_count) -> str:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute("""
                MERGE [ras_procurement].[attachment_classification] WITH (HOLDLOCK) AS target
                USING (SELECT ? AS ras_uuid_pk, ? AS attachment_id) AS src
                  ON target.[attachment_id] = src.[attachment_id]
                WHEN MATCHED THEN
                    UPDATE SET [file_path]=?, [embedded_file_flag]=?, [embedded_file_count]=?, [updated_at]=SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT ([ras_uuid_pk],[attachment_id],[file_path],[embedded_file_flag],[embedded_file_count])
                    VALUES (?,?,?,?,?);
            """, ras_uuid_pk, att_id,
                file_path, embedded_flag, embedded_count,
                ras_uuid_pk, att_id, file_path, embedded_flag, embedded_count)
            cur.execute(
                "SELECT [attachment_classify_uuid_pk] FROM [ras_procurement].[attachment_classification] WHERE [attachment_id]=?",
                att_id,
            )
            row = cur.fetchone()
            conn.commit()
            return str(row[0]) if row else ""
        finally:
            conn.close()

    def _upsert_embedded_classification(self, tgt_cs, parent_pk, parent_att_id, file_path) -> None:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute("""
                MERGE [ras_procurement].[embedded_attachment_classification] WITH (HOLDLOCK) AS target
                USING (SELECT ? AS attachment_classification_id, ? AS file_path) AS src
                  ON target.[attachment_classification_id] = src.[attachment_classification_id]
                 AND target.[file_path] = src.[file_path]
                WHEN MATCHED THEN UPDATE SET [parent_attachment_id]=?, [updated_at]=SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT ([attachment_classification_id],[parent_attachment_id],[file_path])
                    VALUES (?,?,?);
            """, parent_pk, file_path,
                parent_att_id,
                parent_pk, parent_att_id, file_path)
            conn.commit()
        finally:
            conn.close()

    # ── Text extraction ───────────────────────────────────────────────────

    def _extract_text(self, filename: str, raw: bytes) -> str:
        import os
        ext = os.path.splitext(filename.lower())[1]
        try:
            if ext in _TEXT_EXT:  return raw.decode("utf-8", errors="replace")
            if ext in _PDF_EXT:   return _extract_pdf(raw)
            if ext in _EXCEL_EXT: return _extract_excel(raw)
            if ext in _WORD_EXT:  return _extract_word(raw)
            if ext in _PPTX_EXT:  return _extract_pptx(raw)
            if ext in _MSG_EXT:   return _extract_msg_text(raw)
            return f"[Binary — {ext} not text-extractable]"
        except Exception as exc:
            return f"[Extraction error: {exc}]"

    # ── BI dashboard sync ─────────────────────────────────────────────────

    def _sync_bi_dashboard(self, src_cs: str, tgt_cs: str, pr_no: str) -> None:
        """Refresh the BI dashboard row for this PR.

        Reads from on-prem view [dbo].[vw_get_ras_data_for_bidashboard],
        deletes the old Azure row, inserts fresh data.
        Non-fatal — caller wraps in try/except.
        """
        import pyodbc
        # ── 1. read from on-prem ──────────────────────────────────────
        src_conn = self._connect(src_cs)
        try:
            cur = src_conn.cursor()
            cur.execute(
                "SELECT * FROM [dbo].[vw_get_ras_data_for_bidashboard] "
                "WHERE [PURCHASE_REQ_NO] = ?",
                pr_no,
            )
            columns = [d[0] for d in cur.description] if cur.description else []
            rows    = cur.fetchall()
        finally:
            src_conn.close()

        # ── 2. delete old + insert fresh in Azure ─────────────────────
        tgt_conn = self._connect(tgt_cs)
        try:
            tc = tgt_conn.cursor()
            tc.execute(
                "DELETE FROM [ras_procurement].[vw_get_ras_data_for_bidashboard] "
                "WHERE [PURCHASE_REQ_NO] = ?",
                pr_no,
            )
            deleted = tc.rowcount
            if rows and columns:
                col_list     = ", ".join(f"[{c}]" for c in columns)
                placeholders = ", ".join(["?"] * len(columns))
                ins_sql = (
                    f"INSERT INTO [ras_procurement].[vw_get_ras_data_for_bidashboard] "
                    f"({col_list}) VALUES ({placeholders})"
                )
                tc.fast_executemany = True
                tc.executemany(ins_sql, [list(r) for r in rows])
            tgt_conn.commit()
            self.log(
                f"[{pr_no}] BI dashboard synced — "
                f"deleted {deleted}, inserted {len(rows)} row(s)"
            )
        except Exception:
            tgt_conn.rollback()
            raise
        finally:
            tgt_conn.close()

    # ── Blob helpers ──────────────────────────────────────────────────────

    def _container_client(self):
        """Return a cached ContainerClient — credential is initialised once per component instance."""
        if not hasattr(self, "_container_client_cache"):
            from azure.identity import DefaultAzureCredential
            from azure.storage.blob import BlobServiceClient
            cfg        = self._blob_cfg()
            credential = DefaultAzureCredential(
                exclude_environment_credential=True,
                exclude_interactive_browser_credential=True,
            )
            self._container_client_cache = BlobServiceClient(
                account_url=cfg["account_url"],
                credential=credential,
            ).get_container_client(cfg["container_name"])
        return self._container_client_cache

    def _delete_blob_folder(self, pr_no: str) -> None:
        """Delete every blob under procurement/{pr_no}/ in Azure Blob Storage."""
        safe_pr = pr_no.replace("/", "_")
        prefix  = f"procurement/{safe_pr}/"
        cc      = self._container_client()
        blobs   = [b.name for b in cc.list_blobs(name_starts_with=prefix)]
        if not blobs:
            return
        _BATCH = 256
        for i in range(0, len(blobs), _BATCH):
            cc.delete_blobs(*blobs[i : i + _BATCH])
        self.log(f"[{pr_no}] Deleted {len(blobs)} blob(s) from {prefix}")

    def _upload_blob(self, raw: bytes, blob_path: str) -> None:
        self._container_client().get_blob_client(blob_path).upload_blob(raw, overwrite=True)

    # ── Full-pipeline continuation (stages 4-8) ──────────────────────────

    def _run_stages_48(self, pr_no: str, tgt_cs: str, result: dict) -> None:
        """Run stages 4-8 for a PR that has already completed stages 1-3.

        Imports processing functions from pipeline_stage_58 at call time so
        Stage 1-3 can still load independently if Stage 4-8 is absent.
        Exceptions are caught here, recorded to ras_pipeline_exceptions, and
        reflected in result["status"] / result["error"] — they do NOT propagate
        to the outer _process_pr try/except so stages 1-3 are not re-rolled back.
        """
        try:
            try:
                from agentcore_components.pipeline_stage_58 import (
                    _run_classification, _run_extraction,
                    _run_embeddings, _run_benchmark,
                    _advance_tracker as _adv,
                    _STAGE_CLASSIFICATION, _STAGE_EXTRACTION,
                    _STAGE_EMBEDDINGS, _STAGE_PRICE_BENCHMARK, _STAGE_COMPLETE,
                )
            except ImportError:
                from pipeline_stage_58 import (  # type: ignore[import]
                    _run_classification, _run_extraction,
                    _run_embeddings, _run_benchmark,
                    _advance_tracker as _adv,
                    _STAGE_CLASSIFICATION, _STAGE_EXTRACTION,
                    _STAGE_EMBEDDINGS, _STAGE_PRICE_BENCHMARK, _STAGE_COMPLETE,
                )
        except Exception as imp_exc:
            result["status"] = "success_stage3_only"
            result["error"]  = f"Stage 4-8 import failed — stages 1-3 are saved: {imp_exc}"
            self.log(f"[{pr_no}] Warning — could not import Stage 4-8 functions: {imp_exc}")
            return

        blob_cfg      = self._blob_cfg()
        prompts: dict = {}  # use Stage 4-8 default prompts
        current_stage = _STAGE_CLASSIFICATION
        top_k         = int(getattr(self, "pinecone_top_k", 5))

        try:
            # Stage 4 — Classification
            self.log(f"[{pr_no}] Stage 4 — classifying attachments…")
            _run_classification(self.llm, tgt_cs, blob_cfg, pr_no, prompts)
            _adv(tgt_cs, pr_no, _STAGE_CLASSIFICATION)
            self.log(f"[{pr_no}] Stage 4 — classification complete")

            # Stage 5 — Extraction
            current_stage = _STAGE_EXTRACTION
            self.log(f"[{pr_no}] Stage 5 — extracting quotation items…")
            n_items = _run_extraction(self.llm, tgt_cs, blob_cfg, pr_no, prompts)
            _adv(tgt_cs, pr_no, _STAGE_EXTRACTION)
            self.log(f"[{pr_no}] Stage 5 — {n_items} item(s) extracted")

            # Stage 6 — Embeddings
            current_stage = _STAGE_EMBEDDINGS
            _run_embeddings(tgt_cs, pr_no, self.embed_model,
                            self.pinecone_index, self.pinecone_namespace)
            _adv(tgt_cs, pr_no, _STAGE_EMBEDDINGS)
            self.log(f"[{pr_no}] Stage 6 — embeddings done")

            # Stage 7 — Benchmark
            current_stage = _STAGE_PRICE_BENCHMARK
            _run_benchmark(self.llm, tgt_cs, pr_no, self.embed_model,
                           self.pinecone_index, self.pinecone_namespace, top_k)
            _adv(tgt_cs, pr_no, _STAGE_PRICE_BENCHMARK)
            self.log(f"[{pr_no}] Stage 7 — benchmark done")

            # Stage 8 — Complete
            _adv(tgt_cs, pr_no, _STAGE_COMPLETE)
            self.log(f"[{pr_no}] Stage 8 — pipeline complete")
            result["status"] = "complete"

        except Exception as exc:
            logger.opt(exception=True).error(
                "[{}] Stage 4-8 failed at stage {}: {}", pr_no, current_stage, exc
            )
            result["error"] = str(exc)
            self._record_exception(tgt_cs, pr_no, current_stage, str(exc))

    # ── Process single PR ─────────────────────────────────────────────────

    def _process_pr(self, pr_no: str, src_cs: str, tgt_cs: str) -> dict:
        import os, shutil, tempfile
        result        = {"pr_no": pr_no, "files": [], "status": "failed", "error": ""}
        current_stage = _STAGE_INGESTION
        work_dir      = tempfile.mkdtemp()
        self.log(f"[{pr_no}] Worker started")
        try:
            # ── Cleanup at start — only for PRs that already have prior data ──
            # Checks ras_tracker: new PR → skipped; existing/retried PR → blobs +
            # Pinecone vectors + quotation_extracted_items + benchmark_result deleted.
            self._cleanup_for_pr(tgt_cs, pr_no)

            # Stage 1 — INGESTION
            current_stage = _STAGE_INGESTION
            self._advance_tracker(tgt_cs, pr_no, _STAGE_INGESTION)
            self.log(f"[{pr_no}] Stage 1 — ingested")

            ras_uuid    = self._get_tracker_uuid(tgt_cs, pr_no)
            attachments = self._fetch_attachments(src_cs, tgt_cs, pr_no)

            # BI dashboard sync: read on-prem view → refresh Azure row.
            # Non-fatal — log and continue if it fails.
            try:
                self._sync_bi_dashboard(src_cs, tgt_cs, pr_no)
            except Exception as _bi_exc:
                self.log(f"[{pr_no}] Warning — BI dashboard sync failed (non-fatal): {_bi_exc}")

            if not attachments:
                self.log(f"[{pr_no}] No attachments — skipping")
                result["status"] = "skipped"
                return result

            safe_pr  = pr_no.replace("/", "_")
            extractor = FileExtractor()
            all_files = []   # dicts: filename, content, blob_path, att_id, is_embedded

            for att in attachments:
                att_id   = att["attachment_id"]
                att_dir  = os.path.join(work_dir, att_id)
                emb_dir  = os.path.join(att_dir, "extracted")
                os.makedirs(att_dir, exist_ok=True)
                os.makedirs(emb_dir,  exist_ok=True)

                # Save parent file to temp disk so FileExtractor can open it
                parent_path = os.path.join(att_dir, att["filename"])
                with open(parent_path, "wb") as fh:
                    fh.write(att["content"])

                # Run embedded extraction
                extractor.parent_prefix = os.path.splitext(att["filename"])[0]
                extractor.process_file(parent_path, emb_dir)

                # Collect embedded file contents
                embedded = []
                if os.path.isdir(emb_dir):
                    for emb_name in sorted(os.listdir(emb_dir)):
                        emb_path = os.path.join(emb_dir, emb_name)
                        if os.path.isfile(emb_path):
                            with open(emb_path, "rb") as fh:
                                emb_content = fh.read()
                            embedded.append({
                                "filename":    emb_name,
                                "content":     emb_content,
                                # Include procurement/ prefix so upload path == DB path
                                "blob_path":   f"procurement/{safe_pr}/{att_id}/extracted/{emb_name}",
                                "att_id":      att_id,
                                "is_embedded": True,
                            })

                # Record parent in attachment_classification
                parent_blob_path = f"procurement/{safe_pr}/{att_id}/{att['filename']}"
                parent_pk = self._upsert_parent_classification(
                    tgt_cs, pr_no, ras_uuid, att_id,
                    parent_blob_path,
                    embedded_flag  = len(embedded) > 0,
                    embedded_count = len(embedded),
                )

                # Record embedded files — blob_path already has procurement/ prefix
                for emb in embedded:
                    self._upsert_embedded_classification(tgt_cs, parent_pk, att_id, emb["blob_path"])

                all_files.append({
                    "filename":    att["filename"],
                    "content":     att["content"],
                    # Include procurement/ prefix so upload path == DB path
                    "blob_path":   f"procurement/{safe_pr}/{att_id}/{att['filename']}",
                    "att_id":      att_id,
                    "is_embedded": False,
                })
                all_files.extend(embedded)

                self.log(f"[{pr_no}] [{att_id}] {att['filename']} — {len(embedded)} embedded file(s) extracted")

            # Stage 2 — EMBED_DOC_EXTRACTION
            current_stage = _STAGE_EMBED_DOC
            self._advance_tracker(tgt_cs, pr_no, _STAGE_EMBED_DOC)
            self.log(f"[{pr_no}] Stage 2 — {len(all_files)} file(s) ready (parent + embedded)")

            # Upload all files to blob + build text batch
            file_data = []
            for f_info in all_files:
                self._upload_blob(f_info["content"], f_info["blob_path"])
                text      = self._extract_text(f_info["filename"], f_info["content"])
                file_type = _detect_file_type(f_info["filename"])
                extra     = _build_extra_metadata(f_info["filename"], f_info["content"], text)
                if self.max_content_chars and len(text) > int(self.max_content_chars):
                    text = text[: int(self.max_content_chars)]
                file_data.append({
                    "filename":    f_info["filename"],
                    "pr_no":       pr_no,
                    "att_id":      f_info["att_id"],
                    "is_embedded": f_info["is_embedded"],
                    "file_type":   file_type,
                    "extra":       extra,
                    "text":        text,
                    "blob":        f_info["blob_path"],
                })

            # Stage 3 — BLOB_UPLOAD
            current_stage = _STAGE_BLOB_UPLOAD
            self._advance_tracker(tgt_cs, pr_no, _STAGE_BLOB_UPLOAD)
            self.log(f"[{pr_no}] Stage 3 — {len(file_data)} file(s) uploaded to blob")

            result["files"]  = file_data
            result["status"] = "success"

            # ── Full-pipeline mode: continue to stages 4-8 in this same worker ──
            # Triggered when both llm and embed_model inputs are wired.
            # Each parallel worker processes one PR end-to-end (stages 1→8)
            # without waiting for other PRs to finish Stage 1-3 first.
            _llm = getattr(self, "llm", None)
            _emb = getattr(self, "embed_model", None)
            if _llm is not None and _emb is not None:
                self._run_stages_48(pr_no, tgt_cs, result)

        except Exception as exc:
            logger.opt(exception=True).error("[{}] Stage 1-3 failed at stage {}: {}", pr_no, current_stage, exc)
            result["error"] = str(exc)
            self._record_exception(tgt_cs, pr_no, current_stage, str(exc))
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)
        return result

    # ── Entry point ───────────────────────────────────────────────────────

    def build_file_batch(self) -> Message:
        if hasattr(self, "_cached_result"):
            return self._cached_result

        src_cs    = self._conn_str(self.source_connection)
        tgt_cs    = self._conn_str(self.target_connection)
        pr_filter = (self.pr_no_filter or "").strip()
        pr_list   = self._fetch_pending_prs(tgt_cs)

        if not pr_list:
            msg = Message(text="[No pending PRs to process]")
            self._cached_result = msg
            self._cached_pr_numbers = []
            return msg

        if pr_filter:
            # Reset tracker from exception/any stage back to NULL so _process_pr
            # picks it up cleanly. Actual data cleanup (blobs, Pinecone, DB tables)
            # happens inside _process_pr at the start via _cleanup_for_pr.
            self._reset_for_reprocess(tgt_cs, pr_filter)
            self.log(f"Single-PR reprocess: {pr_filter!r} — tracker reset, cleanup will run at processing start")

        workers = max(1, int(self.parallel_workers))
        self.log(f"Processing {len(pr_list)} PR(s) with {workers} parallel worker(s)…")

        # Pre-warm shared caches before threads start so all workers reuse the
        # same already-initialised ContainerClient / blob config rather than
        # each racing to create their own copy.
        self._blob_cfg()
        self._container_client()

        import concurrent.futures
        results: list[dict] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._process_pr, pr, src_cs, tgt_cs): pr for pr in pr_list}
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())

        parts: list[str] = []
        for r in results:
            for fd in r.get("files", []):
                label = f"{fd['filename']} (PR: {fd['pr_no']}, att_id: {fd['att_id']}"
                label += ", embedded" if fd["is_embedded"] else ", parent"
                label += ")"
                parts.append(
                    f"=== FILE: {label} ===\n"
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
        self._cached_pr_numbers = [
            r["pr_no"] for r in results if r.get("status") in ("success", "skipped")
        ]
        return msg

    def get_processed_prs(self) -> Data:
        """Returns the PR numbers that reached stage 3 in this run.
        Wire this output to Stage 4-8's 'Stage 1-3 Result' input so Stage 4-8
        processes exactly those PRs synchronously after Stage 1-3 finishes."""
        if not hasattr(self, "_cached_pr_numbers"):
            self.build_file_batch()
        return Data(data={"pr_numbers": self._cached_pr_numbers})


# ── Text extraction helpers ───────────────────────────────────────────────

def _extract_pdf(raw: bytes) -> str:
    import io, fitz
    parts: list[str] = []
    with fitz.open(stream=raw, filetype="pdf") as doc:
        for page in doc:
            parts.append(page.get_text())
    return "\n".join(parts)


def _extract_excel(raw: bytes) -> str:
    import io, openpyxl
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
    import io, docx
    doc = docx.Document(io.BytesIO(raw))
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def _extract_pptx(raw: bytes) -> str:
    import io
    from pptx import Presentation
    prs   = Presentation(io.BytesIO(raw))
    parts: list[str] = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if hasattr(shape, "text") and shape.text.strip():
                parts.append(shape.text)
    return "\n".join(parts)


def _extract_msg_text(raw: bytes) -> str:
    import tempfile, os
    try:
        import extract_msg
        with tempfile.NamedTemporaryFile(suffix=".msg", delete=False) as tmp:
            tmp.write(raw)
            tmp_path = tmp.name
        try:
            msg   = extract_msg.Message(tmp_path)
            parts = []
            if msg.subject:   parts.append(f"Subject: {msg.subject}")
            if msg.sender:    parts.append(f"From: {msg.sender}")
            if msg.body:      parts.append(msg.body)
            msg.close()
            return "\n".join(parts)
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        return f"[MSG extraction error: {exc}]"


def _detect_file_type(filename: str) -> str:
    import os
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
    import os, io
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
