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
        IntInput(name="batch_limit",       display_name="Max PRs per Run",          value=50,    advanced=True),
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
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
            cur.execute(
                """
                SELECT TOP (?) prm.PURCHASE_REQ_NO
                  FROM [ras_procurement].[purchase_req_mst] prm
                  LEFT JOIN [ras_procurement].[ras_tracker] rt
                    ON prm.PURCHASE_REQ_NO = rt.purchase_req_no
                 WHERE (rt.purchase_req_no IS NULL
                        OR rt.current_stage_fk IS NULL
                        OR rt.current_stage_fk NOT IN (8, 90, 91, 99))
                   AND UPPER(prm.PURCHASEFINALAPPROVALSTATUS)
                           IN ('APPROVED BY ALL', 'APPROVED BY ALL EXCEPTION')
                 ORDER BY prm.C_DATETIME ASC
                """,
                int(self.batch_limit),
            )
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    # ── Pre-run cleanup ───────────────────────────────────────────────────

    def _cleanup_for_pr(self, tgt_cs: str, pr_no: str) -> None:
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        cur.execute("SELECT ras_uuid_pk FROM [ras_procurement].[ras_tracker] WHERE purchase_req_no = ?", pr_no)
        existing = cur.fetchone()
        conn.close()
        if existing is None:
            self.log(f"[{pr_no}] New PR — no prior data to clean")
            return
        self.log(f"[{pr_no}] Existing PR — cleaning prior pipeline output")
        conn = self._connect(tgt_cs)
        cur  = conn.cursor()
        try:
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
            cur.execute("""
                DELETE br FROM [ras_procurement].[benchmark_result] br
                  JOIN [ras_procurement].[quotation_extracted_items] qi ON br.extracted_item_uuid_fk = qi.extracted_item_uuid_pk
                  JOIN [ras_procurement].[attachment_classification] ac ON qi.attachment_classify_fk = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            cur.execute("""
                DELETE qi FROM [ras_procurement].[quotation_extracted_items] qi
                  JOIN [ras_procurement].[attachment_classification] ac ON qi.attachment_classify_fk = ac.attachment_classify_uuid_pk
                  JOIN [ras_procurement].[ras_tracker] rt ON ac.ras_uuid_pk = rt.ras_uuid_pk
                 WHERE rt.purchase_req_no = ?""", pr_no)
            cur.execute("EXEC [ras_procurement].[usp_cleanup_pr_data] ?", pr_no)
            conn.commit()
            self.log(f"[{pr_no}] Cleanup complete")
        except Exception as exc:
            conn.rollback()
            raise RuntimeError(f"Pre-run cleanup failed for {pr_no}: {exc}") from exc
        finally:
            conn.close()

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

    # ── Blob upload ───────────────────────────────────────────────────────

    def _upload_blob(self, raw: bytes, blob_path: str) -> None:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient
        cfg        = self._blob_cfg()
        credential = DefaultAzureCredential(
            exclude_environment_credential=True,
            exclude_interactive_browser_credential=True,
        )
        BlobServiceClient(
            account_url=cfg["account_url"],
            credential=credential,
        ).get_blob_client(container=cfg["container_name"], blob=blob_path).upload_blob(raw, overwrite=True)

    # ── Process single PR ─────────────────────────────────────────────────

    def _process_pr(self, pr_no: str, src_cs: str, tgt_cs: str) -> dict:
        import os, shutil, tempfile
        result        = {"pr_no": pr_no, "files": [], "status": "failed", "error": ""}
        current_stage = _STAGE_INGESTION
        work_dir      = tempfile.mkdtemp()
        try:
            # Pre-run cleanup (no-op for new PRs)
            self._cleanup_for_pr(tgt_cs, pr_no)

            # Stage 1 — INGESTION
            current_stage = _STAGE_INGESTION
            self._advance_tracker(tgt_cs, pr_no, _STAGE_INGESTION)
            self.log(f"[{pr_no}] Stage 1 — ingested")

            ras_uuid    = self._get_tracker_uuid(tgt_cs, pr_no)
            attachments = self._fetch_attachments(src_cs, tgt_cs, pr_no)
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
            self._cleanup_for_pr(tgt_cs, pr_filter)
            self._reset_for_reprocess(tgt_cs, pr_filter)
            self.log(f"Single-PR reprocess: {pr_filter!r} — all existing pipeline data wiped")

        self.log(f"Processing {len(pr_list)} PR(s)…")

        import concurrent.futures
        results: list[dict] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(self.parallel_workers)) as pool:
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
