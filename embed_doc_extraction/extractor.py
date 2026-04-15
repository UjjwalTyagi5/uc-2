"""
embed_doc_extraction.extractor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Extracts embedded files from Office documents and PDFs.

Supported parent formats
------------------------
  OOXML  : .xlsx, .docx, .pptx  (ZIP-based Office Open XML)
  OLE    : .xls,  .doc,  .ppt   (legacy compound-document format)
  PDF    : .pdf                  (embedded files + FileAttachment annotations)

Archives found inside parent files are also expanded one level:
  .zip, .7z, .rar, .tar, .tar.gz, .tgz, .tar.bz2

Usage
-----
    extractor = FileExtractor()
    extractor.parent_prefix = "invoice"      # prefix added to every output filename
    extractor.process_file("/path/to/file.xlsx", "/path/to/output/")
"""

from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path

import olefile
import py7zr
import rarfile
import fitz  # PyMuPDF
from loguru import logger

# ── Constants ──────────────────────────────────────────────────────────────

SUPPORTED_PARENTS = (".xlsx", ".xls", ".docx", ".doc", ".pptx", ".ppt", ".pdf")

EXTRACT_EXTENSIONS = (
    ".pdf", ".xls", ".xlsx", ".doc", ".docx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".zip", ".7z", ".rar", ".tar", ".tar.gz", ".tgz", ".tar.bz2",
    ".txt",
)

SKIP_EXTENSIONS = (".emf", ".bin")

ARCHIVE_EXTENSIONS = (".zip", ".7z", ".rar", ".tar", ".tar.gz", ".tgz", ".tar.bz2")

FILE_SIGNATURES = [
    (b"%PDF",               ".pdf"),
    (b"PK\x03\x04",         ".zip"),
    (b"7z\xBC\xAF\x27\x1C", ".7z"),
    (b"Rar!\x1A\x07",       ".rar"),
    (b"\xFF\xD8\xFF",       ".jpg"),
    (b"\x89PNG\r\n\x1A\n",  ".png"),
    (b"II*\x00",            ".tif"),
    (b"MM\x00*",            ".tif"),
]


class FileExtractor:
    """
    Extracts embedded / attached files from a single parent document.

    Instance state
    --------------
    parent_prefix : str
        Added to every output filename as ``{prefix}__{filename}``.
        Set before each call to process_file().
    extracted_count : int
        Running total of files extracted across all process_file() calls.
    """

    def __init__(self) -> None:
        self.extracted_count: int = 0
        self.parent_prefix:   str = ""

    # ── Utilities ──────────────────────────────────────────────────────────

    def should_extract(self, filename: str) -> bool:
        fl = filename.lower()
        if any(fl.endswith(s) for s in SKIP_EXTENSIONS):
            return False
        return any(fl.endswith(e) for e in EXTRACT_EXTENSIONS)

    def sanitize(self, name: str) -> str:
        name = str(name).replace("\x00", "").replace("\x01", "").strip()
        name = os.path.basename(name)
        for ch in '<>:"/\\|?*':
            name = name.replace(ch, "_")
        name = "".join(ch for ch in name if ch.isprintable() and ord(ch) > 31)
        return name.strip(" .") or "file"

    def with_parent_prefix(self, filename: str) -> str:
        filename = self.sanitize(filename)
        prefix   = self.sanitize(self.parent_prefix).strip()
        return f"{prefix}__{filename}" if prefix else filename

    def unique_path(self, directory: str, filename: str) -> str:
        fp = os.path.join(directory, filename)
        if not os.path.exists(fp):
            return fp
        base, ext = os.path.splitext(filename)
        c = 1
        while os.path.exists(os.path.join(directory, f"{base}_{c}{ext}")):
            c += 1
        return os.path.join(directory, f"{base}_{c}{ext}")

    def _find_payload(self, data: bytes) -> tuple[int | None, str | None]:
        best = None
        for sig, ext in FILE_SIGNATURES:
            pos = data.find(sig)
            if pos != -1 and (best is None or pos < best[0]):
                best = (pos, ext)
        return best if best else (None, None)

    def _classify_zip(self, path: str) -> str:
        try:
            with zipfile.ZipFile(path, "r") as z:
                names = set(z.namelist())
                if "[Content_Types].xml" in names:
                    if any(n.startswith("xl/")   for n in names): return ".xlsx"
                    if any(n.startswith("word/") for n in names): return ".docx"
                    if any(n.startswith("ppt/")  for n in names): return ".pptx"
        except Exception:
            pass
        return ".zip"

    # ── Archive extraction (1 level, flat) ─────────────────────────────────

    def _extract_zip(self, path: str, out: str) -> None:
        try:
            logger.debug(f"Extracting ZIP: {os.path.basename(path)}")
            with zipfile.ZipFile(path, "r") as z:
                for m in z.infolist():
                    if m.is_dir():
                        continue
                    fn = os.path.basename(m.filename.replace("\\", "/").rstrip("/"))
                    if fn:
                        t = self.unique_path(out, self.with_parent_prefix(fn))
                        with z.open(m) as s, open(t, "wb") as d:
                            shutil.copyfileobj(s, d)
                        self.extracted_count += 1
                        logger.debug(f"  -> {os.path.basename(t)}")
        except Exception as exc:
            logger.error(f"ZIP extraction error: {exc}")

    def _extract_7z(self, path: str, out: str) -> None:
        try:
            logger.debug(f"Extracting 7Z: {os.path.basename(path)}")
            with tempfile.TemporaryDirectory() as tmp:
                with py7zr.SevenZipFile(path, "r") as ar:
                    ar.extractall(path=tmp)
                for root, _, files in os.walk(tmp):
                    for f in files:
                        t = self.unique_path(out, self.with_parent_prefix(f))
                        shutil.copy2(os.path.join(root, f), t)
                        self.extracted_count += 1
                        logger.debug(f"  -> {os.path.basename(t)}")
        except Exception as exc:
            logger.error(f"7Z extraction error: {exc}")

    def _extract_rar(self, path: str, out: str) -> None:
        try:
            logger.debug(f"Extracting RAR: {os.path.basename(path)}")
            with tempfile.TemporaryDirectory() as tmp:
                with rarfile.RarFile(path, "r") as rar:
                    rar.extractall(tmp)
                for root, _, files in os.walk(tmp):
                    for f in files:
                        t = self.unique_path(out, self.with_parent_prefix(f))
                        shutil.copy2(os.path.join(root, f), t)
                        self.extracted_count += 1
                        logger.debug(f"  -> {os.path.basename(t)}")
        except Exception as exc:
            logger.error(f"RAR extraction error: {exc}")

    def _extract_tar(self, path: str, out: str) -> None:
        try:
            logger.debug(f"Extracting TAR: {os.path.basename(path)}")
            with tarfile.open(path, "r:*") as tar:
                for m in tar.getmembers():
                    if not m.isfile():
                        continue
                    fn = os.path.basename(m.name)
                    if fn:
                        t = self.unique_path(out, self.with_parent_prefix(fn))
                        with tar.extractfile(m) as s, open(t, "wb") as d:
                            shutil.copyfileobj(s, d)
                        self.extracted_count += 1
                        logger.debug(f"  -> {os.path.basename(t)}")
        except Exception as exc:
            logger.error(f"TAR extraction error: {exc}")

    def extract_archive(self, path: str, out: str) -> None:
        if not os.path.exists(path):
            return
        fl = path.lower()
        if   fl.endswith(".zip"):                               self._extract_zip(path, out)
        elif fl.endswith(".7z"):                                self._extract_7z(path, out)
        elif fl.endswith(".rar"):                               self._extract_rar(path, out)
        elif fl.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2")): self._extract_tar(path, out)

    # ── Save extracted file + handle if archive ────────────────────────────

    def _save_file(self, data: bytes, filename: str, out: str) -> str:
        target = self.unique_path(out, self.with_parent_prefix(filename))
        with open(target, "wb") as f:
            f.write(data)
        self.extracted_count += 1
        logger.debug(f"Extracted: {os.path.basename(target)}")
        if target.lower().endswith(ARCHIVE_EXTENSIONS):
            self.extract_archive(target, out)
        return target

    def _save_payload(self, payload: bytes, name: str | None, ext: str, out: str) -> str | None:
        if not payload or len(payload) < 16:
            return None

        filename = self.sanitize(name) if name else "embedded_file"
        base, existing_ext = os.path.splitext(filename)
        if not existing_ext:
            filename = base + ext

        target = self.unique_path(out, self.with_parent_prefix(filename))
        with open(target, "wb") as f:
            f.write(payload)

        if ext == ".zip":
            correct = self._classify_zip(target)
            if correct != ".zip":
                new = self.unique_path(out, os.path.splitext(os.path.basename(target))[0] + correct)
                os.replace(target, new)
                target = new

        self.extracted_count += 1
        logger.debug(f"Extracted from OLE: {os.path.basename(target)}")

        if target.lower().endswith(ARCHIVE_EXTENSIONS):
            self.extract_archive(target, out)
        return target

    # ── BIN / OLE object scanning ───────────────────────────────────────────

    def _extract_from_bin(self, bin_path: str, out: str) -> None:
        try:
            if not olefile.isOleFile(bin_path):
                return
            logger.debug(f"Scanning OLE bin: {os.path.basename(bin_path)}")
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

                    if not suggested:
                        suggested = f"embedded_file{ext}"

                    self._save_payload(payload, suggested, ext, out)
                except Exception:
                    pass

            ole.close()
        except Exception as exc:
            logger.warning(f"OLE bin error ({os.path.basename(bin_path)}): {exc}")

    # ── XLSX / DOCX / PPTX (Office Open XML) ───────────────────────────────

    def extract_from_ooxml(self, file_path: str, out: str) -> bool:
        try:
            logger.debug(f"Reading OOXML: {os.path.basename(file_path)}")
            with tempfile.TemporaryDirectory() as tmp:
                with zipfile.ZipFile(file_path, "r") as zr:
                    zr.extractall(tmp)

                for prefix in ("xl", "word", "ppt"):
                    for sub in ("media", "embeddings"):
                        folder = os.path.join(tmp, prefix, sub)
                        if not os.path.exists(folder):
                            continue
                        logger.debug(f"  Scanning {prefix}/{sub}")
                        for fn in os.listdir(folder):
                            src = os.path.join(folder, fn)
                            if not os.path.isfile(src):
                                continue
                            if fn.lower().endswith(".bin"):
                                self._extract_from_bin(src, out)
                            elif self.should_extract(fn):
                                dst = self.unique_path(out, self.with_parent_prefix(fn))
                                shutil.copy2(src, dst)
                                self.extracted_count += 1
                                logger.debug(f"  Extracted: {os.path.basename(dst)}")
                                if dst.lower().endswith(ARCHIVE_EXTENSIONS):
                                    self.extract_archive(dst, out)
            return True
        except Exception as exc:
            logger.error(f"OOXML error ({os.path.basename(file_path)}): {exc}")
            return False

    # ── XLS / DOC / PPT (legacy OLE) ───────────────────────────────────────

    def extract_from_ole(self, file_path: str, out: str) -> bool:
        try:
            if not olefile.isOleFile(file_path):
                logger.error(f"Not a valid OLE file: {os.path.basename(file_path)}")
                return False
            logger.debug(f"Reading OLE: {os.path.basename(file_path)}")
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

                    if not suggested:
                        suggested = f"embedded_file{ext}"

                    self._save_payload(payload, suggested, ext, out)
                except Exception:
                    pass

            ole.close()
            return True
        except Exception as exc:
            logger.error(f"OLE error ({os.path.basename(file_path)}): {exc}")
            return False

    # ── PDF ─────────────────────────────────────────────────────────────────

    def extract_from_pdf(self, file_path: str, out: str) -> bool:
        try:
            logger.debug(f"Reading PDF: {os.path.basename(file_path)}")
            doc = fitz.open(file_path)

            # Embedded file attachments
            if doc.embfile_count() > 0:
                logger.debug(f"  {doc.embfile_count()} embedded file(s)")
                for i in range(doc.embfile_count()):
                    info = doc.embfile_info(i)
                    name = self.sanitize(info.get("filename", f"pdf_attachment_{i}"))
                    data = doc.embfile_get(i)
                    if data:
                        self._save_file(data, name, out)

            # FileAttachment annotations
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

    # ── Detect + dispatch ────────────────────────────────────────────────────

    def detect_type(self, path: str) -> str | None:
        lower = path.lower()
        if lower.endswith((".xlsx", ".docx", ".pptx")): return "ooxml"
        if lower.endswith((".xls",  ".doc",  ".ppt")):  return "ole"
        if lower.endswith(".pdf"):                       return "pdf"

        # Fallback: read file header
        try:
            with open(path, "rb") as f:
                h = f.read(8)
            if h.startswith(b"PK\x03\x04"):                    return "ooxml"
            if h.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"): return "ole"
            if h.startswith(b"%PDF"):                           return "pdf"
        except Exception:
            pass
        return None

    def process_file(self, file_path: str, out: str) -> bool:
        """
        Extract all embedded files from a single parent document.

        Parameters
        ----------
        file_path : path to the parent document
        out       : directory where extracted files are written

        Returns True if the file was handled, False if unsupported / error.
        """
        ft = self.detect_type(file_path)
        if not ft:
            logger.debug(f"Skipping unsupported file: {os.path.basename(file_path)}")
            return False

        os.makedirs(out, exist_ok=True)
        logger.info(f"Processing {os.path.basename(file_path)} [{ft.upper()}]")

        if ft == "ooxml": return self.extract_from_ooxml(file_path, out)
        if ft == "ole":   return self.extract_from_ole(file_path, out)
        if ft == "pdf":   return self.extract_from_pdf(file_path, out)
        return False
