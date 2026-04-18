"""
Extractor for legacy .doc and .ppt files.

Strategy (cross-platform):
  1. Try subprocess conversion: libreoffice (Linux/Windows) or textutil (macOS)
  2. If conversion works, read the resulting text
  3. If conversion fails, try olefile to extract raw text streams (best-effort)
  4. If text is still empty, extract embedded images and route to Vision LLM
"""

import io
import os
import re
import sys
import base64
import shutil
import tempfile
import subprocess
import structlog
from pathlib import Path
from PIL import Image
from file_classifier.extractors.base import BaseExtractor, ExtractionResult

logger = structlog.get_logger()

MAX_CHARS = 15000
MIN_TEXT_LENGTH = 50
MIN_READABLE_RATIO = 0.5
MAX_IMAGE_DIMENSION = 2048


def _is_readable_text(text: str) -> bool:
    if not text or len(text.strip()) < MIN_TEXT_LENGTH:
        return False
    sample = text[:3000]

    binary_markers = ["JFIF", "IHDR", "\x89PNG", "ÿØÿ", "Exif"]
    if any(marker in sample[:500] for marker in binary_markers):
        return False

    non_printable = sum(1 for c in sample if not c.isprintable() and c not in "\n\r\t")
    if non_printable / len(sample) > 0.15:
        return False

    real_words = re.findall(r'\b\w{2,25}\b', sample)
    short_words = [w for w in real_words if len(w) <= 15]
    return len(short_words) >= 10


_MACOS_SOFFICE = "/Applications/LibreOffice.app/Contents/MacOS/soffice"


def _find_converter() -> str | None:
    if sys.platform == "darwin" and shutil.which("textutil"):
        return "textutil"
    if shutil.which("libreoffice"):
        return "libreoffice"
    if shutil.which("soffice"):
        return "soffice"
    if os.path.isfile(_MACOS_SOFFICE):
        return _MACOS_SOFFICE
    return None


def _find_libreoffice() -> str | None:
    if shutil.which("libreoffice"):
        return "libreoffice"
    if shutil.which("soffice"):
        return "soffice"
    if os.path.isfile(_MACOS_SOFFICE):
        return _MACOS_SOFFICE
    return None


def _convert_with_textutil(file_path: str) -> str | None:
    try:
        result = subprocess.run(
            ["textutil", "-convert", "txt", "-stdout", file_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout
    except Exception as e:
        logger.warning("textutil conversion failed", error=str(e))
    return None


def _convert_with_libreoffice(file_path: str) -> str | None:
    lo = _find_libreoffice()
    if not lo:
        return None
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [lo, "--headless", "--convert-to", "txt:Text", "--outdir", tmpdir, file_path],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                txt_files = list(Path(tmpdir).glob("*.txt"))
                if txt_files:
                    return txt_files[0].read_text(errors="replace")
    except Exception as e:
        logger.warning("LibreOffice text conversion failed", error=str(e))
    return None


def _convert_to_pdf_with_libreoffice(file_path: str) -> str | None:
    lo = _find_libreoffice()
    if not lo:
        return None
    try:
        import pdfplumber

        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [lo, "--headless", "--convert-to", "pdf", "--outdir", tmpdir, file_path],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                return None

            pdf_files = list(Path(tmpdir).glob("*.pdf"))
            if not pdf_files:
                return None

            with pdfplumber.open(str(pdf_files[0])) as pdf:
                if not pdf.pages:
                    return None
                page = pdf.pages[0]
                img = page.to_image(resolution=200)
                buf = io.BytesIO()
                img.original.save(buf, format="PNG")
                return base64.b64encode(buf.getvalue()).decode("utf-8")
    except Exception as e:
        logger.warning("LibreOffice PDF conversion failed", error=str(e))
    return None


def _extract_text_olefile(file_bytes: bytes) -> str | None:
    try:
        import olefile
        ole = olefile.OleFileIO(io.BytesIO(file_bytes))

        text_parts = []
        for stream_name in ole.listdir():
            try:
                data = ole.openstream(stream_name).read()
                for encoding in ("utf-16-le", "utf-8", "latin-1"):
                    try:
                        decoded = data.decode(encoding, errors="ignore")
                        for line in decoded.split("\n"):
                            cleaned = "".join(c for c in line if c.isprintable() or c in "\t\n")
                            cleaned = cleaned.strip()
                            if len(cleaned) > 5:
                                text_parts.append(cleaned)
                        if text_parts:
                            break
                    except Exception:
                        continue
            except Exception:
                continue
        ole.close()
        if text_parts:
            return "\n".join(text_parts)
    except ImportError:
        logger.warning("olefile not installed, skipping OLE extraction")
    except Exception as e:
        logger.warning("OLE extraction failed", error=str(e))
    return None


def _extract_images_olefile(file_bytes: bytes) -> str | None:
    try:
        import olefile
        ole = olefile.OleFileIO(io.BytesIO(file_bytes))

        if not ole.exists("Pictures"):
            ole.close()
            return None

        pics_data = ole.openstream("Pictures").read()
        ole.close()

        if not pics_data:
            return None

        images = []

        for m in re.finditer(b'\xff\xd8\xff', pics_data):
            start = m.start()
            end = pics_data.find(b'\xff\xd9', start)
            if end > start:
                images.append(pics_data[start:end + 2])

        for m in re.finditer(b'\x89PNG\r\n\x1a\n', pics_data):
            start = m.start()
            end = pics_data.find(b'IEND', start)
            if end > start:
                images.append(pics_data[start:end + 8])

        if not images:
            return None

        largest = max(images, key=len)

        img = Image.open(io.BytesIO(largest))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        if max(img.size) > MAX_IMAGE_DIMENSION:
            img.thumbnail((MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION), Image.LANCZOS)

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    except Exception as e:
        logger.warning("Failed to extract images from OLE file", error=str(e))
    return None


class LegacyDocExtractor(BaseExtractor):
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        logger.info("Extracting legacy doc/ppt content", filename=filename)

        text = None

        converter = _find_converter()
        if converter:
            with tempfile.NamedTemporaryFile(
                suffix=Path(filename).suffix, delete=False
            ) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            try:
                if converter == "textutil":
                    text = _convert_with_textutil(tmp_path)
                else:
                    text = _convert_with_libreoffice(tmp_path)
            finally:
                os.unlink(tmp_path)

        if text and not _is_readable_text(text):
            logger.info("Extracted text is garbled/binary, discarding", filename=filename)
            text = None

        is_ppt = Path(filename).suffix.lower() in (".ppt",)

        if not is_ppt and (not text or not _is_readable_text(text)):
            ole_text = _extract_text_olefile(file_bytes)
            if ole_text and _is_readable_text(ole_text):
                text = ole_text

        if not text or not _is_readable_text(text):
            logger.info("No readable text, attempting LibreOffice PDF conversion", filename=filename)
            with tempfile.NamedTemporaryFile(suffix=Path(filename).suffix, delete=False) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
            try:
                img_b64 = _convert_to_pdf_with_libreoffice(tmp_path)
            finally:
                os.unlink(tmp_path)

            if img_b64:
                return ExtractionResult(
                    text_content="[Legacy document converted to PDF image via LibreOffice for visual analysis]",
                    metadata={"extraction_method": "libreoffice_pdf_image"},
                    is_image_based=True,
                    image_base64=img_b64,
                )

        if not text or not _is_readable_text(text):
            logger.info("No text found, attempting image extraction from OLE", filename=filename)
            img_b64 = _extract_images_olefile(file_bytes)
            if img_b64:
                return ExtractionResult(
                    text_content="[Legacy document is image-based - content sent as image for visual analysis]",
                    metadata={
                        "extraction_method": "image_from_ole",
                        "converter_available": converter or "none",
                    },
                    is_image_based=True,
                    image_base64=img_b64,
                )

        if not text or len(text.strip()) < 10:
            return ExtractionResult(
                text_content="[Legacy document format - could not extract text. Install LibreOffice for full support.]",
                metadata={"extraction_method": "failed", "converter_available": converter or "none"},
            )

        text = text[:MAX_CHARS]

        return ExtractionResult(
            text_content=text,
            metadata={
                "extraction_method": converter or "olefile",
                "chars_extracted": len(text),
            },
        )
