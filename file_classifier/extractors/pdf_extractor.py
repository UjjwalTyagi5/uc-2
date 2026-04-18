import io
import base64
import structlog
import pdfplumber
from PIL import Image
from file_classifier.extractors.base import BaseExtractor, ExtractionResult

logger = structlog.get_logger()

MAX_PAGES = 5
MIN_TEXT_LENGTH = 50


class PdfExtractor(BaseExtractor):
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        logger.info("Extracting PDF content", filename=filename)
        buf = io.BytesIO(file_bytes)

        with pdfplumber.open(buf) as pdf:
            total_pages = len(pdf.pages)
            pages_to_process = min(total_pages, MAX_PAGES)

            text_parts = []
            tables_parts = []

            for i in range(pages_to_process):
                page = pdf.pages[i]

                page_text = page.extract_text() or ""
                if page_text.strip():
                    text_parts.append(f"--- Page {i + 1} ---\n{page_text}")

                tables = page.extract_tables()
                for t_idx, table in enumerate(tables):
                    if table:
                        header = table[0]
                        rows = table[1:]
                        table_str = " | ".join(str(c) for c in header) + "\n"
                        table_str += " | ".join("---" for _ in header) + "\n"
                        for row in rows[:20]:
                            table_str += " | ".join(str(c) for c in row) + "\n"
                        tables_parts.append(f"Table {t_idx + 1} (Page {i + 1}):\n{table_str}")

            combined_text = "\n\n".join(text_parts)
            if tables_parts:
                combined_text += "\n\n### Extracted Tables\n" + "\n\n".join(tables_parts)

        if len(combined_text.strip()) < MIN_TEXT_LENGTH:
            logger.info("PDF has minimal text, treating as scanned/image-based", filename=filename)
            return self._extract_as_image(file_bytes, filename, total_pages)

        return ExtractionResult(
            text_content=combined_text,
            metadata={"total_pages": total_pages, "pages_processed": pages_to_process},
        )

    def _extract_as_image(self, file_bytes: bytes, filename: str, total_pages: int) -> ExtractionResult:
        buf = io.BytesIO(file_bytes)

        with pdfplumber.open(buf) as pdf:
            page = pdf.pages[0]
            img = page.to_image(resolution=200)

            img_buf = io.BytesIO()
            img.original.save(img_buf, format="PNG")
            img_base64 = base64.b64encode(img_buf.getvalue()).decode("utf-8")

        return ExtractionResult(
            text_content="[Scanned PDF - content sent as image]",
            metadata={"total_pages": total_pages, "extraction_method": "image"},
            is_image_based=True,
            image_base64=img_base64,
        )
