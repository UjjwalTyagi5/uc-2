import io
import structlog
from file_classifier.extractors.base import BaseExtractor, ExtractionResult

logger = structlog.get_logger()

MAX_CHARS = 15000


class HtmlExtractor(BaseExtractor):
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        logger.info("Extracting HTML content", filename=filename)

        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(file_bytes, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
        except ImportError:
            import re
            raw = file_bytes.decode("utf-8", errors="replace")
            text = re.sub(r"<[^>]+>", " ", raw)
            text = re.sub(r"\s+", " ", text).strip()

        text = text[:MAX_CHARS]

        return ExtractionResult(
            text_content=text,
            metadata={"chars_extracted": len(text)},
        )
