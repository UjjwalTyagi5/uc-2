import structlog
import chardet
from file_classifier.extractors.base import BaseExtractor, ExtractionResult

logger = structlog.get_logger()

MAX_CHARS = 15000


class TextExtractor(BaseExtractor):
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        logger.info("Extracting text content", filename=filename)

        detected = chardet.detect(file_bytes)
        encoding = detected.get("encoding", "utf-8") or "utf-8"

        try:
            text = file_bytes.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            text = file_bytes.decode("utf-8", errors="replace")

        truncated = len(text) > MAX_CHARS
        text = text[:MAX_CHARS]

        return ExtractionResult(
            text_content=text,
            metadata={"encoding": encoding, "truncated": truncated},
        )
