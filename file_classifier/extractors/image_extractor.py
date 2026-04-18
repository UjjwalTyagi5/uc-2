import io
import base64
import structlog
from PIL import Image
from file_classifier.extractors.base import BaseExtractor, ExtractionResult

logger = structlog.get_logger()

MAX_IMAGE_DIMENSION = 2048


class ImageExtractor(BaseExtractor):
    def extract(self, file_bytes: bytes, filename: str) -> ExtractionResult:
        logger.info("Extracting image content", filename=filename)

        img = Image.open(io.BytesIO(file_bytes))

        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")

        if max(img.size) > MAX_IMAGE_DIMENSION:
            img.thumbnail((MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION), Image.LANCZOS)

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        img_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

        return ExtractionResult(
            text_content="[Image file - content sent as image for visual analysis]",
            metadata={
                "original_size": f"{img.size[0]}x{img.size[1]}",
                "format": img.format or "unknown",
            },
            is_image_based=True,
            image_base64=img_base64,
        )
