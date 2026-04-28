import time
import structlog
from file_classifier.config import settings
from file_classifier.extractors.base import ExtractionResult
from file_classifier.extractors.extractor_factory import get_extractor
from file_classifier.classifier.llm_client import call_llm_text, call_llm_vision
from file_classifier.classifier.prompt_templates import SYSTEM_PROMPT, USER_PROMPT_TEXT, USER_PROMPT_IMAGE
from file_classifier.utils.file_utils import detect_file_type
from file_classifier.utils.token_utils import truncate_to_token_limit

logger = structlog.get_logger()

VALID_CLASSIFICATIONS = {"RFQ", "Quotation", "MPBC", "BER", "E-Auction", "Other"}


def classify_file(file_bytes: bytes, filename: str) -> dict:
    start_time = time.time()

    # Step 1: Detect file type
    file_type = detect_file_type(filename)
    logger.info("File type detected", filename=filename, file_type=file_type)

    # Step 2: Extract content
    extractor = get_extractor(file_type)
    extraction = extractor.extract(file_bytes, filename)
    logger.info(
        "Content extracted",
        filename=filename,
        is_image=extraction.is_image_based,
        metadata=extraction.metadata,
    )

    # Step 3: Classify
    result = _classify_with_llm(extraction, filename, file_type)

    # Step 4: Validate and return
    result = _validate_result(result)
    elapsed_ms = int((time.time() - start_time) * 1000)

    return {
        "filename": filename,
        "file_type": file_type,
        "classification": result["classification"],
        "confidence": result["confidence"],
        "reason": result["reason"],
        "key_signals": result["key_signals"],
        "fields_matched": result["fields_matched"],
        "fields_missing": result["fields_missing"],
        "processing_time_ms": elapsed_ms,
    }


def _classify_with_llm(
    extraction: ExtractionResult,
    filename: str,
    file_type: str,
) -> dict:
    extra_meta = ""
    for key, value in extraction.metadata.items():
        extra_meta += f"- {key}: {value}\n"

    if extraction.is_image_based and extraction.image_base64:
        user_prompt = USER_PROMPT_IMAGE.format(
            filename=filename,
            file_type=file_type,
            extra_metadata=extra_meta,
        )
        return call_llm_vision(SYSTEM_PROMPT, user_prompt, extraction.image_base64)
    else:
        truncated_content = truncate_to_token_limit(
            extraction.text_content,
            max_tokens=settings.max_content_tokens,
        )
        user_prompt = USER_PROMPT_TEXT.format(
            filename=filename,
            file_type=file_type,
            extra_metadata=extra_meta,
            extracted_content=truncated_content,
        )
        return call_llm_text(SYSTEM_PROMPT, user_prompt)


def _validate_result(result: dict) -> dict:
    classification = result.get("classification", "Other")
    if classification not in VALID_CLASSIFICATIONS:
        classification = "Other"

    confidence = result.get("confidence", 0.0)
    if not isinstance(confidence, (int, float)) or not (0 <= confidence <= 1):
        confidence = 0.0

    reason = result.get("reason", "No reason provided")

    def _sanitize_list(val, limit):
        if not isinstance(val, list):
            return []
        return [str(s) for s in val][:limit]

    return {
        "classification": classification,
        "confidence": round(confidence, 3),
        "reason": reason,
        "key_signals": _sanitize_list(result.get("key_signals"), 5),
        "fields_matched": _sanitize_list(result.get("fields_matched"), 15),
        "fields_missing": _sanitize_list(result.get("fields_missing"), 15),
    }
