"""Azure Document Intelligence OCR client.

Wraps the `azure-ai-documentintelligence` SDK with a simple `extract_markdown()`
function used by the quotation extraction pipeline.

Returns markdown (when using `prebuilt-layout`) so tables, headers, and
paragraph structure are preserved for the LLM.

Returns None on any failure — callers fall back to image-based extraction.
"""

from __future__ import annotations

import pathlib
from typing import Optional

from loguru import logger

try:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    from azure.ai.documentintelligence.models import (
        AnalyzeDocumentRequest,
        AnalyzeResult,
        ContentFormat,
    )
    from azure.core.credentials import AzureKeyCredential
    _SDK_OK = True
except ImportError:
    _SDK_OK = False
    logger.warning(
        "azure-ai-documentintelligence not installed — OCR disabled. "
        "Run: pip install azure-ai-documentintelligence"
    )


_MIN_CONTENT_LEN = 30   # below this, treat as empty / failed OCR


class AzureDocIntelClient:
    """Thin wrapper around DocumentIntelligenceClient for OCR extraction."""

    def __init__(
        self,
        endpoint: str,
        key: str,
        model: str = "prebuilt-layout",
    ) -> None:
        if not _SDK_OK:
            raise RuntimeError(
                "azure-ai-documentintelligence not installed — "
                "cannot create AzureDocIntelClient"
            )
        if not endpoint or not key:
            raise ValueError("AzureDocIntelClient requires endpoint and key")

        self._endpoint = endpoint
        self._model    = model
        self._client   = DocumentIntelligenceClient(
            endpoint=endpoint,
            credential=AzureKeyCredential(key),
        )

    def extract_markdown(self, file_path: str | pathlib.Path) -> Optional[str]:
        """Run OCR on a file and return the markdown content.

        Returns None on any error or when the result is empty / too short
        to be useful.  Caller should fall back to image-based extraction
        in that case.
        """
        fp = pathlib.Path(file_path)
        try:
            with fp.open("rb") as f:
                data = f.read()
        except OSError as exc:
            logger.warning("OCR: cannot open {} — {}", fp.name, exc)
            return None

        try:
            poller = self._client.begin_analyze_document(
                model_id=self._model,
                body=AnalyzeDocumentRequest(bytes_source=data),
                output_content_format=ContentFormat.MARKDOWN,
            )
            result: AnalyzeResult = poller.result()
        except Exception as exc:
            logger.warning(
                "OCR failed for {} (model={}): {}",
                fp.name, self._model, exc,
            )
            return None

        content = (result.content or "").strip()
        if len(content) < _MIN_CONTENT_LEN:
            logger.info(
                "OCR returned only {} char(s) for {} — treating as empty",
                len(content), fp.name,
            )
            return None

        logger.debug(
            "OCR extracted {} char(s) from {} ({} page(s))",
            len(content), fp.name, len(result.pages or []),
        )
        return content


def build_client_from_config(config) -> Optional[AzureDocIntelClient]:
    """Return a configured client, or None if OCR is disabled / not configured."""
    if not getattr(config, "EXTRACTION_USE_OCR", False):
        return None
    endpoint = getattr(config, "AZURE_DOC_INTEL_ENDPOINT", "")
    key      = getattr(config, "AZURE_DOC_INTEL_KEY",      "")
    model    = getattr(config, "AZURE_DOC_INTEL_MODEL",    "prebuilt-layout")
    if not endpoint or not key:
        logger.info("OCR: endpoint/key not configured — OCR disabled")
        return None
    if not _SDK_OK:
        return None
    try:
        return AzureDocIntelClient(endpoint=endpoint, key=key, model=model)
    except Exception as exc:
        logger.warning("OCR client init failed: {} — OCR disabled", exc)
        return None
