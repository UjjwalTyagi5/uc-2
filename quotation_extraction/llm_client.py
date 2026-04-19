"""Azure OpenAI GPT-5.2 wrapper via LangChain for quotation extraction.

Supports both text-only and multimodal (text + images) messages.
"""

from __future__ import annotations

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import AzureChatOpenAI
from loguru import logger

from .config import ExtractionConfig
from .models import DocumentContent


class ExtractionLLMClient:
    """Thin wrapper around AzureChatOpenAI that builds multimodal messages."""

    def __init__(self, config: ExtractionConfig) -> None:
        self._llm = AzureChatOpenAI(
            azure_deployment=config.AOAI_DEPLOYMENT,
            azure_endpoint=config.AOAI_ENDPOINT,
            api_key=config.AOAI_API_KEY,
            api_version=config.AOAI_API_VERSION,
            temperature=config.LLM_TEMPERATURE,
            max_tokens=config.LLM_MAX_TOKENS,
        )
        self._config = config

    def extract(
        self,
        system_prompt: str,
        user_prompt: str,
        document: DocumentContent,
    ) -> str:
        """Send a system + user message (with optional images) and return the
        raw LLM text response.

        For image-based documents the user message is a multimodal content
        list; for text-based documents it is a plain string.
        """

        messages = [SystemMessage(content=system_prompt)]

        if document.is_image_based:
            content_parts: list[dict] = [{"type": "text", "text": user_prompt}]
            for b64_img in document.images:  # type: ignore[union-attr]
                content_parts.append(
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{b64_img}",
                            "detail": "high",
                        },
                    }
                )
            messages.append(HumanMessage(content=content_parts))
        else:
            messages.append(HumanMessage(content=user_prompt))

        logger.info(
            "Calling Azure OpenAI ({}) — {} images, ~{:.0f} kB prompt text",
            self._config.AOAI_DEPLOYMENT,
            len(document.images) if document.images else 0,
            len(user_prompt) / 1024,
        )

        response = self._llm.invoke(messages)
        raw: str = response.content  # type: ignore[assignment]

        logger.debug(
            "LLM response length: {} chars",
            len(raw),
        )
        return raw
