"""Azure OpenAI GPT wrapper via LangChain for quotation extraction.

Supports both text-only and multimodal (text + images) messages.
Transient API errors (rate-limit, timeout, connection, server) are retried
with exponential back-off; LLM_MAX_RETRIES and LLM_RETRY_BASE_DELAY control
the behaviour (see utils/config.py).
"""

from __future__ import annotations

import random
import time

import openai
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import AzureChatOpenAI
from loguru import logger

from .config import ExtractionConfig
from .models import DocumentContent

# OpenAI error types that are worth retrying (transient / capacity issues)
_RETRYABLE = (
    openai.RateLimitError,
    openai.APITimeoutError,
    openai.APIConnectionError,
    openai.InternalServerError,
)


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

    # ── public ──

    def extract(
        self,
        system_prompt: str,
        user_prompt: str,
        document: DocumentContent,
    ) -> str:
        """Send a system + user message (with optional images) and return the
        raw LLM text response.

        Retries up to LLM_MAX_RETRIES times on transient API errors, with
        exponential back-off: delay = base * 2^attempt + jitter(0..1 s).
        """
        messages = self._build_messages(system_prompt, user_prompt, document)

        logger.info(
            "Calling Azure OpenAI ({}) — {} image(s), ~{:.0f} kB prompt",
            self._config.AOAI_DEPLOYMENT,
            len(document.images) if document.images else 0,
            len(user_prompt) / 1024,
        )

        max_retries = self._config.LLM_MAX_RETRIES
        base_delay  = self._config.LLM_RETRY_BASE_DELAY

        for attempt in range(max_retries + 1):
            try:
                response = self._llm.invoke(messages)
                raw: str = response.content  # type: ignore[assignment]
                logger.debug("LLM response: {} chars", len(raw))
                return raw

            except _RETRYABLE as exc:
                if attempt >= max_retries:
                    logger.error(
                        "LLM call failed after {} attempt(s), giving up: {}",
                        attempt + 1, exc,
                    )
                    raise

                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "LLM transient error (attempt {}/{}), retrying in {:.1f}s: {}",
                    attempt + 1, max_retries + 1, delay, exc,
                )
                time.sleep(delay)

        # unreachable — loop always returns or raises
        raise RuntimeError("LLM retry loop exited unexpectedly")

    def query(self, system_prompt: str, user_prompt: str) -> str:
        """Text-only LLM call with the same retry logic as extract()."""
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]

        logger.info(
            "Calling Azure OpenAI ({}) — text-only query, ~{:.0f} kB prompt",
            self._config.AOAI_DEPLOYMENT,
            len(user_prompt) / 1024,
        )

        max_retries = self._config.LLM_MAX_RETRIES
        base_delay  = self._config.LLM_RETRY_BASE_DELAY

        for attempt in range(max_retries + 1):
            try:
                response = self._llm.invoke(messages)
                return response.content  # type: ignore[return-value]
            except _RETRYABLE as exc:
                if attempt >= max_retries:
                    logger.error(
                        "LLM query failed after {} attempt(s), giving up: {}",
                        attempt + 1, exc,
                    )
                    raise
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "LLM transient error (attempt {}/{}), retrying in {:.1f}s: {}",
                    attempt + 1, max_retries + 1, delay, exc,
                )
                time.sleep(delay)

        raise RuntimeError("LLM retry loop exited unexpectedly")

    # ── private ──

    def _build_messages(
        self,
        system_prompt: str,
        user_prompt: str,
        document: DocumentContent,
    ) -> list:
        messages: list = [SystemMessage(content=system_prompt)]

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

        return messages
