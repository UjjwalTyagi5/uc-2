"""Azure OpenAI text-embedding-3-large client with batching and retry."""

from __future__ import annotations

import time
import random

from loguru import logger
from openai import AzureOpenAI, RateLimitError, APITimeoutError, APIConnectionError

from .config import EmbeddingConfig

_EMBED_FIELDS = [
    "item_level_1", "item_level_2", "item_level_3", "item_level_4",
    "item_level_5", "item_level_6", "item_level_7", "item_level_8",
    "commodity_tag", "item_summary",
]

_BATCH_SIZE = 100  # Azure OpenAI embeddings batch limit


class EmbeddingClient:
    """Wraps Azure OpenAI embeddings endpoint for text-embedding-3-large."""

    def __init__(self, config: EmbeddingConfig) -> None:
        self._client = AzureOpenAI(
            azure_endpoint=config.AOAI_ENDPOINT,
            api_key=config.AOAI_API_KEY,
            api_version=config.AOAI_API_VERSION,
        )
        self._deployment   = config.AOAI_EMBEDDING_DEPLOYMENT
        self._max_retries  = config.LLM_MAX_RETRIES
        self._base_delay   = config.LLM_RETRY_BASE_DELAY

    # ── Public ────────────────────────────────────────────────────────────

    @staticmethod
    def build_text(row: dict) -> str:
        """Join the 10 embedding fields with ' | ', skipping None/empty values."""
        parts = [str(row[f]) for f in _EMBED_FIELDS if row.get(f)]
        return " | ".join(parts)

    def embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a list of texts, returning one float vector per input."""
        results: list[list[float]] = []
        for start in range(0, len(texts), _BATCH_SIZE):
            batch = texts[start : start + _BATCH_SIZE]
            results.extend(self._embed_batch_with_retry(batch))
        return results

    # ── Private ───────────────────────────────────────────────────────────

    def _embed_batch_with_retry(self, texts: list[str]) -> list[list[float]]:
        _transient = (RateLimitError, APITimeoutError, APIConnectionError)
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.embeddings.create(
                    model=self._deployment,
                    input=texts,
                )
                return [e.embedding for e in response.data]
            except _transient as exc:
                if attempt >= self._max_retries:
                    raise
                delay = self._base_delay * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Embedding API error (attempt {}/{}), retrying in {:.1f}s: {}",
                    attempt + 1, self._max_retries + 1, delay, exc,
                )
                time.sleep(delay)
        raise RuntimeError("unreachable")
