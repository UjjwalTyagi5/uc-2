"""Upsert embedding vectors into Pinecone."""

from __future__ import annotations

from loguru import logger
from pinecone import Pinecone

from .config import EmbeddingConfig

_UPSERT_BATCH = 100


class PineconeWriter:
    """Wraps a Pinecone index for vector upserts."""

    def __init__(self, config: EmbeddingConfig) -> None:
        pc = Pinecone(api_key=config.PINECONE_API_KEY)
        self._index = pc.Index(config.PINECONE_INDEX_NAME)

    def upsert(self, vectors: list[dict]) -> None:
        """Upsert vectors in batches of {_UPSERT_BATCH}.

        Each element of *vectors* must be a dict with keys:
            id      : str   — stable vector ID
            values  : list[float]
            metadata: dict
        """
        if not vectors:
            return
        for start in range(0, len(vectors), _UPSERT_BATCH):
            batch = vectors[start : start + _UPSERT_BATCH]
            self._index.upsert(vectors=batch)
            logger.debug(
                "Pinecone upserted batch {}-{} ({} vectors)",
                start + 1, start + len(batch), len(batch),
            )
        logger.info("Pinecone upsert complete: {} vector(s) written", len(vectors))
