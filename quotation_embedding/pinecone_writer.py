"""Upsert embedding vectors into Pinecone."""

from __future__ import annotations

from loguru import logger
from pinecone import Pinecone

from utils.config import AppConfig

class PineconeWriter:
    """Wraps a Pinecone index for vector upserts and similarity queries."""

    def __init__(self, config: AppConfig) -> None:
        pc = Pinecone(api_key=config.PINECONE_API_KEY)
        self._index       = pc.Index(config.PINECONE_INDEX_NAME)
        self._batch_size  = config.PINECONE_UPSERT_BATCH
        self._top_k       = config.PINECONE_TOP_K
        self._threshold   = config.PINECONE_THRESHOLD

    def upsert(self, vectors: list[dict]) -> None:
        """Upsert vectors in batches.

        Each element of *vectors* must be a dict with keys:
            id      : str   — stable vector ID
            values  : list[float]
            metadata: dict
        """
        if not vectors:
            return
        for start in range(0, len(vectors), self._batch_size):
            batch = vectors[start : start + self._batch_size]
            self._index.upsert(vectors=batch)
            logger.debug(
                "Pinecone upserted batch {}-{} ({} vectors)",
                start + 1, start + len(batch), len(batch),
            )
        logger.info("Pinecone upsert complete: {} vector(s) written", len(vectors))

    def query(
        self,
        vector: list[float],
        *,
        top_k: int | None = None,
        threshold: float | None = None,
        filter: dict | None = None,
    ) -> list[dict]:
        """Return the top-k nearest neighbours for *vector* above *threshold*.

        Parameters
        ----------
        vector:
            Query embedding (must match the index dimension).
        top_k:
            Number of candidates to fetch. Defaults to PINECONE_TOP_K from config.
        threshold:
            Minimum similarity score to include (0.0–1.0).
            Defaults to PINECONE_THRESHOLD from config.
        filter:
            Optional Pinecone metadata filter, e.g. ``{"commodity_tag": {"$eq": "pump"}}``.

        Returns
        -------
        list[dict]
            Each dict has keys: ``id``, ``score``, ``metadata``.
            Results with score < threshold are excluded.
        """
        k     = top_k     if top_k     is not None else self._top_k
        thr   = threshold if threshold is not None else self._threshold
        response = self._index.query(
            vector=vector,
            top_k=k,
            include_metadata=True,
            filter=filter,
        )
        return [
            {"id": m.id, "score": m.score, "metadata": m.metadata or {}}
            for m in response.matches
            if m.score >= thr
        ]
