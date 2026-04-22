"""Embed selected quotation items for one PR and upsert to Pinecone.

Typical usage:
    from quotation_embedding.run import run_embedding
    count = run_embedding("R_260647/2026")
"""

from __future__ import annotations

from typing import Optional

from loguru import logger

from utils.config import AppConfig
from .embedding_client import EmbeddingClient
from .pinecone_writer import PineconeWriter
from .reader import EmbeddingItemReader


def run_embedding(
    purchase_req_no: str,
    *,
    config: Optional[AppConfig] = None,
) -> int:
    """Embed is_selected_quote=1 items for one PR and upsert to Pinecone.

    Embedding text is built by joining these 10 columns with ' | ':
        item_level_1 … item_level_8, commodity_tag, item_summary

    Each Pinecone vector uses a stable ID of the form ``dtl_{purchase_dtl_id}``
    so re-runs overwrite the previous vector rather than creating duplicates.

    Parameters
    ----------
    purchase_req_no:
        The PURCHASE_REQ_NO value (e.g. "R_260647/2026").
    config:
        Optional config override; created from env if not supplied.

    Returns
    -------
    int
        Number of vectors upserted.
    """
    if config is None:
        config = AppConfig()

    # 1. Fetch selected items (one per DTL_ID)
    reader = EmbeddingItemReader(config.get_azure_conn_str())
    rows = reader.fetch(purchase_req_no)

    if not rows:
        logger.warning(
            "No is_selected_quote=1 items found for {} — skipping embedding",
            purchase_req_no,
        )
        return 0

    logger.info(
        "Embedding {} selected item(s) for PR={}",
        len(rows), purchase_req_no,
    )

    # 2. Build embedding texts, skip rows with no content
    emb_client = EmbeddingClient(config)
    pairs = [(row, emb_client.build_text(row)) for row in rows]
    pairs = [(row, text) for row, text in pairs if text.strip()]

    if not pairs:
        logger.warning(
            "All embedding texts are empty for {} — no vectors to upsert",
            purchase_req_no,
        )
        return 0

    embed_rows, embed_texts = zip(*pairs)

    # 3. Get embeddings from Azure OpenAI
    embeddings = emb_client.embed(list(embed_texts))

    # 4. Build Pinecone vectors
    vectors = []
    for row, embedding in zip(embed_rows, embeddings):
        dtl_id  = row["purchase_dtl_id"]
        created = row.get("item_created_date")
        vectors.append({
            "id": f"dtl_{dtl_id}",
            "values": embedding,
            "metadata": {
                "purchase_req_no":        purchase_req_no,
                "purchase_dtl_id":        int(dtl_id),
                "extracted_item_uuid_pk": str(row["extracted_item_uuid_pk"]),
                "commodity_tag":          row.get("commodity_tag") or "",
                "item_created_date":      created.isoformat() if created else "",
            },
        })

    # 5. Delete stale vectors then upsert fresh ones
    writer = PineconeWriter(config)
    writer.delete_for_pr(purchase_req_no)
    writer.upsert(vectors)

    return len(vectors)
