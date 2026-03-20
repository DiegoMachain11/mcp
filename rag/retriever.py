"""
RAG Retriever — used by domain agents at inference time.

Embeds the query and retrieves the most relevant scientific chunks
from the ChromaDB vector store built by rag/ingest.py.

Gracefully returns an empty string if the DB doesn't exist yet (no papers
have been ingested), so agents work fine without the RAG layer.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DB_DIR = REPO_ROOT / "rag" / "db"
COLLECTION_NAME = "dairy_science"
EMBEDDING_MODEL = "text-embedding-3-small"


@lru_cache(maxsize=1)
def _get_collection():
    """Load ChromaDB collection once per process. Returns None if DB not ready."""
    if not DB_DIR.exists():
        return None
    try:
        import chromadb
        client = chromadb.PersistentClient(path=str(DB_DIR))
        col = client.get_collection(COLLECTION_NAME)
        if col.count() == 0:
            return None
        return col
    except Exception:
        return None


def _embed_query(query: str) -> list[float]:
    from openai import OpenAI
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=[query])
    return resp.data[0].embedding


def get_rag_context(
    query: str,
    domain: str | None = None,
    n_results: int = 4,
) -> str:
    """
    Retrieve the top-n most relevant scientific passages for a given query.

    Args:
        query  : Search query built from anomalous KPI topics.
        domain : Optional domain name to prefer domain-tagged chunks.
        n_results: How many chunks to retrieve.

    Returns:
        A formatted string ready to inject into an LLM prompt, or "" if
        no papers have been ingested yet.
    """
    collection = _get_collection()
    if collection is None:
        return ""

    try:
        query_embedding = _embed_query(query)

        # Try domain-filtered retrieval first
        where_filter = None
        if domain:
            where_filter = {"domains": {"$contains": domain}}

        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=min(n_results, collection.count()),
            where=where_filter,
            include=["documents", "metadatas", "distances"],
        )

        # Fall back to unfiltered if domain filter returns nothing
        if not results["documents"][0] and domain:
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=min(n_results, collection.count()),
                include=["documents", "metadatas", "distances"],
            )

        docs = results["documents"][0]
        metas = results["metadatas"][0]

        if not docs:
            return ""

        parts = []
        for doc, meta in zip(docs, metas):
            source = meta.get("source", "unknown")
            parts.append(f"[Source: {source}]\n{doc.strip()}")

        return "\n\n---\n\n".join(parts)

    except Exception as exc:
        # Never crash an agent because of RAG failure
        print(f"[RAG] retrieval failed: {exc}")
        return ""


def rag_available() -> bool:
    """Quick check whether papers have been ingested."""
    return _get_collection() is not None
