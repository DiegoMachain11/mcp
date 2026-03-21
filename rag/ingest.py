"""
RAG Ingestion Pipeline — run once (or again after adding new papers).

Reads all PDFs and .txt files from the papers/ directory, chunks them,
embeds them with OpenAI text-embedding-3-small, and stores them in a
local ChromaDB vector store at rag/db/.

Usage:
    cd /Users/diego/Documents/mcp
    python -m rag.ingest

Dependencies:
    pip install chromadb pypdf openai
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
PAPERS_DIR = REPO_ROOT / "papers"
DB_DIR = REPO_ROOT / "rag" / "db"

CHUNK_SIZE = 1400  # characters per chunk
CHUNK_OVERLAP = 180  # overlap between consecutive chunks
EMBEDDING_MODEL = "text-embedding-3-small"
COLLECTION_NAME = "dairy_science"


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        print("pypdf not installed. Run: pip install pypdf")
        sys.exit(1)
    reader = PdfReader(str(path))
    pages = []
    for page in reader.pages:
        text = page.extract_text() or ""
        pages.append(text)
    return "\n".join(pages)


def _read_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _chunk_text(
    text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP
) -> list[str]:
    """Split text into overlapping chunks."""
    # Normalize whitespace
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return []

    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start += chunk_size - overlap

    return chunks


def _stable_id(source: str, idx: int) -> str:
    h = hashlib.md5(f"{source}:{idx}".encode()).hexdigest()[:12]
    return f"{Path(source).stem}_{idx}_{h}"


def _infer_domain_tags(filename: str, text_snippet: str) -> list[str]:
    """Guess which farm domains are most relevant based on filename and content."""
    text = (filename + " " + text_snippet[:500]).lower()
    tags = []
    if any(
        w in text
        for w in [
            "fertility",
            "conception",
            "reproduction",
            "pregnancy",
            "estrus",
            "calving",
        ]
    ):
        tags.append("Fertility")
    if any(w in text for w in ["milk yield", "production", "lactation", "peak milk"]):
        tags.append("Production")
    if any(
        w in text
        for w in [
            "ketosis",
            "metritis",
            "lameness",
            "health",
            "disease",
            "hypocalcemia",
            "transition",
        ]
    ):
        tags.append("Health")
    if any(w in text for w in ["calf", "heifer", "weaning", "colostrum", "preweaning"]):
        tags.append("Calf Raising")
    if any(w in text for w in ["culling", "removal", "longevity", "involuntary"]):
        tags.append("Culling")
    return tags or ["General"]


def run_ingestion():
    import chromadb
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY")

    print("API_KEY = ", api_key)

    openai_client = OpenAI(api_key=api_key)

    DB_DIR.mkdir(parents=True, exist_ok=True)
    chroma = chromadb.PersistentClient(path=str(DB_DIR))
    collection = chroma.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    paper_files = list(PAPERS_DIR.glob("*.pdf")) + list(PAPERS_DIR.glob("*.txt"))
    if not paper_files:
        print(f"No papers found in {PAPERS_DIR}. Add PDF or TXT files and run again.")
        return

    print(f"Found {len(paper_files)} paper(s) to ingest.")
    total_chunks = 0

    for paper_path in paper_files:
        print(f"\nProcessing: {paper_path.name}")

        if paper_path.suffix.lower() == ".pdf":
            text = _read_pdf(paper_path)
        else:
            text = _read_txt(paper_path)

        if not text.strip():
            print(f"  ⚠ Empty text extracted, skipping.")
            continue

        chunks = _chunk_text(text)
        print(f"  → {len(chunks)} chunks")

        domain_tags = _infer_domain_tags(paper_path.name, text)
        print(f"  → Domains: {domain_tags}")

        # Embed in batches of 100
        batch_size = 100
        for batch_start in range(0, len(chunks), batch_size):
            batch = chunks[batch_start : batch_start + batch_size]

            response = openai_client.embeddings.create(
                model=EMBEDDING_MODEL,
                input=batch,
            )
            embeddings = [item.embedding for item in response.data]

            ids = [
                _stable_id(paper_path.name, batch_start + i) for i in range(len(batch))
            ]
            metadatas = [
                {
                    "source": paper_path.name,
                    "chunk_idx": batch_start + i,
                    "domains": ",".join(domain_tags),
                }
                for i in range(len(batch))
            ]

            collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=batch,
                metadatas=metadatas,
            )

        total_chunks += len(chunks)
        print(f"  ✓ Ingested {len(chunks)} chunks from {paper_path.name}")

    print(
        f"\n✅ Done. Total chunks in DB: {collection.count()} (added {total_chunks} this run)."
    )


if __name__ == "__main__":
    run_ingestion()
