"""
Quick test for the RAG retriever.

Usage:
    conda run -n mcp python -m rag.test_retriever
"""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

from rag.retriever import get_rag_context, rag_available

TESTS = [
    ("Health", "subclinical ketosis transition cow BHBA prevention management"),
    ("Health", "milk fever hypocalcemia calcium supplementation DCAD"),
    ("Health", "metritis uterine infection fresh cow treatment"),
    ("Fertility", "21-day pregnancy rate conception dairy cow reproductive efficiency"),
    ("Fertility", "estrus detection activity monitors heat detection"),
    ("Production", "peak milk yield lactation persistency 305-day production"),
    ("Calf Raising", "calf preweaning daily gain colostrum mortality"),
    ("Culling", "involuntary culling early lactation fresh cow longevity"),
]


def run():
    print(f"RAG available: {rag_available()}\n")
    if not rag_available():
        print("No papers ingested yet. Run: python -m rag.ingest")
        return

    for domain, query in TESTS:
        print(f"{'='*70}")
        print(f"Domain : {domain}")
        print(f"Query  : {query}")
        print(f"{'='*70}")

        result = get_rag_context(query, domain=domain, n_results=2)

        if not result:
            print("  ⚠  No results returned.\n")
            continue

        print(result)
        print()


if __name__ == "__main__":
    run()
