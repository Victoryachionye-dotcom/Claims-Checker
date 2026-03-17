"""
Policy Ingestion Script — run ONCE to index all PDFs in data/policy_kb/.

Usage:
    python scripts/ingest_policies.py

What it does:
    1. Loads all PDF files from data/policy_kb/ (e.g., LCD L39240)
    2. Parses and chunks them using LlamaIndex
    3. Embeds chunks via OpenAI text-embedding-3-small
    4. Stores vectors in ChromaDB (default) or Qdrant (set VECTOR_STORE_TYPE=qdrant)

Re-run whenever you add new PDFs to data/policy_kb/.
"""

import sys
from pathlib import Path

# Allow running from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

from app.core.logging import configure_logging, get_logger
from app.rag.indexer import build_index

configure_logging()
logger = get_logger("ingest_policies")


def main() -> None:
    logger.info("=== Healthcare Claims Policy Ingestion ===")

    policy_dir = Path("data/policy_kb")
    pdf_files = list(policy_dir.glob("*.pdf"))

    if not pdf_files:
        logger.error(
            "no_pdfs_found",
            directory=str(policy_dir),
            action="Drop your CMS LCD/NCD PDFs into data/policy_kb/ and re-run.",
        )
        sys.exit(1)

    logger.info("pdfs_found", count=len(pdf_files), files=[f.name for f in pdf_files])

    try:
        index = build_index()
        logger.info(
            "ingestion_complete",
            message="Policy index built successfully. You can now start the API server.",
        )
    except Exception as exc:
        logger.error("ingestion_failed", error=str(exc))
        sys.exit(1)


if __name__ == "__main__":
    main()
