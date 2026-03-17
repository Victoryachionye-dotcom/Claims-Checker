"""
Policy Knowledge Base Indexer.

Loads CMS LCD/NCD PDFs from data/policy_kb/, chunks them, embeds them via
OpenAI, and stores vectors in ChromaDB (default) or Qdrant (via .env toggle).

Run once via: python scripts/ingest_policies.py
"""

import os
from pathlib import Path

from llama_index.core import Settings as LlamaSettings
from llama_index.core import SimpleDirectoryReader, StorageContext, VectorStoreIndex
from llama_index.llms.anthropic import Anthropic as AnthropicLLM

from app.core.config import settings
from app.core.logging import get_logger
from app.rag.embeddings import get_embedding_model

logger = get_logger(__name__)


def _get_vector_store():
    """
    Return the configured vector store.
    Controlled by VECTOR_STORE_TYPE in .env.
    'chroma' → ChromaDB (default, zero-friction)
    'qdrant' → Qdrant (requires docker-compose up -d)
    """
    if settings.vector_store_type == "qdrant":
        from llama_index.vector_stores.qdrant import QdrantVectorStore
        from qdrant_client import QdrantClient

        logger.info("vector_store", type="qdrant", url=settings.qdrant_url)
        client = QdrantClient(url=settings.qdrant_url)
        return QdrantVectorStore(
            client=client,
            collection_name=settings.qdrant_collection,
        )

    # Default: ChromaDB
    import chromadb
    from llama_index.vector_stores.chroma import ChromaVectorStore

    logger.info("vector_store", type="chroma", path=settings.chroma_persist_dir)
    os.makedirs(settings.chroma_persist_dir, exist_ok=True)
    chroma_client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
    chroma_collection = chroma_client.get_or_create_collection("healthcare_policies")
    return ChromaVectorStore(chroma_collection=chroma_collection)


def _configure_llama_settings() -> None:
    """Inject embedding model and LLM into LlamaIndex global settings."""
    LlamaSettings.embed_model = get_embedding_model()
    LlamaSettings.llm = AnthropicLLM(
        model=settings.llm_model,
        api_key=settings.anthropic_api_key,
        max_tokens=settings.llm_max_tokens,
    )


def build_index() -> VectorStoreIndex:
    """
    Ingest all PDFs in data/policy_kb/ and create the vector index.
    Call this once via scripts/ingest_policies.py.
    """
    _configure_llama_settings()

    policy_dir = Path(settings.policy_kb_dir)
    if not policy_dir.exists() or not any(policy_dir.glob("*.pdf")):
        raise FileNotFoundError(
            f"No PDF files found in '{policy_dir}'. "
            "Drop your CMS LCD/NCD PDFs there and re-run."
        )

    logger.info("loading_documents", directory=str(policy_dir))
    documents = SimpleDirectoryReader(
        input_dir=str(policy_dir),
        required_exts=[".pdf"],
        recursive=False,
    ).load_data()
    logger.info("documents_loaded", count=len(documents))

    vector_store = _get_vector_store()
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    logger.info("building_index", doc_count=len(documents))
    index = VectorStoreIndex.from_documents(
        documents,
        storage_context=storage_context,
        show_progress=True,
    )
    logger.info("index_built_successfully")
    return index


def load_index() -> VectorStoreIndex:
    """
    Load an existing index from the vector store without re-ingesting documents.
    Called at FastAPI startup.
    """
    _configure_llama_settings()

    vector_store = _get_vector_store()
    storage_context = StorageContext.from_defaults(vector_store=vector_store)

    logger.info("loading_existing_index")
    index = VectorStoreIndex.from_vector_store(
        vector_store=vector_store,
        storage_context=storage_context,
    )
    return index
