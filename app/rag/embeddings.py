"""
Embedding model configuration for LlamaIndex.

Uses OpenAI text-embedding-3-small for high-performance medical text matching.
Swap the model string here if you want to test text-embedding-3-large or
a local HuggingFace model.
"""

from llama_index.embeddings.openai import OpenAIEmbedding

from app.core.config import settings


def get_embedding_model() -> OpenAIEmbedding:
    """Return a configured OpenAI embedding model instance."""
    return OpenAIEmbedding(
        model=settings.embedding_model,
        api_key=settings.openai_api_key,
    )
