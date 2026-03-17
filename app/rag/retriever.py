"""
Policy Retriever — query the vector index for relevant LCD/NCD sections.

Exposes a single async function `retrieve_policy_context()` that accepts
a natural-language query built from the claim and returns the most relevant
policy passages as a formatted string ready to be injected into Claude prompts.
"""

from llama_index.core import VectorStoreIndex
from llama_index.core.response.schema import RESPONSE_TYPE

from app.core.logging import get_logger

logger = get_logger(__name__)

# Global index — populated at FastAPI startup via app/main.py lifespan event
_index: VectorStoreIndex | None = None


def set_index(index: VectorStoreIndex) -> None:
    """Called by the FastAPI lifespan handler to inject the loaded index."""
    global _index
    _index = index
    logger.info("retriever_index_set")


def retrieve_policy_context(query: str, top_k: int = 5) -> str:
    """
    Query the policy knowledge base and return the most relevant passages.

    Args:
        query: Natural-language query derived from the claim under review.
        top_k: Number of document chunks to retrieve.

    Returns:
        Formatted string of retrieved policy passages for prompt injection.

    Raises:
        RuntimeError: If the index has not been loaded yet.
    """
    if _index is None:
        raise RuntimeError(
            "Policy index is not loaded. "
            "Run scripts/ingest_policies.py first, then start the server."
        )

    logger.info("retrieving_policy_context", query=query[:100], top_k=top_k)

    query_engine = _index.as_query_engine(
        similarity_top_k=top_k,
        response_mode="no_text",  # Return source nodes only; Claude does the synthesis
    )
    response: RESPONSE_TYPE = query_engine.query(query)

    # Build a structured context block from source nodes
    passages = []
    for i, node in enumerate(response.source_nodes, start=1):
        score = getattr(node, "score", None)
        score_str = f" (relevance: {score:.2f})" if score is not None else ""
        source = node.metadata.get("file_name", "Unknown Source")
        passages.append(
            f"[Policy Passage {i} — {source}{score_str}]\n{node.text.strip()}"
        )

    context = "\n\n---\n\n".join(passages)
    logger.info("context_retrieved", passage_count=len(passages))
    return context
