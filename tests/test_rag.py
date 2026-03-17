"""
Tests for RAG retriever module.
Uses a mock index so no real ChromaDB or Qdrant connection is required.
"""

from unittest.mock import MagicMock, patch

import pytest

from app.rag import retriever


def test_retrieve_raises_when_index_not_loaded():
    """Verify helpful error when the index hasn't been loaded yet."""
    retriever._index = None
    with pytest.raises(RuntimeError, match="Policy index is not loaded"):
        retriever.retrieve_policy_context("spinal injection coverage criteria")


def test_retrieve_returns_formatted_passages():
    """Verify the passage formatter produces clean, numbered output."""
    # Build a mock index and query engine
    mock_node = MagicMock()
    mock_node.text = "Coverage is limited to patients who have failed conservative therapy."
    mock_node.score = 0.91
    mock_node.metadata = {"file_name": "LCD_L39240.pdf"}

    mock_response = MagicMock()
    mock_response.source_nodes = [mock_node]

    mock_query_engine = MagicMock()
    mock_query_engine.query.return_value = mock_response

    mock_index = MagicMock()
    mock_index.as_query_engine.return_value = mock_query_engine

    retriever.set_index(mock_index)

    result = retriever.retrieve_policy_context("lumbar epidural steroid injection", top_k=1)

    assert "Policy Passage 1" in result
    assert "LCD_L39240.pdf" in result
    assert "conservative therapy" in result
    assert "0.91" in result

    # Cleanup
    retriever._index = None
