"""
Integration tests for the FastAPI endpoints.
Uses TestClient — no running server required.
LLM and RAG calls are mocked.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.output.formatter import ClaimDecision, DecisionCode
from app.rag import retriever

MOCK_CLAIMS_DIR = Path(__file__).parent.parent / "data" / "mock_claims"
client = TestClient(app)


def load_claim_json(filename: str) -> dict:
    with open(MOCK_CLAIMS_DIR / filename) as f:
        return json.load(f)


class TestHealthEndpoint:
    def test_health_returns_ok(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "llm_model" in data
        assert "index_loaded" in data


class TestVerifyClaimEndpoint:
    def test_returns_503_when_index_not_loaded(self):
        retriever._index = None
        payload = load_claim_json("inpatient_claim_001.json")
        response = client.post("/verify-claim", json=payload)
        assert response.status_code == 503

    def test_returns_decision_when_index_loaded(self):
        """Mock both retriever and decision engine for a full happy-path test."""
        mock_decision = ClaimDecision(
            claim_id="CLM-2024-001",
            decision=DecisionCode.DENIED,
            denial_code="CO-50",
            clinical_gap="No formal physical therapy course documented prior to injection.",
            policy_reference="LCD L39240 Section 4.1 — Indications and Limitations",
            confidence_score=0.88,
            reasoning="The claim was denied because conservative therapy was not adequately documented.",
            appeal_letter="Dear Blue Cross Blue Shield,\n\nWe are writing to appeal...",
        )

        # Inject a dummy index so the 503 guard passes
        retriever._index = MagicMock()

        with patch(
            "app.api.routes.claims.evaluate_claim", return_value=mock_decision
        ):
            payload = load_claim_json("inpatient_claim_001.json")
            response = client.post("/verify-claim", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["claim_id"] == "CLM-2024-001"
        assert data["decision"] == "DENIED"
        assert data["denial_code"] == "CO-50"
        assert data["appeal_letter"] is not None
        assert data["confidence_score"] == pytest.approx(0.88)

        retriever._index = None

    def test_invalid_claim_returns_422(self):
        """Missing required fields should return 422 Unprocessable Entity."""
        response = client.post("/verify-claim", json={"claim_id": "BAD"})
        assert response.status_code == 422
