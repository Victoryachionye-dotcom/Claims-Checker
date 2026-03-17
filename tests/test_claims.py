"""
Unit tests for claim models and processor.
No LLM or API calls required — pure Python.
"""

import json
from datetime import date
from pathlib import Path

import pytest

from app.claims.models import FHIRClaim
from app.claims.processor import build_claim_context, build_rag_query

MOCK_CLAIMS_DIR = Path(__file__).parent.parent / "data" / "mock_claims"


def load_mock_claim(filename: str) -> FHIRClaim:
    with open(MOCK_CLAIMS_DIR / filename) as f:
        return FHIRClaim.model_validate(json.load(f))


class TestFHIRClaimModel:
    def test_spinal_claim_parses(self):
        claim = load_mock_claim("inpatient_claim_001.json")
        assert claim.claim_id == "CLM-2024-001"
        assert len(claim.diagnoses) == 3
        assert len(claim.procedures) == 1
        assert claim.procedures[0].code == "62323"

    def test_cardiology_claim_parses(self):
        claim = load_mock_claim("outpatient_claim_002.json")
        assert claim.prior_authorization_number == "PA-2024-88991"
        assert len(claim.procedures) == 3

    def test_claim_requires_at_least_one_diagnosis(self):
        with pytest.raises(Exception):
            FHIRClaim(
                claim_id="BAD-001",
                claim_type="outpatient",
                service_date=date.today(),
                patient={"id": "p", "name": "Test", "dob": "1980-01-01", "member_id": "m"},
                provider={"npi": "1234567890", "name": "Dr Test", "specialty": "General"},
                insurance={
                    "payer_id": "p",
                    "payer_name": "Payer",
                    "plan_id": "plan",
                    "group_number": "grp",
                    "member_id": "m",
                },
                diagnoses=[],  # must fail
                procedures=[{"code": "99213", "description": "E&M", "quantity": 1, "unit_price": 100}],
                total_amount=100.0,
            )


class TestClaimProcessor:
    def test_build_claim_context_contains_key_fields(self):
        claim = load_mock_claim("inpatient_claim_001.json")
        context = build_claim_context(claim)
        assert "CLM-2024-001" in context
        assert "62323" in context
        assert "M54.4" in context
        assert "NOT OBTAINED" in context  # no prior auth

    def test_build_rag_query_includes_procedure_and_diagnosis(self):
        claim = load_mock_claim("inpatient_claim_001.json")
        query = build_rag_query(claim)
        assert "62323" in query or "epidural" in query.lower()
        assert "M54.4" in query or "sciatica" in query.lower()

    def test_cardiology_query_mentions_specialty(self):
        claim = load_mock_claim("outpatient_claim_002.json")
        query = build_rag_query(claim)
        assert "Cardiology" in query or "cardiology" in query.lower()
