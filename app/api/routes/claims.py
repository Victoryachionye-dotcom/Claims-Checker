"""
Claims verification endpoint.

POST /verify-claim
  - Accepts a FHIR-structured claim JSON body
  - Triggers the full RAG → Claude evaluation pipeline
  - Returns a structured ClaimDecision JSON

This endpoint is the primary webhook target for n8n automation flows.
"""

from fastapi import APIRouter, HTTPException, status

from app.claims.decision_engine import evaluate_claim
from app.claims.models import FHIRClaim
from app.core.logging import get_logger
from app.output.formatter import ClaimDecision
from app.rag.retriever import _index

router = APIRouter()
logger = get_logger(__name__)


@router.post(
    "/verify-claim",
    response_model=ClaimDecision,
    status_code=status.HTTP_200_OK,
    tags=["Claims"],
    summary="Verify a healthcare claim for medical necessity",
    description=(
        "Accepts a FHIR-structured claim, queries the CMS policy knowledge base via RAG, "
        "and returns an Approve / Deny / Appeal decision with full reasoning and an "
        "optional long-form appeal letter."
    ),
)
def verify_claim(claim: FHIRClaim) -> ClaimDecision:
    """
    Main claims verification endpoint.

    **n8n webhook target:** POST http://localhost:8000/verify-claim
    Content-Type: application/json
    Body: FHIRClaim JSON (see mock_claims/ for examples)
    """
    if _index is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "Policy knowledge base is not loaded. "
                "Run: python scripts/ingest_policies.py — then restart the server."
            ),
        )

    logger.info("claim_received", claim_id=claim.claim_id, claim_type=claim.claim_type)

    try:
        decision = evaluate_claim(claim)
        logger.info(
            "claim_processed",
            claim_id=claim.claim_id,
            decision=decision.decision,
            confidence=decision.confidence_score,
        )
        return decision

    except FileNotFoundError as exc:
        logger.error("index_not_found", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc

    except Exception as exc:
        logger.error("claim_evaluation_failed", claim_id=claim.claim_id, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Claim evaluation failed: {exc}",
        ) from exc
