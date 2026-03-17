"""
Claim Processor — normalizes and enriches a raw FHIRClaim for RAG query building.

Responsibilities:
- Validate claim completeness
- Build a natural-language summary for RAG query generation
- Extract key medical facts to drive policy retrieval
"""

from app.claims.models import FHIRClaim
from app.core.logging import get_logger

logger = get_logger(__name__)


def build_claim_context(claim: FHIRClaim) -> str:
    """
    Convert a FHIRClaim into a structured, human-readable context block
    suitable for injection into Claude prompts.

    Returns:
        Multi-line string summarizing the claim for the LLM.
    """
    primary_dx = next(
        (dx for dx in sorted(claim.diagnoses, key=lambda d: d.sequence) if dx.sequence == 1),
        claim.diagnoses[0],
    )

    procedures_str = "\n".join(
        f"  - CPT {p.code}: {p.description} (qty: {p.quantity}, billed: ${p.unit_price:.2f})"
        for p in claim.procedures
    )

    diagnoses_str = "\n".join(
        f"  - [{dx.sequence}] ICD-10 {dx.code}: {dx.description}"
        for dx in sorted(claim.diagnoses, key=lambda d: d.sequence)
    )

    notes_str = "\n\n".join(
        f"  [{note.note_type.upper()}]:\n  {note.content}"
        for note in claim.clinical_notes
    ) or "  No clinical notes attached."

    prior_auth = claim.prior_authorization_number or "NOT OBTAINED"

    return f"""Claim ID: {claim.claim_id}
Claim Type: {claim.claim_type.value.upper()}
Service Date: {claim.service_date.isoformat()}
Total Billed: ${claim.total_amount:,.2f}
Prior Authorization: {prior_auth}

PATIENT:
  ID: {claim.patient.id}
  Member ID: {claim.patient.member_id}
  DOB: {claim.patient.dob.isoformat()}

PROVIDER:
  NPI: {claim.provider.npi}
  Name: {claim.provider.name}
  Specialty: {claim.provider.specialty}

PAYER:
  {claim.insurance.payer_name} (Plan: {claim.insurance.plan_id})

PRIMARY DIAGNOSIS:
  ICD-10 {primary_dx.code}: {primary_dx.description}

ALL DIAGNOSES:
{diagnoses_str}

PROCEDURES:
{procedures_str}

CLINICAL DOCUMENTATION:
{notes_str}"""


def build_rag_query(claim: FHIRClaim) -> str:
    """
    Build a targeted natural-language query for the policy RAG retrieval.

    Focuses on the primary procedure and diagnosis combination — the most
    likely candidates for coverage criteria lookup.
    """
    primary_dx = sorted(claim.diagnoses, key=lambda d: d.sequence)[0]
    primary_proc = claim.procedures[0]
    specialty = claim.provider.specialty

    query = (
        f"Medical necessity coverage criteria for {primary_proc.description} "
        f"(CPT {primary_proc.code}) "
        f"in a patient with {primary_dx.description} "
        f"(ICD-10 {primary_dx.code}). "
        f"Provider specialty: {specialty}. "
        f"What are the indications, limitations, and documentation requirements?"
    )

    logger.info("rag_query_built", claim_id=claim.claim_id, query=query[:120])
    return query
