"""
FHIR-inspired Pydantic v2 models for healthcare claims.

Structure mirrors the FHIR R4 Claim resource to demonstrate modern
healthcare interoperability standards, while remaining simple enough
to be ingested via JSON (e.g., from n8n webhooks).

Reference: https://www.hl7.org/fhir/claim.html
"""

from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ClaimType(str, Enum):
    INPATIENT = "inpatient"
    OUTPATIENT = "outpatient"


class DiagnosisCode(BaseModel):
    """ICD-10-CM diagnosis code with sequence priority."""

    sequence: int = Field(..., ge=1, description="Priority order; 1 = primary diagnosis")
    code: str = Field(..., description="ICD-10-CM code, e.g. M54.5")
    description: str = Field(..., description="Human-readable diagnosis description")


class ProcedureCode(BaseModel):
    """CPT / HCPCS procedure code."""

    code: str = Field(..., description="CPT or HCPCS code, e.g. 62323")
    description: str = Field(..., description="Procedure description")
    quantity: int = Field(default=1, ge=1)
    unit_price: float = Field(..., ge=0.0, description="Billed amount per unit in USD")


class Patient(BaseModel):
    """Simplified FHIR Patient demographics."""

    id: str = Field(..., description="Internal patient identifier")
    name: str
    dob: date = Field(..., description="Date of birth")
    member_id: str = Field(..., description="Insurance member ID")


class Provider(BaseModel):
    """Rendering provider / facility."""

    npi: str = Field(..., description="10-digit National Provider Identifier")
    name: str
    specialty: str = Field(..., description="Medical specialty, e.g. Pain Management")


class Insurance(BaseModel):
    """Primary insurance / coverage information."""

    payer_id: str
    payer_name: str
    plan_id: str
    group_number: str
    member_id: str


class ClinicalNote(BaseModel):
    """Free-text clinical documentation attached to the claim."""

    note_type: str = Field(
        ...,
        description="One of: history, examination, assessment, plan, radiology, labs",
    )
    content: str = Field(..., description="Full text of the clinical note")


class FHIRClaim(BaseModel):
    """
    Top-level FHIR-inspired claim resource.

    This is the primary input to the RAG decision engine.
    """

    claim_id: str = Field(..., description="Unique claim identifier")
    claim_type: ClaimType
    service_date: date
    patient: Patient
    provider: Provider
    insurance: Insurance
    diagnoses: list[DiagnosisCode] = Field(..., min_length=1)
    procedures: list[ProcedureCode] = Field(..., min_length=1)
    clinical_notes: list[ClinicalNote] = Field(default_factory=list)
    total_amount: float = Field(..., ge=0.0, description="Total billed amount in USD")
    prior_authorization_number: Optional[str] = Field(
        default=None,
        description="Prior auth number if obtained before service",
    )
