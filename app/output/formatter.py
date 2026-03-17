"""
Structured output models for the claims decision engine.

Every API response wraps a ClaimDecision, which contains:
  - A machine-readable denial/approval code
  - A structured clinical gap analysis
  - The specific policy section that drives the decision
  - A long-form appeal letter (when decision = DENIED)
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class DecisionCode(str, Enum):
    APPROVED = "APPROVED"
    DENIED = "DENIED"
    APPEAL_RECOMMENDED = "APPEAL_RECOMMENDED"
    PENDING_INFO = "PENDING_INFO"


# Standard CMS/ANSI denial reason codes
DENIAL_CODE_DESCRIPTIONS: dict[str, str] = {
    "CO-4": "The procedure code is inconsistent with the modifier used.",
    "CO-11": "The diagnosis is inconsistent with the procedure.",
    "CO-50": "Non-covered service — not deemed medically necessary.",
    "CO-97": "The benefit for this service is included in the payment for another service.",
    "CO-119": "Benefit maximum for this time period has been reached.",
    "CO-167": "This (these) diagnosis(es) is (are) not covered.",
    "PR-204": "This service/equipment/drug is not covered under the patient's plan.",
    "N479": "Missing/incomplete/invalid clinical documentation supporting medical necessity.",
}


class ClaimDecision(BaseModel):
    """
    Full structured output for a single claim evaluation.

    Designed to be serialized as the JSON body returned to n8n
    or displayed in a Streamlit human-in-the-loop dashboard.
    """

    claim_id: str
    decision: DecisionCode
    denial_code: Optional[str] = Field(
        default=None,
        description="ANSI/CMS reason code, e.g. CO-50. Null when APPROVED.",
    )
    denial_code_description: Optional[str] = Field(
        default=None,
        description="Human-readable description of the denial code.",
    )
    clinical_gap: str = Field(
        ...,
        description=(
            "Specific gap between submitted clinical documentation "
            "and the policy's medical necessity criteria."
        ),
    )
    policy_reference: str = Field(
        ...,
        description="LCD/NCD section number and title that governs this decision.",
    )
    confidence_score: float = Field(
        ..., ge=0.0, le=1.0, description="Model confidence in the decision (0–1)."
    )
    reasoning: str = Field(
        ..., description="Step-by-step reasoning trace used to reach the decision."
    )
    appeal_letter: Optional[str] = Field(
        default=None,
        description="Full long-form appeal letter. Populated when decision = DENIED.",
    )
    processed_at: datetime = Field(default_factory=datetime.utcnow)

    def model_post_init(self, __context) -> None:
        """Auto-populate denial code description if code is known."""
        if self.denial_code and self.denial_code in DENIAL_CODE_DESCRIPTIONS:
            object.__setattr__(
                self,
                "denial_code_description",
                DENIAL_CODE_DESCRIPTIONS[self.denial_code],
            )
