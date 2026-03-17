"""
Claims Decision Engine — the core orchestration layer.

Flow:
  FHIRClaim
    → build_rag_query()           (processor.py)
    → retrieve_policy_context()   (retriever.py)
    → Claude: medical necessity   (anthropic SDK)
    → if DENIED → Claude: appeal  (anthropic SDK, streaming)
    → ClaimDecision               (formatter.py)
"""

import json

import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential

from app.claims.models import FHIRClaim
from app.claims.processor import build_claim_context, build_rag_query
from app.core.config import settings
from app.core.logging import get_logger
from app.output.formatter import ClaimDecision, DecisionCode
from app.prompts.appeal_letter import APPEAL_LETTER_SYSTEM_PROMPT, build_appeal_letter_prompt
from app.prompts.medical_necessity import (
    MEDICAL_NECESSITY_SYSTEM_PROMPT,
    build_medical_necessity_prompt,
)
from app.rag.retriever import retrieve_policy_context

logger = get_logger(__name__)

# Module-level Anthropic client (reused across requests)
_client = anthropic.Anthropic(api_key=settings.anthropic_api_key)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _call_medical_necessity_eval(claim_context: str, policy_context: str) -> dict:
    """
    Call Claude for the structured medical necessity evaluation.
    Returns the parsed JSON decision dict.
    """
    logger.info("calling_claude_for_evaluation", model=settings.llm_model)

    response = _client.messages.create(
        model=settings.llm_model,
        max_tokens=settings.llm_max_tokens,
        system=MEDICAL_NECESSITY_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": build_medical_necessity_prompt(claim_context, policy_context),
            }
        ],
    )

    raw_text = response.content[0].text.strip()

    # Strip markdown code fences if Claude wraps the JSON
    if raw_text.startswith("```"):
        raw_text = raw_text.split("```")[1]
        if raw_text.startswith("json"):
            raw_text = raw_text[4:]

    decision_dict = json.loads(raw_text)
    logger.info(
        "evaluation_complete",
        decision=decision_dict.get("decision"),
        confidence=decision_dict.get("confidence_score"),
    )
    return decision_dict


def _generate_appeal_letter(
    claim_context: str,
    decision_dict: dict,
    policy_context: str,
) -> str:
    """
    Stream an appeal letter from Claude for denied claims.
    Uses streaming to handle the longer output reliably.
    """
    logger.info("generating_appeal_letter")

    full_letter = ""
    with _client.messages.stream(
        model=settings.llm_model,
        max_tokens=2048,
        system=APPEAL_LETTER_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": build_appeal_letter_prompt(
                    claim_context,
                    json.dumps(decision_dict, indent=2),
                    policy_context,
                ),
            }
        ],
    ) as stream:
        for text_chunk in stream.text_stream:
            full_letter += text_chunk

    logger.info("appeal_letter_generated", length=len(full_letter))
    return full_letter


def evaluate_claim(claim: FHIRClaim) -> ClaimDecision:
    """
    Main entry point — evaluate a single FHIR claim and return a decision.

    Args:
        claim: Validated FHIRClaim object from the API request.

    Returns:
        ClaimDecision with structured denial info and optional appeal letter.
    """
    logger.info("evaluating_claim", claim_id=claim.claim_id, claim_type=claim.claim_type)

    # Step 1: Build human-readable claim context
    claim_context = build_claim_context(claim)

    # Step 2: Build RAG query and retrieve relevant policy passages
    rag_query = build_rag_query(claim)
    policy_context = retrieve_policy_context(rag_query, top_k=5)

    # Step 3: Medical necessity evaluation via Claude
    decision_dict = _call_medical_necessity_eval(claim_context, policy_context)

    # Step 4: Generate appeal letter for denied claims
    appeal_letter = None
    if decision_dict.get("decision") in (
        DecisionCode.DENIED.value,
        DecisionCode.APPEAL_RECOMMENDED.value,
    ):
        appeal_letter = _generate_appeal_letter(claim_context, decision_dict, policy_context)

    # Step 5: Build and return the structured decision
    return ClaimDecision(
        claim_id=claim.claim_id,
        decision=DecisionCode(decision_dict["decision"]),
        denial_code=decision_dict.get("denial_code"),
        clinical_gap=decision_dict.get("clinical_gap", ""),
        policy_reference=decision_dict.get("policy_reference", ""),
        confidence_score=float(decision_dict.get("confidence_score", 0.0)),
        reasoning=decision_dict.get("reasoning", ""),
        appeal_letter=appeal_letter,
    )
