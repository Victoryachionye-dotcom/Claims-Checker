"""
Mock Claims Generator — generates FHIR-structured test claims as JSON files.

Usage:
    python scripts/generate_mock_claims.py

Generates additional mock claims beyond the two provided in data/mock_claims/.
Useful for load testing and expanding test coverage.
"""

import json
import sys
from datetime import date, timedelta
from pathlib import Path
import random

sys.path.insert(0, str(Path(__file__).parent.parent))

OUTPUT_DIR = Path("data/mock_claims")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Spinal injection scenarios (high-denial area per LCD L39240)
SPINAL_CLAIMS = [
    {
        "claim_id": "CLM-2024-003",
        "claim_type": "outpatient",
        "service_date": str(date.today() - timedelta(days=10)),
        "total_amount": 950.00,
        "prior_authorization_number": None,
        "patient": {
            "id": "PAT-33301",
            "name": "David Williams",
            "dob": "1972-05-18",
            "member_id": "MEM-33301",
        },
        "provider": {
            "npi": "1122334455",
            "name": "Dr. Susan Park",
            "specialty": "Anesthesiology / Pain Management",
        },
        "insurance": {
            "payer_id": "UHC-001",
            "payer_name": "UnitedHealthcare",
            "plan_id": "PPO-CHOICE-2024",
            "group_number": "GRP-7788",
            "member_id": "MEM-33301",
        },
        "diagnoses": [
            {
                "sequence": 1,
                "code": "M54.50",
                "description": "Low back pain, unspecified",
            }
        ],
        "procedures": [
            {
                "code": "62322",
                "description": "Injection(s) of therapeutic substance(s), epidural or subarachnoid; lumbar or sacral (caudal), without imaging guidance",
                "quantity": 1,
                "unit_price": 950.00,
            }
        ],
        "clinical_notes": [
            {
                "note_type": "assessment",
                "content": "Patient with 2-week history of low back pain. No imaging. No prior treatment documented. Proceeding with epidural injection.",
            }
        ],
    },
    {
        "claim_id": "CLM-2024-004",
        "claim_type": "outpatient",
        "service_date": str(date.today() - timedelta(days=5)),
        "total_amount": 2400.00,
        "prior_authorization_number": "PA-2024-55512",
        "patient": {
            "id": "PAT-44402",
            "name": "Patricia Lee",
            "dob": "1960-11-30",
            "member_id": "MEM-44402",
        },
        "provider": {
            "npi": "5566778899",
            "name": "Dr. Michael Torres",
            "specialty": "Interventional Pain Management",
        },
        "insurance": {
            "payer_id": "CIGNA-001",
            "payer_name": "Cigna",
            "plan_id": "EPO-2024",
            "group_number": "GRP-9900",
            "member_id": "MEM-44402",
        },
        "diagnoses": [
            {
                "sequence": 1,
                "code": "M47.816",
                "description": "Spondylosis with radiculopathy, lumbar region",
            },
            {
                "sequence": 2,
                "code": "M51.17",
                "description": "Intervertebral disc degeneration, lumbosacral region",
            },
        ],
        "procedures": [
            {
                "code": "64483",
                "description": "Injection(s), anesthetic agent and/or steroid, transforaminal epidural, with imaging guidance (fluoroscopy or CT); lumbar or sacral, single level",
                "quantity": 1,
                "unit_price": 2400.00,
            }
        ],
        "clinical_notes": [
            {
                "note_type": "history",
                "content": "Patient with 4-month history of left-sided lumbar radiculopathy with radiation to the foot. Failed 8 weeks of physical therapy (3x/week) and 6 weeks of oral NSAIDs. EMG confirms L5 radiculopathy.",
            },
            {
                "note_type": "radiology",
                "content": "MRI Lumbar Spine: L4-L5 left paracentral disc extrusion with severe left foraminal stenosis. Moderate L5-S1 disc bulge.",
            },
            {
                "note_type": "assessment",
                "content": "Meets criteria for transforaminal ESI per LCD. Conservative treatment failure documented. Prior auth obtained. Fluoroscopic guidance will be used.",
            },
        ],
    },
]


def main() -> None:
    print(f"Generating {len(SPINAL_CLAIMS)} additional mock claims...")
    for claim in SPINAL_CLAIMS:
        filepath = OUTPUT_DIR / f"{claim['claim_id'].lower()}.json"
        with open(filepath, "w") as f:
            json.dump(claim, f, indent=2)
        print(f"  ✓ Written: {filepath}")
    print(f"\nAll mock claims saved to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
