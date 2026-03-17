# n8n Workflow Automation

This directory contains pre-built n8n workflow files for the Healthcare Claims RAG Engine.
Import them directly into your n8n instance to get the full autonomous pipeline running without building anything from scratch.

---

## Workflows

### `01_nightly_claims_pipeline.json` — Autonomous Nightly Pipeline
Runs every night at 23:55. Pulls claims, scrubs them through the RAG engine, routes decisions, and emails a PDF report to the billing team by morning.

```
Schedule Trigger (23:55)
  → Execute Command: nightly_pipeline.py
  → IF: pipeline succeeded?
      YES → Parse Summary Stats
              → Email billing team (morning report)
              → Slack notification (optional)
      NO  → Email admin (failure alert)
```

### `02_single_claim_verification.json` — On-Demand Single Claim Check
Exposes a webhook endpoint. Send any FHIR-structured claim via POST and get a real-time adjudication decision back. Routes the result to the right team automatically.

```
Webhook: POST /webhook/verify-claim
  → POST to FastAPI /verify-claim
  → Switch on decision:
      APPROVED          → Submit to clearinghouse API
      DENIED            → Email billing team (with pre-written appeal letter)
      APPEAL_RECOMMENDED → Email billing team (flagged for review)
      PENDING_INFO      → Email provider (request missing documentation)
  → Respond to caller with ClaimDecision JSON
```

---

## How to Import

1. Open your n8n instance
2. Go to **Workflows** → click the **⋮** menu → **Import from file**
3. Select the `.json` file from this directory
4. The workflow opens with all nodes pre-configured
5. Update paths and configure credentials (see below)
6. Click **Activate**

---

## Prerequisites

Before activating either workflow:

1. **n8n running** — locally (`npx n8n`) or cloud ([n8n.io](https://n8n.io))
2. **FastAPI server running** — `python app/main.py` (for workflow 02)
3. **Policy index built** — `python scripts/ingest_policies.py`
4. **Credentials configured** — see [credentials/credentials_template.md](credentials/credentials_template.md)

---

## Required Configuration

### Workflow 01 — Update the Execute Command path
Open the **Run Nightly Pipeline** node and update the command to your server's actual paths:
```bash
cd /your/path/to/healthcare-claims-rag && /your/path/.venv/bin/python scripts/nightly_pipeline.py --source csv 2>&1
```

### Both workflows — Set n8n Variables
Go to **n8n → Settings → Variables** and add:

| Variable | Description |
|---|---|
| `EMAIL_SENDER` | From address for all report emails |
| `BILLING_TEAM_EMAIL` | Billing team inbox |
| `ADMIN_EMAIL` | Admin/engineering alert address |
| `CLAIMS_API_HOST` | FastAPI server hostname (default: `localhost`) |
| `CLAIMS_API_PORT` | FastAPI server port (default: `8000`) |

Full variable list: [credentials/credentials_template.md](credentials/credentials_template.md)

### Both workflows — Configure SMTP Credential
Go to **n8n → Credentials → Add Credential → SMTP**.
Name it exactly: `SMTP — Billing Reports`

---

## EHR Source Options

Workflow 01 calls `nightly_pipeline.py`. Change `--source` in the Execute Command node to match your EHR setup:

| Flag | Data Source |
|---|---|
| `--source csv` | Local CSV file in `data/batch_input/` (default, good for testing) |
| `--source sql` | Direct database connection (set `SQL_CONNECTION_STRING` in `.env`) |
| `--source fhir` | EHR FHIR R4 API (set `FHIR_BASE_URL` + `FHIR_ACCESS_TOKEN` in `.env`) |
| `--source sftp` | SFTP nightly file drop (set `SFTP_HOST` etc. in `.env`) |

---

## Testing

**Test workflow 02 manually:**
```bash
# Start the FastAPI server first
python app/main.py

# Send a test claim to the n8n webhook
curl -X POST http://localhost:5678/webhook/verify-claim \
  -H "Content-Type: application/json" \
  -d @data/mock_claims/denial_test_001.json
```

**Test workflow 01 manually (trigger without waiting for schedule):**
In n8n, open the workflow → click **Test workflow** → the pipeline runs immediately.
