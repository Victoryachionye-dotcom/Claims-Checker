# n8n Credentials Setup Guide

Configure these credentials in your n8n instance before activating the workflows.
Go to **n8n → Credentials → Add Credential** for each one below.

---

## 1. SMTP (Email Reporting)

Used by: both workflows for billing team reports and alerts.

| Field | Value |
|---|---|
| Credential name | `SMTP — Billing Reports` |
| Host | Your SMTP server (e.g. `smtp.gmail.com`) |
| Port | `587` (TLS) or `465` (SSL) |
| User | Your sending email address |
| Password | App password (not your account password — generate one in Gmail/Outlook security settings) |
| SSL/TLS | `STARTTLS` |

**Gmail:** Enable 2FA → Google Account → Security → App Passwords → generate one for n8n.

---

## 2. n8n Variables (Settings → Variables)

Set these in **n8n → Settings → Variables** — they are referenced across both workflows with `$vars.VARIABLE_NAME`.

| Variable | Example Value | Used For |
|---|---|---|
| `EMAIL_SENDER` | `claims-engine@yourorg.com` | From address for all emails |
| `BILLING_TEAM_EMAIL` | `billing@yourorg.com` | Recipient for denied/appeal alerts |
| `ADMIN_EMAIL` | `admin@yourorg.com` | Recipient for pipeline failure alerts |
| `PROVIDER_EMAIL` | `providers@yourorg.com` | Recipient for pending info requests |
| `CLAIMS_API_HOST` | `localhost` or your server IP | FastAPI server host |
| `CLAIMS_API_PORT` | `8000` | FastAPI server port |
| `SLACK_WEBHOOK_URL` | `https://hooks.slack.com/services/...` | Optional Slack notifications |
| `CLEARINGHOUSE_API_URL` | `https://api.waystar.com/v1` | Optional clearinghouse endpoint |
| `CLEARINGHOUSE_API_KEY` | `your-api-key` | Optional clearinghouse auth |

---

## 3. Execute Command Node — Path Configuration

In workflow `01_nightly_claims_pipeline.json`, update the **Run Nightly Pipeline** node command to match your server paths:

```bash
# Replace these with your actual paths:
cd /path/to/healthcare-claims-rag && /path/to/.venv/bin/python scripts/nightly_pipeline.py --source csv 2>&1
```

**Finding your paths:**
```bash
# Project path
pwd   # run from inside the project directory

# Python venv path
which python   # run after activating your venv (source .venv/bin/activate)
```

---

## 4. Webhook URL (Workflow 02)

After importing and activating workflow `02_single_claim_verification.json`, the webhook URL will be:

```
http://your-n8n-host:5678/webhook/verify-claim
```

Use this URL anywhere you want real-time single-claim verification — EHR system callbacks, billing software integrations, or direct API calls.

**Test it:**
```bash
curl -X POST http://localhost:5678/webhook/verify-claim \
  -H "Content-Type: application/json" \
  -d @data/mock_claims/denial_test_001.json
```

---

## Security Notes

- Never commit real credentials to this repository — this file is a template only.
- Use n8n's built-in credential encryption for all API keys and passwords.
- For production deployments, use environment variable injection rather than hardcoded values in n8n variables.
- Restrict n8n access to internal network only if it has access to PHI.
