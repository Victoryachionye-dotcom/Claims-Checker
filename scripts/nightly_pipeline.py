"""
nightly_pipeline.py — Autonomous Nightly Claims Processing Orchestrator

Runs the full 3-layer pipeline:
  1. INGESTION  — Pull claims from EHR (FHIR API, SQL, or SFTP CSV)
  2. SCRUBBING  — Run every claim through the RAG engine (pre-submission review)
  3. ROUTING    — Route decisions: auto-submit approved, hold denied, alert billing team

Designed to run nightly at ~11:55 PM via:
  - System cron:      55 23 * * * python /path/to/nightly_pipeline.py
  - n8n Execute node: Trigger this script from an n8n Schedule node
  - APScheduler:      python nightly_pipeline.py --scheduler (persistent daemon)

Usage:
    python scripts/nightly_pipeline.py
    python scripts/nightly_pipeline.py --source fhir --dry-run
    python scripts/nightly_pipeline.py --source sql --dry-run
    python scripts/nightly_pipeline.py --source sftp
    python scripts/nightly_pipeline.py --scheduler        # run as persistent daemon
    python scripts/nightly_pipeline.py --run-now          # trigger immediately (bypass schedule)
"""

import sys
import csv
import os
import shutil
import argparse
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.logging import configure_logging, get_logger
from app.core.config import settings

configure_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Configuration — set via environment variables or .env file
# ---------------------------------------------------------------------------

# EHR Source: "fhir" | "sql" | "sftp" | "csv"
EHR_SOURCE = os.getenv("EHR_SOURCE", "csv")

# FHIR API config
FHIR_BASE_URL = os.getenv("FHIR_BASE_URL", "")
FHIR_ACCESS_TOKEN = os.getenv("FHIR_ACCESS_TOKEN", "")

# SQL config
SQL_CONNECTION_STRING = os.getenv("SQL_CONNECTION_STRING", "")
SQL_QUERY = os.getenv(
    "SQL_QUERY",
    "SELECT * FROM claims WHERE submission_status = 'pending' AND created_date >= CURRENT_DATE - 1",
)

# SFTP config
SFTP_HOST = os.getenv("SFTP_HOST", "")
SFTP_USERNAME = os.getenv("SFTP_USERNAME", "")
SFTP_PASSWORD = os.getenv("SFTP_PASSWORD", "")
SFTP_PRIVATE_KEY = os.getenv("SFTP_PRIVATE_KEY_PATH", "")
SFTP_REMOTE_DIR = os.getenv("SFTP_REMOTE_DIR", "/exports/claims")

# CSV input (for manual / testing mode)
CSV_INPUT_PATH = os.getenv("CSV_INPUT_PATH", "data/batch_input/claims.csv")

# Output paths
BATCH_OUTPUT_DIR = os.getenv("BATCH_OUTPUT_DIR", "data/batch_output")
REPORTS_ARCHIVE_DIR = os.getenv("REPORTS_ARCHIVE_DIR", "data/reports")

# Email reporting (optional)
EMAIL_ENABLED = os.getenv("EMAIL_ENABLED", "false").lower() == "true"
EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "587"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_SMTP_PASSWORD", "")
EMAIL_RECIPIENTS = [r.strip() for r in os.getenv("EMAIL_RECIPIENTS", "").split(",") if r.strip()]

# Clearinghouse / payer API (optional — for auto-submitting approved claims)
CLEARINGHOUSE_API_URL = os.getenv("CLEARINGHOUSE_API_URL", "")
CLEARINGHOUSE_API_KEY = os.getenv("CLEARINGHOUSE_API_KEY", "")

# Hold queue path (DENIED/APPEAL claims written here for billing team review)
HOLD_QUEUE_DIR = os.getenv("HOLD_QUEUE_DIR", "data/hold_queue")


# ---------------------------------------------------------------------------
# Step 1: Ingest Claims from EHR
# ---------------------------------------------------------------------------

def ingest_claims(source: str, dry_run: bool = False) -> list[dict]:
    """
    Pull claims from the configured EHR source.
    Returns a list of flat row dicts matching the batch_processor input schema.
    """
    from scripts.ehr_connector import pull_fhir_claims, pull_sql_claims, pull_sftp_claims

    logger.info("ingestion_start", source=source)

    if source == "fhir":
        if not FHIR_BASE_URL or not FHIR_ACCESS_TOKEN:
            logger.error(
                "fhir_config_missing",
                action="Set FHIR_BASE_URL and FHIR_ACCESS_TOKEN in .env",
            )
            sys.exit(1)
        rows = pull_fhir_claims(
            fhir_base_url=FHIR_BASE_URL,
            access_token=FHIR_ACCESS_TOKEN,
            since_hours=24,
        )

    elif source == "sql":
        if not SQL_CONNECTION_STRING:
            logger.error(
                "sql_config_missing",
                action="Set SQL_CONNECTION_STRING in .env",
            )
            sys.exit(1)
        rows = pull_sql_claims(
            connection_string=SQL_CONNECTION_STRING,
            query=SQL_QUERY,
        )

    elif source == "sftp":
        if not SFTP_HOST:
            logger.error("sftp_config_missing", action="Set SFTP_HOST in .env")
            sys.exit(1)
        rows = pull_sftp_claims(
            host=SFTP_HOST,
            username=SFTP_USERNAME,
            password=SFTP_PASSWORD,
            private_key_path=SFTP_PRIVATE_KEY,
            remote_dir=SFTP_REMOTE_DIR,
        )

    elif source == "csv":
        csv_path = Path(CSV_INPUT_PATH)
        if not csv_path.exists():
            logger.error("csv_not_found", path=str(csv_path))
            sys.exit(1)
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    else:
        logger.error("unknown_ehr_source", source=source)
        sys.exit(1)

    logger.info("ingestion_complete", claims_pulled=len(rows))
    return rows


def _save_ingested_csv(rows: list[dict], staging_dir: Path) -> Path:
    """Write ingested rows to a timestamped CSV in the staging dir."""
    from scripts.batch_processor import INPUT_COLUMNS

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    csv_path = staging_dir / f"claims_{timestamp}.csv"

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in INPUT_COLUMNS})

    logger.info("ingested_csv_saved", path=str(csv_path), rows=len(rows))
    return csv_path


# ---------------------------------------------------------------------------
# Step 2: RAG Scrubbing (batch_processor)
# ---------------------------------------------------------------------------

def scrub_claims(csv_path: Path, output_dir: Path, dry_run: bool = False) -> Path:
    """
    Run the batch processor on the ingested CSV.
    Returns path to the output CSV containing all decisions.
    """
    from scripts.batch_processor import main as batch_main

    logger.info("scrubbing_start", input=str(csv_path))
    batch_main(
        input_source=str(csv_path),
        output_dir=str(output_dir),
        dry_run=dry_run,
    )

    # Find the most recent output CSV in the output dir
    output_csvs = sorted(output_dir.glob("batch_results_*.csv"), reverse=True)
    if not output_csvs:
        logger.error("no_batch_output_found", output_dir=str(output_dir))
        sys.exit(1)

    return output_csvs[0]


# ---------------------------------------------------------------------------
# Step 3: Route Decisions
# ---------------------------------------------------------------------------

def route_decisions(results_csv: Path) -> dict:
    """
    Read the batch output CSV and route each claim by decision:
      APPROVED          → submit to clearinghouse API (or log for manual submission)
      DENIED            → hold queue + alert billing team
      APPEAL_RECOMMENDED→ hold queue + alert billing team
      PENDING_INFO      → hold queue + request additional docs
      errors            → hold queue + alert billing team

    Returns a summary dict with counts per decision.
    """
    hold_dir = Path(HOLD_QUEUE_DIR)
    hold_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    hold_csv = hold_dir / f"hold_queue_{timestamp}.csv"

    with open(results_csv, newline="", encoding="utf-8") as f:
        results = list(csv.DictReader(f))

    approved = []
    held = []
    routing_summary = {
        "APPROVED": 0,
        "DENIED": 0,
        "APPEAL_RECOMMENDED": 0,
        "PENDING_INFO": 0,
        "ERRORS": 0,
    }

    for row in results:
        decision = row.get("decision", "")
        status = row.get("batch_status", "")

        if status != "success":
            held.append(row)
            routing_summary["ERRORS"] += 1
        elif decision == "APPROVED":
            approved.append(row)
            routing_summary["APPROVED"] += 1
        else:
            held.append(row)
            key = decision if decision in routing_summary else "ERRORS"
            routing_summary[key] += 1

    # Write hold queue CSV
    if held:
        with open(hold_csv, "w", newline="", encoding="utf-8") as f:
            if held:
                writer = csv.DictWriter(f, fieldnames=list(held[0].keys()))
                writer.writeheader()
                writer.writerows(held)
        logger.info("hold_queue_written", path=str(hold_csv), count=len(held))

    # Submit approved claims to clearinghouse
    if approved:
        _submit_approved_claims(approved)

    logger.info("routing_complete", **routing_summary)
    return routing_summary


def _submit_approved_claims(approved: list[dict]) -> None:
    """
    Submit approved claims to the clearinghouse API.
    If CLEARINGHOUSE_API_URL is not configured, logs the claims for manual submission.
    """
    if not CLEARINGHOUSE_API_URL or not CLEARINGHOUSE_API_KEY:
        logger.info(
            "clearinghouse_not_configured",
            approved_count=len(approved),
            action="Set CLEARINGHOUSE_API_URL and CLEARINGHOUSE_API_KEY in .env to enable auto-submission",
        )
        print(f"\n  {len(approved)} claim(s) APPROVED — ready for payer submission.")
        print("  Configure CLEARINGHOUSE_API_URL in .env to enable automatic submission.\n")
        return

    try:
        import httpx
    except ImportError:
        logger.warning("httpx_not_installed_skipping_auto_submit")
        return

    headers = {
        "Authorization": f"Bearer {CLEARINGHOUSE_API_KEY}",
        "Content-Type": "application/json",
    }

    submitted = 0
    for claim_row in approved:
        try:
            resp = httpx.post(
                f"{CLEARINGHOUSE_API_URL}/submit",
                json={"claim_id": claim_row.get("claim_id"), "data": claim_row},
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            submitted += 1
            logger.info("claim_submitted", claim_id=claim_row.get("claim_id"))
        except Exception as exc:
            logger.error("claim_submission_failed", claim_id=claim_row.get("claim_id"), error=str(exc))

    logger.info("auto_submission_complete", submitted=submitted, total=len(approved))


# ---------------------------------------------------------------------------
# Step 4: Email Morning Report
# ---------------------------------------------------------------------------

def send_morning_report(
    pdf_path: Path,
    results_csv: Path,
    routing_summary: dict,
    run_date: str,
) -> None:
    """
    Email the PDF report and results CSV to the billing team.
    Requires EMAIL_ENABLED=true and SMTP settings in .env.
    """
    if not EMAIL_ENABLED:
        logger.info("email_disabled", action="Set EMAIL_ENABLED=true in .env to enable reports")
        return

    if not EMAIL_RECIPIENTS:
        logger.warning("no_email_recipients_configured")
        return

    total = sum(routing_summary.values())
    approved = routing_summary.get("APPROVED", 0)
    denied = routing_summary.get("DENIED", 0) + routing_summary.get("APPEAL_RECOMMENDED", 0)
    pending = routing_summary.get("PENDING_INFO", 0)
    errors = routing_summary.get("ERRORS", 0)

    subject = f"[Claims Report] {run_date} — {approved} Approved, {denied} Held, {errors} Errors"

    body = f"""
Healthcare Claims Batch Processing Report — {run_date}

SUMMARY
-------
Total Claims Processed:    {total}
Approved (auto-submitted): {approved}
Denied / Appeal:           {denied}
Pending Info:              {pending}
Processing Errors:         {errors}

ACTION REQUIRED
---------------
• {denied} claim(s) are in the hold queue awaiting billing team review.
  Appeal letters have been pre-generated for all denied claims.
• {pending} claim(s) require additional documentation from the provider.
• {errors} claim(s) failed processing and need manual review.

Full details in the attached PDF report and CSV.

—
Healthcare Claims RAG Engine
Automated report generated at {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
"""

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for attach_path in [pdf_path, results_csv]:
        if attach_path.exists():
            with open(attach_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{attach_path.name}"',
            )
            msg.attach(part)

    try:
        with smtplib.SMTP(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())
        logger.info("morning_report_sent", recipients=EMAIL_RECIPIENTS)
        print(f"\n  Morning report emailed to: {', '.join(EMAIL_RECIPIENTS)}\n")
    except Exception as exc:
        logger.error("email_send_failed", error=str(exc))


# ---------------------------------------------------------------------------
# Archive Report
# ---------------------------------------------------------------------------

def archive_report(pdf_path: Path, csv_path: Path) -> None:
    """Copy the report to the archive directory for historical retention."""
    archive_dir = Path(REPORTS_ARCHIVE_DIR)
    archive_dir.mkdir(parents=True, exist_ok=True)

    for f in [pdf_path, csv_path]:
        if f.exists():
            dest = archive_dir / f.name
            shutil.copy2(f, dest)
            logger.info("report_archived", path=str(dest))


# ---------------------------------------------------------------------------
# Main Pipeline Orchestration
# ---------------------------------------------------------------------------

def run_pipeline(source: str = EHR_SOURCE, dry_run: bool = False) -> None:
    """Execute the full nightly pipeline end-to-end."""
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("nightly_pipeline_start", source=source, dry_run=dry_run, date=run_date)
    print(f"\n{'=' * 60}")
    print(f"  Healthcare Claims Nightly Pipeline — {run_date}")
    print(f"  Source: {source} | Dry Run: {dry_run}")
    print(f"{'=' * 60}\n")

    staging_dir = Path("data/batch_input")
    output_dir = Path(BATCH_OUTPUT_DIR)
    staging_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Ingest
    print("  [1/4] Ingesting claims from EHR ...")
    raw_rows = ingest_claims(source=source, dry_run=dry_run)
    if not raw_rows:
        logger.info("no_claims_to_process")
        print("  No claims found for today. Pipeline complete.\n")
        return
    ingested_csv = _save_ingested_csv(raw_rows, staging_dir)
    print(f"         {len(raw_rows)} claim(s) ingested → {ingested_csv.name}\n")

    # Step 2: Scrub
    print("  [2/4] Running RAG pre-submission scrubber ...")
    results_csv = scrub_claims(ingested_csv, output_dir, dry_run=dry_run)
    print(f"         Scrubbing complete → {results_csv.name}\n")

    # Find matching PDF (same timestamp prefix)
    pdf_name = results_csv.name.replace(".csv", ".pdf")
    results_pdf = output_dir / pdf_name

    # Step 3: Route
    if not dry_run:
        print("  [3/4] Routing decisions ...")
        routing_summary = route_decisions(results_csv)
        print(f"         APPROVED: {routing_summary['APPROVED']} | "
              f"DENIED: {routing_summary['DENIED']} | "
              f"APPEAL: {routing_summary['APPEAL_RECOMMENDED']} | "
              f"PENDING: {routing_summary['PENDING_INFO']} | "
              f"ERRORS: {routing_summary['ERRORS']}\n")
    else:
        routing_summary = {}
        print("  [3/4] Routing skipped (dry run)\n")

    # Step 4: Report
    print("  [4/4] Sending morning report ...")
    if not dry_run:
        send_morning_report(results_pdf, results_csv, routing_summary, run_date)
        archive_report(results_pdf, results_csv)
        print("         Report archived.\n")
    else:
        print("         Email skipped (dry run)\n")

    logger.info("nightly_pipeline_complete", date=run_date)
    print(f"{'=' * 60}")
    print(f"  Pipeline complete.")
    print(f"  Output CSV: {results_csv}")
    if results_pdf.exists():
        print(f"  Output PDF: {results_pdf}")
    print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# APScheduler Daemon Mode
# ---------------------------------------------------------------------------

def run_as_scheduler() -> None:
    """
    Run as a persistent background process using APScheduler.
    Triggers nightly at 23:55 (11:55 PM) server local time.
    Use this if you're not using cron or n8n.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error("apscheduler_not_installed", action="pip install apscheduler")
        sys.exit(1)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        "cron",
        hour=23,
        minute=55,
        id="nightly_claims_pipeline",
        replace_existing=True,
    )

    logger.info("scheduler_starting", trigger="cron", hour=23, minute=55)
    print("\n  APScheduler started — pipeline will run nightly at 23:55.")
    print("  Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("scheduler_stopped")
        print("\n  Scheduler stopped.\n")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Claims Nightly Pipeline Orchestrator"
    )
    parser.add_argument(
        "--source",
        choices=["fhir", "sql", "sftp", "csv"],
        default=EHR_SOURCE,
        help=(
            "EHR data source: fhir (FHIR R4 API), sql (database), "
            "sftp (nightly file drop), csv (local file). "
            f"Default from EHR_SOURCE env var: {EHR_SOURCE}"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ingest and validate only — do not evaluate claims or send reports.",
    )
    parser.add_argument(
        "--run-now",
        action="store_true",
        help="Trigger the pipeline immediately (bypass schedule).",
    )
    parser.add_argument(
        "--scheduler",
        action="store_true",
        help="Run as a persistent daemon via APScheduler (cron: 23:55 nightly).",
    )

    args = parser.parse_args()

    if args.scheduler:
        run_as_scheduler()
    else:
        run_pipeline(source=args.source, dry_run=args.dry_run)
