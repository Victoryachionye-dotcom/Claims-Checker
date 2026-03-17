"""
batch_processor.py — Healthcare Claims RAG Batch Processing Engine

Reads claims from a CSV file or SQL database, runs each through evaluate_claim()
directly (no FastAPI server required), and writes a CSV + PDF output report.

Usage:
    python scripts/batch_processor.py --input data/batch_input/claims.csv
    python scripts/batch_processor.py --input data/batch_input/claims.csv --output-dir data/batch_output
    python scripts/batch_processor.py --input "sql:postgresql://user:pass@host/db" --sql-query "SELECT * FROM claims"
    python scripts/batch_processor.py --input data/batch_input/claims.csv --dry-run
"""

import sys
import csv
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

# Ensure project root is on sys.path for direct script execution
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from pydantic import ValidationError

from app.claims.models import (
    FHIRClaim,
    ClaimType,
    Patient,
    Provider,
    Insurance,
    DiagnosisCode,
    ProcedureCode,
    ClinicalNote,
)
from app.output.formatter import ClaimDecision
from app.claims.decision_engine import evaluate_claim
from app.rag import indexer, retriever
from app.core.logging import configure_logging, get_logger
from app.core.config import settings

configure_logging()
logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# CSV Input Schema — all expected column headers
# ---------------------------------------------------------------------------

INPUT_COLUMNS = [
    "claim_id", "claim_type", "service_date", "total_amount",
    "prior_authorization_number",
    "patient_id", "patient_name", "patient_dob", "patient_member_id",
    "provider_npi", "provider_name", "provider_specialty",
    "insurance_payer_id", "insurance_payer_name", "insurance_plan_id",
    "insurance_group_number", "insurance_member_id",
    "diagnosis_1_code", "diagnosis_1_description",
    "diagnosis_2_code", "diagnosis_2_description",
    "diagnosis_3_code", "diagnosis_3_description",
    "procedure_1_code", "procedure_1_description",
    "procedure_1_quantity", "procedure_1_unit_price",
    "procedure_2_code", "procedure_2_description",
    "procedure_2_quantity", "procedure_2_unit_price",
    "procedure_3_code", "procedure_3_description",
    "procedure_3_quantity", "procedure_3_unit_price",
    "clinical_notes",
]

DECISION_COLUMNS = [
    "decision", "denial_code", "denial_code_description", "clinical_gap",
    "policy_reference", "confidence_score", "reasoning", "appeal_letter",
    "processed_at",
]

METADATA_COLUMNS = ["batch_status", "error_message", "retry_count"]

OUTPUT_COLUMNS = INPUT_COLUMNS + DECISION_COLUMNS + METADATA_COLUMNS


# ---------------------------------------------------------------------------
# RAG Index Bootstrap
# ---------------------------------------------------------------------------

def _bootstrap_rag_index() -> None:
    """
    Load the vector index into the retriever module global before any
    evaluate_claim() calls. Mirrors the lifespan handler in app/main.py.
    Exits with code 1 if the index has never been built.
    """
    try:
        index = indexer.load_index()
        retriever.set_index(index)
        logger.info("rag_index_loaded_for_batch")
    except Exception as exc:
        logger.error(
            "rag_index_load_failed",
            error=str(exc),
            action="Run: python scripts/ingest_policies.py first",
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# CSV Row → FHIRClaim Parsing
# ---------------------------------------------------------------------------

def _parse_diagnoses(row: dict) -> list[DiagnosisCode]:
    diagnoses = []
    for seq in range(1, 4):
        code = (row.get(f"diagnosis_{seq}_code") or "").strip()
        desc = (row.get(f"diagnosis_{seq}_description") or "").strip()
        if code and desc:
            diagnoses.append(DiagnosisCode(sequence=seq, code=code, description=desc))
    return diagnoses


def _parse_procedures(row: dict) -> list[ProcedureCode]:
    procedures = []
    for n in range(1, 4):
        code = (row.get(f"procedure_{n}_code") or "").strip()
        desc = (row.get(f"procedure_{n}_description") or "").strip()
        if code and desc:
            raw_qty = (row.get(f"procedure_{n}_quantity") or "1").strip()
            raw_price = (row.get(f"procedure_{n}_unit_price") or "0").strip()
            procedures.append(
                ProcedureCode(
                    code=code,
                    description=desc,
                    quantity=int(raw_qty) if raw_qty else 1,
                    unit_price=float(raw_price) if raw_price else 0.0,
                )
            )
    return procedures


def _parse_clinical_notes(raw: str) -> list[ClinicalNote]:
    """
    Parse pipe-delimited multi-value clinical notes.
    Format: "note_type_1|content_1||note_type_2|content_2"
    Returns empty list if raw is blank.
    """
    if not raw or not raw.strip():
        return []
    notes = []
    for record in raw.split("||"):
        record = record.strip()
        if "|" in record:
            note_type, _, content = record.partition("|")
            if note_type.strip() and content.strip():
                notes.append(
                    ClinicalNote(note_type=note_type.strip(), content=content.strip())
                )
    return notes


def _format_validation_errors(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}"
        for e in exc.errors()
    )


def parse_csv_row(row: dict) -> FHIRClaim:
    """
    Convert a flat CSV row dict into a validated FHIRClaim.
    Raises pydantic.ValidationError for schema violations.
    """
    prior_auth = (row.get("prior_authorization_number") or "").strip() or None

    return FHIRClaim(
        claim_id=row["claim_id"].strip(),
        claim_type=ClaimType(row["claim_type"].strip().lower()),
        service_date=row["service_date"].strip(),
        total_amount=float(row["total_amount"]),
        prior_authorization_number=prior_auth,
        patient=Patient(
            id=row["patient_id"].strip(),
            name=row["patient_name"].strip(),
            dob=row["patient_dob"].strip(),
            member_id=row["patient_member_id"].strip(),
        ),
        provider=Provider(
            npi=row["provider_npi"].strip(),
            name=row["provider_name"].strip(),
            specialty=row["provider_specialty"].strip(),
        ),
        insurance=Insurance(
            payer_id=row["insurance_payer_id"].strip(),
            payer_name=row["insurance_payer_name"].strip(),
            plan_id=row["insurance_plan_id"].strip(),
            group_number=row["insurance_group_number"].strip(),
            member_id=row["insurance_member_id"].strip(),
        ),
        diagnoses=_parse_diagnoses(row),
        procedures=_parse_procedures(row),
        clinical_notes=_parse_clinical_notes(row.get("clinical_notes", "")),
    )


# ---------------------------------------------------------------------------
# CSV / SQL Ingestion
# ---------------------------------------------------------------------------

def read_csv_claims(filepath: Path) -> list[dict]:
    """Read a CSV file and return a list of raw row dicts."""
    with open(filepath, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_sql_claims(connection_string: str, query: str) -> list[dict]:
    """
    Connect via SQLAlchemy, execute query, return list of row dicts.
    Column names must match the CSV input schema.
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        logger.error("sqlalchemy_not_installed", action="pip install sqlalchemy")
        sys.exit(1)

    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [dict(row._mapping) for row in result]


# ---------------------------------------------------------------------------
# Claim Evaluation with Retry
# ---------------------------------------------------------------------------

def process_claim_with_retry(
    claim: FHIRClaim, max_retries: int = 3
) -> tuple[Optional[ClaimDecision], str, int]:
    """
    Call evaluate_claim() with autonomous outer retry for transient failures.

    Auto-retried errors: RateLimitError, APITimeoutError, APIConnectionError,
                         JSONDecodeError (malformed Claude response)
    Fatal errors:        AuthenticationError (re-raised to abort the batch)

    Returns: (decision | None, batch_status_string, retry_count_used)
    """
    import anthropic

    last_error = None
    for attempt in range(max_retries):
        try:
            decision = evaluate_claim(claim)
            return decision, "success", attempt
        except anthropic.AuthenticationError:
            raise  # fatal — bubble up to abort the entire batch
        except (
            anthropic.RateLimitError,
            anthropic.APITimeoutError,
            anthropic.APIConnectionError,
        ) as exc:
            last_error = exc
            wait = 2 ** attempt  # 1s, 2s, 4s
            logger.warning(
                "transient_api_error_retrying",
                attempt=attempt + 1,
                max_retries=max_retries,
                error=str(exc),
                wait_seconds=wait,
            )
            time.sleep(wait)
        except json.JSONDecodeError as exc:
            last_error = exc
            logger.warning(
                "claude_json_parse_error_retrying",
                attempt=attempt + 1,
                error=str(exc),
            )
            time.sleep(2)
        except Exception as exc:
            last_error = exc
            logger.error("unexpected_evaluation_error", error=str(exc))
            break

    return None, "api_error", max_retries


# ---------------------------------------------------------------------------
# Decision → Dict
# ---------------------------------------------------------------------------

def _decision_to_dict(decision: ClaimDecision) -> dict:
    return {
        "decision": decision.decision.value,
        "denial_code": decision.denial_code or "",
        "denial_code_description": decision.denial_code_description or "",
        "clinical_gap": decision.clinical_gap or "",
        "policy_reference": decision.policy_reference or "",
        "confidence_score": decision.confidence_score,
        "reasoning": decision.reasoning or "",
        "appeal_letter": decision.appeal_letter or "",
        "processed_at": decision.processed_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Output: CSV
# ---------------------------------------------------------------------------

def write_output_csv(results: list[dict], output_path: Path) -> None:
    """Write all result rows to CSV with the full output schema."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in results:
            writer.writerow({col: row.get(col, "") for col in OUTPUT_COLUMNS})
    logger.info("csv_written", path=str(output_path), rows=len(results))


# ---------------------------------------------------------------------------
# Output: PDF Report
# ---------------------------------------------------------------------------

def _build_summary_stats(results: list[dict]) -> dict:
    stats = {"APPROVED": 0, "DENIED": 0, "APPEAL_RECOMMENDED": 0, "PENDING_INFO": 0, "ERRORS": 0}
    total_billed = 0.0
    denial_codes: dict[str, int] = {}

    for row in results:
        status = row.get("batch_status", "")
        if status != "success":
            stats["ERRORS"] += 1
        else:
            decision = row.get("decision", "")
            if decision in stats:
                stats[decision] += 1
            dc = row.get("denial_code", "")
            if dc:
                denial_codes[dc] = denial_codes.get(dc, 0) + 1

        try:
            total_billed += float(row.get("total_amount", 0) or 0)
        except (ValueError, TypeError):
            pass

    top_denial_codes = sorted(denial_codes.items(), key=lambda x: x[1], reverse=True)[:3]
    scores = [
        float(r.get("confidence_score", 0))
        for r in results
        if r.get("batch_status") == "success" and r.get("confidence_score")
    ]
    avg_confidence = round(sum(scores) / len(scores), 2) if scores else 0.0

    return {
        "decision_counts": stats,
        "total_billed": total_billed,
        "top_denial_codes": top_denial_codes,
        "avg_confidence": avg_confidence,
    }


def write_output_pdf(results: list[dict], output_path: Path, input_filename: str) -> None:
    """Generate a formatted PDF report using reportlab."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter, landscape
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
        )
    except ImportError:
        logger.warning("reportlab_not_installed_skipping_pdf", action="pip install reportlab")
        return

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=landscape(letter),
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title", parent=styles["Title"], fontSize=18, spaceAfter=12)
    h2_style = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, spaceAfter=8)
    body_style = ParagraphStyle("Body", parent=styles["Normal"], fontSize=9, spaceAfter=4)
    small_style = ParagraphStyle("Small", parent=styles["Normal"], fontSize=8)
    appeal_style = ParagraphStyle(
        "Appeal", parent=styles["Normal"], fontSize=9, leading=13, spaceAfter=6
    )

    stats = _build_summary_stats(results)
    run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(results)

    story = []

    # ---- Page 1: Cover ----
    story.append(Spacer(1, 1.5 * inch))
    story.append(Paragraph("Healthcare Claims Batch Processing Report", title_style))
    story.append(Spacer(1, 0.3 * inch))
    cover_data = [
        ["Report Generated:", run_time],
        ["Input Source:", input_filename],
        ["Total Claims Processed:", str(total)],
        ["Total Amount Billed:", f"${stats['total_billed']:,.2f}"],
    ]
    cover_table = Table(cover_data, colWidths=[2.5 * inch, 4 * inch])
    cover_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 11),
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.whitesmoke, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(cover_table)
    story.append(PageBreak())

    # ---- Page 2: Executive Summary ----
    story.append(Paragraph("Executive Summary", h2_style))

    decision_data = [["Decision", "Count", "% of Total", "Notes"]]
    decision_labels = {
        "APPROVED": "Auto-submit to payer",
        "DENIED": "Hold — appeal letter generated",
        "APPEAL_RECOMMENDED": "Hold — billing team review needed",
        "PENDING_INFO": "Hold — request additional docs",
        "ERRORS": "Failed processing — manual review",
    }
    for decision, count in stats["decision_counts"].items():
        pct = f"{round(count / total * 100, 1)}%" if total else "0%"
        decision_data.append([decision, str(count), pct, decision_labels.get(decision, "")])

    decision_table = Table(decision_data, colWidths=[2.2 * inch, 0.8 * inch, 1 * inch, 4 * inch])
    decision_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a3c5e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(decision_table)
    story.append(Spacer(1, 0.3 * inch))

    story.append(Paragraph(f"Average Confidence Score: <b>{stats['avg_confidence']}</b>", body_style))

    if stats["top_denial_codes"]:
        story.append(Paragraph("Top Denial Codes:", body_style))
        for code, count in stats["top_denial_codes"]:
            story.append(Paragraph(f"  • {code}: {count} claim(s)", body_style))

    story.append(PageBreak())

    # ---- Page 3+: Claims Detail Table ----
    story.append(Paragraph("Claims Detail", h2_style))

    # Sort: DENIED first, then APPEAL_RECOMMENDED, then rest
    sort_order = {"DENIED": 0, "APPEAL_RECOMMENDED": 1, "PENDING_INFO": 2, "APPROVED": 3}
    sorted_results = sorted(
        results,
        key=lambda r: sort_order.get(r.get("decision", ""), 4),
    )

    detail_headers = [
        "Claim ID", "Patient", "Service Date", "Procedure(s)",
        "Decision", "Denial Code", "Confidence", "Policy Ref",
    ]
    detail_data = [detail_headers]
    for r in sorted_results:
        procs = r.get("procedure_1_code", "")
        if r.get("procedure_2_code"):
            procs += f", {r['procedure_2_code']}"
        detail_data.append([
            Paragraph(r.get("claim_id", ""), small_style),
            Paragraph(r.get("patient_name", ""), small_style),
            r.get("service_date", ""),
            Paragraph(procs, small_style),
            Paragraph(r.get("decision", r.get("batch_status", "")), small_style),
            r.get("denial_code", ""),
            str(r.get("confidence_score", "")),
            Paragraph(r.get("policy_reference", ""), small_style),
        ])

    col_widths = [1.5 * inch, 1.5 * inch, 1 * inch, 1.5 * inch, 1.5 * inch, 0.9 * inch, 0.8 * inch, 1.5 * inch]
    detail_table = Table(detail_data, colWidths=col_widths, repeatRows=1)
    detail_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a3c5e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(detail_table)

    # ---- Appeal Letters ----
    appeal_claims = [
        r for r in results
        if r.get("batch_status") == "success"
        and r.get("decision") in ("DENIED", "APPEAL_RECOMMENDED")
        and r.get("appeal_letter")
    ]

    for r in appeal_claims:
        story.append(PageBreak())
        story.append(Paragraph(
            f"APPEAL LETTER — Claim ID: {r.get('claim_id', 'Unknown')}",
            h2_style,
        ))
        story.append(Paragraph(
            f"Patient: {r.get('patient_name', '')} | "
            f"Service Date: {r.get('service_date', '')} | "
            f"Payer: {r.get('insurance_payer_name', '')}",
            body_style,
        ))
        story.append(Spacer(1, 0.2 * inch))
        for line in r.get("appeal_letter", "").split("\n"):
            story.append(Paragraph(line or "&nbsp;", appeal_style))

    doc.build(story)
    logger.info("pdf_written", path=str(output_path), appeal_letters=len(appeal_claims))


# ---------------------------------------------------------------------------
# Main Orchestration
# ---------------------------------------------------------------------------

def main(
    input_source: str,
    output_dir: str = "data/batch_output",
    sql_query: str = "SELECT * FROM claims WHERE batch_status = 'pending'",
    dry_run: bool = False,
) -> None:
    import anthropic

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_csv = output_path / f"batch_results_{timestamp}.csv"
    output_pdf = output_path / f"batch_results_{timestamp}.pdf"

    # Determine input filename for reporting
    input_filename = input_source if not input_source.startswith("sql:") else "SQL query"

    logger.info("batch_start", input=input_source, dry_run=dry_run)

    # Step 1: Bootstrap RAG index (must happen before any evaluate_claim calls)
    if not dry_run:
        _bootstrap_rag_index()

    # Step 2: Load claims
    if input_source.startswith("sql:"):
        connection_string = input_source[4:]
        raw_rows = read_sql_claims(connection_string, sql_query)
    else:
        raw_rows = read_csv_claims(Path(input_source))

    logger.info("claims_loaded", count=len(raw_rows))

    # Step 3: Process each claim
    results = []
    fatal_error = False

    for i, raw_row in enumerate(raw_rows):
        claim_id = raw_row.get("claim_id", f"row_{i + 1}")
        result_row = dict(raw_row)

        # Parse and validate
        try:
            claim = parse_csv_row(raw_row)
        except (ValidationError, KeyError, ValueError) as exc:
            error_msg = _format_validation_errors(exc) if isinstance(exc, ValidationError) else str(exc)
            result_row.update({
                "batch_status": "validation_error",
                "error_message": error_msg,
                "retry_count": 0,
            })
            results.append(result_row)
            logger.warning("validation_error", claim_id=claim_id, error=error_msg)
            continue

        if dry_run:
            result_row.update({
                "batch_status": "skipped",
                "error_message": "",
                "retry_count": 0,
            })
            results.append(result_row)
            logger.info("dry_run_skip", claim_id=claim_id)
            continue

        # Evaluate with retry
        try:
            decision, status, retries = process_claim_with_retry(claim)
        except anthropic.AuthenticationError:
            logger.error("fatal_auth_error_aborting_batch")
            fatal_error = True
            break

        if decision:
            result_row.update(_decision_to_dict(decision))
        result_row["batch_status"] = status
        result_row["retry_count"] = retries
        result_row.setdefault("error_message", "")
        results.append(result_row)

        logger.info(
            "claim_processed",
            claim_id=claim_id,
            decision=result_row.get("decision", ""),
            status=status,
            retries=retries,
        )

    if fatal_error:
        logger.error("batch_aborted_due_to_auth_error")

    # Step 4: Write outputs
    write_output_csv(results, output_csv)
    if not dry_run and results:
        write_output_pdf(results, output_pdf, input_filename)

    # Step 5: Print summary
    total = len(results)
    success = sum(1 for r in results if r.get("batch_status") == "success")
    errors = total - success

    print(f"\n{'=' * 60}")
    print(f"  Batch Complete")
    print(f"  Total claims:   {total}")
    print(f"  Successful:     {success}")
    print(f"  Errors/Skipped: {errors}")
    print(f"  Output CSV:     {output_csv}")
    if not dry_run:
        print(f"  Output PDF:     {output_pdf}")
    print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Healthcare Claims RAG Batch Processor"
    )
    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Path to input CSV file, or SQL connection string prefixed with 'sql:'. "
            "Example: 'sql:postgresql://user:pass@host/db'"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="data/batch_output",
        help="Directory for output CSV and PDF (default: data/batch_output)",
    )
    parser.add_argument(
        "--sql-query",
        default="SELECT * FROM claims WHERE batch_status = 'pending'",
        help="SQL query to execute when using SQL input mode",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate claims only — do not call evaluate_claim()",
    )

    args = parser.parse_args()
    main(
        input_source=args.input,
        output_dir=args.output_dir,
        sql_query=args.sql_query,
        dry_run=args.dry_run,
    )
