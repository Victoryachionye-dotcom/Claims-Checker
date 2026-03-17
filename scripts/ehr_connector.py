"""
ehr_connector.py — EHR Data Ingestion Connector

Pulls claims from an EHR system using one of three methods:
  1. FHIR R4 API  (Epic, Cerner, Athenahealth — any SMART on FHIR compliant EHR)
  2. Direct SQL   (on-premise billing DB or EHR database)
  3. SFTP CSV     (legacy EHR nightly file drop)

Returns a list of flat row dicts matching the batch_processor CSV input schema
so they can be passed directly to batch_processor.parse_csv_row().

Usage (called by nightly_pipeline.py — not typically run directly):
    from scripts.ehr_connector import pull_fhir_claims, pull_sql_claims, pull_sftp_claims
"""

import sys
import csv
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# FHIR R4 API Connector
# ---------------------------------------------------------------------------

def pull_fhir_claims(
    fhir_base_url: str,
    access_token: str,
    since_hours: int = 24,
    max_count: int = 500,
) -> list[dict]:
    """
    Pull FHIR R4 Claim resources updated in the last `since_hours` hours.

    Args:
        fhir_base_url: e.g. "https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4"
        access_token:  OAuth 2.0 / SMART on FHIR bearer token
        since_hours:   lookback window (default 24 hours = nightly pull)
        max_count:     max claims per request (paged if more)

    Returns:
        List of flat row dicts matching batch_processor CSV input schema.

    FHIR Claim resource → CSV schema mapping:
        Claim.id                          → claim_id
        Claim.use                         → claim_type (institutional=inpatient, professional=outpatient)
        Claim.billablePeriod.start        → service_date
        Claim.total.value                 → total_amount
        Claim.preAuthRef[0]               → prior_authorization_number
        Claim.patient.reference           → patient lookup
        Claim.provider.reference          → provider lookup
        Claim.insurance[0].coverage       → insurance lookup
        Claim.diagnosis[*]                → diagnoses
        Claim.item[*]                     → procedures
    """
    try:
        import httpx
    except ImportError:
        logger.error("httpx_not_installed", action="pip install httpx")
        sys.exit(1)

    since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/fhir+json",
    }

    all_claims = []
    url = f"{fhir_base_url}/Claim"
    params = {"_lastUpdated": f"ge{since}", "_count": max_count}

    while url:
        logger.info("fhir_fetching_page", url=url)
        response = httpx.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        bundle = response.json()

        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            if resource.get("resourceType") == "Claim":
                row = _fhir_claim_to_row(resource)
                if row:
                    all_claims.append(row)

        # Follow FHIR pagination (next link)
        next_url = next(
            (link["url"] for link in bundle.get("link", []) if link.get("relation") == "next"),
            None,
        )
        url = next_url
        params = {}  # next URL already includes params

    logger.info("fhir_pull_complete", claims_fetched=len(all_claims))
    return all_claims


def _fhir_claim_to_row(resource: dict) -> Optional[dict]:
    """Map a FHIR R4 Claim resource dict to the batch_processor flat row schema."""
    try:
        claim_type_raw = resource.get("use", "professional")
        claim_type = "inpatient" if claim_type_raw == "institutional" else "outpatient"

        # Billable period
        period = resource.get("billablePeriod", {})
        service_date = period.get("start", "")[:10]  # YYYY-MM-DD

        # Total
        total = resource.get("total", {}).get("value", 0.0)

        # Prior auth
        insurance_list = resource.get("insurance", [{}])
        prior_auth_refs = insurance_list[0].get("preAuthRef", []) if insurance_list else []
        prior_auth = prior_auth_refs[0] if prior_auth_refs else ""

        # Patient (reference only — full lookup would need additional FHIR calls)
        patient_ref = resource.get("patient", {}).get("reference", "")
        patient_id = patient_ref.split("/")[-1] if patient_ref else ""

        # Provider
        provider_ref = resource.get("provider", {}).get("reference", "")
        provider_id = provider_ref.split("/")[-1] if provider_ref else ""

        # Diagnoses (up to 3)
        diagnoses = resource.get("diagnosis", [])
        diag_rows = {}
        for i, d in enumerate(diagnoses[:3], start=1):
            coding = d.get("diagnosisCodeableConcept", {}).get("coding", [{}])[0]
            diag_rows[f"diagnosis_{i}_code"] = coding.get("code", "")
            diag_rows[f"diagnosis_{i}_description"] = coding.get("display", "")

        # Procedures (items)
        items = resource.get("item", [])
        proc_rows = {}
        for i, item in enumerate(items[:3], start=1):
            coding = item.get("productOrService", {}).get("coding", [{}])[0]
            proc_rows[f"procedure_{i}_code"] = coding.get("code", "")
            proc_rows[f"procedure_{i}_description"] = coding.get("display", "")
            proc_rows[f"procedure_{i}_quantity"] = str(item.get("quantity", {}).get("value", 1))
            unit_price = item.get("unitPrice", {}).get("value", 0.0)
            proc_rows[f"procedure_{i}_unit_price"] = str(unit_price)

        row = {
            "claim_id": resource.get("id", ""),
            "claim_type": claim_type,
            "service_date": service_date,
            "total_amount": str(total),
            "prior_authorization_number": prior_auth,
            "patient_id": patient_id,
            "patient_name": "",          # Requires Patient resource lookup
            "patient_dob": "",           # Requires Patient resource lookup
            "patient_member_id": patient_id,
            "provider_npi": provider_id,  # Requires Practitioner resource lookup for NPI
            "provider_name": "",
            "provider_specialty": "",
            "insurance_payer_id": "",
            "insurance_payer_name": "",
            "insurance_plan_id": "",
            "insurance_group_number": "",
            "insurance_member_id": patient_id,
            "clinical_notes": "",
            **diag_rows,
            **proc_rows,
        }

        # Fill in missing diagnosis/procedure columns with blanks
        for n in range(1, 4):
            row.setdefault(f"diagnosis_{n}_code", "")
            row.setdefault(f"diagnosis_{n}_description", "")
            row.setdefault(f"procedure_{n}_code", "")
            row.setdefault(f"procedure_{n}_description", "")
            row.setdefault(f"procedure_{n}_quantity", "")
            row.setdefault(f"procedure_{n}_unit_price", "")

        return row

    except Exception as exc:
        logger.warning("fhir_claim_mapping_failed", claim_id=resource.get("id"), error=str(exc))
        return None


# ---------------------------------------------------------------------------
# SQL Connector
# ---------------------------------------------------------------------------

def pull_sql_claims(
    connection_string: str,
    query: str = "SELECT * FROM claims WHERE submission_status = 'pending' AND created_date >= CURRENT_DATE - INTERVAL '1 day'",
) -> list[dict]:
    """
    Pull pending claims from a SQL database.

    Args:
        connection_string: SQLAlchemy DSN.
            PostgreSQL: "postgresql://user:pass@host:5432/db"
            MySQL:      "mysql+pymysql://user:pass@host:3306/db"
            SQLite:     "sqlite:///data/claims.db"
            SQL Server: "mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"
        query: SQL query returning rows whose columns match the CSV input schema.

    The SQL table should have columns matching batch_processor INPUT_COLUMNS.
    See the DDL in the project plan for the full schema.
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        logger.error("sqlalchemy_not_installed", action="pip install sqlalchemy")
        sys.exit(1)

    logger.info("sql_connecting", dsn=connection_string.split("@")[-1])  # log host only, not creds
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        result = conn.execute(text(query))
        rows = [dict(row._mapping) for row in result]

    logger.info("sql_pull_complete", claims_fetched=len(rows))
    return rows


# ---------------------------------------------------------------------------
# SFTP CSV Connector
# ---------------------------------------------------------------------------

def pull_sftp_claims(
    host: str,
    username: str,
    password: str = "",
    private_key_path: str = "",
    remote_dir: str = "/exports/claims",
    filename_pattern: str = "claims_*.csv",
    local_staging_dir: str = "data/batch_input",
) -> list[dict]:
    """
    Download the latest claims CSV from an SFTP server.
    Uses paramiko for SSH/SFTP access.

    Args:
        host:              SFTP hostname or IP
        username:          SFTP username
        password:          Password (leave blank if using key-based auth)
        private_key_path:  Path to SSH private key file (alternative to password)
        remote_dir:        Remote directory containing the CSV exports
        filename_pattern:  Glob pattern to match the nightly export file
        local_staging_dir: Local directory to download the file into

    Returns:
        List of flat row dicts from the downloaded CSV.
    """
    try:
        import paramiko
        import fnmatch
    except ImportError:
        logger.error("paramiko_not_installed", action="pip install paramiko")
        sys.exit(1)

    staging = Path(local_staging_dir)
    staging.mkdir(parents=True, exist_ok=True)

    transport = paramiko.Transport((host, 22))

    if private_key_path:
        key = paramiko.RSAKey.from_private_key_file(private_key_path)
        transport.connect(username=username, pkey=key)
    else:
        transport.connect(username=username, password=password)

    sftp = paramiko.SFTPClient.from_transport(transport)

    # Find the most recent matching file
    remote_files = sftp.listdir(remote_dir)
    matching = [f for f in remote_files if fnmatch.fnmatch(f, filename_pattern)]
    if not matching:
        raise FileNotFoundError(
            f"No files matching '{filename_pattern}' found in {remote_dir}"
        )

    latest_file = sorted(matching)[-1]  # lexicographic sort — assumes YYYYMMDD in filename
    remote_path = f"{remote_dir}/{latest_file}"
    local_path = staging / latest_file

    logger.info("sftp_downloading", remote=remote_path, local=str(local_path))
    sftp.get(remote_path, str(local_path))
    sftp.close()
    transport.close()

    logger.info("sftp_download_complete", file=latest_file)

    with open(local_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    logger.info("sftp_claims_loaded", count=len(rows))
    return rows
