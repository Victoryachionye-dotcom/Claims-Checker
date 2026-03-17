"""
add_policy.py — Policy Knowledge Base Expansion

Adds a new CMS LCD/NCD PDF to data/policy_kb/ and re-indexes the vector store.
Supports local file paths and direct HTTPS URLs.

Usage:
    python scripts/add_policy.py --source /path/to/LCD_L38672.pdf
    python scripts/add_policy.py --source https://www.cms.gov/...pdf
    python scripts/add_policy.py --source /path/to/policy.pdf --skip-reindex
    python scripts/add_policy.py --reindex-only
"""

import sys
import shutil
import argparse
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings
from app.core.logging import configure_logging, get_logger
from app.rag.indexer import build_index

configure_logging()
logger = get_logger(__name__)


def fetch_pdf_from_url(url: str, dest_dir: Path) -> Path:
    """
    Download a PDF from a URL into dest_dir.
    Derives filename from the URL path component.
    """
    filename = Path(url.split("?")[0]).name  # strip query params
    if not filename.lower().endswith(".pdf"):
        filename = filename + ".pdf"

    dest_path = dest_dir / filename

    logger.info("downloading_pdf", url=url, destination=str(dest_path))
    try:
        urllib.request.urlretrieve(url, dest_path)
    except urllib.error.URLError as exc:
        logger.error("pdf_download_failed", url=url, error=str(exc))
        raise

    logger.info("pdf_downloaded", path=str(dest_path), size_bytes=dest_path.stat().st_size)
    return dest_path


def copy_pdf_from_local(source_path: Path, dest_dir: Path) -> Path:
    """
    Copy a local PDF file into dest_dir.
    """
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")
    if source_path.suffix.lower() != ".pdf":
        raise ValueError(f"Expected a .pdf file, got: {source_path.suffix}")

    dest_path = dest_dir / source_path.name
    if dest_path.exists():
        logger.warning("pdf_already_exists_overwriting", path=str(dest_path))

    shutil.copy2(source_path, dest_path)
    logger.info("pdf_copied", source=str(source_path), destination=str(dest_path))
    return dest_path


def main(
    source: str = "",
    skip_reindex: bool = False,
    reindex_only: bool = False,
) -> None:
    kb_dir = Path(settings.policy_kb_dir)
    kb_dir.mkdir(parents=True, exist_ok=True)

    before_count = len(list(kb_dir.glob("*.pdf")))

    if not reindex_only:
        if not source:
            logger.error("no_source_provided", action="Pass --source or use --reindex-only")
            sys.exit(1)

        if source.startswith("http://") or source.startswith("https://"):
            added_path = fetch_pdf_from_url(source, kb_dir)
        else:
            added_path = copy_pdf_from_local(Path(source), kb_dir)

        print(f"\nAdded: {added_path.name}")

    after_count = len(list(kb_dir.glob("*.pdf")))

    if not skip_reindex:
        print(f"Re-indexing {after_count} PDF(s) in {kb_dir} ...")
        logger.info("reindexing_policy_kb", pdf_count=after_count)
        try:
            build_index()
            logger.info("reindex_complete")
            print("Index updated successfully.\n")
        except Exception as exc:
            logger.error("reindex_failed", error=str(exc))
            print(f"\nReindex failed: {exc}")
            sys.exit(1)
    else:
        print("--skip-reindex set: index not updated. Run with --reindex-only when ready.\n")

    print(f"Policy KB summary:")
    print(f"  PDFs before: {before_count}")
    print(f"  PDFs after:  {after_count}")
    for pdf in sorted(kb_dir.glob("*.pdf")):
        print(f"    • {pdf.name}")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Add a CMS LCD/NCD PDF to the policy knowledge base and re-index."
    )
    parser.add_argument(
        "--source",
        default="",
        help="Local file path or HTTPS URL to the PDF to add.",
    )
    parser.add_argument(
        "--skip-reindex",
        action="store_true",
        help="Copy/download the PDF but do not re-run indexing (useful for batching additions).",
    )
    parser.add_argument(
        "--reindex-only",
        action="store_true",
        help="Skip file addition and only re-index all existing PDFs in policy_kb/.",
    )

    args = parser.parse_args()

    if not args.reindex_only and not args.source:
        parser.error("--source is required unless using --reindex-only")

    main(
        source=args.source,
        skip_reindex=args.skip_reindex,
        reindex_only=args.reindex_only,
    )
