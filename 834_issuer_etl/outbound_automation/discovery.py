"""Local discovery of downloaded outbound TO files (outbound source root only)."""

from __future__ import annotations

from pathlib import Path

from connectors.base_connector import SourceFile
from ingestion.file_discovery import discover_source_files
from outbound_automation.config import resolve_outbound_extracted_root
from outbound_automation.file_rules import is_outbound_local_xml
from utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_month(month: str | int) -> str:
    return str(int(month)).zfill(2)


def discover_outbound_files(
    source_root: Path,
    *,
    year_filter: str | None,
    all_years: bool,
    issuer_filter: list[str] | None,
    month_filter: str | None,
) -> list[SourceFile]:
    """
    Discover {issuer}/{year}/{MM}/ XML under the outbound source root.

    Any file that is not a TO file (e.g. a FROM file copied in by mistake) is dropped.
    """
    year = None if all_years else year_filter
    month = _normalize_month(month_filter) if month_filter else None
    extracted_root = resolve_outbound_extracted_root()

    issuers: list[str | None] = list(issuer_filter) if issuer_filter else [None]
    found: list[SourceFile] = []
    for issuer in issuers:
        found.extend(
            discover_source_files(
                source_root,
                issuer_filter=issuer,
                year_filter=year,
                month_filter=month,
                extracted_root=extracted_root,
            )
        )

    kept = [f for f in found if is_outbound_local_xml(f.file_name)]
    rejected = [f for f in found if not is_outbound_local_xml(f.file_name)]
    for f in rejected:
        logger.warning("Ignoring non-TO file in outbound source root: %s", f.file_path)

    logger.info(
        "Outbound discovery: %d TO file(s) kept, %d non-TO ignored "
        "(root=%s issuers=%s year=%s month=%s)",
        len(kept),
        len(rejected),
        source_root,
        ",".join(issuer_filter) if issuer_filter else "ALL",
        year or "ALL",
        month or "ALL",
    )
    return kept
