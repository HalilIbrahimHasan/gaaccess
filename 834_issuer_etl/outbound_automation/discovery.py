"""Local discovery of downloaded outbound TO files (outbound source root only)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from connectors.base_connector import SourceFile
from ingestion.file_discovery import discover_source_files
from outbound_automation.config import resolve_outbound_extracted_root
from outbound_automation.file_rules import classify_outbound_filename
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class OutboundDiscovery:
    kept: list[SourceFile] = field(default_factory=list)
    rejected: list[tuple[SourceFile, str]] = field(default_factory=list)


def _normalize_month(month: str | int) -> str:
    return str(int(month)).zfill(2)


def discover_outbound_candidates(
    source_root: Path,
    *,
    year_filter: str | None,
    all_years: bool,
    issuer_filter: list[str] | None,
    month_filter: str | None,
    exchange_codes: frozenset[str] | None = None,
) -> OutboundDiscovery:
    """
    Discover {issuer}/{year}/{MM}/ XML under the outbound source root.

    Only standard TO files are kept. Everything else (FROM files, non-standard
    TO names, wrong exchange code) is returned in `rejected` with its class.
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

    result = OutboundDiscovery()
    for f in found:
        cls = classify_outbound_filename(f.file_name, exchange_codes=exchange_codes)
        if cls == "valid_xml":
            result.kept.append(f)
        else:
            result.rejected.append((f, cls))
            logger.warning("Outbound discovery: not loading %s (class=%s)", f.file_path, cls)

    logger.info(
        "Outbound discovery: %d TO file(s) kept, %d rejected "
        "(root=%s issuers=%s year=%s month=%s)",
        len(result.kept),
        len(result.rejected),
        source_root,
        ",".join(issuer_filter) if issuer_filter else "ALL",
        year or "ALL",
        month or "ALL",
    )
    return result


def discover_outbound_files(
    source_root: Path,
    *,
    year_filter: str | None,
    all_years: bool,
    issuer_filter: list[str] | None,
    month_filter: str | None,
    exchange_codes: frozenset[str] | None = None,
) -> list[SourceFile]:
    return discover_outbound_candidates(
        source_root,
        year_filter=year_filter,
        all_years=all_years,
        issuer_filter=issuer_filter,
        month_filter=month_filter,
        exchange_codes=exchange_codes,
    ).kept
