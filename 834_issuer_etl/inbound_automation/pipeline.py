"""Dry-run pipeline — discover, parse, enrich (no Azure writes)."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from connectors.base_connector import SourceFile
from config.config import settings
from database.loaders import file_hash
from inbound_automation.discovery import discover_for_run
from inbound_automation.enrich import collect_file_warnings, enrich_parser_row
from inbound_automation.filename_utils import parse_filename_year_month
from inbound_automation.run_context import LoadRunContext
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FileProcessResult:
    source: SourceFile
    file_hash: str
    parse_status: str
    row_count: int = 0
    parse_duration_ms: int | None = None
    filename_file_year: int | None = None
    filename_file_month: int | None = None
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class DryRunResult:
    context: LoadRunContext
    file_results: list[FileProcessResult]
    enriched_rows: list[dict[str, Any]]
    completed_at: datetime

    @property
    def files_discovered(self) -> int:
        return len(self.file_results)

    @property
    def files_parsed(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "parsed")

    @property
    def files_failed(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "failed")

    @property
    def rows_parsed(self) -> int:
        return len(self.enriched_rows)

    @property
    def total_warning_count(self) -> int:
        return sum(int(r.get("warning_count") or 0) for r in self.enriched_rows)


def _process_file(
    source: SourceFile,
    *,
    parser: Parser834,
    context: LoadRunContext,
    cli_year: str | None,
    loaded_at: datetime,
) -> FileProcessResult:
    fhash = file_hash(source.file_path)
    fn_year, fn_month = parse_filename_year_month(source.file_name)
    warnings = collect_file_warnings(source, fn_year, fn_month)

    started = time.perf_counter()
    try:
        xml_bytes = read_xml_bytes(source)
        records = parser.parse_file(
            xml_bytes,
            issuer=source.issuer,
            year=source.year,
            month=source.month,
            file_name=source.file_name,
            file_path=str(source.file_path),
        )
        enriched: list[dict[str, Any]] = []
        for idx, parser_row in enumerate(records, start=1):
            enriched.append(
                enrich_parser_row(
                    parser_row,
                    source=source,
                    file_hash=fhash,
                    row_number_in_file=idx,
                    load_run_id=context.load_run_id,
                    loaded_at=loaded_at,
                    cli_year=cli_year,
                    filename_file_year=fn_year,
                    filename_file_month=fn_month,
                    parser_version=context.parser_version,
                    runner_version=context.runner_version,
                    git_commit=context.git_commit,
                    file_warnings=warnings,
                )
            )
        duration_ms = int((time.perf_counter() - started) * 1000)
        return FileProcessResult(
            source=source,
            file_hash=fhash,
            parse_status="parsed",
            row_count=len(enriched),
            parse_duration_ms=duration_ms,
            filename_file_year=fn_year,
            filename_file_month=fn_month,
            warnings=warnings,
            rows=enriched,
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        msg = f"{source.file_path}: {exc}"
        logger.error("Parse failed: %s", msg)
        return FileProcessResult(
            source=source,
            file_hash=fhash,
            parse_status="failed",
            parse_duration_ms=duration_ms,
            filename_file_year=fn_year,
            filename_file_month=fn_month,
            error_message=msg,
            warnings=warnings,
        )


def run_dry_run(context: LoadRunContext) -> DryRunResult:
    """
    Execute Phase 1 dry-run: discover, parse, enrich.

    No Azure connection, no DDL, no database writes.
    """
    settings.refresh_from_env()
    source_root = settings.source_data_path
    cli_year = None if context.all_years else context.year_filter

    sources = discover_for_run(
        source_root,
        year_filter=context.year_filter,
        all_years=context.all_years,
        issuer_filter=context.issuer_filter,
        month_filter=context.month_filter,
    )

    parser = Parser834()
    loaded_at = datetime.now(timezone.utc)
    file_results: list[FileProcessResult] = []
    enriched_rows: list[dict[str, Any]] = []

    for source in sources:
        result = _process_file(
            source,
            parser=parser,
            context=context,
            cli_year=cli_year,
            loaded_at=loaded_at,
        )
        file_results.append(result)
        enriched_rows.extend(result.rows)

    return DryRunResult(
        context=context,
        file_results=file_results,
        enriched_rows=enriched_rows,
        completed_at=datetime.now(timezone.utc),
    )
