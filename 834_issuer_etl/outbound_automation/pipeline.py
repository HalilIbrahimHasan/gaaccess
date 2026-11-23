"""Outbound automation pipeline — discover TO files, parse, enrich, optional Azure load."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from connectors.base_connector import SourceFile
from database.loaders import file_hash
from inbound_automation.enrich import collect_file_warnings, enrich_parser_row
from inbound_automation.filename_utils import parse_filename_year_month
from inbound_automation.load_metrics import FileInsertMetrics
from ingestion.xml_reader import read_xml_bytes
from outbound_automation.discovery import discover_outbound_files
from outbound_automation.run_context import OutboundRunContext
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)

# Parser834 only reads direct <enrollment>/<enrollee> children. A TO file with a
# different root layout parses to 0 rows without raising, so such files are logged
# as failed (retried next run) instead of loaded (permanently skipped by file_hash).
ZERO_ROWS_MESSAGE = (
    "Parser834 returned 0 enrollee rows — outbound XML structure not confirmed "
    "compatible (expected root/enrollment/enrollee)."
)


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
    prior_load_run_id: str | None = None
    warnings: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    insert_metrics: FileInsertMetrics | None = None


@dataclass
class RunResult:
    context: OutboundRunContext
    file_results: list[FileProcessResult]
    enriched_rows: list[dict[str, Any]]
    completed_at: datetime
    rows_inserted: int = 0

    @property
    def files_discovered(self) -> int:
        return len(self.file_results)

    @property
    def files_parsed(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status in ("parsed", "loaded"))

    @property
    def files_loaded(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "loaded")

    @property
    def files_skipped_duplicate(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "skipped_duplicate")

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
    context: OutboundRunContext,
    cli_year: str | None,
    loaded_at: datetime,
) -> FileProcessResult:
    fhash = file_hash(source.file_path)
    fn_year, fn_month = parse_filename_year_month(source.file_name)
    warnings = collect_file_warnings(source, fn_year, fn_month)

    started = time.perf_counter()
    try:
        records = parser.parse_file(
            read_xml_bytes(source),
            issuer=source.issuer,
            year=source.year,
            month=source.month,
            file_name=source.file_name,
            file_path=str(source.file_path),
        )
    except Exception as exc:
        msg = f"{source.file_path}: {exc}"
        logger.error("Parse failed: %s", msg)
        return FileProcessResult(
            source=source,
            file_hash=fhash,
            parse_status="failed",
            parse_duration_ms=int((time.perf_counter() - started) * 1000),
            filename_file_year=fn_year,
            filename_file_month=fn_month,
            error_message=msg,
            warnings=warnings,
        )

    duration_ms = int((time.perf_counter() - started) * 1000)
    if not records:
        logger.warning("%s: %s", source.file_name, ZERO_ROWS_MESSAGE)
        return FileProcessResult(
            source=source,
            file_hash=fhash,
            parse_status="failed",
            parse_duration_ms=duration_ms,
            filename_file_year=fn_year,
            filename_file_month=fn_month,
            error_message=f"{source.file_path}: {ZERO_ROWS_MESSAGE}",
            warnings=warnings,
        )

    enriched = [
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
        for idx, parser_row in enumerate(records, start=1)
    ]
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


def _discover(context: OutboundRunContext) -> list[SourceFile]:
    return discover_outbound_files(
        context.source_root,
        year_filter=context.year_filter,
        all_years=context.all_years,
        issuer_filter=context.issuer_filter,
        month_filter=context.month_filter,
    )


def run_dry_run(context: OutboundRunContext) -> RunResult:
    """Discover, parse, enrich. No Azure connection, no database writes."""
    cli_year = None if context.all_years else context.year_filter
    parser = Parser834()
    loaded_at = datetime.now(timezone.utc)
    file_results: list[FileProcessResult] = []
    enriched_rows: list[dict[str, Any]] = []

    for source in _discover(context):
        result = _process_file(
            source, parser=parser, context=context, cli_year=cli_year, loaded_at=loaded_at,
        )
        file_results.append(result)
        enriched_rows.extend(result.rows)

    return RunResult(
        context=context,
        file_results=file_results,
        enriched_rows=enriched_rows,
        completed_at=datetime.now(timezone.utc),
    )


def run_load(context: OutboundRunContext) -> RunResult:
    """
    Discover, parse, enrich, insert.

    Writes only to outbound_automation, outbound_automation_run_log and
    outbound_automation_file_log.
    """
    from outbound_automation.azure_common import (
        connect_outbound_engine,
        fast_executemany_enabled,
        require_env_gate,
    )
    from outbound_automation.azure_writer import (
        fetch_loaded_file_hashes,
        insert_run_log_start,
        load_file_rows,
        update_run_log_finish,
        upsert_file_log,
        verify_tables_exist,
    )

    require_env_gate("load")
    engine = connect_outbound_engine()
    use_fast_executemany = fast_executemany_enabled()
    verify_tables_exist(engine)

    loaded_hashes = fetch_loaded_file_hashes(engine)
    logger.info("Found %d previously loaded outbound file_hash value(s)", len(loaded_hashes))

    insert_run_log_start(engine, context)

    cli_year = None if context.all_years else context.year_filter
    parser = Parser834()
    loaded_at = datetime.now(timezone.utc)
    file_results: list[FileProcessResult] = []
    enriched_rows: list[dict[str, Any]] = []
    rows_inserted = 0
    run_error: str | None = None

    try:
        for source in _discover(context):
            fhash = file_hash(source.file_path)
            if fhash in loaded_hashes:
                file_results.append(
                    FileProcessResult(
                        source=source,
                        file_hash=fhash,
                        parse_status="skipped_duplicate",
                        prior_load_run_id=loaded_hashes[fhash],
                    )
                )
                continue

            result = _process_file(
                source, parser=parser, context=context, cli_year=cli_year, loaded_at=loaded_at,
            )

            if result.parse_status == "parsed":
                try:
                    inserted, metrics = load_file_rows(
                        engine, context, result, loaded_at=loaded_at,
                        fast_executemany=use_fast_executemany,
                    )
                    result.insert_metrics = metrics
                    rows_inserted += inserted
                    enriched_rows.extend(result.rows)
                    loaded_hashes[fhash] = context.load_run_id
                except Exception as exc:
                    result.parse_status = "failed"
                    result.error_message = f"{source.file_path}: Azure insert failed: {exc}"
                    logger.error("Azure insert failed for %s: %s", source.file_name, exc)
                    try:
                        upsert_file_log(engine, context, result, loaded_at=loaded_at)
                    except Exception as log_exc:
                        logger.error("Could not write file_log for %s: %s", source.file_name, log_exc)
            elif result.parse_status == "failed":
                try:
                    upsert_file_log(engine, context, result, loaded_at=loaded_at)
                except Exception as log_exc:
                    logger.error("Could not write file_log for %s: %s", source.file_name, log_exc)

            file_results.append(result)

    except Exception as exc:
        run_error = str(exc)
        logger.exception("Outbound load run failed: %s", exc)
    finally:
        completed_at = datetime.now(timezone.utc)
        stats = {
            "files_discovered": len(file_results),
            "files_parsed": sum(1 for f in file_results if f.parse_status != "skipped_duplicate"),
            "files_loaded": sum(1 for f in file_results if f.parse_status == "loaded"),
            "files_skipped_duplicate": sum(
                1 for f in file_results if f.parse_status == "skipped_duplicate"
            ),
            "files_failed": sum(1 for f in file_results if f.parse_status == "failed"),
            "rows_parsed": len(enriched_rows),
            "rows_inserted": rows_inserted,
            "rows_skipped": sum(
                r.row_count for r in file_results if r.parse_status == "skipped_duplicate"
            ),
            "total_warning_count": sum(int(r.get("warning_count") or 0) for r in enriched_rows),
        }
        try:
            update_run_log_finish(
                engine,
                context,
                completed_at=completed_at,
                stats=stats,
                status="failed" if run_error else "success",
                error_summary=run_error,
            )
        except Exception as log_exc:
            logger.error("Could not update outbound run_log: %s", log_exc)

    return RunResult(
        context=context,
        file_results=file_results,
        enriched_rows=enriched_rows,
        completed_at=datetime.now(timezone.utc),
        rows_inserted=rows_inserted,
    )
