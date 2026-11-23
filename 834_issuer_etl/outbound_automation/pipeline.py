"""
Outbound automation pipeline — discover TO files, parse (Parser834Outbound),
enrich (outbound_automation.enrich), optional atomic per-file Azure load.

File failure policy (whole file, nothing loaded, file_log 'failed' => retryable):
malformed XML, root not 'enrollments', zero enrollees, missing member/policy
identity, issuer attribution conflict, parent/child integrity failure,
DB transaction failure.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from connectors.base_connector import SourceFile
from database.loaders import file_hash
from ingestion.xml_reader import read_xml_bytes
from outbound_automation.config import (
    load_sensitive_local_names,
    resolve_exchange_codes,
    resolve_source_json_policy,
)
from outbound_automation.discovery import discover_outbound_candidates
from outbound_automation.enrich import (
    FileAttribution,
    OutboundFileFatalError,
    RunStamp,
    build_outbound_file,
    check_issuer_attribution,
)
from outbound_automation.file_rules import parse_to_filename
from outbound_automation.integrity import ParentChildIntegrityError, validate_parent_child
from outbound_automation.load_metrics import FileInsertMetrics
from outbound_automation.mappings import load_insurance_type_map
from outbound_automation.run_context import OutboundRunContext
from outbound_automation.schema_observation import KnownPathRegistry, SchemaObservationCollector
from outbound_automation.source_json import SourceJsonPolicy
from parsers.parser_834_outbound import OutboundParseError, Parser834Outbound, mapped_paths
from utils.logger import get_logger

logger = get_logger(__name__)

ZERO_ROWS_MESSAGE = "No <enrollee> elements found under enrollments/GS_Info/enrollment."


@dataclass
class FileProcessResult:
    source: SourceFile
    file_hash: str
    parse_status: str
    row_count: int = 0
    premium_row_count: int = 0
    unknown_path_count: int | None = None
    parse_duration_ms: int | None = None
    filename_file_year: int | None = None
    filename_file_month: int | None = None
    txn_create_datetime: datetime | None = None
    job_execution_id: str | None = None
    error_message: str | None = None
    prior_load_run_id: str | None = None
    file_warnings: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    premium_rows: list[dict[str, Any]] = field(default_factory=list)
    insert_metrics: FileInsertMetrics | None = None


@dataclass
class RunResult:
    context: OutboundRunContext
    file_results: list[FileProcessResult]
    rejected_files: list[tuple[SourceFile, str]]
    observations: SchemaObservationCollector
    pii_policy: str
    completed_at: datetime
    rows_inserted: int = 0
    premium_rows_inserted: int = 0

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
    def rows(self) -> list[dict[str, Any]]:
        return [r for f in self.file_results if f.parse_status in ("parsed", "loaded") for r in f.rows]

    @property
    def rows_parsed(self) -> int:
        return sum(len(f.rows) for f in self.file_results if f.parse_status in ("parsed", "loaded"))

    @property
    def premium_rows_parsed(self) -> int:
        return sum(
            len(f.premium_rows) for f in self.file_results if f.parse_status in ("parsed", "loaded")
        )

    @property
    def total_warning_count(self) -> int:
        row_warnings = sum(int(r.get("warning_count") or 0) for r in self.rows)
        return row_warnings + sum(len(f.file_warnings) for f in self.file_results)

    @property
    def unknown_path_count(self) -> int:
        return self.observations.unknown_path_count


@dataclass
class _Runtime:
    parser: Parser834Outbound
    policy: SourceJsonPolicy
    insurance_map: dict[str, str]
    collector: SchemaObservationCollector
    exchange_codes: frozenset[str]


def _new_runtime(policy: SourceJsonPolicy) -> _Runtime:
    return _Runtime(
        parser=Parser834Outbound(),
        policy=policy,
        insurance_map=load_insurance_type_map(),
        collector=SchemaObservationCollector(
            KnownPathRegistry.load(), mapped_paths(), load_sensitive_local_names(),
        ),
        exchange_codes=resolve_exchange_codes(),
    )


def _fail(result: FileProcessResult, message: str, started: float) -> FileProcessResult:
    result.parse_status = "failed"
    result.error_message = f"{result.source.file_name}: {message}"
    result.parse_duration_ms = int((time.perf_counter() - started) * 1000)
    result.rows = []
    result.premium_rows = []
    logger.error("Outbound file failed: %s", result.error_message)
    return result


def _process_file(
    source: SourceFile,
    *,
    fhash: str,
    runtime: _Runtime,
    context: OutboundRunContext,
    loaded_at: datetime,
) -> FileProcessResult:
    name = parse_to_filename(source.file_name, exchange_codes=runtime.exchange_codes)
    result = FileProcessResult(
        source=source,
        file_hash=fhash,
        parse_status="failed",
        filename_file_year=name.file_year if name else None,
        filename_file_month=name.file_month if name else None,
    )
    started = time.perf_counter()
    if name is None:
        return _fail(result, "filename is not a standard TO 834 filename", started)

    try:
        parsed = runtime.parser.parse_bytes(read_xml_bytes(source))
    except OutboundParseError as exc:
        return _fail(result, str(exc), started)
    except OSError as exc:
        return _fail(result, f"cannot read file ({type(exc).__name__})", started)

    result.job_execution_id = parsed.header.get("job_execution_id")
    try:
        result.file_warnings.extend(
            check_issuer_attribution(
                folder_issuer=source.issuer,
                filename_issuer=name.issuer,
                header_issuer=parsed.header.get("hios_issuer_id"),
            )
        )
    except OutboundFileFatalError as exc:
        return _fail(result, str(exc), started)

    if not parsed.enrollees:
        return _fail(result, ZERO_ROWS_MESSAGE, started)

    structure = runtime.collector.observe_file(source.file_name, parsed)
    result.unknown_path_count = structure.unknown_path_count

    try:
        enriched = build_outbound_file(
            parsed,
            attribution=FileAttribution(
                issuer=source.issuer,
                folder_year=int(source.year),
                folder_month=int(source.month),
                filename_file_year=name.file_year,
                filename_file_month=name.file_month,
                source_file=source.file_name,
                source_file_path=str(source.file_path),
                file_hash=fhash,
            ),
            stamp=RunStamp(
                load_run_id=context.load_run_id,
                loaded_at=loaded_at,
                parser_version=context.parser_version,
                runner_version=context.runner_version,
                git_commit=context.git_commit,
            ),
            policy=runtime.policy,
            insurance_map=runtime.insurance_map,
            structure=structure,
            collector=runtime.collector,
        )
        validate_parent_child(fhash, enriched.rows, enriched.premium_rows)
    except OutboundFileFatalError as exc:
        return _fail(result, str(exc), started)
    except ParentChildIntegrityError as exc:
        return _fail(result, f"parent/child integrity: {exc}", started)

    result.file_warnings.extend(enriched.file_warnings)
    result.txn_create_datetime = enriched.txn_create_datetime
    result.job_execution_id = enriched.job_execution_id
    result.rows = enriched.rows
    result.premium_rows = enriched.premium_rows
    result.row_count = len(enriched.rows)
    result.premium_row_count = len(enriched.premium_rows)
    result.parse_status = "parsed"
    result.parse_duration_ms = int((time.perf_counter() - started) * 1000)
    return result


def _discover(context: OutboundRunContext, exchange_codes: frozenset[str]):
    return discover_outbound_candidates(
        context.source_root,
        year_filter=context.year_filter,
        all_years=context.all_years,
        issuer_filter=context.issuer_filter,
        month_filter=context.month_filter,
        exchange_codes=exchange_codes,
    )


def _release_payloads(result: FileProcessResult) -> None:
    """Drop JSON payloads once a file is done; reports never use them."""
    for row in result.rows:
        row.pop("source_xml_json", None)
    for row in result.premium_rows:
        row.pop("premium_json", None)


def _skipped(source: SourceFile, fhash: str, prior: str) -> FileProcessResult:
    return FileProcessResult(
        source=source, file_hash=fhash, parse_status="skipped_duplicate", prior_load_run_id=prior,
    )


def run_dry_run(
    context: OutboundRunContext, *, policy: SourceJsonPolicy | None = None,
) -> RunResult:
    """Discover, parse, enrich, validate. No Azure connection, no database writes."""
    runtime = _new_runtime(policy or resolve_source_json_policy(required=False))
    loaded_at = datetime.now(timezone.utc)
    discovery = _discover(context, runtime.exchange_codes)
    file_results: list[FileProcessResult] = []
    seen: dict[str, str] = {}

    for source in discovery.kept:
        fhash = file_hash(source.file_path)
        if fhash in seen:
            file_results.append(_skipped(source, fhash, f"same run ({seen[fhash]})"))
            continue
        seen[fhash] = source.file_name
        result = _process_file(
            source, fhash=fhash, runtime=runtime, context=context, loaded_at=loaded_at,
        )
        _release_payloads(result)
        file_results.append(result)

    return RunResult(
        context=context,
        file_results=file_results,
        rejected_files=discovery.rejected,
        observations=runtime.collector,
        pii_policy=runtime.policy.name,
        completed_at=datetime.now(timezone.utc),
    )


def run_load(
    context: OutboundRunContext, *, policy: SourceJsonPolicy | None = None,
) -> RunResult:
    """
    Discover, parse, enrich, then load each file in its own transaction.

    Writes only to dbo.outbound_automation, dbo.outbound_automation_monthly_premium,
    dbo.outbound_automation_run_log and dbo.outbound_automation_file_log.
    """
    runtime = _new_runtime(policy or resolve_source_json_policy(required=True))

    from outbound_automation.azure_common import (
        connect_outbound_engine,
        fast_executemany_enabled,
        require_env_gate,
    )
    from outbound_automation.azure_writer import (
        fetch_loaded_file_hashes,
        insert_run_log_start,
        load_file_rows,
        sanitize_db_error,
        update_run_log_finish,
        upsert_file_log,
        verify_tables_exist,
        verify_writer_columns,
    )

    require_env_gate("load")
    engine = connect_outbound_engine()
    use_fast_executemany = fast_executemany_enabled()
    verify_tables_exist(engine)
    verify_writer_columns(engine)

    loaded_hashes = fetch_loaded_file_hashes(engine)
    logger.info("Found %d previously loaded outbound file_hash value(s)", len(loaded_hashes))

    insert_run_log_start(engine, context)

    loaded_at = datetime.now(timezone.utc)
    file_results: list[FileProcessResult] = []
    rejected: list[tuple[SourceFile, str]] = []
    rows_inserted = 0
    premium_rows_inserted = 0
    run_error: str | None = None

    try:
        discovery = _discover(context, runtime.exchange_codes)
        rejected = discovery.rejected
        for source in discovery.kept:
            fhash = file_hash(source.file_path)
            if fhash in loaded_hashes:
                file_results.append(_skipped(source, fhash, loaded_hashes[fhash]))
                continue

            result = _process_file(
                source, fhash=fhash, runtime=runtime, context=context, loaded_at=loaded_at,
            )
            if result.parse_status == "parsed":
                try:
                    inserted, premium_inserted, metrics = load_file_rows(
                        engine, context, result, loaded_at=loaded_at,
                        fast_executemany=use_fast_executemany,
                    )
                    result.insert_metrics = metrics
                    rows_inserted += inserted
                    premium_rows_inserted += premium_inserted
                    loaded_hashes[fhash] = context.load_run_id
                except Exception as exc:
                    result.parse_status = "failed"
                    result.error_message = (
                        f"{source.file_name}: load failed, file transaction rolled back: "
                        f"{sanitize_db_error(exc)}"
                    )
                    logger.error("%s", result.error_message)

            if result.parse_status == "failed":
                try:
                    upsert_file_log(engine, context, result, loaded_at=loaded_at)
                except Exception as log_exc:
                    logger.error(
                        "Could not write file_log for %s: %s",
                        source.file_name, sanitize_db_error(log_exc),
                    )
            _release_payloads(result)
            file_results.append(result)

    except Exception as exc:
        run_error = sanitize_db_error(exc)
        logger.error("Outbound load run failed: %s", run_error)
    finally:
        completed_at = datetime.now(timezone.utc)
        loaded_rows = [r for f in file_results if f.parse_status == "loaded" for r in f.rows]
        parsed_rows = sum(len(f.rows) for f in file_results)
        stats = {
            "files_discovered": len(file_results),
            "files_parsed": sum(1 for f in file_results if f.parse_status != "skipped_duplicate"),
            "files_loaded": sum(1 for f in file_results if f.parse_status == "loaded"),
            "files_skipped_duplicate": sum(
                1 for f in file_results if f.parse_status == "skipped_duplicate"
            ),
            "files_failed": sum(1 for f in file_results if f.parse_status == "failed"),
            "rows_parsed": parsed_rows,
            "rows_inserted": rows_inserted,
            "rows_skipped": 0,
            "premium_rows_inserted": premium_rows_inserted,
            "total_warning_count": sum(int(r.get("warning_count") or 0) for r in loaded_rows),
            "unknown_path_count": runtime.collector.unknown_path_count,
        }
        try:
            update_run_log_finish(
                engine, context, completed_at=completed_at, stats=stats,
                status="failed" if run_error else "success", error_summary=run_error,
            )
        except Exception as log_exc:
            logger.error("Could not update outbound run_log: %s", sanitize_db_error(log_exc))

    return RunResult(
        context=context,
        file_results=file_results,
        rejected_files=rejected,
        observations=runtime.collector,
        pii_policy=runtime.policy.name,
        completed_at=datetime.now(timezone.utc),
        rows_inserted=rows_inserted,
        premium_rows_inserted=premium_rows_inserted,
    )
