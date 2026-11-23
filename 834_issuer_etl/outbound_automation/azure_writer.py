"""
Azure writer for outbound automation.

Targets ONLY dbo.outbound_automation, dbo.outbound_automation_run_log and
dbo.outbound_automation_file_log. Column lists mirror sql/outbound_automation_ddl.sql.
"""

from __future__ import annotations

import statistics
import time
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from inbound_automation.load_metrics import FileInsertMetrics
from outbound_automation.azure_common import batch_size
from outbound_automation.constants import OUTBOUND_TABLES, SCHEMA
from utils.logger import get_logger

if TYPE_CHECKING:
    from outbound_automation.pipeline import FileProcessResult
    from outbound_automation.run_context import OutboundRunContext

logger = get_logger(__name__)

ALLOWED_TABLES = frozenset(OUTBOUND_TABLES)


def _assert_outbound_target(table: str) -> str:
    if table not in ALLOWED_TABLES or not table.startswith("outbound_automation"):
        raise RuntimeError(f"Refusing to write to non-outbound table: {SCHEMA}.{table}")
    return table


OUTBOUND_AUTOMATION_COLUMNS = [
    "load_run_id",
    "loaded_at",
    "folder_year",
    "folder_month",
    "filename_file_year",
    "filename_file_month",
    "source_file",
    "source_file_path",
    "file_hash",
    "row_number_in_file",
    "raw_record_hash",
    "parser_version",
    "runner_version",
    "git_commit",
    "coverage_year",
    "coverage_year_source",
    "warning_count",
    "insurance_type",
    "enrolleeStatus",
    "issuer",
    "year",
    "month",
    "file_name",
    "raw_xml_path",
    "created_at",
    "policy_id",
    "member_id",
    "subscriber_id",
    "exchg_assigned_enrollee_id",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
    "member_first_name",
    "member_last_name",
    "relationship",
    "subscriber_flag",
    "enrollee_event_type_code",
    "enrollee_event_reason_code",
    "action_code",
    "action_code_description",
    "maintenance_type_code",
    "additional_maint_reason_code",
    "coverage_status",
    "benefit_effective_date",
    "benefit_end_date",
    "member_maint_effective_date",
    "last_premium_paid_date",
    "request_submit_timestamp",
    "total_premium_amount",
    "individual_responsibility_amount",
    "aptc_amount",
    "user_fee_amount",
    "insurance_type_code",
    "health_coverage_policy_no",
    "household_or_employee_case_id",
    "rating_area",
    "source_exchg_id",
    "enrollment_action_code",
    "insurer_tax_id_number",
    "qtyn",
    "qtyy",
    "qtyt",
    "raw_payload",
    "raw_json",
]

_DATE_COLUMNS = frozenset({
    "benefit_effective_date",
    "benefit_end_date",
    "member_maint_effective_date",
})


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text_val = str(value).strip()
    if not text_val:
        return None
    try:
        return date.fromisoformat(text_val[:10])
    except ValueError:
        return None


def _parse_loaded_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.now().astimezone()
    text_val = str(value).strip()
    if text_val.endswith("Z"):
        text_val = text_val[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text_val)
    except ValueError:
        return datetime.now().astimezone()


def row_to_insert_params(enriched: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for col in OUTBOUND_AUTOMATION_COLUMNS:
        val = enriched.get(col)
        if col == "loaded_at":
            params[col] = _parse_loaded_at(val)
        elif col in _DATE_COLUMNS:
            params[col] = _parse_date(val)
        else:
            params[col] = val
    return params


def verify_tables_exist(engine: Engine) -> None:
    missing: list[str] = []
    with engine.connect() as conn:
        for table in OUTBOUND_TABLES:
            _assert_outbound_target(table)
            row = conn.exec_driver_sql(
                f"SELECT CASE WHEN OBJECT_ID(N'{SCHEMA}.{table}', N'U') IS NULL "
                f"THEN 0 ELSE 1 END"
            ).fetchone()
            if not row or int(row[0]) != 1:
                missing.append(f"{SCHEMA}.{table}")
    if missing:
        raise RuntimeError(
            "Required outbound_automation tables are missing: "
            + ", ".join(missing)
            + ". Run sql/outbound_automation_ddl.sql in SSMS first."
        )


def fetch_loaded_file_hashes(engine: Engine) -> dict[str, str]:
    sql = (
        "SELECT file_hash, load_run_id FROM [dbo].[outbound_automation_file_log] "
        "WHERE parse_status = 'loaded'"
    )
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(sql).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


_RUN_LOG_INSERT_SQL = text("""
    INSERT INTO [dbo].[outbound_automation_run_log] (
        load_run_id, started_at, completed_at, run_mode, source_mode,
        year_filter, issuer_filter, month_filter,
        parser_version, runner_version, git_commit,
        files_discovered, files_parsed, files_loaded, files_skipped_duplicate,
        files_failed, rows_parsed, rows_inserted, rows_skipped,
        total_warning_count, status, error_summary, report_output_path
    ) VALUES (
        :load_run_id, :started_at, NULL, :run_mode, :source_mode,
        :year_filter, :issuer_filter, :month_filter,
        :parser_version, :runner_version, :git_commit,
        0, 0, 0, 0, 0, 0, 0, 0, NULL, 'running', NULL, :report_output_path
    )
""")

_RUN_LOG_UPDATE_SQL = text("""
    UPDATE [dbo].[outbound_automation_run_log]
    SET completed_at = :completed_at,
        files_discovered = :files_discovered,
        files_parsed = :files_parsed,
        files_loaded = :files_loaded,
        files_skipped_duplicate = :files_skipped_duplicate,
        files_failed = :files_failed,
        rows_parsed = :rows_parsed,
        rows_inserted = :rows_inserted,
        rows_skipped = :rows_skipped,
        total_warning_count = :total_warning_count,
        status = :status,
        error_summary = :error_summary
    WHERE load_run_id = :load_run_id
""")


def insert_run_log_start(engine: Engine, context: "OutboundRunContext") -> None:
    params = {
        "load_run_id": context.load_run_id,
        "started_at": context.started_at,
        "run_mode": context.run_mode,
        "source_mode": context.source_mode,
        "year_filter": context.year_filter or ("ALL" if context.all_years else None),
        "issuer_filter": ",".join(context.issuer_filter) if context.issuer_filter else None,
        "month_filter": context.month_filter,
        "parser_version": context.parser_version,
        "runner_version": context.runner_version,
        "git_commit": context.git_commit,
        "report_output_path": str(context.output_dir),
    }
    with engine.begin() as conn:
        conn.execute(_RUN_LOG_INSERT_SQL, params)


def update_run_log_finish(
    engine: Engine,
    context: "OutboundRunContext",
    *,
    completed_at: datetime,
    stats: dict[str, Any],
    status: str,
    error_summary: str | None = None,
) -> None:
    params = {
        "load_run_id": context.load_run_id,
        "completed_at": completed_at,
        **stats,
        "status": status,
        "error_summary": error_summary,
    }
    with engine.begin() as conn:
        conn.execute(_RUN_LOG_UPDATE_SQL, params)


_FILE_LOG_INSERT_SQL = text("""
    INSERT INTO [dbo].[outbound_automation_file_log] (
        load_run_id, loaded_at, issuer, folder_year, folder_month,
        filename_file_year, filename_file_month,
        source_file, source_file_path, file_hash, file_size_bytes,
        parse_status, row_count, parse_duration_ms, error_message
    ) VALUES (
        :load_run_id, :loaded_at, :issuer, :folder_year, :folder_month,
        :filename_file_year, :filename_file_month,
        :source_file, :source_file_path, :file_hash, :file_size_bytes,
        :parse_status, :row_count, :parse_duration_ms, :error_message
    )
""")

_FILE_LOG_UPDATE_SQL = text("""
    UPDATE [dbo].[outbound_automation_file_log]
    SET load_run_id = :load_run_id,
        loaded_at = :loaded_at,
        issuer = :issuer,
        folder_year = :folder_year,
        folder_month = :folder_month,
        filename_file_year = :filename_file_year,
        filename_file_month = :filename_file_month,
        source_file = :source_file,
        source_file_path = :source_file_path,
        file_size_bytes = :file_size_bytes,
        parse_status = :parse_status,
        row_count = :row_count,
        parse_duration_ms = :parse_duration_ms,
        error_message = :error_message
    WHERE file_hash = :file_hash
""")

_FILE_LOG_EXISTS_SQL = text(
    "SELECT 1 FROM [dbo].[outbound_automation_file_log] WHERE file_hash = :file_hash"
)


def _row_insert_sql():
    col_list = ", ".join(f"[{c}]" for c in OUTBOUND_AUTOMATION_COLUMNS)
    placeholders = ", ".join(f":{c}" for c in OUTBOUND_AUTOMATION_COLUMNS)
    return text(
        f"INSERT INTO [dbo].[outbound_automation] ({col_list}) VALUES ({placeholders})"
    )


def _file_log_params(
    context: "OutboundRunContext",
    result: "FileProcessResult",
    *,
    loaded_at: datetime,
) -> dict[str, Any]:
    return {
        "load_run_id": context.load_run_id,
        "loaded_at": loaded_at,
        "issuer": result.source.issuer,
        "folder_year": int(result.source.year),
        "folder_month": int(result.source.month),
        "filename_file_year": result.filename_file_year,
        "filename_file_month": result.filename_file_month,
        "source_file": result.source.file_name,
        "source_file_path": str(result.source.file_path),
        "file_hash": result.file_hash,
        "file_size_bytes": result.source.file_size,
        "parse_status": result.parse_status,
        "row_count": result.row_count,
        "parse_duration_ms": result.parse_duration_ms,
        "error_message": result.error_message,
    }


def _file_log_exists(conn, file_hash: str) -> bool:
    return conn.execute(_FILE_LOG_EXISTS_SQL, {"file_hash": file_hash}).fetchone() is not None


def _write_file_log(conn, params: dict[str, Any], *, exists: bool) -> None:
    """Insert or update file_log by file_hash (never duplicate INSERT)."""
    conn.execute(_FILE_LOG_UPDATE_SQL if exists else _FILE_LOG_INSERT_SQL, params)


def upsert_file_log(
    engine: Engine,
    context: "OutboundRunContext",
    result: "FileProcessResult",
    *,
    loaded_at: datetime,
) -> None:
    params = _file_log_params(context, result, loaded_at=loaded_at)
    with engine.begin() as conn:
        exists = _file_log_exists(conn, result.file_hash)
        _write_file_log(conn, params, exists=exists)


def _insert_rows_in_batches(conn, row_sql, rows: list[dict[str, Any]], *, bs: int) -> list[float]:
    durations: list[float] = []
    for start in range(0, len(rows), bs):
        batch = [row_to_insert_params(r) for r in rows[start : start + bs]]
        t0 = time.perf_counter()
        conn.execute(row_sql, batch)
        durations.append(time.perf_counter() - t0)
    return durations


def load_file_rows(
    engine: Engine,
    context: "OutboundRunContext",
    result: "FileProcessResult",
    *,
    loaded_at: datetime,
    fast_executemany: bool = True,
) -> tuple[int, FileInsertMetrics]:
    """Insert all rows for one file plus its file_log entry in one transaction."""
    bs = batch_size()
    metrics = FileInsertMetrics(batch_size=bs, fast_executemany=fast_executemany)
    t_start = time.perf_counter()

    row_count = len(result.rows)
    result.parse_status = "loaded"
    result.row_count = row_count
    result.error_message = None
    file_params = _file_log_params(context, result, loaded_at=loaded_at)

    with engine.begin() as conn:
        durations = _insert_rows_in_batches(conn, _row_insert_sql(), result.rows, bs=bs)
        t_log = time.perf_counter()
        exists = _file_log_exists(conn, result.file_hash)
        _write_file_log(conn, file_params, exists=exists)
        file_log_sec = time.perf_counter() - t_log

    load_sec = time.perf_counter() - t_start
    insert_sec = sum(durations)
    metrics.row_count = row_count
    metrics.batch_count = len(durations)
    metrics.insert_sql_duration_ms = int(insert_sec * 1000)
    metrics.file_log_duration_ms = int(file_log_sec * 1000)
    metrics.commit_duration_ms = int(max(0.0, load_sec - insert_sec - file_log_sec) * 1000)
    metrics.load_duration_ms = int(load_sec * 1000)
    if durations:
        metrics.avg_batch_duration_ms = round(statistics.mean(durations) * 1000, 2)
        metrics.max_batch_duration_ms = round(max(durations) * 1000, 2)
        metrics.min_batch_duration_ms = round(min(durations) * 1000, 2)
    if insert_sec > 0:
        metrics.rows_per_sec = round(row_count / insert_sec, 2)

    logger.info(
        "Outbound loaded %s: %d rows in %d batches (batch_size=%d), load=%dms",
        result.source.file_name, row_count, metrics.batch_count, bs, metrics.load_duration_ms,
    )
    return row_count, metrics
