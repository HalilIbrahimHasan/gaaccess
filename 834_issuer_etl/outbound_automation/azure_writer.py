"""
Azure writer for outbound automation.

Targets ONLY dbo.outbound_automation, dbo.outbound_automation_monthly_premium,
dbo.outbound_automation_run_log and dbo.outbound_automation_file_log.
Column lists: outbound_automation.schema_v2 (mirrors sql/outbound_automation_ddl_v2.sql).

The premium table has no SQL FOREIGN KEY; integrity is enforced here: children are
validated against their parents first, then parents, children and the file_log
'loaded' row are written in ONE transaction (all or nothing per file).
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from outbound_automation.azure_common import batch_size
from outbound_automation.constants import (
    OUTBOUND_FILE_LOG_TABLE,
    OUTBOUND_MONTHLY_PREMIUM_TABLE,
    OUTBOUND_RUN_LOG_TABLE,
    OUTBOUND_TABLE,
    OUTBOUND_TABLES,
    SCHEMA,
)
from outbound_automation.integrity import validate_parent_child
from outbound_automation.load_metrics import FileInsertMetrics
from outbound_automation.schema_v2 import MONTHLY_PREMIUM_COLUMNS, OUTBOUND_AUTOMATION_COLUMNS
from utils.logger import get_logger

if TYPE_CHECKING:
    from outbound_automation.pipeline import FileProcessResult
    from outbound_automation.run_context import OutboundRunContext

logger = get_logger(__name__)

ALLOWED_TABLES = frozenset(OUTBOUND_TABLES)

RUN_LOG_COLUMNS = (
    "load_run_id", "started_at", "completed_at", "run_mode", "source_mode",
    "year_filter", "issuer_filter", "month_filter",
    "parser_version", "runner_version", "git_commit",
    "files_discovered", "files_parsed", "files_loaded", "files_skipped_duplicate",
    "files_failed", "rows_parsed", "rows_inserted", "rows_skipped",
    "premium_rows_inserted", "total_warning_count", "unknown_path_count",
    "status", "error_summary", "report_output_path",
)

FILE_LOG_COLUMNS = (
    "load_run_id", "loaded_at", "issuer", "folder_year", "folder_month",
    "filename_file_year", "filename_file_month",
    "source_file", "source_file_path", "file_hash", "file_size_bytes",
    "txn_create_datetime", "job_execution_id",
    "parse_status", "row_count", "premium_row_count", "unknown_path_count",
    "parse_duration_ms", "error_message",
)

REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    OUTBOUND_TABLE: OUTBOUND_AUTOMATION_COLUMNS,
    OUTBOUND_MONTHLY_PREMIUM_TABLE: MONTHLY_PREMIUM_COLUMNS,
    OUTBOUND_RUN_LOG_TABLE: RUN_LOG_COLUMNS,
    OUTBOUND_FILE_LOG_TABLE: FILE_LOG_COLUMNS,
}


def _assert_outbound_target(table: str) -> str:
    if table not in ALLOWED_TABLES or not table.startswith("outbound_automation"):
        raise RuntimeError(f"Refusing to write to non-outbound table: {SCHEMA}.{table}")
    return table


# ------------------------------------------------------------------ error text
_QUOTED_RE = re.compile(r"'([^']*)'")
_IDENT_CONTEXT_RE = re.compile(r"(?i)(column|object|table|constraint|index|key|name)\s*$")
_IDENT_RE = re.compile(r"^[A-Za-z_][\w.\[\]]*$")


def sanitize_db_error(exc: BaseException) -> str:
    """
    Driver error text without SQL parameters or quoted data values.

    SQLAlchemy appends '[SQL: ...] [parameters: ...]' and SQL Server can echo
    'Truncated value: ...'; both may contain member data.
    """
    orig = getattr(exc, "orig", None)
    msg = str(orig) if orig is not None else str(exc)
    msg = msg.split("[SQL:", 1)[0].split("[parameters:", 1)[0]
    cut = msg.lower().find("truncated value:")
    if cut >= 0:
        msg = msg[:cut] + "Truncated value: <redacted>."

    def _quoted(match: re.Match) -> str:
        before = msg[max(0, match.start() - 24):match.start()]
        if _IDENT_CONTEXT_RE.search(before) and _IDENT_RE.match(match.group(1)):
            return match.group(0)
        return "'<redacted>'"

    msg = _QUOTED_RE.sub(_quoted, msg)
    msg = " ".join(msg.split())
    return f"{type(exc).__name__}: {msg}"[:1000]


# ------------------------------------------------------------------ parameters
def _naive_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _params(row: dict[str, Any], columns: tuple[str, ...]) -> dict[str, Any]:
    params = {col: row.get(col) for col in columns}
    if "loaded_at" in params:
        params["loaded_at"] = _naive_utc(params["loaded_at"])
    return params


def row_to_insert_params(row: dict[str, Any]) -> dict[str, Any]:
    return _params(row, OUTBOUND_AUTOMATION_COLUMNS)


def premium_to_insert_params(row: dict[str, Any]) -> dict[str, Any]:
    return _params(row, MONTHLY_PREMIUM_COLUMNS)


def _insert_sql(table: str, columns: tuple[str, ...]):
    _assert_outbound_target(table)
    col_list = ", ".join(f"[{c}]" for c in columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    return text(f"INSERT INTO [dbo].[{table}] ({col_list}) VALUES ({placeholders})")


def _row_insert_sql():
    return _insert_sql(OUTBOUND_TABLE, OUTBOUND_AUTOMATION_COLUMNS)


def _premium_insert_sql():
    return _insert_sql(OUTBOUND_MONTHLY_PREMIUM_TABLE, MONTHLY_PREMIUM_COLUMNS)


# ------------------------------------------------------------------ preflight
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
            "Required outbound tables are missing: " + ", ".join(missing)
            + ". Run sql/outbound_automation_ddl_v2.sql (or the monthly premium "
            "recovery script) in SSMS first."
        )


def verify_writer_columns(engine: Engine) -> None:
    """Fail before any write if a table does not have the V2 columns this writer uses."""
    problems: list[str] = []
    with engine.connect() as conn:
        for table, required in REQUIRED_COLUMNS.items():
            _assert_outbound_target(table)
            rows = conn.exec_driver_sql(
                f"SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(N'{SCHEMA}.{table}')"
            ).fetchall()
            present = {str(r[0]).lower() for r in rows}
            missing = [c for c in required if c.lower() not in present]
            if missing:
                problems.append(f"{SCHEMA}.{table} missing {', '.join(missing)}")
    if problems:
        raise RuntimeError(
            "Outbound tables do not match the V2 writer contract: " + "; ".join(problems)
        )


def fetch_loaded_file_hashes(engine: Engine) -> dict[str, str]:
    sql = (
        "SELECT file_hash, load_run_id FROM [dbo].[outbound_automation_file_log] "
        "WHERE parse_status = 'loaded'"
    )
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(sql).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


# ------------------------------------------------------------------ run log
_RUN_LOG_INSERT_SQL = text("""
    INSERT INTO [dbo].[outbound_automation_run_log] (
        load_run_id, started_at, completed_at, run_mode, source_mode,
        year_filter, issuer_filter, month_filter,
        parser_version, runner_version, git_commit,
        files_discovered, files_parsed, files_loaded, files_skipped_duplicate,
        files_failed, rows_parsed, rows_inserted, rows_skipped,
        premium_rows_inserted, total_warning_count, unknown_path_count,
        status, error_summary, report_output_path
    ) VALUES (
        :load_run_id, :started_at, NULL, :run_mode, :source_mode,
        :year_filter, :issuer_filter, :month_filter,
        :parser_version, :runner_version, :git_commit,
        0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, NULL, 'running', NULL, :report_output_path
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
        premium_rows_inserted = :premium_rows_inserted,
        total_warning_count = :total_warning_count,
        unknown_path_count = :unknown_path_count,
        status = :status,
        error_summary = :error_summary
    WHERE load_run_id = :load_run_id
""")


def insert_run_log_start(engine: Engine, context: "OutboundRunContext") -> None:
    params = {
        "load_run_id": context.load_run_id,
        "started_at": _naive_utc(context.started_at),
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
        "completed_at": _naive_utc(completed_at),
        **stats,
        "status": status,
        "error_summary": error_summary,
    }
    with engine.begin() as conn:
        conn.execute(_RUN_LOG_UPDATE_SQL, params)


# ------------------------------------------------------------------ file log
_FILE_LOG_INSERT_SQL = text("""
    INSERT INTO [dbo].[outbound_automation_file_log] (
        load_run_id, loaded_at, issuer, folder_year, folder_month,
        filename_file_year, filename_file_month,
        source_file, source_file_path, file_hash, file_size_bytes,
        txn_create_datetime, job_execution_id,
        parse_status, row_count, premium_row_count, unknown_path_count,
        parse_duration_ms, error_message
    ) VALUES (
        :load_run_id, :loaded_at, :issuer, :folder_year, :folder_month,
        :filename_file_year, :filename_file_month,
        :source_file, :source_file_path, :file_hash, :file_size_bytes,
        :txn_create_datetime, :job_execution_id,
        :parse_status, :row_count, :premium_row_count, :unknown_path_count,
        :parse_duration_ms, :error_message
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
        txn_create_datetime = :txn_create_datetime,
        job_execution_id = :job_execution_id,
        parse_status = :parse_status,
        row_count = :row_count,
        premium_row_count = :premium_row_count,
        unknown_path_count = :unknown_path_count,
        parse_duration_ms = :parse_duration_ms,
        error_message = :error_message
    WHERE file_hash = :file_hash
""")

_FILE_LOG_EXISTS_SQL = text(
    "SELECT 1 FROM [dbo].[outbound_automation_file_log] WHERE file_hash = :file_hash"
)


def _file_log_params(
    context: "OutboundRunContext",
    result: "FileProcessResult",
    *,
    loaded_at: datetime,
    parse_status: str,
) -> dict[str, Any]:
    return {
        "load_run_id": context.load_run_id,
        "loaded_at": _naive_utc(loaded_at),
        "issuer": result.source.issuer,
        "folder_year": int(result.source.year),
        "folder_month": int(result.source.month),
        "filename_file_year": result.filename_file_year,
        "filename_file_month": result.filename_file_month,
        "source_file": result.source.file_name,
        "source_file_path": str(result.source.file_path),
        "file_hash": result.file_hash,
        "file_size_bytes": result.source.file_size,
        "txn_create_datetime": result.txn_create_datetime,
        "job_execution_id": result.job_execution_id,
        "parse_status": parse_status,
        "row_count": len(result.rows) if parse_status == "loaded" else result.row_count,
        "premium_row_count": (
            len(result.premium_rows) if parse_status == "loaded" else result.premium_row_count
        ),
        "unknown_path_count": result.unknown_path_count,
        "parse_duration_ms": result.parse_duration_ms,
        "error_message": None if parse_status == "loaded" else result.error_message,
    }


def _file_log_exists(conn, file_hash: str) -> bool:
    return conn.execute(_FILE_LOG_EXISTS_SQL, {"file_hash": file_hash}).fetchone() is not None


def _write_file_log(conn, params: dict[str, Any]) -> None:
    """Insert or update file_log by file_hash (never duplicate INSERT)."""
    exists = _file_log_exists(conn, params["file_hash"])
    conn.execute(_FILE_LOG_UPDATE_SQL if exists else _FILE_LOG_INSERT_SQL, params)


def upsert_file_log(
    engine: Engine,
    context: "OutboundRunContext",
    result: "FileProcessResult",
    *,
    loaded_at: datetime,
) -> None:
    """Record a non-loaded outcome (failed). Never used to mark a file loaded."""
    if result.parse_status == "loaded":
        raise RuntimeError("upsert_file_log must not mark a file loaded; use load_file_rows")
    params = _file_log_params(context, result, loaded_at=loaded_at, parse_status=result.parse_status)
    with engine.begin() as conn:
        _write_file_log(conn, params)


# ------------------------------------------------------------------ file load
def _insert_in_batches(conn, sql, rows: list[dict[str, Any]], to_params, *, bs: int) -> list[float]:
    durations: list[float] = []
    for start in range(0, len(rows), bs):
        batch = [to_params(r) for r in rows[start:start + bs]]
        t0 = time.perf_counter()
        conn.execute(sql, batch)
        durations.append(time.perf_counter() - t0)
    return durations


def load_file_rows(
    engine: Engine,
    context: "OutboundRunContext",
    result: "FileProcessResult",
    *,
    loaded_at: datetime,
    fast_executemany: bool = True,
) -> tuple[int, int, FileInsertMetrics]:
    """
    Load one file atomically: parents, premium children, file_log 'loaded'.

    Any exception inside the transaction rolls everything back; the caller then
    records the file as failed (retryable). result.parse_status becomes 'loaded'
    only after commit.
    """
    validate_parent_child(result.file_hash, result.rows, result.premium_rows)

    bs = batch_size()
    metrics = FileInsertMetrics(batch_size=bs, fast_executemany=fast_executemany)
    file_params = _file_log_params(context, result, loaded_at=loaded_at, parse_status="loaded")
    t_start = time.perf_counter()

    with engine.begin() as conn:
        parent_durations = _insert_in_batches(
            conn, _row_insert_sql(), result.rows, row_to_insert_params, bs=bs,
        )
        premium_durations = _insert_in_batches(
            conn, _premium_insert_sql(), result.premium_rows, premium_to_insert_params, bs=bs,
        )
        t_log = time.perf_counter()
        _write_file_log(conn, file_params)
        file_log_sec = time.perf_counter() - t_log

    result.parse_status = "loaded"
    result.row_count = len(result.rows)
    result.premium_row_count = len(result.premium_rows)
    result.error_message = None

    load_sec = time.perf_counter() - t_start
    parent_sec = sum(parent_durations)
    premium_sec = sum(premium_durations)
    metrics.row_count = len(result.rows)
    metrics.premium_row_count = len(result.premium_rows)
    metrics.batch_count = len(parent_durations)
    metrics.premium_batch_count = len(premium_durations)
    metrics.insert_sql_duration_ms = int(parent_sec * 1000)
    metrics.premium_insert_sql_duration_ms = int(premium_sec * 1000)
    metrics.file_log_duration_ms = int(file_log_sec * 1000)
    metrics.commit_duration_ms = int(
        max(0.0, load_sec - parent_sec - premium_sec - file_log_sec) * 1000
    )
    metrics.load_duration_ms = int(load_sec * 1000)
    if parent_sec > 0:
        metrics.rows_per_sec = round(len(result.rows) / parent_sec, 2)

    logger.info(
        "Outbound loaded %s: %d rows, %d premium rows, load=%dms",
        result.source.file_name, metrics.row_count, metrics.premium_row_count,
        metrics.load_duration_ms,
    )
    return metrics.row_count, metrics.premium_row_count, metrics
