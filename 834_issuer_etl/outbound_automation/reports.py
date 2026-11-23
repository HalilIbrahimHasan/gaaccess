"""Excel reports for outbound automation runs (written under outputs/outbound_automation/)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from azure_reconciliation.safe_export import safe_write_excel
from inbound_automation.reports import (
    build_dry_run_manifest,
    build_failed_files,
    build_file_level_counts,
    build_file_load_log,
    build_filename_month_summary,
    build_load_summary,
    build_skipped_duplicate_files,
)
from outbound_automation.pipeline import RunResult
from utils.logger import get_logger

logger = get_logger(__name__)


def build_run_summary(result: RunResult) -> pd.DataFrame:
    ctx = result.context
    is_load = ctx.run_mode == "load"
    metrics = [f.insert_metrics for f in result.file_results if f.insert_metrics is not None]
    total_insert_sql_ms = sum(m.insert_sql_duration_ms for m in metrics)
    total_rows_loaded = sum(m.row_count for m in metrics)
    row = {
        "pipeline": "outbound",
        "load_run_id": ctx.load_run_id,
        "run_mode": ctx.run_mode,
        "source_mode": ctx.source_mode,
        "source_root": str(ctx.source_root),
        "year_filter": ctx.year_filter or ("ALL" if ctx.all_years else ""),
        "issuer_filter": ",".join(ctx.issuer_filter) if ctx.issuer_filter else "ALL",
        "month_filter": ctx.month_filter or "ALL",
        "parser_version": ctx.parser_version,
        "runner_version": ctx.runner_version,
        "git_commit": ctx.git_commit or "",
        "started_at": ctx.started_at.isoformat(timespec="seconds"),
        "completed_at": result.completed_at.isoformat(timespec="seconds"),
        "elapsed_seconds": round((result.completed_at - ctx.started_at).total_seconds(), 2),
        "files_discovered": result.files_discovered,
        "files_parsed": result.files_parsed,
        "files_failed": result.files_failed,
        "files_skipped_duplicate": result.files_skipped_duplicate,
        "files_loaded": result.files_loaded if is_load else 0,
        "rows_parsed": result.rows_parsed,
        "rows_inserted": result.rows_inserted if is_load else 0,
        "insert_sql_duration_ms": total_insert_sql_ms if is_load else None,
        "avg_rows_per_sec": (
            round(total_rows_loaded / (total_insert_sql_ms / 1000), 2)
            if is_load and total_insert_sql_ms > 0
            else None
        ),
        "total_warning_count": result.total_warning_count,
        "azure_writes": "LOAD (outbound_automation tables only)" if is_load else "NONE (dry-run)",
        "status": "success" if is_load else "dry_run",
        "report_output_path": str(ctx.output_dir),
    }
    return pd.DataFrame([row])


def write_run_reports(result: RunResult) -> dict[str, Path]:
    out = result.context.output_dir
    rows = result.enriched_rows
    sheets = {
        "run_summary": (out / "run_summary.xlsx", build_run_summary(result)),
        "load_summary": (
            out / "load_summary_by_issuer_year_month_status.xlsx", build_load_summary(rows),
        ),
        "file_level": (out / "file_level_raw_counts.xlsx", build_file_level_counts(rows)),
        "filename_month": (out / "filename_month_summary.xlsx", build_filename_month_summary(rows)),
        "failed_files": (out / "failed_files.xlsx", build_failed_files(result)),
        "skipped_duplicate": (
            out / "skipped_duplicate_files.xlsx", build_skipped_duplicate_files(result),
        ),
        "dry_run_manifest": (out / "dry_run_manifest.xlsx", build_dry_run_manifest(result)),
        "file_load_log": (out / "file_load_log.xlsx", build_file_load_log(result)),
    }
    paths: dict[str, Path] = {}
    for name, (path, df) in sheets.items():
        safe_write_excel(path, {name: df}, drop_duplicate_value_columns=False)
        paths[name] = path
    logger.info("Wrote outbound automation reports to %s", out)
    return paths
