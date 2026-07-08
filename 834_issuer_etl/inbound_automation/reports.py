"""Excel report builders for inbound automation dry-run."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.safe_export import safe_write_excel
from inbound_automation.pipeline import DryRunResult
from utils.logger import get_logger

logger = get_logger(__name__)


def _empty(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def build_load_summary(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return _empty(
            [
                "issuer",
                "folder_year",
                "folder_month",
                "insurance_type",
                "enrolleeStatus",
                "row_count",
                "distinct_policy_count",
                "distinct_member_count",
            ]
        )
    df = pd.DataFrame(rows)
    grouped = (
        df.groupby(
            ["issuer", "folder_year", "folder_month", "insurance_type", "enrolleeStatus"],
            dropna=False,
        )
        .agg(
            row_count=("source_file", "size"),
            distinct_policy_count=("policy_id", pd.Series.nunique),
            distinct_member_count=("member_id", pd.Series.nunique),
        )
        .reset_index()
    )
    return grouped.sort_values(
        ["issuer", "folder_year", "folder_month", "insurance_type", "enrolleeStatus"],
        kind="stable",
    )


def build_file_level_counts(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return _empty(
            [
                "issuer",
                "folder_year",
                "folder_month",
                "filename_file_year",
                "filename_file_month",
                "source_file",
                "insurance_type",
                "enrolleeStatus",
                "row_count",
                "distinct_policy_count",
                "distinct_member_count",
            ]
        )
    df = pd.DataFrame(rows)
    grouped = (
        df.groupby(
            [
                "issuer",
                "folder_year",
                "folder_month",
                "filename_file_year",
                "filename_file_month",
                "source_file",
                "insurance_type",
                "enrolleeStatus",
            ],
            dropna=False,
        )
        .agg(
            row_count=("member_id", "size"),
            distinct_policy_count=("policy_id", pd.Series.nunique),
            distinct_member_count=("member_id", pd.Series.nunique),
        )
        .reset_index()
    )
    return grouped.sort_values(
        [
            "issuer",
            "folder_year",
            "folder_month",
            "filename_file_year",
            "filename_file_month",
            "source_file",
        ],
        kind="stable",
    )


def build_filename_month_summary(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return _empty(
            [
                "issuer",
                "filename_file_year",
                "filename_file_month",
                "insurance_type",
                "enrolleeStatus",
                "row_count",
                "distinct_policy_count",
                "distinct_member_count",
            ]
        )
    df = pd.DataFrame(rows)
    grouped = (
        df.groupby(
            [
                "issuer",
                "filename_file_year",
                "filename_file_month",
                "insurance_type",
                "enrolleeStatus",
            ],
            dropna=False,
        )
        .agg(
            row_count=("source_file", "size"),
            distinct_policy_count=("policy_id", pd.Series.nunique),
            distinct_member_count=("member_id", pd.Series.nunique),
        )
        .reset_index()
    )
    return grouped.sort_values(
        ["issuer", "filename_file_year", "filename_file_month", "enrolleeStatus"],
        kind="stable",
    )


def build_failed_files(result: DryRunResult) -> pd.DataFrame:
    rows = [
        {
            "source_file": f.source.file_name,
            "source_file_path": str(f.source.file_path),
            "issuer": f.source.issuer,
            "folder_year": f.source.year,
            "folder_month": f.source.month,
            "file_hash": f.file_hash,
            "parse_duration_ms": f.parse_duration_ms,
            "error_message": f.error_message,
        }
        for f in result.file_results
        if f.parse_status == "failed"
    ]
    if not rows:
        return _empty(
            [
                "source_file",
                "source_file_path",
                "issuer",
                "folder_year",
                "folder_month",
                "file_hash",
                "parse_duration_ms",
                "error_message",
            ]
        )
    return pd.DataFrame(rows)


def build_skipped_duplicate_files() -> pd.DataFrame:
    """Phase 1 dry-run has no Azure file_log — duplicates not evaluated yet."""
    return _empty(
        [
            "source_file",
            "file_hash",
            "reason",
            "prior_load_run_id",
            "note",
        ]
    )


def build_dry_run_manifest(result: DryRunResult) -> pd.DataFrame:
    rows = [
        {
            "source_file": f.source.file_name,
            "source_file_path": str(f.source.file_path),
            "issuer": f.source.issuer,
            "folder_year": int(f.source.year),
            "folder_month": int(f.source.month),
            "filename_file_year": f.filename_file_year,
            "filename_file_month": f.filename_file_month,
            "file_hash": f.file_hash,
            "file_size_bytes": f.source.file_size,
            "parse_status": f.parse_status,
            "row_count": f.row_count,
            "parse_duration_ms": f.parse_duration_ms,
            "warning_count": len(f.warnings),
            "warnings": "; ".join(f.warnings) if f.warnings else "",
            "error_message": f.error_message or "",
        }
        for f in result.file_results
    ]
    if not rows:
        return _empty(
            [
                "source_file",
                "source_file_path",
                "issuer",
                "folder_year",
                "folder_month",
                "filename_file_year",
                "filename_file_month",
                "file_hash",
                "file_size_bytes",
                "parse_status",
                "row_count",
                "parse_duration_ms",
                "warning_count",
                "warnings",
                "error_message",
            ]
        )
    return pd.DataFrame(rows)


def build_run_summary(result: DryRunResult) -> pd.DataFrame:
    ctx = result.context
    elapsed_s = (result.completed_at - ctx.started_at).total_seconds()
    row = {
        "load_run_id": ctx.load_run_id,
        "run_mode": ctx.run_mode,
        "source_mode": ctx.source_mode,
        "year_filter": ctx.year_filter or ("ALL" if ctx.all_years else ""),
        "issuer_filter": ",".join(ctx.issuer_filter) if ctx.issuer_filter else "ALL",
        "month_filter": ctx.month_filter or "ALL",
        "parser_version": ctx.parser_version,
        "runner_version": ctx.runner_version,
        "git_commit": ctx.git_commit or "",
        "started_at": ctx.started_at.isoformat(timespec="seconds"),
        "completed_at": result.completed_at.isoformat(timespec="seconds"),
        "elapsed_seconds": round(elapsed_s, 2),
        "files_discovered": result.files_discovered,
        "files_parsed": result.files_parsed,
        "files_failed": result.files_failed,
        "files_skipped_duplicate": 0,
        "rows_parsed": result.rows_parsed,
        "rows_inserted": 0,
        "total_warning_count": result.total_warning_count,
        "azure_writes": "NONE (dry-run)",
        "status": "dry_run",
        "report_output_path": str(ctx.output_dir),
    }
    return pd.DataFrame([row])


def write_dry_run_reports(result: DryRunResult) -> dict[str, Path]:
    """Write all Phase 1 Excel reports; return path map."""
    out = result.context.output_dir
    paths = {
        "run_summary": out / "run_summary.xlsx",
        "load_summary": out / "load_summary_by_issuer_year_month_status.xlsx",
        "file_level": out / "file_level_raw_counts.xlsx",
        "filename_month": out / "filename_month_summary.xlsx",
        "failed_files": out / "failed_files.xlsx",
        "skipped_duplicate": out / "skipped_duplicate_files.xlsx",
        "dry_run_manifest": out / "dry_run_manifest.xlsx",
    }

    rows = result.enriched_rows
    safe_write_excel(
        paths["run_summary"],
        {"run_summary": build_run_summary(result)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["load_summary"],
        {"load_summary": build_load_summary(rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["file_level"],
        {"file_level": build_file_level_counts(rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["filename_month"],
        {"filename_month": build_filename_month_summary(rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["failed_files"],
        {"failed_files": build_failed_files(result)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["skipped_duplicate"],
        {
            "skipped_duplicate": build_skipped_duplicate_files(),
            "note": pd.DataFrame(
                [{"note": "Phase 1 dry-run: duplicate skip requires Azure file_log (Phase 2+)."}]
            ),
        },
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["dry_run_manifest"],
        {"dry_run_manifest": build_dry_run_manifest(result)},
        drop_duplicate_value_columns=False,
    )

    logger.info("Wrote inbound automation reports to %s", out)
    return paths
