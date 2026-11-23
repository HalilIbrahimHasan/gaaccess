"""
Excel reports for outbound automation runs (outputs/outbound_automation/<run_id>/).

PII-safe by construction: reports are built only from file names, counts, codes,
dates-as-periods and structural paths. No member/policy identifiers, names,
amounts per member or JSON payloads are written.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from azure_reconciliation.safe_export import safe_write_excel
from outbound_automation.pipeline import RunResult
from utils.logger import get_logger

logger = get_logger(__name__)

# Row keys that may appear in reports. Everything else (identifiers, names,
# amounts, JSON) is excluded.
REPORT_ROW_KEYS = (
    "issuer",
    "folder_year",
    "folder_month",
    "filename_file_year",
    "filename_file_month",
    "source_file",
    "coverage_year",
    "coverage_year_source",
    "insurance_type_code",
    "insurance_type",
    "outbound_transaction_type",
    "enrollee_event_type_code",
    "additional_maint_reason_code",
    "subscriber_flag",
    "warning_count",
    "warning_codes",
)


def build_run_summary(result: RunResult) -> pd.DataFrame:
    ctx = result.context
    is_load = ctx.run_mode == "load"
    metrics = [f.insert_metrics for f in result.file_results if f.insert_metrics is not None]
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
        "source_json_pii_policy": result.pii_policy,
        "started_at": ctx.started_at.isoformat(timespec="seconds"),
        "completed_at": result.completed_at.isoformat(timespec="seconds"),
        "elapsed_seconds": round((result.completed_at - ctx.started_at).total_seconds(), 2),
        "files_discovered": result.files_discovered,
        "files_rejected_by_name": len(result.rejected_files),
        "files_parsed": result.files_parsed,
        "files_failed": result.files_failed,
        "files_skipped_duplicate": result.files_skipped_duplicate,
        "files_loaded": result.files_loaded if is_load else 0,
        "rows_parsed": result.rows_parsed,
        "premium_rows_parsed": result.premium_rows_parsed,
        "rows_inserted": result.rows_inserted if is_load else 0,
        "premium_rows_inserted": result.premium_rows_inserted if is_load else 0,
        "insert_sql_duration_ms": sum(m.insert_sql_duration_ms for m in metrics) if is_load else None,
        "premium_insert_sql_duration_ms": (
            sum(m.premium_insert_sql_duration_ms for m in metrics) if is_load else None
        ),
        "total_warning_count": result.total_warning_count,
        "unknown_path_count": result.unknown_path_count,
        "azure_writes": "LOAD (dbo.outbound_automation* only)" if is_load else "NONE (dry-run)",
        "report_output_path": str(ctx.output_dir),
    }
    return pd.DataFrame([row])


def build_file_summary(result: RunResult) -> pd.DataFrame:
    rows = []
    for f in result.file_results:
        m = f.insert_metrics
        rows.append({
            "issuer": f.source.issuer,
            "folder_year": f.source.year,
            "folder_month": f.source.month,
            "source_file": f.source.file_name,
            "file_hash": f.file_hash,
            "parse_status": f.parse_status,
            "prior_load_run_id": f.prior_load_run_id or "",
            "filename_file_year": f.filename_file_year,
            "filename_file_month": f.filename_file_month,
            "txn_create_datetime": (
                f.txn_create_datetime.isoformat() if f.txn_create_datetime else ""
            ),
            "job_execution_id": f.job_execution_id or "",
            "row_count": f.row_count,
            "premium_row_count": f.premium_row_count,
            "unknown_path_count": f.unknown_path_count,
            "row_warning_count": sum(int(r.get("warning_count") or 0) for r in f.rows),
            "file_warnings": ";".join(dict.fromkeys(f.file_warnings)),
            "parse_duration_ms": f.parse_duration_ms,
            "load_duration_ms": m.load_duration_ms if m else None,
            "error_message": f.error_message or "",
        })
    return pd.DataFrame(rows)


def build_warning_summary(result: RunResult) -> pd.DataFrame:
    counts: Counter = Counter()
    files: dict[tuple[str, str], set[str]] = defaultdict(set)
    for f in result.file_results:
        for code in f.file_warnings:
            counts[("file", code)] += 1
            files[("file", code)].add(f.source.file_name)
        if f.parse_status not in ("parsed", "loaded"):
            continue
        for r in f.rows:
            for code in (r.get("warning_codes") or "").split(";"):
                if code:
                    counts[("row", code)] += 1
                    files[("row", code)].add(f.source.file_name)
    rows = [
        {
            "scope": scope,
            "warning_code": code,
            "rows_or_files": n,
            "files_seen": len(files[(scope, code)]),
        }
        for (scope, code), n in sorted(counts.items())
    ]
    return pd.DataFrame(rows)


def build_transaction_summary(result: RunResult) -> pd.DataFrame:
    rows = [{k: r.get(k) for k in REPORT_ROW_KEYS} for r in result.rows]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    keys = [
        "issuer", "folder_year", "folder_month", "coverage_year",
        "insurance_type_code", "insurance_type", "outbound_transaction_type",
        "enrollee_event_type_code", "additional_maint_reason_code", "subscriber_flag",
    ]
    return (
        df.fillna({k: "" for k in keys})
        .groupby(keys, dropna=False)
        .size()
        .reset_index(name="row_count")
    )


def build_rejected_files(result: RunResult) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "issuer": s.issuer,
            "folder_year": s.year,
            "folder_month": s.month,
            "file_name": s.file_name,
            "classification": cls,
        }
        for s, cls in result.rejected_files
    ])


def write_run_reports(result: RunResult) -> dict[str, Path]:
    out = result.context.output_dir
    obs = result.observations
    workbooks = {
        "run_summary": (out / "run_summary.xlsx", {"run_summary": build_run_summary(result)}),
        "file_summary": (out / "file_summary.xlsx", {"file_summary": build_file_summary(result)}),
        "warning_summary": (
            out / "warning_summary.xlsx", {"warning_summary": build_warning_summary(result)},
        ),
        "transaction_summary": (
            out / "transaction_summary.xlsx",
            {"transaction_summary": build_transaction_summary(result)},
        ),
        "rejected_files": (
            out / "rejected_files.xlsx", {"rejected_files": build_rejected_files(result)},
        ),
        "schema_observations": (
            out / "schema_observations.xlsx",
            {
                "paths": obs.paths_frame(),
                "codes": obs.codes_frame(),
                "attributes_namespaces": obs.attributes_namespaces_frame(),
            },
        ),
    }
    paths: dict[str, Path] = {}
    for name, (path, sheets) in workbooks.items():
        safe_write_excel(path, sheets, drop_duplicate_value_columns=False)
        paths[name] = path
    logger.info("Wrote outbound automation reports to %s", out)
    return paths
