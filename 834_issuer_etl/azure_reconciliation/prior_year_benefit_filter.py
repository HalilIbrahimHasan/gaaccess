"""
Prior-year benefit effective filter — read-only comparison layer.

For reporting year Y:
  - exclude records where benefit_effective_year < Y
  - keep records where benefit_effective_year >= Y
  - keep null benefit_effective_date (flagged in audit)

Does not modify the unfiltered production pipeline or existing exports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.full_data_exports import write_full_export_excel
from azure_reconciliation.safe_export import safe_write_csv, safe_write_excel
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

AUDIT_COLUMNS = [
    "issuer",
    "reporting_year",
    "source_file",
    "policy_id",
    "member_id",
    "benefit_effective_date",
    "benefit_effective_year",
    "filter_action",
    "filter_reason",
]

BENEFIT_DATE_COLS = (
    "benefit_effective_date",
    "benefit_effective_begin_date",
)


def filtered_review_root() -> Path:
    return settings.outputs_path / "business_review_filtered"


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def resolve_reporting_year(
    *,
    cli_year: str | None = None,
    partition_year: str | None = None,
) -> str:
    """
    Reporting year precedence: CLI --year → settings.reporting_year → partition year.
    """
    if cli_year and str(cli_year).strip():
        return str(cli_year).strip()
    if settings.reporting_year and str(settings.reporting_year).strip():
        return str(settings.reporting_year).strip()
    if partition_year and str(partition_year).strip():
        return str(partition_year).strip()
    raise ValueError("reporting year required — pass --year, set REPORTING_YEAR, or provide partition year")


def parse_benefit_effective_year(value: Any) -> int | None:
    """Extract 4-digit year from a benefit effective date value."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "nat", ""):
        return None
    dt = pd.to_datetime(s[:10].replace("/", "-"), errors="coerce")
    if pd.notna(dt):
        return int(dt.year)
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _benefit_date_series(df: pd.DataFrame) -> pd.Series:
    for col in BENEFIT_DATE_COLS:
        if col in df.columns:
            return df[col]
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _source_file_series(df: pd.DataFrame) -> pd.Series:
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in df.columns:
            s = df[col].astype(str)
            if col == "raw_xml_path":
                return s.map(lambda p: Path(p).name if p and p != "nan" else "")
            return s
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _policy_id_series(df: pd.DataFrame) -> pd.Series:
    for col in ("policy_id", "exchg_assigned_policy_id", "health_coverage_policy_no"):
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _member_id_series(df: pd.DataFrame) -> pd.Series:
    for col in ("member_id", "exchg_assigned_enrollee_id", "exchg_indiv_identifier"):
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def classify_benefit_filter(
    benefit_effective_year: int | None,
    reporting_year: int | str,
) -> tuple[str, str]:
    """Return (filter_action, filter_reason)."""
    y = int(reporting_year)
    if benefit_effective_year is None:
        return (
            "KEEP",
            "benefit_effective_date is null — kept and flagged for review",
        )
    if benefit_effective_year < y:
        return (
            "EXCLUDE",
            f"benefit_effective_year {benefit_effective_year} < reporting_year {y}",
        )
    return (
        "KEEP",
        f"benefit_effective_year {benefit_effective_year} >= reporting_year {y}",
    )


def build_filter_audit(
    df: pd.DataFrame,
    *,
    issuer: str,
    reporting_year: str,
    dataset_label: str = "",
) -> pd.DataFrame:
    """Build per-record audit rows for a dataframe."""
    if df.empty:
        return pd.DataFrame(columns=AUDIT_COLUMNS)

    work = df.copy()
    benefit_dates = _benefit_date_series(work)
    benefit_years = benefit_dates.map(parse_benefit_effective_year)
    benefit_year_out = benefit_years.apply(lambda y: "" if y is None else int(y))
    actions: list[str] = []
    reasons: list[str] = []
    for bey in benefit_years:
        action, reason = classify_benefit_filter(bey, reporting_year)
        actions.append(action)
        if dataset_label:
            reason = f"[{dataset_label}] {reason}"
        reasons.append(reason)

    return pd.DataFrame({
        "issuer": issuer,
        "reporting_year": str(reporting_year),
        "source_file": _source_file_series(work),
        "policy_id": _policy_id_series(work),
        "member_id": _member_id_series(work),
        "benefit_effective_date": benefit_dates.astype(str),
        "benefit_effective_year": benefit_year_out,
        "filter_action": actions,
        "filter_reason": reasons,
    })


def apply_prior_year_benefit_filter(
    df: pd.DataFrame,
    *,
    reporting_year: str,
) -> pd.DataFrame:
    """Return rows that pass the prior-year benefit effective filter."""
    if df.empty:
        return df.copy()
    audit = build_filter_audit(df, issuer="", reporting_year=reporting_year)
    keep = audit["filter_action"] == "KEEP"
    return df.loc[keep.values].reset_index(drop=True)


def build_filter_summary(
    raw_audit: pd.DataFrame,
    business_audit: pd.DataFrame,
    *,
    reporting_year: str,
) -> tuple[pd.DataFrame, str]:
    """Summary counts for one reporting year (raw + business combined view)."""
    combined = pd.concat([raw_audit, business_audit], ignore_index=True)
    raw_n = len(raw_audit)
    biz_n = len(business_audit)
    before = raw_n + biz_n

    excluded = int((combined["filter_action"] == "EXCLUDE").sum()) if not combined.empty else 0
    after = int((combined["filter_action"] == "KEEP").sum()) if not combined.empty else 0
    null_count = 0
    if not combined.empty:
        null_count = int(
            combined["benefit_effective_year"].isna().sum()
            + (combined["benefit_effective_year"].astype(str) == "").sum()
        )

    by_year_rows: list[dict[str, Any]] = []
    if not combined.empty:
        for bey, grp in combined.groupby(
            combined["benefit_effective_year"].fillna("null").astype(str),
            dropna=False,
        ):
            by_year_rows.append({
                "benefit_effective_year": bey,
                "record_count": len(grp),
                "excluded_count": int((grp["filter_action"] == "EXCLUDE").sum()),
                "kept_count": int((grp["filter_action"] == "KEEP").sum()),
            })

    summary_df = pd.DataFrame([{
        "reporting_year": str(reporting_year),
        "raw_before_count": raw_n,
        "business_ready_before_count": biz_n,
        "before_count": before,
        "excluded_prior_year_count": excluded,
        "after_count": after,
        "null_benefit_effective_count": null_count,
    }])
    by_year_df = pd.DataFrame(by_year_rows).sort_values("benefit_effective_year")

    md_lines = [
        "# Prior-Year Benefit Effective Filter Summary",
        "",
        f"**Reporting year:** {reporting_year}",
        f"**Generated:** {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        "## Rule",
        "",
        f"For reporting year **{reporting_year}**:",
        f"- Exclude records where `benefit_effective_year < {reporting_year}`",
        f"- Keep records where `benefit_effective_year >= {reporting_year}`",
        "- Keep records with null `benefit_effective_date` (flagged in audit)",
        "",
        "## Counts",
        "",
        f"| Metric | Value |",
        f"|--------|------:|",
        f"| Raw records (before) | {raw_n:,} |",
        f"| Business-ready records (before) | {biz_n:,} |",
        f"| Total before | {before:,} |",
        f"| Excluded (prior year) | {excluded:,} |",
        f"| After filter | {after:,} |",
        f"| Null benefit effective (kept, flagged) | {null_count:,} |",
        "",
        "## Counts by benefit_effective_year",
        "",
    ]
    if by_year_df.empty:
        md_lines.append("(no records)")
    else:
        md_lines.append("| benefit_effective_year | records | excluded | kept |")
        md_lines.append("|------------------------|--------:|---------:|-----:|")
        for _, row in by_year_df.iterrows():
            md_lines.append(
                f"| {row['benefit_effective_year']} | {int(row['record_count'])} "
                f"| {int(row['excluded_count'])} | {int(row['kept_count'])} |"
            )
    md_lines.append("")
    md_lines.append(
        "Unfiltered raw and business exports are unchanged. "
        "Filtered outputs live under `outputs/business_review_filtered/`."
    )
    md_lines.append("")
    return summary_df, "\n".join(md_lines), by_year_df


def _load_raw_export(issuer: str, year: str) -> pd.DataFrame:
    path = settings.outputs_path / "full_data_exports" / issuer / year / "raw" / "raw_all_months.csv"
    if path.exists():
        return pd.read_csv(path, dtype=str, low_memory=False)
    xlsx = path.with_suffix(".xlsx")
    if xlsx.exists():
        return pd.read_excel(xlsx, sheet_name=0, dtype=str)
    return pd.DataFrame()


def _load_business_ready_export(issuer: str, year: str) -> pd.DataFrame:
    path = (
        settings.outputs_path / "business_data_exports" / issuer / year
        / "business_ready" / "business_ready_all_months.csv"
    )
    if path.exists():
        return pd.read_csv(path, dtype=str, low_memory=False)
    xlsx = path.with_suffix(".xlsx")
    if xlsx.exists():
        return pd.read_excel(xlsx, sheet_name="BUSINESS_READY_ALL_MONTHS", dtype=str)
    return pd.DataFrame()


def write_filtered_package(
    *,
    issuer: str,
    year: str,
    reporting_year: str,
    raw_df: pd.DataFrame,
    business_df: pd.DataFrame,
) -> dict[str, Any]:
    """Write filtered comparison outputs for one issuer/year."""
    root = filtered_review_root() / issuer / year
    root.mkdir(parents=True, exist_ok=True)

    raw_audit = build_filter_audit(raw_df, issuer=issuer, reporting_year=reporting_year, dataset_label="RAW")
    biz_audit = build_filter_audit(
        business_df, issuer=issuer, reporting_year=reporting_year, dataset_label="BUSINESS_READY",
    )
    filter_audit = pd.concat([raw_audit, biz_audit], ignore_index=True)

    raw_filtered = apply_prior_year_benefit_filter(raw_df, reporting_year=reporting_year)
    biz_filtered = apply_prior_year_benefit_filter(business_df, reporting_year=reporting_year)

    summary_df, summary_md, by_year_df = build_filter_summary(
        raw_audit, biz_audit, reporting_year=reporting_year,
    )

    comparison_xlsx = root / "filtered_comparison.xlsx"
    write_full_export_excel(
        comparison_xlsx,
        {
            "README": pd.DataFrame({
                "note": [
                    f"Prior-year benefit filter for reporting year {reporting_year}.",
                    "RAW_FILTERED and BUSINESS_READY_FILTERED exclude benefit_effective_year < reporting_year.",
                    "Null benefit_effective_date rows are kept and flagged in FILTER_AUDIT.",
                    "Original unfiltered exports are not modified.",
                ],
            }),
            "RAW_FILTERED": raw_filtered,
            "BUSINESS_READY_FILTERED": biz_filtered,
            "FILTER_AUDIT": filter_audit,
            "FILTER_SUMMARY": summary_df,
            "COUNTS_BY_BENEFIT_YEAR": by_year_df,
        },
        csv_fallback_paths={
            "RAW_FILTERED": root / "raw_filtered.csv",
            "BUSINESS_READY_FILTERED": root / "business_ready_filtered.csv",
        },
    )
    safe_write_csv(root / "raw_filtered.csv", raw_filtered, table_name="RAW_FILTERED",
                   drop_duplicate_value_columns=False)
    safe_write_csv(
        root / "business_ready_filtered.csv", biz_filtered,
        table_name="BUSINESS_READY_FILTERED", drop_duplicate_value_columns=False,
    )

    readme = root / "README.md"
    readme.write_text(summary_md + f"\n## Files\n\n- `filtered_comparison.xlsx`\n", encoding="utf-8")

    return {
        "issuer": issuer,
        "year": year,
        "reporting_year": reporting_year,
        "comparison_xlsx": str(comparison_xlsx),
        "raw_before": len(raw_df),
        "raw_after": len(raw_filtered),
        "business_before": len(business_df),
        "business_after": len(biz_filtered),
        "excluded": int((filter_audit["filter_action"] == "EXCLUDE").sum()),
        "filter_audit": filter_audit,
        "summary_df": summary_df,
        "summary_md": summary_md,
        "by_year_df": by_year_df,
    }


def write_global_diagnostics(results: list[dict[str, Any]]) -> dict[str, str]:
    """Write consolidated debug audit xlsx and summary md."""
    if not results:
        return {}

    all_audit = pd.concat([r["filter_audit"] for r in results], ignore_index=True)
    all_summary = pd.concat([r["summary_df"] for r in results], ignore_index=True)
    by_year_parts = [r["by_year_df"].assign(reporting_year=r["reporting_year"]) for r in results]
    all_by_year = pd.concat(by_year_parts, ignore_index=True) if by_year_parts else pd.DataFrame()

    audit_path = _debug_dir() / "prior_year_benefit_filter_audit.xlsx"
    safe_write_excel(
        audit_path,
        {
            "FILTER_AUDIT": all_audit,
            "SUMMARY": all_summary,
            "COUNTS_BY_BENEFIT_YEAR": all_by_year,
        },
        drop_duplicate_value_columns=False,
    )

    md_parts = [r["summary_md"] for r in results]
    md_path = _debug_dir() / "prior_year_benefit_filter_summary.md"
    md_path.write_text("\n\n---\n\n".join(md_parts), encoding="utf-8")

    logger.info("Wrote filter diagnostics → %s, %s", audit_path, md_path)
    return {"audit_xlsx": str(audit_path), "summary_md": str(md_path)}


def run_prior_year_benefit_filter(
    *,
    issuer: str,
    year: str,
    reporting_year: str | None = None,
) -> dict[str, Any]:
    """Run filtered export for one issuer/year using existing unfiltered exports as input."""
    ry = reporting_year or resolve_reporting_year(partition_year=year)
    logger.info(
        "Prior-year benefit filter: issuer=%s partition_year=%s reporting_year=%s",
        issuer, year, ry,
    )

    raw_df = _load_raw_export(issuer, year)
    business_df = _load_business_ready_export(issuer, year)
    if raw_df.empty and business_df.empty:
        raise RuntimeError(
            f"No raw or business-ready exports found for {issuer}/{year}. "
            "Run full_data_exports and business_ready_exports first."
        )

    result = write_filtered_package(
        issuer=issuer,
        year=year,
        reporting_year=ry,
        raw_df=raw_df,
        business_df=business_df,
    )
    return result


def run_prior_year_benefit_filter_batch(
    pairs: list[tuple[str, str]],
    *,
    reporting_year_override: str | None = None,
) -> dict[str, Any]:
    """Run filter for multiple issuer/year pairs; write global diagnostics."""
    results: list[dict[str, Any]] = []
    for issuer, year in pairs:
        try:
            ry = reporting_year_override or resolve_reporting_year(partition_year=year)
            results.append(run_prior_year_benefit_filter(issuer=issuer, year=year, reporting_year=ry))
        except Exception as exc:
            logger.error("Filter failed for %s/%s: %s", issuer, year, exc)

    diagnostics = write_global_diagnostics(results)
    return {
        "issuer_years": len(results),
        "results": results,
        "diagnostics": diagnostics,
        "output_root": str(filtered_review_root()),
    }
