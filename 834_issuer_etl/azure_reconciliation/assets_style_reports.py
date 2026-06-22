"""
Assets-style Model H reports under assets/{issuer}/.../reports/.

Mirrors the legacy practical navigation layout while using XML-only Model H output.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_excel,
    safe_write_html_report,
)
from azure_reconciliation.xml_business_reports import (
    BUSINESS_SUMMARY_COLS,
    IssuerBusinessResult,
    yearly_rollup,
)
from config.config import settings
from utils.logger import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


def assets_reports_root() -> Path:
    d = settings.assets_path
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _filter_partition_df(
    df: pd.DataFrame,
    part: Partition,
    *,
    kind: str = "canonical",
) -> pd.DataFrame:
    if df.empty:
        return df
    if kind == "lifecycle_snapshot":
        mask = df["issuer"].astype(str) == str(part.issuer)
        if "coverage_year" in df.columns:
            mask &= df["coverage_year"].astype(str) == str(part.year)
        if "snapshot_month" in df.columns:
            mask &= df["snapshot_month"].astype(str).map(_zmonth) == _zmonth(part.month)
        return df[mask].copy()
    if kind == "xml_raw":
        mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(part.issuer)
        if "year" in df.columns:
            mask &= df["year"].astype(str) == str(part.year)
        if "month" in df.columns:
            mask &= df["month"].astype(str).map(_zmonth) == _zmonth(part.month)
        return df[mask].copy()
    mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(part.issuer)
    if "year" in df.columns:
        mask &= df["year"].astype(str) == str(part.year)
    if "month" in df.columns:
        mask &= df["month"].astype(str).map(_zmonth) == _zmonth(part.month)
    return df[mask].copy()


def _filter_year_df(df: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df["issuer"].astype(str) == str(issuer)
    if "year" in df.columns:
        mask &= df["year"].astype(str) == str(year)
    return df[mask].copy()


def _partition_cleanup_summary(result: IssuerBusinessResult, part: Partition) -> pd.DataFrame:
    raw = _filter_partition_df(result.xml_raw, part, kind="xml_raw")
    dup = _filter_partition_df(result.duplicate_df, part)
    maint = _filter_partition_df(result.maintenance_df, part)
    sup = _filter_partition_df(result.superseded_df, part)
    canon = _filter_partition_df(result.canonical, part)
    life = _filter_partition_df(result.lifecycle_input, part)
    return pd.DataFrame([{
        "issuer": part.issuer,
        "year": part.year,
        "month": _zmonth(part.month),
        "raw_xml_rows": len(raw),
        "canonical_rows": len(canon),
        "lifecycle_rows": len(life),
        "duplicate_count": len(dup),
        "maintenance_only_count": len(maint),
        "superseded_count": len(sup),
    }])


def _issuer_cleanup_summary(result: IssuerBusinessResult) -> pd.DataFrame:
    if not result.cleanup_summary.empty:
        return result.cleanup_summary.assign(issuer=result.issuer)
    return pd.DataFrame([{
        "issuer": result.issuer,
        "raw_canonical_rows": len(result.canonical),
        "duplicate_count": len(result.duplicate_df),
        "maintenance_only_count": len(result.maintenance_df),
        "superseded_count": len(result.superseded_df),
        "lifecycle_collapsed_count": len(result.lifecycle_input),
    }])


def issuer_all_years_rollup(monthly: pd.DataFrame) -> pd.DataFrame:
    """Aggregate all years/months to issuer grain."""
    if monthly.empty:
        return monthly
    sum_cols = [
        "Enrollment_Count", "Enrollee_Count", "Subscriber_Count",
        "Raw_XML_Row_Count", "Canonical_Row_Count", "Latest_State_Record_Count",
        "Duplicate_Count", "Maintenance_Only_Count", "Superseded_Count",
    ]
    keys = ["issuer", "insurance_type", "status"]
    agg = {c: "sum" for c in sum_cols if c in monthly.columns}
    out = monthly.groupby(keys, dropna=False).agg(agg).reset_index()
    out["year"] = "ALL"
    out["month"] = "ALL"
    if "Month_Basis_Used" in monthly.columns:
        basis = monthly.groupby(keys, dropna=False)["Month_Basis_Used"].agg(
            lambda s: s.mode().iloc[0] if not s.mode().empty else ""
        ).reset_index()
        out = out.merge(basis, on=keys, how="left")
    return out[[c for c in BUSINESS_SUMMARY_COLS if c in out.columns]]


def _write_assets_bundle(
    reports_dir: Path,
    *,
    prefix: str,
    summary: pd.DataFrame,
    detail: pd.DataFrame,
    export_errors: ExportErrors | None = None,
    processing_html: str = "",
) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    safe_write_csv(reports_dir / f"{prefix}_detail.csv", detail, export_errors=export_errors)
    safe_write_excel(
        reports_dir / f"{prefix}_rollup.xlsx",
        {"summary": summary},
        drop_duplicate_value_columns=False,
        export_errors=export_errors,
    )
    safe_write_html_report(
        reports_dir / f"{prefix}_rollup.html",
        title=prefix.replace("_", " ").title(),
        summary_df=summary,
        detail_df=detail.head(500) if not detail.empty else pd.DataFrame(),
        extra_html=processing_html,
        export_errors=export_errors,
    )


def export_assets_style_reports(
    results: list[IssuerBusinessResult],
    *,
    export_errors: ExportErrors | None = None,
) -> Path:
    """Write Model H reports under assets/{issuer}/.../reports/."""
    from azure_reconciliation.business_output_validation import (
        build_business_processing_summary,
        lifecycle_snapshot_export_df,
        processing_summary_html,
    )

    root = assets_reports_root()

    for result in results:
        issuer = result.issuer
        issuer_dir = root / issuer / "reports"
        issuer_proc = processing_summary_html(build_business_processing_summary(result))
        all_years_sum = issuer_all_years_rollup(result.business_monthly)
        all_years_detail = result.lifecycle_input.copy()
        _write_assets_bundle(
            issuer_dir,
            prefix="issuer_all_years",
            summary=all_years_sum,
            detail=all_years_detail,
            export_errors=export_errors,
            processing_html=issuer_proc,
        )
        safe_write_excel(
            issuer_dir / "cleanup_summary.xlsx",
            {"cleanup_summary": _issuer_cleanup_summary(result)},
            export_errors=export_errors,
        )
        safe_write_html_report(
            issuer_dir / "cleanup_summary.html",
            title=f"Issuer {issuer} — cleanup summary",
            summary_df=_issuer_cleanup_summary(result),
            export_errors=export_errors,
        )

        years = sorted({p.year for p in result.partitions})
        for year in years:
            year_reports = root / issuer / str(year) / "reports"
            year_monthly = _filter_year_df(result.business_monthly, issuer, year)
            year_sum = yearly_rollup(year_monthly)
            year_detail = _filter_year_df(result.lifecycle_input, issuer, year)
            year_proc = processing_summary_html(
                build_business_processing_summary(
                    result,
                    next((p for p in result.partitions if p.year == year), None),
                )
            )
            _write_assets_bundle(
                year_reports,
                prefix="issuer_year",
                summary=year_sum,
                detail=year_detail,
                export_errors=export_errors,
                processing_html=year_proc,
            )
            year_cleanup = pd.concat(
                [_partition_cleanup_summary(result, p) for p in result.partitions if p.year == year],
                ignore_index=True,
            )
            safe_write_excel(
                year_reports / "cleanup_summary.xlsx",
                {"cleanup_summary": year_cleanup},
                export_errors=export_errors,
            )
            safe_write_html_report(
                year_reports / "cleanup_summary.html",
                title=f"Issuer {issuer} {year} — cleanup summary",
                summary_df=year_cleanup,
                export_errors=export_errors,
            )

            for part in sorted(
                (p for p in result.partitions if p.year == year),
                key=lambda p: (p.year, p.month),
            ):
                month_reports = root / issuer / str(year) / _zmonth(part.month) / "reports"
                month_reports.mkdir(parents=True, exist_ok=True)

                month_sum = year_monthly[
                    year_monthly["month"].astype(str).map(_zmonth) == _zmonth(part.month)
                ]
                month_detail = _filter_partition_df(result.lifecycle_input, part)
                xml_detail = _filter_partition_df(result.canonical, part)

                month_proc = processing_summary_html(
                    build_business_processing_summary(result, part),
                )

                safe_write_excel(
                    month_reports / "xml_month_summary.xlsx",
                    {"summary": month_sum},
                    drop_duplicate_value_columns=False,
                    export_errors=export_errors,
                )
                safe_write_html_report(
                    month_reports / "xml_month_summary.html",
                    title=f"{issuer}/{year}/{_zmonth(part.month)} — XML month summary",
                    summary_df=month_sum,
                    detail_df=xml_detail.head(500),
                    extra_html=month_proc,
                    export_errors=export_errors,
                )
                safe_write_csv(month_reports / "xml_month_detail.csv", xml_detail, export_errors=export_errors)

                safe_write_excel(
                    month_reports / "model_h_month_summary.xlsx",
                    {"summary": month_sum},
                    drop_duplicate_value_columns=False,
                    export_errors=export_errors,
                )
                safe_write_html_report(
                    month_reports / "model_h_month_summary.html",
                    title=f"{issuer}/{year}/{_zmonth(part.month)} — Model H month summary",
                    summary_df=month_sum,
                    detail_df=month_detail.head(500),
                    extra_html=month_proc,
                    export_errors=export_errors,
                )

                part_cleanup = _partition_cleanup_summary(result, part)
                safe_write_excel(
                    month_reports / "cleanup_summary.xlsx",
                    {"cleanup_summary": part_cleanup},
                    export_errors=export_errors,
                )
                safe_write_html_report(
                    month_reports / "cleanup_summary.html",
                    title=f"{issuer}/{year}/{_zmonth(part.month)} — cleanup",
                    summary_df=part_cleanup,
                    export_errors=export_errors,
                )

                safe_write_csv(
                    month_reports / "duplicate_transactions.csv",
                    _filter_partition_df(result.duplicate_df, part),
                    export_errors=export_errors,
                )
                safe_write_csv(
                    month_reports / "maintenance_only_events.csv",
                    _filter_partition_df(result.maintenance_df, part),
                    export_errors=export_errors,
                )
                safe_write_csv(
                    month_reports / "superseded_events.csv",
                    _filter_partition_df(result.superseded_df, part),
                    export_errors=export_errors,
                )
                safe_write_csv(
                    month_reports / "lifecycle_snapshot.csv",
                    lifecycle_snapshot_export_df(
                        _filter_partition_df(result.lifecycle_snapshots, part, kind="lifecycle_snapshot"),
                    ),
                    export_errors=export_errors,
                )

        logger.info("Wrote assets-style reports for issuer %s", issuer)

    write_assets_report_index(results)
    return root


def write_assets_report_index(results: list[IssuerBusinessResult]) -> Path:
    """Top-level assets/report_index.html linking all issuer/year/month reports."""
    root = assets_reports_root()
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Assets Report Index</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:2rem}",
        "ul{line-height:1.8} a{text-decoration:none;color:#0645ad}",
        "h2{margin-top:1.5rem}</style>",
        "</head><body>",
        "<h1>Assets Report Index (Model H)</h1>",
        f"<p>Generated: {generated}</p>",
        "<p>Also see centralized reports: "
        "<a href='../outputs/xml_business_reports/index.html'>outputs/xml_business_reports/</a></p>",
    ]

    for result in sorted(results, key=lambda r: r.issuer):
        issuer = result.issuer
        lines.append(f"<h2>Issuer {issuer}</h2><ul>")
        lines.append(
            f"<li><a href='{issuer}/reports/issuer_all_years_rollup.html'>"
            f"All-years rollup</a></li>"
        )
        lines.append(
            f"<li><a href='{issuer}/reports/cleanup_summary.html'>"
            f"Issuer cleanup summary</a></li>"
        )
        years = sorted({p.year for p in result.partitions})
        for year in years:
            lines.append(
                f"<li><a href='{issuer}/{year}/reports/issuer_year_rollup.html'>"
                f"Year {year} rollup</a></li>"
            )
            lines.append("<ul>")
            for part in sorted(
                (p for p in result.partitions if p.year == year),
                key=lambda p: p.month,
            ):
                m = _zmonth(part.month)
                lines.append(
                    f"<li><a href='{issuer}/{year}/{m}/reports/model_h_month_summary.html'>"
                    f"{year}/{m} Model H month</a> | "
                    f"<a href='{issuer}/{year}/{m}/reports/xml_month_summary.html'>XML month</a> | "
                    f"<a href='{issuer}/{year}/{m}/reports/cleanup_summary.html'>cleanup</a></li>"
                )
            lines.append("</ul>")
        lines.append("</ul>")

    lines.append("</body></html>")
    index_path = root / "report_index.html"
    index_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote assets report index → %s", index_path)
    return index_path
