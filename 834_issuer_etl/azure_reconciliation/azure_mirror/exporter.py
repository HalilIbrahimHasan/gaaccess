"""
Export Azure mirror reports under assets/{issuer}/azurevs/.

Mirrors XML report structure without modifying existing XML outputs.
Uses source_data for partition discovery; never reads XML from assets/.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.azure_client import (
    _azure_settings,
    azure_configured,
    list_table_columns,
    verify_azure_connection,
)
from azure_reconciliation.azure_mirror.columns import log_missing_columns
from azure_reconciliation.azure_mirror.dashboard import (
    generate_kpi_dashboard,
    generate_monthly_dashboard,
    generate_rollup_dashboard,
    write_enrollment_summary_html,
)
from azure_reconciliation.azure_mirror.diagnostics import (
    run_issuer_diagnostics,
    write_no_data_html,
    write_query_diagnostic_workbook,
)
from azure_reconciliation.azure_mirror.kpi_builder import (
    build_enrollment_summary,
    build_monthly_kpi_by_insurance_type,
    build_rollup_summary,
    prepare_azure_frame,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _azurevs_dirs(issuer: str) -> dict[str, Path]:
    base = settings.assets_path / issuer / "azurevs"
    return {
        "base": base,
        "excel": base / "excel",
        "html": base / "html",
        "dashboards": base / "dashboards",
        "sqlite": base / "sqlite",
    }


def _issuers_with_xml_assets() -> set[str]:
    root = settings.assets_path
    if not root.exists():
        return set()
    issuers: set[str] = set()
    for child in root.iterdir():
        if not child.is_dir():
            continue
        has_content = (child / "rollups").exists() or any(
            y.is_dir() and y.name.isdigit() and any(m.is_dir() for m in y.iterdir())
            for y in child.iterdir()
            if y.is_dir()
        )
        if has_content:
            issuers.add(child.name)
    return issuers


def _write_sqlite_snapshot(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    try:
        out = df if not df.empty else pd.DataFrame({"note": ["no azure rows"]})
        out.to_sql("azure_snapshot", conn, index=False)
        conn.commit()
    finally:
        conn.close()
    logger.info("Azure SQLite snapshot written: %s (%d rows)", path, len(df))


def _write_excel_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            export = frame if not frame.empty else pd.DataFrame({"note": ["no data"]})
            export.to_excel(writer, sheet_name=name[:31], index=False)
    logger.info("Azure reports written: %s", path)


def export_azure_mirror_reports(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "azure_available": False,
        "issuers_processed": 0,
        "partitions_queried": 0,
        "azure_rows_total": 0,
    }

    partitions = discover_partitions(
        issuer_filter=issuer_filter or settings.issuer_filter,
        year_filter=year_filter or settings.year_filter,
        month_filter=month_filter or settings.month_filter,
    )
    if not partitions:
        logger.info("No source_data partitions — skipping Azure mirror reports")
        return stats

    xml_asset_issuers = _issuers_with_xml_assets()
    target_issuers = sorted({p.issuer for p in partitions} & xml_asset_issuers)
    if not target_issuers:
        logger.info("No issuers with both source_data and XML assets — skipping Azure mirror")
        return stats

    by_issuer: dict[str, list[Partition]] = {}
    for p in partitions:
        if p.issuer in target_issuers:
            by_issuer.setdefault(p.issuer, []).append(p)

    logger.info("source_data discovered issuers (with XML assets): %s", target_issuers)
    for issuer, parts in by_issuer.items():
        months_by_year: dict[str, list[str]] = {}
        for p in parts:
            months_by_year.setdefault(p.year, []).append(p.month)
        logger.info(
            "source_data discovered years/months for issuer %s: %s",
            issuer,
            {y: sorted(set(ms)) for y, ms in months_by_year.items()},
        )

    if not azure_configured():
        logger.warning(
            "Azure connection failed, XML reports unchanged — set SERVER, DATABASE, and USERNAME in .env"
        )
        return stats

    engine, conn_meta = verify_azure_connection()
    if engine is None:
        logger.warning("Azure connection failed, XML reports unchanged")
        stats["connection"] = conn_meta
        return stats

    cfg = _azure_settings()
    logger.info("Azure query table: %s.%s", cfg["schema"], cfg["table"])

    table_columns = list_table_columns(engine, cfg["schema"], cfg["table"])
    log_missing_columns(table_columns, context=f"{cfg['schema']}.{cfg['table']}")
    stats["azure_available"] = True

    for issuer in target_issuers:
        issuer_parts = by_issuer[issuer]
        dirs = _azurevs_dirs(issuer)
        for d in dirs.values():
            d.mkdir(parents=True, exist_ok=True)

        diagnostic_path = dirs["excel"] / f"azure_query_diagnostic_{issuer}.xlsx"

        # --- Diagnostic pass (issuer+year SQL only; month filter in Python) ---
        diag = run_issuer_diagnostics(
            engine,
            issuer=issuer,
            partitions=issuer_parts,
            table_columns=table_columns,
        )
        write_query_diagnostic_workbook(diagnostic_path, diag["diagnostic_sheets"])

        partition_frames: dict[str, pd.DataFrame] = diag["partition_frames"]
        month_column = diag["month_column"]
        total_month_rows = sum(len(df) for df in partition_frames.values())

        stats["partitions_queried"] += len(issuer_parts)
        stats["azure_rows_total"] += total_month_rows

        if total_month_rows == 0:
            write_no_data_html(
                issuer=issuer,
                partitions=issuer_parts,
                output_path=dirs["html"] / f"azure_no_data_{issuer}.html",
                diagnostic_xlsx=diagnostic_path,
            )
            _write_excel_workbook(
                diagnostic_path.parent / f"azure_status_{issuer}.xlsx",
                {
                    "status": pd.DataFrame([{
                        "issuer": issuer,
                        "message": "No Azure rows returned for issuer/year/month partitions",
                        "diagnostic_workbook": diagnostic_path.name,
                        "month_column_used": month_column or "(none)",
                        "issuer_year_rows": len(diag["combined"]),
                    }]),
                },
            )
            stats["issuers_processed"] += 1
            logger.warning(
                "Azure mirror issuer %s: 0 month-filtered rows — see %s",
                issuer, diagnostic_path,
            )
            continue

        monthly_kpi_frames: list[pd.DataFrame] = []
        enrollment_frames: list[pd.DataFrame] = []
        snapshot_frames: list[pd.DataFrame] = []
        prepared_parts: list[pd.DataFrame] = []

        for part in issuer_parts:
            df = partition_frames.get(part.label(), pd.DataFrame())
            logger.info(
                "Azure rows for reports %s/%s/%s = %d (month via %s)",
                part.issuer, part.year, part.month, len(df), month_column or "none",
            )
            if df.empty:
                continue

            tagged = df.copy()
            tagged["_partition"] = part.label()
            tagged["_month_source_column"] = month_column or ""
            snapshot_frames.append(tagged)
            prepared_parts.append(
                prepare_azure_frame(df, issuer=part.issuer, year=part.year, month=part.month)
            )
            monthly_kpi_frames.append(
                build_monthly_kpi_by_insurance_type(
                    df, issuer=part.issuer, year=part.year, month=part.month
                )
            )
            enrollment_frames.append(
                build_enrollment_summary(
                    df, issuer=part.issuer, year=part.year, month=part.month
                )
            )

        monthly_kpi = (
            pd.concat(monthly_kpi_frames, ignore_index=True)
            if monthly_kpi_frames else pd.DataFrame()
        )
        enrollment_summary = (
            pd.concat(enrollment_frames, ignore_index=True)
            if enrollment_frames else pd.DataFrame()
        )
        snapshot_df = (
            pd.concat(snapshot_frames, ignore_index=True)
            if snapshot_frames else pd.DataFrame()
        )
        rollup_prepared = (
            pd.concat(prepared_parts, ignore_index=True)
            if prepared_parts else pd.DataFrame()
        )
        rollup = build_rollup_summary(rollup_prepared)

        _write_excel_workbook(
            dirs["excel"] / f"azure_enrollment_summary_{issuer}.xlsx",
            {"enrollment_summary": enrollment_summary},
        )
        _write_excel_workbook(
            dirs["excel"] / f"azure_monthly_kpi_{issuer}.xlsx",
            {"monthly_kpi": monthly_kpi, "month_source_column": pd.DataFrame([{"column": month_column}])},
        )
        _write_excel_workbook(
            dirs["excel"] / f"azure_rollup_kpi_{issuer}.xlsx",
            {
                "issuer_totals": rollup["issuer_totals"],
                "year_totals": rollup["year_totals"],
                "insurance_type_totals": rollup["insurance_type_totals"],
                "status_totals": rollup["status_totals"],
                "month_trend": rollup["month_trend"],
                "premium_totals": rollup["premium_totals"],
            },
        )
        _write_excel_workbook(
            dirs["excel"] / f"azure_monthly_summary_{issuer}.xlsx",
            {"monthly_kpi": monthly_kpi, "enrollment_summary": enrollment_summary},
        )
        _write_excel_workbook(
            dirs["excel"] / f"azure_rollup_summary_{issuer}.xlsx",
            rollup,
        )

        write_enrollment_summary_html(
            enrollment_summary,
            issuer=issuer,
            output_path=dirs["html"] / f"azure_enrollment_summary_{issuer}.html",
        )

        generate_monthly_dashboard(
            monthly_kpi, enrollment_summary, issuer=issuer,
            output_path=dirs["dashboards"] / f"azure_monthly_dashboard_{issuer}.html",
        )
        generate_rollup_dashboard(
            rollup, issuer=issuer,
            output_path=dirs["dashboards"] / f"azure_rollup_dashboard_{issuer}.html",
        )
        generate_kpi_dashboard(
            monthly_kpi, rollup, issuer=issuer,
            output_path=dirs["dashboards"] / f"azure_kpi_dashboard_{issuer}.html",
        )
        generate_monthly_dashboard(
            monthly_kpi, enrollment_summary, issuer=issuer,
            output_path=dirs["dashboards"] / f"azure_monthly_kpi_dashboard_{issuer}.html",
        )
        generate_rollup_dashboard(
            rollup, issuer=issuer,
            output_path=dirs["dashboards"] / f"azure_rollup_kpi_dashboard_{issuer}.html",
        )

        _write_sqlite_snapshot(snapshot_df, dirs["sqlite"] / f"azure_snapshot_{issuer}.sqlite")

        stats["issuers_processed"] += 1
        logger.info(
            "Azure mirror reports complete for issuer %s (%d rows, month via %s)",
            issuer, total_month_rows, month_column,
        )

    return stats
