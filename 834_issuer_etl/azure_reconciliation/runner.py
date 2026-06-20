"""
Azure vs XML enrollment reconciliation runner.

Extension only — does not modify existing pipeline, SFTP, or parser behavior.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.azure_client import (
    azure_configured,
    fetch_enrollments_partition,
    get_engine,
    list_available_tables,
    list_table_columns,
)
from azure_reconciliation.column_mapper import build_column_mapping, mapping_report_sheets
from azure_reconciliation.dashboard import generate_reconciliation_dashboard
from azure_reconciliation.excel_exporter import build_comparison_workbook, write_excel_report
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots, replay_lifecycle
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.reconciler import compare_snapshots, issuer_month_summary
from azure_reconciliation.sqlite_store import ReconciliationStore
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from azure_reconciliation.xml_loader import load_xml_rows, xml_column_inventory
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

OUTPUTS_ROOT = settings.project_root / "outputs" / "reconciliation"


def _ensure_output_dirs() -> dict[str, Path]:
    dirs = {
        "root": OUTPUTS_ROOT,
        "excel": OUTPUTS_ROOT / "excel",
        "dashboards": OUTPUTS_ROOT / "dashboards",
        "sqlite": OUTPUTS_ROOT / "sqlite",
        "reports": OUTPUTS_ROOT / "reports",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def _azure_snapshot_from_raw(
    az_df: pd.DataFrame,
    partition: Partition,
    mapping,
) -> pd.DataFrame:
    if az_df.empty:
        return pd.DataFrame()
    from azure_reconciliation.column_mapper import rename_to_canonical

    az = rename_to_canonical(az_df, mapping, "azure")
    az["canonical_status"] = az.apply(
        lambda r: normalize_status(
            str(r.get("enrollment_status") or r.get("enrollee_status_description") or "")
        ),
        axis=1,
    )
    if "insurance_type" in az.columns:
        az["insurance_type"] = az["insurance_type"].apply(normalize_insurance_type)
    az["coverage_year"] = partition.year
    az["snapshot_month"] = partition.month
    az["issuer"] = az.get("issuer", partition.issuer)
    return az


def run_reconciliation(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    prefer_staging: bool = True,
    skip_azure: bool = False,
) -> dict[str, Any]:
    """
    Execute full Azure vs XML reconciliation for discovered source_data scope.
    """
    t0 = time.perf_counter()
    dirs = _ensure_output_dirs()
    stats: dict[str, Any] = {
        "partitions": 0,
        "xml_raw_rows": 0,
        "xml_lifecycle_rows": 0,
        "azure_raw_rows": 0,
        "matched_rows": 0,
        "status_differences": 0,
        "azure_available": azure_configured() and not skip_azure,
    }

    partitions = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    stats["partitions"] = len(partitions)
    if not partitions:
        logger.warning("No source_data partitions discovered — nothing to reconcile")
        return stats

    # --- Load XML ---
    xml_raw = load_xml_rows(
        prefer_staging=prefer_staging,
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    stats["xml_raw_rows"] = len(xml_raw)
    logger.info("XML raw rows: %d", len(xml_raw))

    xml_lifecycle = build_all_lifecycle_snapshots(xml_raw, partitions)
    stats["xml_lifecycle_rows"] = len(xml_lifecycle)

    # --- Column mapping (inspect both sides) ---
    xml_cols = xml_column_inventory(xml_raw)
    azure_cols: list[str] = []
    engine = None

    if stats["azure_available"]:
        try:
            engine = get_engine()
            from azure_reconciliation.azure_client import _azure_settings

            cfg = _azure_settings()
            azure_cols = list_table_columns(engine, cfg["schema"], cfg["table"])
            available = list_available_tables(engine, cfg["schema"])
            logger.info("Azure tables available: %s", available[:10])
        except Exception as exc:
            logger.error("Azure connection failed: %s", exc)
            stats["azure_available"] = False
            stats["azure_error"] = str(exc)
    else:
        logger.warning("Azure not configured — XML-only reports will be generated")

    mapping = build_column_mapping(xml_cols, azure_cols)
    mapping_sheets = mapping_report_sheets(mapping)
    mapping_path = dirs["excel"] / "column_mapping_report.xlsx"
    write_excel_report(mapping_path, mapping_sheets)
    stats["column_mapping_report"] = str(mapping_path)

    store = ReconciliationStore(dirs["sqlite"] / "reconciliation.db")
    if not xml_lifecycle.empty:
        store.replace_table("xml_snapshot", xml_lifecycle)

    # --- Per-partition Azure fetch + compare ---
    all_details: list[pd.DataFrame] = []
    all_summaries: list[pd.DataFrame] = []
    azure_frames: list[pd.DataFrame] = []

    for part in partitions:
        xml_part = replay_lifecycle(xml_raw, part)
        az_part = pd.DataFrame()

        if engine is not None and stats["azure_available"]:
            try:
                az_raw = fetch_enrollments_partition(
                    engine, issuer=part.issuer, year=part.year, month=part.month
                )
                stats["azure_raw_rows"] += len(az_raw)
                az_part = _azure_snapshot_from_raw(az_raw, part, mapping)
                if not az_part.empty:
                    azure_frames.append(az_part)
            except Exception as exc:
                logger.error("Azure fetch failed for %s: %s", part.label(), exc)

        detail, summary = compare_snapshots(
            xml_part, az_part, mapping, partition_label=part.label()
        )
        if not detail.empty:
            all_details.append(detail)
        if not summary.empty:
            all_summaries.append(summary)

    if azure_frames:
        azure_all = pd.concat(azure_frames, ignore_index=True)
        store.replace_table("azure_snapshot", azure_all)

    detail_df = pd.concat(all_details, ignore_index=True) if all_details else pd.DataFrame()
    summary_df = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()

    if not summary_df.empty:
        stats["matched_rows"] = int(summary_df["matched_keys"].sum())
        stats["status_differences"] = int(summary_df["status_differences"].sum())

    if not detail_df.empty:
        store.replace_table("comparison_detail", _compact_detail(detail_df))
    if not summary_df.empty:
        store.replace_table("comparison_summary", summary_df)
        im_summary = issuer_month_summary(summary_df)
        store.replace_table("issuer_month_summary", im_summary)

    # --- Excel comparison workbook ---
    status_summary = _status_summary(detail_df)
    xml_summary = _xml_summary(xml_lifecycle)
    azure_summary = _azure_summary(pd.concat(azure_frames, ignore_index=True) if azure_frames else pd.DataFrame())
    match_sample = detail_df.head(500) if not detail_df.empty else pd.DataFrame()
    status_diff = detail_df[detail_df.get("status_match") == False] if "status_match" in detail_df.columns else pd.DataFrame()  # noqa: E712
    xml_only = detail_df[detail_df.get("match_type") == "XML_ONLY"] if "match_type" in detail_df.columns else pd.DataFrame()
    azure_only = detail_df[detail_df.get("match_type") == "AZURE_ONLY"] if "match_type" in detail_df.columns else pd.DataFrame()

    workbook = build_comparison_workbook(
        final_summary=summary_df,
        status_summary=status_summary,
        issuer_month=issuer_month_summary(summary_df) if not summary_df.empty else pd.DataFrame(),
        xml_summary=xml_summary,
        azure_summary=azure_summary,
        match_sample=match_sample,
        status_diff=status_diff,
        xml_not_in_azure=xml_only,
        azure_not_in_xml=azure_only,
        detailed=detail_df,
    )
    comparison_xlsx = dirs["excel"] / "azure_xml_comparison.xlsx"
    write_excel_report(comparison_xlsx, workbook)
    stats["comparison_report"] = str(comparison_xlsx)

    # --- Dashboard ---
    dash_path = dirs["dashboards"] / "reconciliation_dashboard.html"
    generate_reconciliation_dashboard(summary_df, status_summary, dash_path)
    stats["dashboard"] = str(dash_path)

    stats["execution_seconds"] = round(time.perf_counter() - t0, 2)
    logger.info("Reconciliation complete in %.1fs — stats=%s", stats["execution_seconds"], stats)
    return stats


def _compact_detail(detail: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in detail.columns if c in ("partition", "match_type", "status_match", "_join_key")]
    out = detail[cols].copy() if cols else detail.copy()
    if "_join_key" in out.columns:
        out = out.rename(columns={"_join_key": "join_key"})
    if "canonical_status_xml" in detail.columns:
        out["xml_status"] = detail["canonical_status_xml"]
    if "canonical_status_azure" in detail.columns:
        out["azure_status"] = detail["canonical_status_azure"]
    return out


def _status_summary(detail: pd.DataFrame) -> pd.DataFrame:
    if detail.empty or "canonical_status_xml" not in detail.columns:
        return pd.DataFrame()
    return (
        detail.groupby(["canonical_status_xml", "canonical_status_azure"], dropna=False)
        .size()
        .reset_index(name="row_count")
    )


def _xml_summary(lifecycle: pd.DataFrame) -> pd.DataFrame:
    if lifecycle.empty:
        return pd.DataFrame()
    return (
        lifecycle.groupby(
            ["issuer", "coverage_year", "snapshot_month", "insurance_type", "canonical_status"],
            dropna=False,
        )
        .agg(
            enrollment_count=("enrollment_id", "nunique"),
            enrollee_count=("enrollee_id", "nunique"),
            row_count=("enrollee_id", "count"),
        )
        .reset_index()
    )


def _azure_summary(azure: pd.DataFrame) -> pd.DataFrame:
    if azure.empty:
        return pd.DataFrame()
    group_cols = [c for c in ["issuer", "coverage_year", "snapshot_month", "insurance_type", "canonical_status"] if c in azure.columns]
    if not group_cols:
        return pd.DataFrame({"row_count": [len(azure)]})
    return (
        azure.groupby(group_cols, dropna=False)
        .agg(
            enrollment_count=("enrollment_id", "nunique") if "enrollment_id" in azure.columns else ("issuer", "count"),
            enrollee_count=("enrollee_id", "nunique") if "enrollee_id" in azure.columns else ("issuer", "count"),
        )
        .reset_index()
    )
