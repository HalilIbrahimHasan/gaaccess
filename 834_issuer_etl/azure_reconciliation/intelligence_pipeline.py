"""
Azure Intelligence pipeline — full production run with self-validation.

Additive extension. Does not modify SFTP, source_data, or XML parser.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.azure_client import azure_configured, connect_azure, list_table_columns
from azure_reconciliation.azure_fetch import (
    AzureFetchPlan,
    discover_and_select,
    fetch_issuer_year_bulk,
    fetch_with_full_fallback,
)
from azure_reconciliation.azure_lifecycle_engine import (
    azure_final_snapshot,
    build_all_azure_lifecycle_snapshots,
    normalize_azure_events,
)
from azure_reconciliation.business_dashboard import generate_business_dashboard
from azure_reconciliation.column_mapper import build_column_mapping, mapping_report_sheets
from azure_reconciliation.comparison_engine import aggregate_level_stats, compare_at_level
from azure_reconciliation.dashboard import generate_reconciliation_dashboard
from azure_reconciliation.discovery_engine import DiscoverySelection
from azure_reconciliation.excel_exporter import build_comparison_workbook, write_excel_report
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots, replay_lifecycle
from azure_reconciliation.mapping_report import write_column_mapping_html
from azure_reconciliation.metadata_engine import generate_metadata
from azure_reconciliation.reconciler import issuer_month_summary
from azure_reconciliation.self_validation import print_final_validation, validate_run
from azure_reconciliation.source_coverage import discover_source_coverage, coverage_summary_dict
from azure_reconciliation.sqlite_store import ReconciliationStore
from azure_reconciliation.xml_loader import load_xml_rows, xml_column_inventory
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _output_dirs() -> dict[str, Path]:
    root = settings.outputs_path
    dirs = {
        "root": root,
        "excel": root / "excel",
        "sqlite": root / "sqlite",
        "dashboard": root / "dashboard",
        "reports": root / "reports",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


def run_intelligence_pipeline(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    prefer_staging: bool = True,
    skip_azure: bool = False,
) -> dict[str, Any]:
    """Execute complete Azure intelligence pipeline (13 steps)."""
    t0 = time.perf_counter()
    settings.refresh_from_env()
    dirs = _output_dirs()
    azure_requested = settings.azure_enabled and not skip_azure
    stats: dict[str, Any] = {
        "azure_requested": azure_requested,
        "azure_enabled": settings.azure_enabled,
        "skip_azure": skip_azure,
    }

    # 1. Read source_data coverage
    coverage = discover_source_coverage(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    partitions = coverage.partitions
    cov = coverage_summary_dict(coverage)
    stats.update({
        "partitions": len(partitions),
        "source_issuers": cov["issuers"],
        "source_years": cov["years"],
        "source_months": cov["months"],
        "source_files": cov["file_count"],
    })
    if not partitions:
        logger.warning("No source_data partitions — stopping")
        stats["validation"] = validate_run(stats, require_azure=azure_requested)
        print_final_validation(stats)
        if azure_requested:
            raise RuntimeError("ENABLE_AZURE=true but no source_data partitions discovered")
        return stats

    # 2–3. XML profile + lifecycle
    xml_raw = load_xml_rows(
        prefer_staging=prefer_staging,
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    stats["xml_raw_rows"] = len(xml_raw)
    xml_lifecycle = pd.DataFrame()
    if settings.xml_lifecycle_enabled:
        xml_lifecycle = build_all_lifecycle_snapshots(xml_raw, partitions)
    stats["xml_lifecycle_rows"] = len(xml_lifecycle)

    engine = None
    azure_raw = pd.DataFrame()
    azure_lifecycle = pd.DataFrame()
    discovery_by_issuer: dict[str, DiscoverySelection] = {}
    plans_by_issuer: dict[str, AzureFetchPlan] = {}
    azure_cols: list[str] = []

    # 4–6. Azure connect + discover + select
    if azure_requested:
        logger.info("AZURE STAGE STARTED")
        print("AZURE STAGE STARTED")
        if not azure_configured():
            msg = (
                "ENABLE_AZURE=true but Azure credentials missing — "
                "set SERVER, DATABASE, and USERNAME in .env"
            )
            logger.error(msg)
            stats["azure_error"] = msg
            stats["azure_available"] = False
            stats["execution_seconds"] = round(time.perf_counter() - t0, 2)
            stats["validation"] = validate_run(stats, require_azure=True)
            print_final_validation(stats)
            raise RuntimeError(msg)

        engine, conn_meta = connect_azure(runner_name="run_intelligence_pipeline", strict=True)
        stats["azure_available"] = True
        stats["connection"] = conn_meta

        for issuer in coverage.issuers:
            plan, selection = discover_and_select(engine, issuer=issuer, partitions=partitions)
            plans_by_issuer[issuer] = plan
            discovery_by_issuer[issuer] = selection
            if not azure_cols and plan.table:
                azure_cols = list_table_columns(engine, plan.schema, plan.table)

        primary = next(iter(plans_by_issuer.values()), None)
        if primary and primary.table:
            stats["azure_selected_table"] = primary.full_name
            stats["azure_selected_strategy"] = primary.strategy_id
            stats["azure_confidence_score"] = primary.confidence_score
        else:
            msg = "Azure discovery did not select any table — no usable Azure candidates"
            logger.error(msg)
            stats["azure_error"] = msg
            stats["execution_seconds"] = round(time.perf_counter() - t0, 2)
            stats["validation"] = validate_run(stats, require_azure=True)
            print_final_validation(stats)
            raise RuntimeError(msg)

        # 7–8. Extract Azure + build lifecycle
        raw_frames: list[pd.DataFrame] = []
        for issuer in coverage.issuers:
            plan = plans_by_issuer.get(issuer)
            if not plan or not plan.table:
                continue
            years = sorted({p.year for p in partitions if p.issuer == issuer})
            bulk = fetch_issuer_year_bulk(engine, plan, issuer=issuer, years=years)
            if bulk.empty:
                for part in [p for p in partitions if p.issuer == issuer]:
                    part_df, used_plan, _ = fetch_with_full_fallback(
                        engine, part, partitions=partitions, primary_plan=plan,
                        selection=discovery_by_issuer.get(issuer),
                    )
                    if not part_df.empty:
                        bulk = pd.concat([bulk, part_df], ignore_index=True) if not bulk.empty else part_df
                        plans_by_issuer[issuer] = used_plan
            if not bulk.empty:
                bulk["_fetch_issuer"] = issuer
                raw_frames.append(bulk)

        if raw_frames:
            azure_raw = pd.concat(raw_frames, ignore_index=True)
        stats["azure_raw_rows"] = len(azure_raw)

        if stats["azure_raw_rows"] == 0:
            msg = "Azure stage completed but zero rows fetched — check table/strategy selection and source_data scope"
            logger.error(msg)
            stats["azure_error"] = msg

        mapping_pre = build_column_mapping(xml_column_inventory(xml_raw), azure_cols)
        if settings.azure_lifecycle_enabled and not azure_raw.empty:
            primary = next(iter(plans_by_issuer.values()))
            az_events = normalize_azure_events(
                azure_raw, mapping_pre,
                date_col=primary.date_col if primary else None,
                source_table=primary.full_name if primary else "",
            )
            azure_lifecycle = build_all_azure_lifecycle_snapshots(
                az_events, partitions, date_col=primary.date_col if primary else None,
            )
        stats["azure_lifecycle_rows"] = len(azure_lifecycle)
    else:
        stats["azure_available"] = False
        logger.info("Azure stage skipped (azure_enabled=%s skip_azure=%s)", settings.azure_enabled, skip_azure)

    mapping = build_column_mapping(xml_column_inventory(xml_raw), azure_cols)

    # Comparisons at multiple levels
    all_details: list[pd.DataFrame] = []
    all_summaries: list[pd.DataFrame] = []

    for part in partitions:
        xml_life = replay_lifecycle(xml_raw, part) if settings.xml_lifecycle_enabled else pd.DataFrame()
        az_life = pd.DataFrame()
        az_snap = pd.DataFrame()
        az_events_part = pd.DataFrame()

        if not azure_lifecycle.empty:
            az_life = azure_lifecycle[
                (azure_lifecycle["issuer"].astype(str) == part.issuer)
                & (azure_lifecycle["coverage_year"].astype(str) == part.year)
                & (azure_lifecycle["snapshot_month"].astype(str).str.zfill(2) == str(part.month).zfill(2))
            ].copy()
            az_snap = azure_final_snapshot(azure_lifecycle, part)

        if engine and stats.get("azure_available"):
            plan = plans_by_issuer.get(part.issuer)
            if plan:
                az_events_part, _, _ = fetch_with_full_fallback(
                    engine, part, partitions=partitions, primary_plan=plan,
                    selection=discovery_by_issuer.get(part.issuer),
                )
                if not az_events_part.empty:
                    az_events_part = normalize_azure_events(
                        az_events_part, mapping, date_col=plan.date_col, source_table=plan.full_name,
                    )

        if settings.xml_lifecycle_enabled and not xml_life.empty and not az_events_part.empty:
            d, s = compare_at_level(xml_life, az_events_part, mapping, partition_label=part.label(), level="event")
            all_details.append(d)
            all_summaries.append(s)

        if settings.xml_lifecycle_enabled and settings.azure_lifecycle_enabled:
            if not xml_life.empty and not az_life.empty:
                d, s = compare_at_level(xml_life, az_life, mapping, partition_label=part.label(), level="lifecycle")
                all_details.append(d)
                all_summaries.append(s)

            if not xml_life.empty and not az_snap.empty:
                d, s = compare_at_level(
                    xml_life, az_snap, mapping,
                    partition_label=part.label(), level="xml_lifecycle_vs_azure_snapshot",
                )
                all_details.append(d)
                all_summaries.append(s)

            if not az_life.empty and not az_snap.empty:
                d, s = compare_at_level(
                    az_life, az_snap, mapping,
                    partition_label=part.label(), level="azure_lifecycle_vs_azure_snapshot",
                )
                all_details.append(d)
                all_summaries.append(s)

    detail_df = pd.concat(all_details, ignore_index=True) if all_details else pd.DataFrame()
    summary_df = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()
    level_stats = aggregate_level_stats(all_summaries)

    stats["event_level_match_count"] = 0
    stats["lifecycle_level_match_count"] = 0
    if not summary_df.empty and "comparison_level" in summary_df.columns:
        for level in summary_df["comparison_level"].unique():
            sub = summary_df[summary_df["comparison_level"] == level]
            if level == "event":
                stats["event_level_match_count"] = int(sub["matched_keys"].sum())
            elif level == "lifecycle":
                stats["lifecycle_level_match_count"] = int(sub["matched_keys"].sum())
        stats["status_difference_count"] = int(summary_df["status_differences"].sum())
        stats["xml_not_in_azure_count"] = int(summary_df["xml_not_in_azure"].sum())
        stats["azure_not_in_xml_count"] = int(summary_df["azure_not_in_xml"].sum())
    else:
        stats["status_difference_count"] = 0
        stats["xml_not_in_azure_count"] = 0
        stats["azure_not_in_xml_count"] = 0

    # 10–12. Outputs
    store_path = dirs["sqlite"] / "intelligence.db"
    if settings.sqlite_output_enabled:
        store = ReconciliationStore(store_path)
        if not xml_lifecycle.empty:
            store.replace_table("xml_snapshot", xml_lifecycle)
        if not azure_lifecycle.empty:
            store.replace_table("azure_snapshot", azure_lifecycle)
        if not detail_df.empty:
            store.replace_table("comparison_detail", detail_df.head(5000))
        if not summary_df.empty:
            store.replace_table("comparison_summary", summary_df)
        stats["output_sqlite_path"] = str(store_path)

    excel_path = dirs["excel"] / "azure_xml_comparison.xlsx"
    if settings.excel_output_enabled:
        mapping_sheets = mapping_report_sheets(mapping)
        workbook = build_comparison_workbook(
            final_summary=summary_df,
            status_summary=level_stats,
            issuer_month=issuer_month_summary(summary_df) if not summary_df.empty else pd.DataFrame(),
            xml_summary=xml_lifecycle.head(1000) if not xml_lifecycle.empty else pd.DataFrame(),
            azure_summary=azure_lifecycle.head(1000) if not azure_lifecycle.empty else pd.DataFrame(),
            match_sample=detail_df.head(500),
            status_diff=detail_df[detail_df.get("status_match") == False] if "status_match" in detail_df.columns else pd.DataFrame(),  # noqa: E712
            xml_not_in_azure=detail_df[detail_df.get("match_type") == "XML_ONLY"] if "match_type" in detail_df.columns else pd.DataFrame(),
            azure_not_in_xml=detail_df[detail_df.get("match_type") == "AZURE_ONLY"] if "match_type" in detail_df.columns else pd.DataFrame(),
            detailed=detail_df,
            lifecycle_summary=xml_lifecycle.groupby(
                [c for c in ["issuer", "coverage_year", "snapshot_month", "canonical_status"] if c in xml_lifecycle.columns],
                dropna=False,
            ).size().reset_index(name="count") if not xml_lifecycle.empty else pd.DataFrame(),
            column_mapping=mapping_sheets.get("mappings", pd.DataFrame()),
        )
        write_excel_report(excel_path, workbook)
        if settings.excel_output_enabled:
            write_excel_report(dirs["excel"] / "column_mapping_report.xlsx", mapping_sheets)
        stats["output_excel_path"] = str(excel_path)

    recon_dash = dirs["dashboard"] / "reconciliation_dashboard.html"
    life_dash = dirs["dashboard"] / "lifecycle_dashboard.html"
    biz_dash = dirs["dashboard"] / "business_dashboard.html"

    if settings.html_output_enabled:
        write_column_mapping_html(
            dirs["reports"] / "column_mapping_report.html",
            mapping,
            xml_row_count=len(xml_raw),
            azure_row_count=len(azure_raw),
        )
        generate_reconciliation_dashboard(summary_df, level_stats, recon_dash)
        generate_reconciliation_dashboard(
            xml_lifecycle if not xml_lifecycle.empty else summary_df,
            level_stats, life_dash, title="Lifecycle Dashboard",
        )
        generate_business_dashboard(
            xml_lifecycle=xml_lifecycle,
            azure_lifecycle=azure_lifecycle,
            comparison_summary=summary_df,
            level_stats=level_stats,
            output_path=biz_dash,
        )
        stats["output_dashboard_path"] = str(biz_dash)
        stats["reconciliation_dashboard_path"] = str(recon_dash)
        stats["lifecycle_dashboard_path"] = str(life_dash)

    # Metadata
    primary_discovery = next(iter(discovery_by_issuer.values()), None) if discovery_by_issuer else None
    if settings.metadata_enabled:
        meta_paths = generate_metadata(
            coverage=coverage,
            xml_raw=xml_raw,
            xml_lifecycle=xml_lifecycle,
            azure_raw=azure_raw,
            azure_lifecycle=azure_lifecycle,
            discovery=primary_discovery,
            run_stats=stats,
        )
        stats.update(meta_paths)

    stats["execution_seconds"] = round(time.perf_counter() - t0, 2)
    require_azure = bool(stats.get("azure_requested"))
    stats["validation"] = validate_run(stats, require_azure=require_azure)
    print_final_validation(stats)

    if require_azure and not stats["validation"].get("all_passed", True):
        err = stats.get("azure_error") or "Azure validation failed — see logs"
        raise RuntimeError(err)

    return stats
