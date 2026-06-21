"""
Lifecycle snapshot comparison — PRIMARY business-level XML vs Azure match.

Raw event streams are diagnostic only. Snapshots collapse multiple transaction
events per member/policy/month to latest business state at Azure grain.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.azure_mirror.discovery.table_inspector import TableProfile
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.record_comparison import (
    JoinMapping,
    PRIMARY_JOIN,
    _compute_record_rates,
    build_canonical_azure_records,
    build_canonical_xml_records,
    compare_records,
)
from azure_reconciliation.safe_export import safe_write_csv, safe_write_html_report
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

LATEST_STATE_JOIN = ["issuer", "policy_id", "member_id", "insurance_type"]

XML_EVENT_EXPLANATION = (
    "XML contains multiple transaction events per member/policy; "
    "lifecycle snapshot comparison is the correct business-level comparison."
)


def _sort_chronological(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    sort_cols: list[str] = []
    for col in (
        "year", "month",
        "member_maint_effective_date",
        "file_event_date",
        "event_date",
        "source_file",
    ):
        if col in work.columns:
            sort_cols.append(col)
    if not sort_cols:
        return work
    return work.sort_values(sort_cols, ascending=True, na_position="last", kind="mergesort")


def collapse_to_lifecycle_snapshot(records: pd.DataFrame) -> pd.DataFrame:
    """Latest business state per issuer/policy/member/insurance/year/month."""
    if records.empty:
        return records
    work = _sort_chronological(records)
    grouped = work.groupby(PRIMARY_JOIN, dropna=False, as_index=False)
    out = grouped.last()
    if "event_count" not in out.columns and not work.empty:
        counts = work.groupby(PRIMARY_JOIN, dropna=False).size().reset_index(name="event_count")
        out = out.merge(counts, on=PRIMARY_JOIN, how="left")
    return out


def build_latest_state_snapshot(snapshot: pd.DataFrame) -> pd.DataFrame:
    """Final business match — one row per member/policy across all months."""
    if snapshot.empty:
        return snapshot
    work = snapshot.copy()
    work["_ym"] = (
        work["year"].astype(str)
        + work["month"].astype(str).str.zfill(2)
    )
    work = work.sort_values("_ym", ascending=True)
    return work.groupby(LATEST_STATE_JOIN, dropna=False, as_index=False).last()


def build_xml_processed_snapshot(
    xml_raw: pd.DataFrame,
    join_mapping: JoinMapping | None,
    *,
    partitions: list[Partition] | None = None,
) -> pd.DataFrame:
    canonical = build_canonical_xml_records(xml_raw, join_mapping)
    if canonical.empty:
        return canonical
    if partitions:
        issuer_set = {str(p.issuer) for p in partitions}
        ym_allowed = {f"{p.year}-{str(p.month).zfill(2)}" for p in partitions}
        ym_series = (
            canonical["year"].astype(str) + "-" + canonical["month"].astype(str).str.zfill(2)
        )
        canonical = canonical[
            canonical["issuer"].astype(str).isin(issuer_set) & ym_series.isin(ym_allowed)
        ].copy()
    snap = collapse_to_lifecycle_snapshot(canonical)
    snap["snapshot_source"] = "xml"
    logger.info(
        "XML processed snapshot: %d rows from %d raw events",
        len(snap), len(xml_raw),
    )
    return snap


def build_azure_processed_snapshot(
    table_df: pd.DataFrame,
    profile: TableProfile,
    join_mapping: JoinMapping | None,
    *,
    date_col: str = "GAA_834_File_Date",
    partitions: list[Partition] | None = None,
) -> pd.DataFrame:
    canonical = build_canonical_azure_records(
        table_df, profile, date_col=date_col, join_mapping=join_mapping,
    )
    if canonical.empty:
        return canonical
    if partitions:
        issuer_set = {str(p.issuer) for p in partitions}
        ym_allowed = {f"{p.year}-{str(p.month).zfill(2)}" for p in partitions}
        ym_series = (
            canonical["year"].astype(str) + "-" + canonical["month"].astype(str).str.zfill(2)
        )
        canonical = canonical[
            canonical["issuer"].astype(str).isin(issuer_set) & ym_series.isin(ym_allowed)
        ].copy()
    snap = collapse_to_lifecycle_snapshot(canonical)
    snap["snapshot_source"] = "azure"
    logger.info(
        "Azure processed snapshot: %d rows from %d raw events",
        len(snap), len(table_df),
    )
    return snap


def _lifecycle_rates(
    stats: dict[str, Any],
    *,
    xml_snapshot_total: int,
    az_snapshot_total: int,
    raw_event_match_rate: float | None = None,
) -> dict[str, float | bool]:
    rates = dict(stats.get("rates") or {})
    mc = int(stats.get("match_count", 0))
    xml_denom = max(xml_snapshot_total, 1)
    rates["lifecycle_snapshot_match_rate"] = round(100.0 * mc / xml_denom, 2)
    rates["azure_snapshot_coverage_rate"] = round(100.0 * mc / max(az_snapshot_total, 1), 2)
    if raw_event_match_rate is not None:
        rates["raw_event_match_rate"] = raw_event_match_rate
    # Primary business metric replaces raw record_match_rate for accuracy weighting
    rates["record_match_rate"] = rates["lifecycle_snapshot_match_rate"]
    rr = float(rates.get("lifecycle_snapshot_match_rate", 0))
    sr = float(rates.get("status_match_rate", 0))
    rates["relationship_valid"] = (
        rr >= settings.relationship_min_record_match_rate
        and sr >= settings.relationship_min_status_match_rate
    )
    rates["xml_not_in_azure_remaining"] = int(stats.get("xml_not_in_azure_count", 0))
    rates["azure_not_in_xml_remaining"] = int(stats.get("azure_not_in_xml_count", 0))
    return rates


def run_lifecycle_snapshot_comparison(
    xml_raw: pd.DataFrame,
    table_df: pd.DataFrame,
    profile: TableProfile,
    *,
    join_mapping: JoinMapping | None = None,
    date_col: str = "GAA_834_File_Date",
    partitions: list[Partition] | None = None,
    raw_event_rates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build snapshots, compare at lifecycle grain, export debug artifacts."""
    xml_snap = build_xml_processed_snapshot(xml_raw, join_mapping, partitions=partitions)
    az_snap = build_azure_processed_snapshot(
        table_df, profile, join_mapping, date_col=date_col, partitions=partitions,
    )

    lifecycle_stats = compare_records(xml_snap, az_snap, join_mapping=join_mapping)
    lifecycle_stats["rates"] = _lifecycle_rates(
        lifecycle_stats,
        xml_snapshot_total=len(xml_snap),
        az_snapshot_total=len(az_snap),
        raw_event_match_rate=float(raw_event_rates.get("record_match_rate", 0))
        if raw_event_rates else None,
    )

    xml_latest = build_latest_state_snapshot(xml_snap)
    az_latest = build_latest_state_snapshot(az_snap)
    latest_stats = compare_records(xml_latest, az_latest, join_mapping=join_mapping)
    latest_rates = _lifecycle_rates(
        latest_stats,
        xml_snapshot_total=len(xml_latest),
        az_snapshot_total=len(az_latest),
    )
    latest_rates["final_business_match_rate"] = latest_rates.get("lifecycle_snapshot_match_rate", 0)
    latest_stats["rates"] = latest_rates

    debug_paths = export_lifecycle_debug_csvs(
        xml_snap=xml_snap,
        az_snap=az_snap,
        lifecycle_stats=lifecycle_stats,
        latest_stats=latest_stats,
        latest_rates=latest_rates,
        xml_raw_rows=len(xml_raw),
        az_raw_rows=len(table_df),
    )

    return {
        "xml_processed_snapshot": xml_snap,
        "azure_processed_snapshot": az_snap,
        "xml_latest_snapshot": xml_latest,
        "azure_latest_snapshot": az_latest,
        "lifecycle_stats": lifecycle_stats,
        "latest_stats": latest_stats,
        "lifecycle_rates": lifecycle_stats["rates"],
        "latest_rates": latest_rates,
        "xml_snapshot_rows": len(xml_snap),
        "azure_snapshot_rows": len(az_snap),
        "xml_latest_rows": len(xml_latest),
        "azure_latest_rows": len(az_latest),
        "match_count": lifecycle_stats.get("match_count", 0),
        "xml_not_in_azure_count": lifecycle_stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml_count": lifecycle_stats.get("azure_not_in_xml_count", 0),
        "status_diff_count": lifecycle_stats.get("status_diff_count", 0),
        "status_mapping": lifecycle_stats.get("status_mapping", {}),
        "status_mapping_reliable": lifecycle_stats.get("status_mapping_reliable", False),
        "debug_paths": debug_paths,
        "event_explanation": XML_EVENT_EXPLANATION
        if len(xml_raw) > len(table_df) * 1.5 else "",
    }


def export_lifecycle_debug_csvs(
    *,
    xml_snap: pd.DataFrame,
    az_snap: pd.DataFrame,
    lifecycle_stats: dict[str, Any],
    latest_stats: dict[str, Any],
    latest_rates: dict[str, Any],
    xml_raw_rows: int = 0,
    az_raw_rows: int = 0,
) -> dict[str, str]:
    dbg = settings.outputs_path / "debug"
    cmp_dir = settings.outputs_path / "comparison"
    dbg.mkdir(parents=True, exist_ok=True)
    cmp_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    exports = [
        (xml_snap, dbg / "xml_lifecycle_snapshot.csv", "xml_lifecycle_snapshot"),
        (az_snap, dbg / "azure_lifecycle_snapshot.csv", "azure_lifecycle_snapshot"),
    ]
    for df, path, name in exports:
        if isinstance(df, pd.DataFrame) and not df.empty:
            safe_write_csv(path, df, table_name=name)
            paths[name] = str(path)

    summary_rows = [{
        "comparison_type": "lifecycle_snapshot",
        "join_keys": "+".join(PRIMARY_JOIN),
        "xml_raw_rows": xml_raw_rows,
        "azure_raw_rows": az_raw_rows,
        "xml_snapshot_rows": len(xml_snap),
        "azure_snapshot_rows": len(az_snap),
        "match_count": lifecycle_stats.get("match_count", 0),
        "xml_not_in_azure": lifecycle_stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml": lifecycle_stats.get("azure_not_in_xml_count", 0),
        "status_diff": lifecycle_stats.get("status_diff_count", 0),
        **{k: v for k, v in (lifecycle_stats.get("rates") or {}).items() if isinstance(v, (int, float, bool))},
    }, {
        "comparison_type": "latest_state",
        "join_keys": "+".join(LATEST_STATE_JOIN),
        "xml_latest_rows": len(build_latest_state_snapshot(xml_snap)) if not xml_snap.empty else 0,
        "match_count": latest_stats.get("match_count", 0),
        "xml_not_in_azure": latest_stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml": latest_stats.get("azure_not_in_xml_count", 0),
        "status_diff": latest_stats.get("status_diff_count", 0),
        **{k: v for k, v in latest_rates.items() if isinstance(v, (int, float, bool))},
    }]

    p_summary = dbg / "lifecycle_match_summary.csv"
    safe_write_csv(p_summary, pd.DataFrame(summary_rows), table_name="lifecycle_match_summary")
    paths["lifecycle_match_summary"] = str(p_summary)

    diff_exports = [
        ("xml_not_in_azure", "lifecycle_xml_not_in_azure.csv"),
        ("azure_not_in_xml", "lifecycle_azure_not_in_xml.csv"),
        ("status_diff", "lifecycle_status_diff.csv"),
    ]
    for key, fname in diff_exports:
        df = lifecycle_stats.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            p = dbg / fname
            safe_write_csv(p, df.head(100_000), table_name=fname.replace(".csv", ""))
            paths[key] = str(p)

    rates = lifecycle_stats.get("rates") or {}
    html_path = cmp_dir / "final_lifecycle_result.html"
    summary_df = pd.DataFrame([{
        "xml_raw_rows": xml_raw_rows,
        "azure_raw_rows": az_raw_rows,
        "xml_lifecycle_snapshot_rows": len(xml_snap),
        "azure_lifecycle_snapshot_rows": len(az_snap),
        "raw_event_match_rate": rates.get("raw_event_match_rate"),
        "lifecycle_snapshot_match_rate": rates.get("lifecycle_snapshot_match_rate"),
        "status_match_rate": rates.get("status_match_rate"),
        "effective_date_match_rate": rates.get("effective_date_match_rate"),
        "lifecycle_xml_not_in_azure": lifecycle_stats.get("xml_not_in_azure_count", 0),
        "lifecycle_azure_not_in_xml": lifecycle_stats.get("azure_not_in_xml_count", 0),
        "final_business_match_rate": latest_rates.get("final_business_match_rate"),
    }])
    extra = f"<p><em>{XML_EVENT_EXPLANATION}</em></p>" if xml_raw_rows > az_raw_rows * 1.5 else ""
    extra += (
        "<h3>A) Raw event comparison (diagnostic)</h3>"
        f"<p>Raw event match rate: {rates.get('raw_event_match_rate', 'n/a')}%</p>"
        f"<p>XML raw rows: {xml_raw_rows:,} | Azure raw rows: {az_raw_rows:,}</p>"
        "<h3>B) Lifecycle snapshot comparison (primary)</h3>"
        f"<p>Lifecycle snapshot match rate: {rates.get('lifecycle_snapshot_match_rate', 0):.2f}%</p>"
        f"<p>Status match rate: {rates.get('status_match_rate', 0):.2f}%</p>"
        f"<p>XML lifecycle snapshot rows: {len(xml_snap):,} | "
        f"Azure lifecycle snapshot rows: {len(az_snap):,}</p>"
        f"<p>XML lifecycle not in Azure: {lifecycle_stats.get('xml_not_in_azure_count', 0):,}</p>"
        f"<p>Azure not in XML: {lifecycle_stats.get('azure_not_in_xml_count', 0):,}</p>"
    )
    safe_write_html_report(
        html_path,
        title="Final Lifecycle Result",
        summary_df=summary_df,
        detail_df=pd.DataFrame(summary_rows),
        extra_html=extra,
    )
    paths["final_lifecycle_result_html"] = str(html_path)
    return paths
