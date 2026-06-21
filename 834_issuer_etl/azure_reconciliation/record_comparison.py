"""
Record-level XML vs Azure comparison — canonical records and debug CSV exports.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.azure_mirror.discovery.table_inspector import TableProfile
from azure_reconciliation.df_utils import col_series as _col_series, zmonth as _zmonth
from azure_reconciliation.safe_export import safe_write_csv
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

def join_key_series(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    """Build pipe-delimited keys from columns; safe for any key count."""
    if df is None or df.empty:
        return pd.Series(dtype=str)
    parts: list[pd.Series] = []
    for key in keys:
        if key in df.columns:
            parts.append(df[key].fillna("").astype(str))
        else:
            parts.append(pd.Series([""] * len(df), index=df.index, dtype=str))
    if not parts:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    out = parts[0]
    for part in parts[1:]:
        out = out + "|" + part
    return out


PRIMARY_JOIN = ["issuer", "policy_id", "member_id", "insurance_type", "year", "month"]

DIAGNOSTIC_JOINS: list[tuple[str, list[str]]] = [
    ("issuer_policy_member_insurance", ["issuer", "policy_id", "member_id", "insurance_type"]),
    ("issuer_policy_subscriber_insurance", ["issuer", "policy_id", "subscriber_id", "insurance_type"]),
    ("issuer_member_insurance", ["issuer", "member_id", "insurance_type"]),
    ("issuer_subscriber_insurance", ["issuer", "subscriber_id", "insurance_type"]),
]


def build_canonical_xml_records(xml_raw: pd.DataFrame) -> pd.DataFrame:
    if xml_raw.empty:
        return pd.DataFrame(columns=PRIMARY_JOIN + [
            "subscriber_id", "normalized_status", "event_date", "action_code", "source_file",
        ])
    work = xml_raw.copy()
    rec = pd.DataFrame({
        "issuer": _col_series(work, "issuer", "issuer_id").astype(str),
        "year": _col_series(work, "year", "source_year", "coverage_year").astype(str),
        "month": _col_series(work, "month", "source_month", "snapshot_month").astype(str).map(_zmonth),
        "policy_id": _col_series(work, "policy_id", "exchg_assigned_policy_id", "enrollment_id").astype(str),
        "member_id": _col_series(work, "member_id", "exchg_indiv_identifier", "enrollee_id").astype(str),
        "subscriber_id": _col_series(work, "subscriber_id", "exchg_subscriber_identifier").astype(str),
        "insurance_type": _col_series(
            work, "insurance_type_code", "insurance_type",
        ).map(normalize_insurance_type),
        "normalized_status": _col_series(
            work, "additional_maint_reason_code", "coverage_status", "canonical_status",
        ).astype(str).map(normalize_status),
        "event_date": _col_series(work, "member_maint_effective_date", "event_date").astype(str),
        "action_code": _col_series(
            work, "maintenance_type_code", "enrollment_action_code", "event_type_code",
        ).astype(str),
        "source_file": _col_series(work, "file_name", "raw_xml_path", "source_file").astype(str),
        "source_table": "xml",
    })
    rec["_record_key"] = join_key_series(rec, PRIMARY_JOIN)
    return rec


def build_canonical_azure_records(
    table_df: pd.DataFrame,
    profile: TableProfile,
    *,
    date_col: str = "GAA_834_File_Date",
) -> pd.DataFrame:
    if table_df.empty:
        return pd.DataFrame(columns=PRIMARY_JOIN + [
            "subscriber_id", "normalized_status", "event_date", "action_code", "source_table",
        ])
    work = table_df.copy()
    dc = date_col if date_col in work.columns else (profile.file_date_col or "")
    dates = pd.to_datetime(_col_series(work, dc) if dc else pd.Series([None] * len(work)), errors="coerce")

    ins_col = profile.insurance_type_col or "Insurance_Type"
    if ins_col not in work.columns:
        for alt in ("planCoverageDescription", "Insurance_Type"):
            if alt in work.columns:
                ins_col = alt
                break

    st_col = profile.status_col or "enrolleeStatus"
    act_col = profile.action_col or "actionCode"

    month_from_date = dates.dt.month
    month_fallback = _col_series(work, "month").astype(str).map(_zmonth)
    month_vals = month_from_date.apply(lambda x: _zmonth(str(int(x))) if pd.notna(x) else "").where(
        month_from_date.notna(), month_fallback
    )

    rec = pd.DataFrame({
        "issuer": _col_series(work, profile.issuer_col or "GAA_HIOS_ID").astype(str),
        "year": _col_series(work, profile.year_col or "Coverage_Year").astype(str),
        "month": month_vals.astype(str).map(_zmonth),
        "policy_id": _col_series(work, profile.policy_col or "exchgAssignedPolicyID").astype(str),
        "member_id": _col_series(work, profile.member_col or "exchgIndivIdentifier").astype(str),
        "subscriber_id": _col_series(work, profile.subscriber_col or "exchgSubscriberIdentifier").astype(str),
        "insurance_type": _col_series(work, ins_col).map(normalize_insurance_type),
        "normalized_status": _col_series(work, st_col).astype(str).map(normalize_status),
        "event_date": dates.dt.strftime("%Y-%m-%d").fillna(""),
        "action_code": _col_series(work, act_col).astype(str),
        "source_table": profile.full_name,
    })
    rec["_record_key"] = join_key_series(rec, PRIMARY_JOIN)
    return rec


def compare_records(
    xml_records: pd.DataFrame,
    az_records: pd.DataFrame,
) -> dict[str, Any]:
    """Primary join comparison with match / diff counts."""
    empty_stats = {
        "match_count": 0,
        "xml_not_in_azure_count": 0,
        "azure_not_in_xml_count": 0,
        "status_diff_count": 0,
        "date_diff_count": 0,
        "matched_records": pd.DataFrame(),
        "xml_not_in_azure": pd.DataFrame(),
        "azure_not_in_xml": pd.DataFrame(),
        "status_diff": pd.DataFrame(),
        "date_diff": pd.DataFrame(),
        "summary_rows": [],
    }
    if xml_records.empty and az_records.empty:
        return empty_stats

    merged = xml_records.merge(
        az_records,
        on=PRIMARY_JOIN,
        how="outer",
        suffixes=("_xml", "_az"),
        indicator=True,
    )

    matched = merged[merged["_merge"] == "both"].copy()
    xml_only = merged[merged["_merge"] == "left_only"].copy()
    az_only = merged[merged["_merge"] == "right_only"].copy()

    status_diff = pd.DataFrame()
    date_diff = pd.DataFrame()
    if not matched.empty:
        sx_col = "normalized_status_xml" if "normalized_status_xml" in matched.columns else "normalized_status_x"
        sa_col = "normalized_status_az" if "normalized_status_az" in matched.columns else "normalized_status_y"
        if sx_col in matched.columns and sa_col in matched.columns:
            status_diff = matched[matched[sx_col].astype(str) != matched[sa_col].astype(str)].copy()
        dx_col = "event_date_xml" if "event_date_xml" in matched.columns else "event_date_x"
        da_col = "event_date_az" if "event_date_az" in matched.columns else "event_date_y"
        if dx_col in matched.columns and da_col in matched.columns:
            date_diff = matched[
                matched[dx_col].astype(str).str[:10] != matched[da_col].astype(str).str[:10]
            ].copy()

    summary_rows = [{"join_type": "primary", "join_keys": "+".join(PRIMARY_JOIN),
                     "match_count": len(matched),
                     "xml_not_in_azure": len(xml_only),
                     "azure_not_in_xml": len(az_only),
                     "status_diff": len(status_diff),
                     "date_diff": len(date_diff)}]

    for label, keys in DIAGNOSTIC_JOINS:
        if xml_records.empty or az_records.empty:
            continue
        xk = join_key_series(xml_records, keys)
        ak = join_key_series(az_records, keys)
        summary_rows.append({
            "join_type": label,
            "join_keys": "+".join(keys),
            "match_count": len(set(xk) & set(ak)),
            "xml_not_in_azure": len(set(xk) - set(ak)),
            "azure_not_in_xml": len(set(ak) - set(xk)),
            "status_diff": "",
            "date_diff": "",
        })

    return {
        "match_count": len(matched),
        "xml_not_in_azure_count": len(xml_only),
        "azure_not_in_xml_count": len(az_only),
        "status_diff_count": len(status_diff),
        "date_diff_count": len(date_diff),
        "matched_records": matched,
        "xml_not_in_azure": xml_only,
        "azure_not_in_xml": az_only,
        "status_diff": status_diff,
        "date_diff": date_diff,
        "summary_rows": summary_rows,
    }


def export_record_debug_csvs(record_stats: dict[str, Any]) -> dict[str, str]:
    """Write outputs/debug/record_*.csv files."""
    dbg = settings.outputs_path / "debug"
    dbg.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    summary = pd.DataFrame(record_stats.get("summary_rows", []))
    paths["record_match_summary"] = str(dbg / "record_match_summary.csv")
    safe_write_csv(Path(paths["record_match_summary"]), summary, table_name="record_match_summary")

    for key, fname in [
        ("matched_records", "matched_records.csv"),
        ("xml_not_in_azure", "xml_not_in_azure.csv"),
        ("azure_not_in_xml", "azure_not_in_xml.csv"),
        ("status_diff", "status_diff.csv"),
        ("date_diff", "date_diff.csv"),
    ]:
        df = record_stats.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            p = dbg / fname
            safe_write_csv(p, df.head(100_000), table_name=fname.replace(".csv", ""))
            paths[key] = str(p)
    return paths
