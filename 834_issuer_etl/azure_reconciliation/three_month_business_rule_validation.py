"""
Three-month rolling window business rule validation — read-only investigation.

Tests whether Chandra retroactively adjusts prior-month counts when a member
receives CANCEL/TERM within the next two business months. Does NOT change any
pipeline logic, counts, or reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.dashboard_difference_analysis import (
    _build_latest_state_keys,
    _build_reference_status_map,
    _classify_extra_row,
    _current_counts_by_display,
    _enrollment_detail_rows,
    _enrollment_id_series,
    _expected_from_reference,
    _identify_extra_enrollment_ids,
    _to_display_status,
)
from azure_reconciliation.lifecycle_snapshot_comparison import _sort_chronological
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import LIFECYCLE_PRIMARY_JOIN, join_key_series
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_insurance_type
from azure_reconciliation.xml_business_reports import process_issuer_xml_business
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")
CANCEL_STATUSES = frozenset({"CANCEL"})
TERM_STATUSES = frozenset({"TERM"})

OUTPUT_COLUMNS = [
    "Issuer",
    "Enrollment_ID",
    "Policy_ID",
    "Member_ID",
    "Insurance_Type",
    "Current_Month",
    "Current_Status",
    "Next_Month_Status",
    "Following_Month_Status",
    "Cancelled_Within_3_Months",
    "Terminated_Within_3_Months",
    "Potential_Three_Month_Cleanup",
    "Reason",
    "Source_XML_File",
]

UNEXPLAINED_COLUMNS = [
    "Issuer",
    "Enrollment_ID",
    "Policy_ID",
    "Member_ID",
    "Insurance_Type",
    "Current_Month",
    "Current_Status",
    "Gap_Status",
    "Unexplained_Category",
    "Detail",
    "Source_XML_File",
]


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _ym_key(year: str, month: str) -> str:
    return f"{year}-{_zmonth(month)}"


def _shift_month(year: str, month: str, offset: int) -> tuple[str, str]:
    y, m = int(year), int(_zmonth(month))
    m += offset
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return str(y), _zmonth(str(m))


def _entity_key_series(df: pd.DataFrame) -> pd.Series:
    keys = [k for k in LIFECYCLE_PRIMARY_JOIN if k in df.columns]
    if not keys:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return join_key_series(df, keys)


def _build_entity_month_index(lifecycle_input: pd.DataFrame) -> pd.DataFrame:
    """
    Latest display status per primary lifecycle key per business month.
    Read-only diagnostic grain — does not alter production data.
    """
    if lifecycle_input.empty:
        return pd.DataFrame()

    work = lifecycle_input.copy()
    work["_display_status"] = work.get("status", "UNKNOWN").astype(str).map(_to_display_status)
    work["_entity_key"] = _entity_key_series(work)
    work["_enrollment_id"] = _enrollment_id_series(work)
    work["year"] = work["year"].astype(str)
    work["month"] = work["month"].astype(str).map(_zmonth)

    group_cols = ["_entity_key", "year", "month"]
    sorted_work = _sort_chronological(work)
    latest = sorted_work.groupby(group_cols, dropna=False, as_index=False).last()

    pick_cols = [
        "issuer", "policy_id", "member_id", "insurance_type",
        "_enrollment_id", "_display_status", "year", "month", "_entity_key",
    ]
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in latest.columns:
            pick_cols.append(col)
            break

    return latest[[c for c in pick_cols if c in latest.columns]].rename(
        columns={"_display_status": "status", "_enrollment_id": "enrollment_id"},
    )


def _status_lookup(
    index: pd.DataFrame,
    entity_key: str,
    year: str,
    month: str,
) -> str:
    if index.empty:
        return ""
    ym = _ym_key(year, month)
    match = index[
        (index["_entity_key"].astype(str) == entity_key)
        & (index["year"].astype(str) == str(year))
        & (index["month"].astype(str).map(_zmonth) == _zmonth(month))
    ]
    if match.empty:
        return ""
    return str(match.iloc[-1]["status"])


def _build_three_month_rows(
    entity_index: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    zm = _zmonth(month)
    current = entity_index[
        (entity_index.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
        & (entity_index["year"].astype(str) == str(year))
        & (entity_index["month"].astype(str).map(_zmonth) == zm)
    ].copy()
    if current.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    ny, nm = _shift_month(year, zm, 1)
    fy, fm = _shift_month(year, zm, 2)

    rows: list[dict[str, Any]] = []
    for _, row in current.iterrows():
        ek = str(row["_entity_key"])
        cur_status = str(row.get("status", ""))
        next_status = _status_lookup(entity_index, ek, ny, nm)
        follow_status = _status_lookup(entity_index, ek, fy, fm)

        cancelled = next_status in CANCEL_STATUSES or follow_status in CANCEL_STATUSES
        terminated = next_status in TERM_STATUSES or follow_status in TERM_STATUSES

        potential = cur_status == "CONFIRM" and (cancelled or terminated)
        reasons: list[str] = []
        if potential:
            if cancelled and terminated:
                reasons.append("CONFIRM in current month; CANCEL and TERM within next 2 months")
            elif cancelled:
                reasons.append("CONFIRM in current month; CANCEL within next 2 months")
            else:
                reasons.append("CONFIRM in current month; TERM within next 2 months")
        elif cancelled:
            reasons.append("CANCEL observed within 3-month window (current status not CONFIRM)")
        elif terminated:
            reasons.append("TERM observed within 3-month window (current status not CONFIRM)")

        src = ""
        for col in ("source_file", "file_name", "raw_xml_path"):
            if col in row.index and str(row.get(col, "") or "").strip():
                src = str(row[col]).strip()
                break

        rows.append({
            "Issuer": str(row.get("issuer", issuer)),
            "Enrollment_ID": str(row.get("enrollment_id", row.get("policy_id", ""))),
            "Policy_ID": str(row.get("policy_id", "")),
            "Member_ID": str(row.get("member_id", "")),
            "Insurance_Type": str(row.get("insurance_type", "")).upper(),
            "Current_Month": f"{year}-{zm}",
            "Current_Status": cur_status,
            "Next_Month_Status": next_status or "(no record)",
            "Following_Month_Status": follow_status or "(no record)",
            "Cancelled_Within_3_Months": cancelled,
            "Terminated_Within_3_Months": terminated,
            "Potential_Three_Month_Cleanup": potential,
            "Reason": "; ".join(reasons) if reasons else "",
            "Source_XML_File": src,
            "_entity_key": ek,
        })

    return pd.DataFrame(rows)


def _classify_unexplained(
    row: pd.Series,
    *,
    detail_row: pd.Series | None,
    business_month: str,
    is_three_month_candidate: bool,
) -> tuple[str, str]:
    if is_three_month_candidate:
        return "Business rule candidate", str(row.get("Reason", ""))
    if detail_row is not None and not detail_row.empty:
        bucket = _classify_extra_row(detail_row, business_month=business_month)
        mapping = {
            "duplicate transaction": ("Duplicate transaction", "Flagged as duplicate in cleanup diagnostics"),
            "maintenance-only": ("Maintenance-only", "Maintenance-only transaction retained in Model H input"),
            "superseded event": ("Superseded event", "Superseded by later transaction in same month"),
            "status transition issue": ("Latest state issue", "Lifecycle snapshot status differs from Model H input"),
            "latest state missing": ("Latest state issue", "No matching lifecycle snapshot for enrollment"),
            "older transaction kept instead of latest": ("Latest state issue", "Not the latest state row for entity/month"),
            "month assignment issue": ("Month assignment", "Business month basis differs from source folder month"),
            "effective date issue": ("Month assignment", "Benefit vs maintenance effective dates span months"),
            "unknown": ("Unknown", "No single dominant classification"),
        }
        cat, detail = mapping.get(bucket, ("Unknown", bucket))
        return cat, detail
    return "Unknown", "No supporting diagnostic row"


def _hypothesis_verdict(pct_explained: float, *, gap_total: int, has_reference: bool) -> str:
    if not has_reference:
        return "Chandra reference not configured — hypothesis verdict requires expected counts."
    if gap_total <= 0:
        return "No positive CONFIRM gap — three-month hypothesis not testable for this month."
    if pct_explained >= 80.0:
        return (
            "This business rule is likely part of Chandra's dashboard calculation."
        )
    return "This hypothesis is NOT sufficient."


def _rank_business_rules(
    *,
    three_month_pct: float,
    three_month_matched: int,
    gap_total: int,
    bucket_counts: dict[str, int],
) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []

    if gap_total > 0 and three_month_matched > 0:
        conf = "High" if three_month_pct >= 80 else ("Medium" if three_month_pct >= 50 else "Low")
        rules.append({
            "rule": "Three-month retroactive cleanup (CONFIRM removed when CANCEL/TERM within next 2 months)",
            "confidence": conf,
            "evidence": (
                f"{three_month_matched}/{gap_total} extra CONFIRM enrollments ({three_month_pct:.1f}%) "
                "show CONFIRM in the analysis month and CANCEL or TERM in month+1 or month+2 "
                "for the same primary lifecycle key."
            ),
        })

    maint = bucket_counts.get("Maintenance-only", 0)
    if maint:
        rules.append({
            "rule": "Exclude maintenance-only transactions from dashboard enrollment counts",
            "confidence": "Medium" if maint >= 3 else "Low",
            "evidence": f"{maint} unexplained gap enrollments classified as maintenance-only in XML diagnostics.",
        })

    month_assign = bucket_counts.get("Month assignment", 0)
    if month_assign:
        rules.append({
            "rule": "Alternate business-month assignment (file event vs maint vs benefit basis)",
            "confidence": "Medium" if month_assign >= 3 else "Low",
            "evidence": f"{month_assign} unexplained enrollments have month-basis or effective-date assignment differences.",
        })

    latest = bucket_counts.get("Latest state issue", 0)
    if latest:
        rules.append({
            "rule": "Cross-month latest-state selection (prior month revised by later snapshot)",
            "confidence": "Low",
            "evidence": f"{latest} unexplained enrollments show latest-state or status-transition mismatches.",
        })

    dup = bucket_counts.get("Duplicate transaction", 0)
    if dup:
        rules.append({
            "rule": "Stricter duplicate suppression at enrollment grain",
            "confidence": "Low",
            "evidence": f"{dup} unexplained enrollments flagged as duplicate XML transactions.",
        })

    if not rules:
        rules.append({
            "rule": "Insufficient XML evidence for a dominant Chandra rule",
            "confidence": "Low",
            "evidence": "No material gap or no forward-month status transitions detected in available data.",
        })

    order = {"High": 0, "Medium": 1, "Low": 2}
    rules.sort(key=lambda r: order.get(r["confidence"], 9))
    return rules


def run_three_month_business_rule_validation(
    *,
    issuer: str = "15105",
    year: str = "2026",
    month: str = "01",
    insurance_type: str = "HEALTH",
    parse_source: bool = False,
    expected_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """
    Read-only three-month window investigation vs Chandra dashboard gaps.
    """
    settings.ensure_dirs()
    zm = _zmonth(month)
    expected = expected_counts or _expected_from_reference(issuer, year, zm)
    has_reference = expected is not None
    if not expected:
        expected = {s: 0 for s in DISPLAY_STATUSES}
        logger.warning(
            "No Chandra reference for %s/%s/%s — three-month window still generated; gap summary unavailable",
            issuer, year, zm,
        )

    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    xml_raw = load_xml_rows(prefer_staging=not parse_source, issuer_filter=issuer)
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for issuer {issuer}")
    if not partitions:
        partitions = [Partition(issuer=issuer, year=year, month=zm)]

    result = process_issuer_xml_business(issuer, xml_raw, partitions)

    entity_index = _build_entity_month_index(result.lifecycle_input)
    if not entity_index.empty and "insurance_type" in entity_index.columns:
        entity_index = entity_index[
            entity_index["insurance_type"].astype(str).map(normalize_insurance_type) == insurance_type
        ].copy()
    three_month_df = _build_three_month_rows(
        entity_index, issuer=issuer, year=year, month=zm,
    )

    reference_status = _build_reference_status_map(
        result.lifecycle_snapshots, issuer=issuer, year=year, month=zm,
    )
    latest_state_keys = _build_latest_state_keys(
        result.canonical, issuer=issuer, year=year, month=zm,
    )
    detail = _enrollment_detail_rows(
        result.lifecycle_input,
        result.canonical,
        issuer=issuer,
        year=year,
        month=zm,
        insurance_type=insurance_type,
        duplicate_df=result.duplicate_df,
        maintenance_df=result.maintenance_df,
        superseded_df=result.superseded_df,
        reference_status=reference_status,
        latest_state_keys=latest_state_keys,
    )

    actual_counts = _current_counts_by_display(
        result.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )

    status_summaries: dict[str, dict[str, Any]] = {}
    unexplained_frames: list[pd.DataFrame] = []
    all_bucket_counts: dict[str, int] = {}

    for display_status in DISPLAY_STATUSES:
        exp = int(expected.get(display_status, 0))
        act = int(actual_counts.get(display_status, 0))
        delta = act - exp
        extra_ids = _identify_extra_enrollment_ids(
            detail,
            display_status=display_status,
            expected_count=exp,
            reference_status=reference_status,
        )

        if display_status == "CONFIRM" and not three_month_df.empty:
            cleanup_ids = set(
                three_month_df.loc[
                    three_month_df["Potential_Three_Month_Cleanup"]
                    & three_month_df["Enrollment_ID"].astype(str).isin(extra_ids),
                    "Enrollment_ID",
                ].astype(str)
            )
        else:
            cleanup_ids = set()

        explained = len(cleanup_ids) if display_status == "CONFIRM" else 0
        remaining = max(delta - explained, 0) if display_status == "CONFIRM" else max(delta, 0)

        status_summaries[display_status] = {
            "expected": exp,
            "actual": act,
            "delta": delta,
            "three_month_cleanup": explained,
            "remaining_unexplained": remaining,
            "pct_explained": round(100.0 * explained / delta, 1) if delta > 0 else 100.0,
        }

        if delta <= 0:
            continue

        remaining_ids = extra_ids - cleanup_ids if display_status == "CONFIRM" else extra_ids
        for eid in sorted(remaining_ids):
            det = detail[detail["enrollment_id"].astype(str) == eid]
            det_row = det.iloc[0] if not det.empty else None
            tm = three_month_df[three_month_df["Enrollment_ID"].astype(str) == eid]
            tm_row = tm.iloc[0] if not tm.empty else None
            is_candidate = bool(tm_row["Potential_Three_Month_Cleanup"]) if tm_row is not None else False
            cat, cat_detail = _classify_unexplained(
                tm_row if tm_row is not None else pd.Series(dtype=object),
                detail_row=det_row,
                business_month=zm,
                is_three_month_candidate=is_candidate,
            )
            all_bucket_counts[cat] = all_bucket_counts.get(cat, 0) + 1
            unexplained_frames.append({
                "Issuer": issuer,
                "Enrollment_ID": eid,
                "Policy_ID": str(det_row["policy_id"]) if det_row is not None else "",
                "Member_ID": str(det_row["member_id"]) if det_row is not None else "",
                "Insurance_Type": insurance_type,
                "Current_Month": f"{year}-{zm}",
                "Current_Status": display_status,
                "Gap_Status": display_status,
                "Unexplained_Category": cat,
                "Detail": cat_detail,
                "Source_XML_File": (
                    str(det_row["source_file"]) if det_row is not None
                    else (str(tm_row["Source_XML_File"]) if tm_row is not None else "")
                ),
            })

    unexplained_df = pd.DataFrame(unexplained_frames)
    if not unexplained_df.empty:
        unexplained_df = unexplained_df[UNEXPLAINED_COLUMNS]

    confirm_summary = status_summaries.get("CONFIRM", {})
    gap_total = int(confirm_summary.get("delta", 0))
    three_month_matched = int(confirm_summary.get("three_month_cleanup", 0))
    pct = float(confirm_summary.get("pct_explained", 0.0))

    ranked_rules = _rank_business_rules(
        three_month_pct=pct,
        three_month_matched=three_month_matched,
        gap_total=gap_total,
        bucket_counts=all_bucket_counts,
    )

    out_df = three_month_df[[c for c in OUTPUT_COLUMNS if c in three_month_df.columns]]
    cleanup_candidates = (
        out_df[out_df["Potential_Three_Month_Cleanup"]].copy()
        if not out_df.empty and "Potential_Three_Month_Cleanup" in out_df.columns
        else pd.DataFrame(columns=OUTPUT_COLUMNS)
    )

    xlsx_path = _debug_dir() / "three_month_business_rule_validation.xlsx"
    safe_write_excel(
        xlsx_path,
        {
            "Three_Month_Window": out_df,
            "Potential_Cleanup_Candidates": cleanup_candidates,
            "Unexplained_Gap_Records": unexplained_df,
            "Gap_Summary_By_Status": pd.DataFrame([
                {"Status": st, **status_summaries.get(st, {})}
                for st in DISPLAY_STATUSES
            ]),
            "Business_Rule_Ranking": pd.DataFrame(ranked_rules),
        },
        drop_duplicate_value_columns=False,
    )

    md_path = _write_summary_md(
        issuer=issuer, year=year, month=zm,
        status_summaries=status_summaries,
        pct=pct,
        ranked_rules=ranked_rules,
        bucket_counts=all_bucket_counts,
        three_month_matched=three_month_matched,
        gap_total=gap_total,
        has_reference=has_reference,
    )

    logger.info("Wrote three-month business rule validation → %s", xlsx_path)
    logger.info("Wrote three-month business rule summary → %s", md_path)

    return {
        "xlsx": str(xlsx_path),
        "summary_md": str(md_path),
        "status_summaries": status_summaries,
        "three_month_rows": len(out_df),
        "ranked_rules": ranked_rules,
        "hypothesis_verdict": _hypothesis_verdict(pct, gap_total=gap_total, has_reference=has_reference),
    }


def _write_summary_md(
    *,
    issuer: str,
    year: str,
    month: str,
    status_summaries: dict[str, dict[str, Any]],
    pct: float,
    ranked_rules: list[dict[str, Any]],
    bucket_counts: dict[str, int],
    three_month_matched: int,
    gap_total: int,
    has_reference: bool = True,
) -> Path:
    md_path = _debug_dir() / "three_month_business_rule_summary.md"
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Three-Month Business Rule Validation",
        "",
        f"**Issuer / month:** {issuer} / {year} / {month}",
        f"**Generated:** {generated}",
        "",
        "Read-only investigation — no records removed, no logic changed.",
        "",
    ]
    if not has_reference:
        lines.append(
            "> No Chandra reference counts configured for this issuer/month — "
            "gap comparison sections are informational only.",
        )
        lines.append("")
    lines.extend([
        "## Dashboard gap vs three-month hypothesis",
        "",
    ])

    for st in DISPLAY_STATUSES:
        s = status_summaries.get(st, {})
        lines.extend([
            f"### {st}",
            "",
            f"- Expected (Chandra): {s.get('expected', 0)}",
            f"- Current dashboard: {s.get('actual', 0)}",
            f"- Remaining difference: {s.get('delta', 0):+d}",
            "",
        ])
        if st == "CONFIRM":
            lines.extend([
                f"- Potential three-month cleanup: {s.get('three_month_cleanup', 0)}",
                f"- Remaining unexplained: {s.get('remaining_unexplained', 0)}",
                f"- Percentage of extra CONFIRM explained by three-month rule: {s.get('pct_explained', 0):.1f}%",
                "",
            ])

    lines.extend([
        "## Hypothesis verdict (CONFIRM gap)",
        "",
        _hypothesis_verdict(pct, gap_total=gap_total, has_reference=has_reference),
        "",
        "## Unexplained record categories",
        "",
    ])
    if bucket_counts:
        for cat, count in sorted(bucket_counts.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {cat}: {count}")
    else:
        lines.append("- (none — no positive dashboard gap or all gaps explained)")
    lines.extend([
        "",
        "## Business rules ranked by confidence (XML evidence only)",
        "",
    ])
    for i, rule in enumerate(ranked_rules, 1):
        lines.extend([
            f"### {i}. {rule['rule']}",
            "",
            f"**Confidence:** {rule['confidence']}",
            "",
            f"{rule['evidence']}",
            "",
        ])

    lines.extend([
        "## Final assessment",
        "",
        "Based on XML evidence only, the following rule(s) would most likely reproduce "
        "Chandra's dashboard if applied as post-aggregation adjustments:",
        "",
    ])
    for rule in ranked_rules[:3]:
        lines.append(f"- **{rule['confidence']}** — {rule['rule']}")
    lines.append("")
    lines.extend([
        "> This analysis does NOT implement any rule. Production pipeline unchanged.",
        "",
    ])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path
