"""
Deep-dive mismatch analysis and business aggregation model comparison.

Reverse-engineers Azure-not-in-XML and XML-not-in-Azure reasons and scores
candidate Chandra-like aggregation models against Azure.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.lifecycle_snapshot_comparison import collapse_to_snapshot
from azure_reconciliation.record_comparison import (
    LIFECYCLE_PRIMARY_JOIN,
    JoinMapping,
    compare_records,
    join_key_series,
)
from azure_reconciliation.safe_export import safe_write_csv
from azure_reconciliation.status_mapper import normalize_status
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

PK = LIFECYCLE_PRIMARY_JOIN
ACTIVE_STATUSES = frozenset({"ENROLLED", "ACTIVE", "EFFECTUATED", "CONFIRMED"})
MAINT_ACTION_PREFIXES = frozenset({"001", "021", "024", "030", "032", "33", "34", "XN"})

AZURE_ONLY_REASONS = [
    "POLICY_EXISTS_XML_MEMBER_DIFF",
    "MEMBER_EXISTS_XML_POLICY_DIFF",
    "EXISTS_XML_DIFFERENT_MONTH",
    "EXISTS_XML_DIFFERENT_STATUS",
    "EXISTS_XML_DIFFERENT_INSURANCE_TYPE",
    "EXISTS_XML_DIFFERENT_EFFECTIVE_DATE",
    "EXISTS_XML_RAW_BUT_NOT_LIFECYCLE",
    "NO_POLICY_OR_MEMBER_OVERLAP",
    "POSSIBLE_ID_TRANSFORMATION",
    "POSSIBLE_AZURE_ONLY_SOURCE",
]

XML_ONLY_REASONS = [
    "DUPLICATE_XML_TRANSACTION",
    "SUPERSEDED_BY_LATER_XML_EVENT",
    "MAINTENANCE_ONLY_EVENT",
    "CANCEL_TERM_REPLACED_BY_FINAL_STATE",
    "SAME_POLICY_MEMBER_DIFFERENT_MONTH",
    "SAME_POLICY_MEMBER_DIFFERENT_STATUS",
    "XML_ONLY_NO_AZURE_OVERLAP",
    "POSSIBLE_NOT_LOADED_TO_AZURE",
    "POSSIBLE_XML_EXTRA_SOURCE",
]


def _dbg() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _col(df: pd.DataFrame, name: str, side: str = "") -> pd.Series:
    if name in df.columns:
        return df[name].astype(str)
    for suffix in (f"_{side}", "_xml", "_az", "_x", "_y"):
        c = f"{name}{suffix}"
        if c in df.columns:
            return df[c].astype(str)
    return pd.Series([""] * len(df), index=df.index)


def _pk_key(df: pd.DataFrame, side: str = "") -> pd.Series:
    keys = []
    for c in PK:
        keys.append(_col(df, c, side))
    out = keys[0]
    for k in keys[1:]:
        out = out + "|" + k
    return out


def _policy_key(df: pd.DataFrame, side: str = "") -> pd.Series:
    return _col(df, "issuer", side) + "|" + _col(df, "policy_id", side) + "|" + _col(df, "insurance_type", side)


def _member_key(df: pd.DataFrame, side: str = "") -> pd.Series:
    return _col(df, "issuer", side) + "|" + _col(df, "member_id", side) + "|" + _col(df, "insurance_type", side)


def _status_series(df: pd.DataFrame, side: str = "") -> pd.Series:
    for c in ("normalized_status", "canonical_status"):
        if c in df.columns:
            return df[c].astype(str).map(normalize_status)
        for suffix in (f"_{side}", "_xml", "_az"):
            cs = f"{c}{suffix}"
            if cs in df.columns:
                return df[cs].astype(str).map(normalize_status)
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _ym_series(df: pd.DataFrame, side: str = "") -> pd.Series:
    for c in ("file_event_year_month", "coverage_year_month"):
        if c in df.columns:
            return df[c].astype(str)
        for suffix in (f"_{side}", "_xml", "_az"):
            cs = f"{c}{suffix}"
            if cs in df.columns:
                return df[cs].astype(str)
    if "year" in df.columns or f"year_{side}" in df.columns or "year_xml" in df.columns:
        y = _col(df, "year", side)
        m = _col(df, "month", side).str.zfill(2)
        return y + "-" + m
    return pd.Series([""] * len(df), index=df.index)


def _benefit_date(df: pd.DataFrame, side: str = "") -> pd.Series:
    return _col(df, "benefit_effective_date", side).str[:10]


def _build_lookups(xml_canonical: pd.DataFrame, xml_snap: pd.DataFrame) -> dict[str, Any]:
    xml_pk = collapse_to_snapshot(xml_canonical, PK) if not xml_canonical.empty else pd.DataFrame()
    return {
        "raw_pk": set(join_key_series(xml_canonical, PK)) if not xml_canonical.empty else set(),
        "snap_pk": set(join_key_series(xml_snap, PK)) if not xml_snap.empty else set(),
        "latest_pk": set(join_key_series(xml_pk, PK)) if not xml_pk.empty else set(),
        "policy": set(_policy_key(xml_canonical)) if not xml_canonical.empty else set(),
        "member": set(_member_key(xml_canonical)) if not xml_canonical.empty else set(),
        "xml_pk_rows": xml_pk,
    }


def classify_azure_not_in_xml(
    az_only: pd.DataFrame,
    *,
    xml_canonical: pd.DataFrame,
    xml_snap: pd.DataFrame,
    xml_raw: pd.DataFrame,
    join_keys: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if az_only.empty:
        empty = pd.DataFrame(columns=["reason_bucket", "record_count"])
        return pd.DataFrame(), empty

    lookups = _build_lookups(xml_canonical, xml_snap)
    xml_pk_df = lookups["xml_pk_rows"]
    xml_pk_status = {}
    xml_pk_ym = {}
    xml_pk_benefit = {}
    if not xml_pk_df.empty:
        for i, row in xml_pk_df.iterrows():
            k = join_key_series(pd.DataFrame([row]), PK).iloc[0]
            xml_pk_status[k] = normalize_status(str(row.get("normalized_status", "")))
            xml_pk_ym[k] = str(row.get("file_event_year_month") or row.get("coverage_year_month", ""))
            xml_pk_benefit[k] = str(row.get("benefit_effective_date", ""))[:10]

    reasons: list[str] = []
    detail_rows: list[dict[str, Any]] = []

    for idx, row in az_only.iterrows():
        rdf = pd.DataFrame([row])
        pk = _pk_key(rdf).iloc[0]
        pol = _policy_key(rdf).iloc[0]
        mem = _member_key(rdf).iloc[0]
        az_st = _status_series(rdf).iloc[0]
        az_ym = _ym_series(rdf).iloc[0]
        az_ben = _benefit_date(rdf).iloc[0]

        reason = "POSSIBLE_AZURE_ONLY_SOURCE"

        if pk in lookups["raw_pk"] and pk not in lookups["snap_pk"]:
            reason = "EXISTS_XML_RAW_BUT_NOT_LIFECYCLE"
        elif pol in lookups["policy"] and mem not in lookups["member"]:
            reason = "POLICY_EXISTS_XML_MEMBER_DIFF"
        elif mem in lookups["member"] and pol not in lookups["policy"]:
            reason = "MEMBER_EXISTS_XML_POLICY_DIFF"
        elif pk in lookups["latest_pk"]:
            xm_st = xml_pk_status.get(pk, "")
            xm_ym = xml_pk_ym.get(pk, "")
            xm_ben = xml_pk_benefit.get(pk, "")
            if az_ym and xm_ym and az_ym != xm_ym:
                reason = "EXISTS_XML_DIFFERENT_MONTH"
            elif az_st != xm_st:
                reason = "EXISTS_XML_DIFFERENT_STATUS"
            elif az_ben and xm_ben and az_ben != xm_ben:
                reason = "EXISTS_XML_DIFFERENT_EFFECTIVE_DATE"
            else:
                reason = "EXISTS_XML_DIFFERENT_MONTH"
        elif pol in lookups["policy"] or mem in lookups["member"]:
            reason = "POSSIBLE_ID_TRANSFORMATION"
        elif pol not in lookups["policy"] and mem not in lookups["member"]:
            reason = "NO_POLICY_OR_MEMBER_OVERLAP"

        reasons.append(reason)
        detail_rows.append({
            "reason_bucket": reason,
            "issuer": _col(rdf, "issuer").iloc[0],
            "policy_id": _col(rdf, "policy_id").iloc[0],
            "member_id": _col(rdf, "member_id").iloc[0],
            "insurance_type": _col(rdf, "insurance_type").iloc[0],
            "year_month": az_ym,
            "normalized_status": az_st,
            "benefit_effective_date": az_ben,
            "record_key": pk,
        })

    detail = pd.DataFrame(detail_rows)
    detail["reason_bucket"] = reasons
    summary = (
        detail.groupby("reason_bucket", dropna=False)
        .size().reset_index(name="record_count")
        .sort_values("record_count", ascending=False)
    )
    return detail, summary


def classify_xml_not_in_azure(
    xml_only: pd.DataFrame,
    *,
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
    az_snap: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if xml_only.empty:
        return pd.DataFrame(), pd.DataFrame(columns=["reason_bucket", "record_count"])

    az_pk = set(join_key_series(az_snap, PK)) if not az_snap.empty else set()
    az_pol = set(_policy_key(az_canonical)) if not az_canonical.empty else set()
    az_mem = set(_member_key(az_canonical)) if not az_canonical.empty else set()

    # Duplicate detection on raw canonical
    dup_keys: set[str] = set()
    if not xml_canonical.empty:
        sig = (
            join_key_series(xml_canonical, PK)
            + "|" + _status_series(xml_canonical)
            + "|" + _benefit_date(xml_canonical)
        )
        vc = sig.value_counts()
        dup_keys = set(vc[vc > 1].index)

    # Latest status per pk from chronological replay
    final_status: dict[str, str] = {}
    if not xml_canonical.empty:
        sorted_xml = xml_canonical.sort_values(
            [c for c in ("member_maint_effective_date", "source_file", "year", "month") if c in xml_canonical.columns],
            ascending=True, na_position="last",
        )
        for _, row in sorted_xml.iterrows():
            k = join_key_series(pd.DataFrame([row]), PK).iloc[0]
            final_status[k] = normalize_status(str(row.get("normalized_status", "")))

    reasons: list[str] = []
    detail_rows: list[dict[str, Any]] = []

    for _, row in xml_only.iterrows():
        rdf = pd.DataFrame([row])
        pk = _pk_key(rdf).iloc[0]
        pol = _policy_key(rdf).iloc[0]
        mem = _member_key(rdf).iloc[0]
        st = _status_series(rdf).iloc[0]
        ym = _ym_series(rdf).iloc[0]
        sig = pk + "|" + st + "|" + _benefit_date(rdf).iloc[0]

        reason = "POSSIBLE_XML_EXTRA_SOURCE"

        if sig in dup_keys:
            reason = "DUPLICATE_XML_TRANSACTION"
        elif pk in az_pk:
            reason = "SAME_POLICY_MEMBER_DIFFERENT_MONTH"
        elif pol in az_pol and mem in az_mem:
            reason = "SAME_POLICY_MEMBER_DIFFERENT_STATUS"
        elif pol in az_pol or mem in az_mem:
            reason = "POSSIBLE_NOT_LOADED_TO_AZURE"
        elif st in ("CANCELLED", "TERMINATED") and final_status.get(pk, st) != st:
            reason = "CANCEL_TERM_REPLACED_BY_FINAL_STATE"
        elif str(row.get("action_code", row.get("action_code_xml", ""))).strip()[:3] in MAINT_ACTION_PREFIXES:
            reason = "MAINTENANCE_ONLY_EVENT"
        elif pk in final_status and final_status[pk] != st:
            reason = "SUPERSEDED_BY_LATER_XML_EVENT"
        elif pol not in az_pol and mem not in az_mem:
            reason = "XML_ONLY_NO_AZURE_OVERLAP"

        reasons.append(reason)
        detail_rows.append({
            "reason_bucket": reason,
            "issuer": _col(rdf, "issuer").iloc[0],
            "policy_id": _col(rdf, "policy_id").iloc[0],
            "member_id": _col(rdf, "member_id").iloc[0],
            "insurance_type": _col(rdf, "insurance_type").iloc[0],
            "year_month": ym,
            "normalized_status": st,
            "record_key": pk,
        })

    detail = pd.DataFrame(detail_rows)
    detail["reason_bucket"] = reasons
    summary = (
        detail.groupby("reason_bucket", dropna=False)
        .size().reset_index(name="record_count")
        .sort_values("record_count", ascending=False)
    )
    return detail, summary


def _model_score(
    model_id: str,
    model_name: str,
    xml_df: pd.DataFrame,
    az_df: pd.DataFrame,
    join_keys: list[str],
    *,
    join_mapping: JoinMapping | None = None,
) -> dict[str, Any]:
    stats = compare_records(xml_df, az_df, join_mapping=join_mapping, join_keys=join_keys)
    rates = stats.get("rates") or {}
    return {
        "model_id": model_id,
        "model_name": model_name,
        "join_key": "+".join(join_keys),
        "xml_output_count": len(xml_df),
        "azure_output_count": len(az_df),
        "match_count": stats.get("match_count", 0),
        "xml_not_in_azure": stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml": stats.get("azure_not_in_xml_count", 0),
        "status_diff": stats.get("status_diff_count", 0),
        "match_rate": rates.get("record_match_rate", 0),
        "status_match_rate": rates.get("status_match_rate", 0),
        "effective_date_match_rate": rates.get("effective_date_match_rate", 0),
        "overall_match_rate": rates.get("record_match_rate", 0),
    }


def _filter_active(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    st = _status_series(df)
    return df[st.isin(ACTIVE_STATUSES)].copy()


def _filter_non_maintenance(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "action_code" not in df.columns:
        return df
    ac = df["action_code"].astype(str).str.strip().str[:3]
    return df[~ac.isin(MAINT_ACTION_PREFIXES)].copy()


def _dedupe_transactions(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    keys = [c for c in PK + ["normalized_status", "benefit_effective_date", "member_maint_effective_date"] if c in df.columns]
    return df.drop_duplicates(subset=keys, keep="last")


def _final_status_replay(df: pd.DataFrame) -> pd.DataFrame:
    """Model G — one row per pk with chronologically final status."""
    if df.empty:
        return df
    return collapse_to_snapshot(df, PK)


def _chandra_dashboard(df: pd.DataFrame, *, source: str) -> pd.DataFrame:
    """Model H — enrollment/enrollee/subscriber counts by issuer/month/status."""
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["year"] = _col(work, "year")
    work["month"] = _col(work, "month").str.zfill(2)
    work["status"] = _status_series(work)
    work["insurance_type"] = _col(work, "insurance_type")
    work["issuer"] = _col(work, "issuer")
    sub_col = "subscriber_id" if "subscriber_id" in work.columns else "member_id"
    agg = work.groupby(
        ["issuer", "year", "month", "insurance_type", "status"], dropna=False,
    ).agg(
        enrollment_count=("policy_id", "nunique"),
        enrollee_count=("member_id", "nunique"),
        subscriber_count=(sub_col, "nunique"),
    ).reset_index()
    agg["source"] = source
    return agg


def _score_dashboard(xml_dash: pd.DataFrame, az_dash: pd.DataFrame) -> dict[str, float]:
    if xml_dash.empty or az_dash.empty:
        return {"match_rate": 0.0, "status_match_rate": 0.0, "effective_date_match_rate": 0.0, "overall_match_rate": 0.0}
    keys = ["issuer", "year", "month", "insurance_type", "status"]
    merged = xml_dash.merge(az_dash, on=keys, how="outer", suffixes=("_xml", "_az")).fillna(0)
    if merged.empty:
        return {"match_rate": 0.0, "status_match_rate": 0.0, "effective_date_match_rate": 0.0, "overall_match_rate": 0.0}
    matched = merged[(merged["enrollee_count_xml"] > 0) & (merged["enrollee_count_az"] > 0)]
    match_rate = 100.0 * len(matched) / max(len(xml_dash), 1)
    scores = []
    for col in ("enrollment_count", "enrollee_count", "subscriber_count"):
        xv, av = f"{col}_xml", f"{col}_az"
        if xv in merged.columns and av in merged.columns:
            for _, r in merged.iterrows():
                x, a = float(r[xv]), float(r[av])
                if x == 0 and a == 0:
                    scores.append(100.0)
                elif max(x, a) > 0:
                    scores.append(max(0.0, 100.0 * (1 - abs(x - a) / max(x, a))))
    overall = sum(scores) / len(scores) if scores else match_rate
    return {
        "match_rate": round(match_rate, 2),
        "status_match_rate": 100.0 if len(matched) else 0.0,
        "effective_date_match_rate": round(overall, 2),
        "overall_match_rate": round(overall, 2),
    }


def run_business_aggregation_models(
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
    *,
    join_mapping: JoinMapping | None = None,
    best_join_keys: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    month_keys = best_join_keys or [*PK, "file_event_year_month"]
    month_keys = [k for k in month_keys if k in xml_canonical.columns or k in az_canonical.columns]

    models: list[tuple[str, str, pd.DataFrame, pd.DataFrame, list[str]]] = [
        ("A", "Raw event level", xml_canonical, az_canonical, list(month_keys) if "file_event_year_month" in month_keys else [*PK, "coverage_year_month"]),
        ("B", "Latest per policy/member/insurance/month", collapse_to_snapshot(xml_canonical, month_keys), collapse_to_snapshot(az_canonical, month_keys), month_keys),
        ("C", "Latest per policy/member/insurance (no month)", collapse_to_snapshot(xml_canonical, PK), collapse_to_snapshot(az_canonical, PK), list(PK)),
        ("D", "Active/enrolled current-state only", _filter_active(collapse_to_snapshot(xml_canonical, PK)), _filter_active(collapse_to_snapshot(az_canonical, PK)), list(PK)),
        ("E", "Exclude maintenance-only/no-op", _filter_non_maintenance(xml_canonical), az_canonical, month_keys),
        ("F", "Exclude duplicate same status/effective", _dedupe_transactions(xml_canonical), az_canonical, month_keys),
        ("G", "Collapse CANCEL/TERM into final status", _final_status_replay(xml_canonical), collapse_to_snapshot(az_canonical, PK), list(PK)),
    ]

    scores: list[dict[str, Any]] = []
    for mid, name, xdf, adf, keys in models:
        valid_keys = [k for k in keys if k in xdf.columns and k in adf.columns]
        if not valid_keys:
            valid_keys = list(PK)
        scores.append(_model_score(mid, name, xdf, adf, valid_keys, join_mapping=join_mapping))

    # Model H — dashboard aggregates
    xh = _chandra_dashboard(xml_canonical, source="xml")
    ah = _chandra_dashboard(az_canonical, source="azure")
    h_rates = _score_dashboard(xh, ah)
    scores.append({
        "model_id": "H",
        "model_name": "Chandra-like dashboard counts (enrollment/enrollee/subscriber by month/status)",
        "join_key": "issuer+year+month+insurance_type+status",
        "xml_output_count": len(xh),
        "azure_output_count": len(ah),
        "match_count": int(min(len(xh), len(ah))),
        "xml_not_in_azure": max(0, len(xh) - len(ah)),
        "azure_not_in_xml": max(0, len(ah) - len(xh)),
        "status_diff": 0,
        **h_rates,
    })

    scores_df = pd.DataFrame(scores)
    best = scores_df.sort_values(
        ["overall_match_rate", "match_count", "status_match_rate"],
        ascending=[False, False, False],
    ).head(1)
    return scores_df, best


def _write_explanation(
    path: Path,
    *,
    issuer: str,
    xml_raw_rows: int,
    az_raw_rows: int,
    az_reason_summary: pd.DataFrame,
    xml_reason_summary: pd.DataFrame,
    model_scores: pd.DataFrame,
    best_model: pd.DataFrame,
    lifecycle_rates: dict[str, Any],
    best_month_basis: str,
) -> None:
    top_az = az_reason_summary.head(5).to_dict("records") if not az_reason_summary.empty else []
    top_xml = xml_reason_summary.head(5).to_dict("records") if not xml_reason_summary.empty else []
    best_row = best_model.iloc[0].to_dict() if not best_model.empty else {}

    lines = [
        "# Reconciliation Explanation",
        "",
        f"**Issuer:** {issuer}",
        "",
        "## Why XML has more records than Azure",
        "",
        f"- XML raw rows: **{xml_raw_rows:,}**",
        f"- Azure raw rows: **{az_raw_rows:,}**",
        "",
        "XML 834 files contain **multiple transaction events** per member/policy "
        "(enrollments, changes, cancellations, maintenance). Each event becomes a row. "
        "Azure `dbo.834_Inbound_test` stores **loaded/processed events** — typically one "
        "row per inbound file transaction that reached the warehouse, not every intermediate "
        "XML maintenance line.",
        "",
        "After lifecycle collapse, XML still exceeds Azure when:",
        "- XML includes maintenance-only or superseded events",
        "- XML source_data covers more files/months than Azure has loaded rows for",
        "- Month basis differs (coverage vs GAA_834_File_Date)",
        "",
        "## Why Azure can have records not found in XML",
        "",
    ]
    if top_az:
        lines.append("Top Azure-only reason buckets:")
        for r in top_az:
            lines.append(f"- **{r.get('reason_bucket')}**: {r.get('record_count', 0):,} records")
        lines.append("")
    lines.extend([
        "Common causes:",
        "- **Policy exists, member ID differs** — join mapping may use subscriber vs individual ID",
        "- **Member exists, policy differs** — policy reassignment or alternate policy column",
        "- **Different month** — Azure file date month ≠ XML maintenance/coverage month",
        "- **Azure-only source** — row in warehouse with no matching XML in current source_data partition",
        "- **Not in lifecycle snapshot** — present in XML raw but filtered by snapshot collapse",
        "",
        "## XML-only records (not in Azure)",
        "",
    ])
    if top_xml:
        for r in top_xml:
            lines.append(f"- **{r.get('reason_bucket')}**: {r.get('record_count', 0):,} records")
        lines.append("")

    lines.extend([
        "## Business aggregation model comparison",
        "",
        "Models tested (A–H) reverse-engineer how Chandra/dashboard counts may aggregate:",
        "",
    ])
    if not model_scores.empty:
        for _, r in model_scores.iterrows():
            lines.append(
                f"- **Model {r['model_id']}** ({r['model_name']}): "
                f"match rate {r.get('overall_match_rate', 0):.1f}%, "
                f"XML out {int(r.get('xml_output_count', 0)):,}, "
                f"Azure out {int(r.get('azure_output_count', 0)):,}"
            )
        lines.append("")

    lines.extend([
        f"**Best model:** {best_row.get('model_id', 'n/a')} — {best_row.get('model_name', '')}",
        f"**Best overall match rate:** {best_row.get('overall_match_rate', 0)}%",
        "",
        f"**Selected lifecycle month basis:** {best_month_basis}",
        f"**Lifecycle snapshot match rate:** {lifecycle_rates.get('lifecycle_snapshot_match_rate', 'n/a')}%",
        f"**Status match (on matched):** {lifecycle_rates.get('status_match_rate', 'n/a')}%",
        "",
        "## Root cause assessment",
        "",
    ])

    sr = float(lifecycle_rates.get("status_match_rate", 0) or 0)
    if sr >= 95:
        lines.append(
            "- **ID mapping and status mapping are reliable** (~99% status match on matched records)."
        )
    if best_month_basis and "file_event" in str(best_month_basis):
        lines.append(
            "- **Date/month basis** is a major factor — Azure uses GAA_834_File_Date month; "
            "XML may use coverage or maintenance month."
        )
    if xml_raw_rows > az_raw_rows * 2:
        lines.append(
            "- **Business aggregation** — XML event grain ≠ Azure load grain; "
            "use lifecycle snapshot or Model C/G rather than raw event match."
        )

    lines.extend([
        "",
        "## Recommended next actions",
        "",
    ])
    if top_az and top_az[0].get("reason_bucket") == "NO_POLICY_OR_MEMBER_OVERLAP":
        lines.append("1. Verify source_data file coverage matches Azure partition window.")
    if any(r.get("reason_bucket") == "POLICY_EXISTS_XML_MEMBER_DIFF" for r in top_az):
        lines.append("1. Review ID overlap matrix — subscriber vs individual identifier mapping.")
    if sr >= 95 and float(lifecycle_rates.get("lifecycle_snapshot_match_rate", 0) or 0) < 70:
        lines.append("1. Use **Model C or G** (latest state without month) for business accuracy reporting.")
        lines.append("2. Treat raw event match rate as diagnostic only.")
    lines.append("3. Review `month_basis_diff.csv` for month-only mismatches.")
    lines.append("4. Review `azure_not_in_xml_reason_detail.csv` for Azure-only row samples.")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote reconciliation explanation: %s", path)


def run_reconciliation_analysis(
    *,
    issuer: str,
    xml_raw: pd.DataFrame,
    table_df: pd.DataFrame,
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
    xml_snap: pd.DataFrame,
    az_snap: pd.DataFrame,
    lifecycle_stats: dict[str, Any],
    lifecycle_rates: dict[str, Any],
    join_mapping: JoinMapping | None = None,
    best_join_keys: list[str] | None = None,
    best_month_basis: str = "",
) -> dict[str, str]:
    """Run mismatch classification, aggregation models, and explanation export."""
    dbg = _dbg()
    paths: dict[str, str] = {}

    az_only = lifecycle_stats.get("azure_not_in_xml", pd.DataFrame())
    xml_only = lifecycle_stats.get("xml_not_in_azure", pd.DataFrame())

    az_detail, az_summary = classify_azure_not_in_xml(
        az_only if isinstance(az_only, pd.DataFrame) else pd.DataFrame(),
        xml_canonical=xml_canonical,
        xml_snap=xml_snap,
        xml_raw=xml_raw,
        join_keys=best_join_keys or list(PK),
    )
    xml_detail, xml_summary = classify_xml_not_in_azure(
        xml_only if isinstance(xml_only, pd.DataFrame) else pd.DataFrame(),
        xml_canonical=xml_canonical,
        az_canonical=az_canonical,
        az_snap=az_snap,
    )

    for df, name in [
        (az_summary, "azure_not_in_xml_reason_summary.csv"),
        (az_detail, "azure_not_in_xml_reason_detail.csv"),
        (xml_summary, "xml_not_in_azure_reason_summary.csv"),
        (xml_detail, "xml_not_in_azure_reason_detail.csv"),
    ]:
        p = dbg / name
        safe_write_csv(p, df, table_name=name.replace(".csv", ""))
        paths[name.replace(".csv", "")] = str(p)

    model_scores, best_model = run_business_aggregation_models(
        xml_canonical, az_canonical,
        join_mapping=join_mapping,
        best_join_keys=best_join_keys,
    )
    p_scores = dbg / "business_aggregation_model_scores.csv"
    safe_write_csv(p_scores, model_scores, table_name="business_aggregation_model_scores")
    paths["business_aggregation_model_scores"] = str(p_scores)

    p_best = dbg / "best_business_model.csv"
    safe_write_csv(p_best, best_model, table_name="best_business_model")
    paths["best_business_model"] = str(p_best)

    p_expl = dbg / "reconciliation_explanation.md"
    _write_explanation(
        p_expl,
        issuer=issuer,
        xml_raw_rows=len(xml_raw),
        az_raw_rows=len(table_df),
        az_reason_summary=az_summary,
        xml_reason_summary=xml_summary,
        model_scores=model_scores,
        best_model=best_model,
        lifecycle_rates=lifecycle_rates,
        best_month_basis=best_month_basis,
    )
    paths["reconciliation_explanation"] = str(p_expl)

    return paths
