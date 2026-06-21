"""Score Azure logic strategies against XML monthly summaries."""

from __future__ import annotations

from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def _month_totals(strategy_df: pd.DataFrame) -> pd.DataFrame:
    if strategy_df.empty:
        return pd.DataFrame(columns=[
            "strategy_id", "source_table", "year", "month",
            "enrollment_count", "enrollee_count", "raw_rows",
        ])
    return (
        strategy_df.groupby(
            ["strategy_id", "strategy_name", "source_table", "year", "month"],
            dropna=False,
        )
        .agg(
            enrollment_count=("enrollment_count", "sum"),
            enrollee_count=("enrollee_count", "sum"),
            raw_rows=("raw_rows", "sum"),
        )
        .reset_index()
    )


def _status_totals(strategy_df: pd.DataFrame) -> pd.DataFrame:
    if strategy_df.empty:
        return pd.DataFrame()
    return (
        strategy_df.groupby(["strategy_id", "year", "month", "status"], dropna=False)
        ["enrollee_count"].sum().reset_index()
    )


def score_strategy_vs_xml(
    strategy_df: pd.DataFrame,
    xml_totals: pd.DataFrame,
    *,
    strategy_id: str,
    source_table: str,
    logic_type: str,
    source_date_column: str = "",
    source_status_column: str = "",
    source_policy_column: str = "",
    source_member_column: str = "",
    missing_column_penalty: float = 0.0,
) -> dict[str, Any]:
    """Score one strategy/table combination against XML reference."""
    if strategy_df.empty:
        return {
            "strategy_id": strategy_id,
            "source_table": source_table,
            "logic_type": logic_type,
            "confidence_score": 0.0,
            "enrollment_count_diff_total": None,
            "enrollee_count_diff_total": None,
            "month_coverage_pct": 0.0,
            "month_pattern_variance": None,
            "status_similarity": None,
            "missing_column_penalty": missing_column_penalty,
            "notes": "No strategy rows produced",
        }

    st = strategy_df[strategy_df["strategy_id"] == strategy_id].copy()
    if st.empty:
        st = strategy_df.copy()

    st_month = _month_totals(st)
    xml_month = (
        xml_totals.groupby(["year", "month"], dropna=False)
        .agg(enrollment_count=("enrollment_count", "sum"), enrollee_count=("enrollee_count", "sum"))
        .reset_index()
        if not xml_totals.empty else pd.DataFrame()
    )

    merged = st_month.merge(
        xml_month, on=["year", "month"], how="outer", suffixes=("_az", "_xml"), indicator=True
    )

    enrollee_diffs: list[float] = []
    enroll_diffs: list[float] = []
    for _, row in merged.iterrows():
        if row["_merge"] == "both":
            enrollee_diffs.append(abs(float(row.get("enrollee_count_az", 0) or 0) - float(row.get("enrollee_count_xml", 0) or 0)))
            enroll_diffs.append(abs(float(row.get("enrollment_count_az", 0) or 0) - float(row.get("enrollment_count_xml", 0) or 0)))

    xml_months = set(zip(xml_month["year"].astype(str), xml_month["month"].astype(str))) if not xml_month.empty else set()
    az_months = set(zip(st_month["year"].astype(str), st_month["month"].astype(str))) if not st_month.empty else set()
    coverage = len(xml_months & az_months) / len(xml_months) if xml_months else 0.0

    # Month pattern: penalize strategies where every month has identical enrollee count (snapshot)
    monthly_enrollees = st_month.groupby(["year", "month"])["enrollee_count"].sum()
    pattern_var = float(monthly_enrollees.var()) if len(monthly_enrollees) > 1 else 0.0
    pattern_bonus = min(pattern_var / 100.0, 10.0) if pattern_var > 0 else -5.0

    # Status similarity (simple overlap)
    status_sim = None
    if not xml_totals.empty and not st.empty:
        xml_st = set(xml_totals["status"].astype(str).unique())
        az_st = set(st["status"].astype(str).unique())
        if xml_st:
            status_sim = len(xml_st & az_st) / len(xml_st)

    total_enrollee_diff = sum(enrollee_diffs) if enrollee_diffs else None
    total_enroll_diff = sum(enroll_diffs) if enroll_diffs else None

    # Lower diff = better; higher coverage & status sim = better; pattern bonus for varying months
    diff_penalty = (total_enrollee_diff or 9999) / max(1.0, float(xml_month["enrollee_count"].sum()) if not xml_month.empty else 1.0)
    score = 100.0
    score -= min(diff_penalty * 50, 60)
    score += coverage * 20
    score += (status_sim or 0) * 15
    score += pattern_bonus
    score -= missing_column_penalty
    score = max(0.0, min(100.0, score))

    notes_parts = []
    if pattern_var == 0 and len(monthly_enrollees) > 1:
        notes_parts.append("Identical counts all months — likely snapshot not event logic")
    if logic_type == "event":
        notes_parts.append("Event-based logic — check month-to-month variation")
    if "834_Inbound" in source_table:
        notes_parts.append("834 inbound table — candidate for XML transaction match")

    return {
        "strategy_id": strategy_id,
        "source_table": source_table,
        "logic_type": logic_type,
        "source_date_column": source_date_column,
        "source_status_column": source_status_column,
        "source_policy_column": source_policy_column,
        "source_member_column": source_member_column,
        "confidence_score": round(score, 2),
        "enrollment_count_diff_total": total_enroll_diff,
        "enrollee_count_diff_total": total_enrollee_diff,
        "month_coverage_pct": round(coverage * 100, 1),
        "month_pattern_variance": round(pattern_var, 2),
        "status_similarity": round(status_sim, 3) if status_sim is not None else None,
        "missing_column_penalty": missing_column_penalty,
        "notes": "; ".join(notes_parts) if notes_parts else "",
    }


def closest_match_by_month(
    all_strategies: pd.DataFrame,
    xml_totals: pd.DataFrame,
) -> pd.DataFrame:
    """For each XML month, find strategy with smallest enrollee diff."""
    if xml_totals.empty or all_strategies.empty:
        return pd.DataFrame()

    st_month = _month_totals(all_strategies)
    rows: list[dict[str, Any]] = []

    for _, xml_row in xml_totals.groupby(["year", "month"], dropna=False).agg(
        {"enrollee_count": "sum", "enrollment_count": "sum"}
    ).reset_index().iterrows():
        year, month = str(xml_row["year"]), str(xml_row["month"]).zfill(2)
        xml_e = float(xml_row["enrollee_count"])
        subset = st_month[(st_month["year"].astype(str) == year) & (st_month["month"].astype(str).str.zfill(2) == month)]
        if subset.empty:
            rows.append({"year": year, "month": month, "xml_enrollee_count": xml_e, "best_strategy": None, "best_diff": None})
            continue
        subset = subset.copy()
        subset["diff"] = (subset["enrollee_count"] - xml_e).abs()
        best = subset.loc[subset["diff"].idxmin()]
        rows.append({
            "year": year,
            "month": month,
            "xml_enrollee_count": xml_e,
            "best_strategy_id": best["strategy_id"],
            "best_source_table": best["source_table"],
            "best_enrollee_count": best["enrollee_count"],
            "best_diff": best["diff"],
        })
    return pd.DataFrame(rows)


def build_recommendation(scores_df: pd.DataFrame) -> pd.DataFrame:
    if scores_df.empty:
        return pd.DataFrame([{"recommendation": "No strategies scored — check Azure table access and XML summaries"}])

    best = scores_df.sort_values("confidence_score", ascending=False).iloc[0]
    rows = [{
        "best_azure_table": best["source_table"],
        "best_strategy_id": best["strategy_id"],
        "best_logic_type": best["logic_type"],
        "best_date_column": best.get("source_date_column", ""),
        "best_status_column": best.get("source_status_column", ""),
        "best_policy_column": best.get("source_policy_column", ""),
        "best_member_column": best.get("source_member_column", ""),
        "confidence_score": best["confidence_score"],
        "snapshot_or_event": best["logic_type"],
        "why_recommended": (
            f"Strategy {best['strategy_id']} on {best['source_table']} scored highest "
            f"({best['confidence_score']}). {best.get('notes', '')}"
        ),
        "what_still_does_not_match": (
            "Review enrollee_count_diff_total and closest_match_by_month. "
            "Event/lifecycle replay may still differ from Chandra if Azure stores snapshots."
        ),
    }]

    # Also list top 3
    for i, (_, row) in enumerate(scores_df.sort_values("confidence_score", ascending=False).head(3).iterrows()):
        rows.append({
            "rank": i + 1,
            "best_azure_table": row["source_table"],
            "best_strategy_id": row["strategy_id"],
            "confidence_score": row["confidence_score"],
            "why_recommended": row.get("notes", ""),
        })

    return pd.DataFrame(rows)
