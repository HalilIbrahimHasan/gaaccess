"""
Final comparison engine — determine which Azure strategy reproduces XML business logic.

Discovery is frozen (read-only). This module compares strategies A/B/C at 9 business levels.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_mirror.discovery.strategies import (
    STRATEGY_META,
    run_applicable_strategies,
)
from azure_reconciliation.azure_mirror.discovery.table_inspector import (
    TableProfile,
    fetch_issuer_year_sample,
    inspect_table,
)
from azure_reconciliation.discovery_engine import discover_candidate_tables
from azure_reconciliation.df_utils import col_series as _col_series, zmonth as _zmonth
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

# Discovery frozen — final comparison evaluates A/B/C only (full mode only).
FINAL_STRATEGIES = ("A", "B", "C")

LEVEL_WEIGHTS = {
    "enrollment_match": 0.15,
    "enrollee_match": 0.15,
    "subscriber_match": 0.05,
    "status_match": 0.15,
    "lifecycle_match": 0.20,
    "timeline_match": 0.10,
    "join_key_match": 0.05,
    "issuer_accuracy": 0.10,
    "business_timeline_match": 0.05,
}


@dataclass
class ComparisonResult:
    strategy_id: str
    source_table: str
    date_column: str
    status_column: str
    join_key: str
    status_mapping: dict[str, str]
    scores: dict[str, float]
    overall_accuracy: float
    detail_rows: list[dict[str, Any]] = field(default_factory=list)
    low_confidence_reason: str = ""
    behavior_penalty: float = 0.0
    behavior_bonus: float = 0.0
    behavior_notes: str = ""
    rejected: bool = False
    rejection_reason: str = ""


def _strategy_behavior_adjustment(
    *,
    strategy_id: str,
    strat_df: pd.DataFrame,
    profile: TableProfile,
    table_df: pd.DataFrame,
    date_column: str,
) -> tuple[float, float, str]:
    """
    Penalize snapshot-like behavior; reward event-like tables (e.g. 834_Inbound_test).
    Returns (penalty, bonus, notes).
    """
    penalty = 0.0
    bonus = 0.0
    notes: list[str] = []
    monthly = _strategy_monthly(strat_df)

    if len(monthly) >= 2:
        for col in ("enrollee_count", "enrollment_count"):
            if col in monthly.columns:
                vals = monthly[col].astype(float).tolist()
                if vals and len(set(vals)) == 1:
                    penalty += 12.0
                    notes.append(f"same {col} repeated every month (static snapshot)")
                    break

    if not date_column and not profile.file_date_col and not profile.event_date_cols:
        penalty += 8.0
        notes.append("no event/date column detected")

    if not profile.status_col and not profile.action_col:
        penalty += 8.0
        notes.append("no status/action lifecycle fields")

    if strategy_id == "A":
        penalty += 6.0
        notes.append("Strategy A is active-coverage snapshot logic")

    if strategy_id == "B" and len(monthly) >= 2:
        enrollee_std = monthly["enrollee_count"].astype(float).std()
        if enrollee_std == 0 or pd.isna(enrollee_std):
            penalty += 5.0
            notes.append("Strategy B shows no monthly enrollee movement")

    # Event-like bonus — especially 834_Inbound_test
    table_l = profile.table.lower()
    cols_l = {c.lower() for c in table_df.columns}
    inbound_markers = {
        "gaa_834_file_date", "actioncode", "enrolleestatus",
        "exchgassignedpolicyid", "exchgindividentifier",
    }
    if "834_inbound" in table_l:
        present = inbound_markers & cols_l
        if len(present) >= 4:
            bonus += 8.0
            notes.append(f"834 inbound event candidate (+{len(present)} key columns)")
        elif present:
            bonus += 4.0
            notes.append("834 inbound partial event columns")

    if date_column or profile.file_date_col or profile.event_date_cols:
        bonus += 3.0
    if profile.status_col or profile.action_col:
        bonus += 2.0
    if strategy_id == "C":
        bonus += 4.0
        notes.append("Strategy C event-date logic preferred")

    if len(monthly) >= 3:
        spread = monthly["enrollee_count"].astype(float).max() - monthly["enrollee_count"].astype(float).min()
        if spread > 0:
            bonus += min(5.0, spread / max(1.0, monthly["enrollee_count"].astype(float).mean()) * 10)

    return penalty, bonus, "; ".join(notes)


def historical_partitions(partitions: list[Partition]) -> list[Partition]:
    """Chronological window from discovered source_data (includes 2025 lookback + 2026)."""
    return sorted(partitions, key=lambda p: p.sort_key)


def historical_window_label(partitions: list[Partition]) -> str:
    parts = historical_partitions(partitions)
    if not parts:
        return "none"
    first, last = parts[0], parts[-1]
    return f"{first.year}-{first.month.zfill(2)} through {last.year}-{last.month.zfill(2)}"


def _aggregate_monthly(df: pd.DataFrame, *, status_col: str = "canonical_status") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["year", "month", "enrollment_count", "enrollee_count", "subscriber_count"])
    work = df.copy()
    work["year"] = _col_series(work, "coverage_year", "year").astype(str)
    work["month"] = _col_series(work, "snapshot_month", "month").astype(str).map(_zmonth)
    if status_col in work.columns:
        work["status"] = work[status_col].astype(str).map(normalize_status)
    else:
        work["status"] = "UNKNOWN"
    pol = "enrollment_id" if "enrollment_id" in work.columns else "policy_id"
    mem = "enrollee_id" if "enrollee_id" in work.columns else "member_id"
    rows = []
    for (yr, mo), grp in work.groupby(["year", "month"], dropna=False):
        sub_count = None
        if "subscriber_flag" in grp.columns:
            sub_count = int((grp["subscriber_flag"].astype(str).str.upper() == "Y").sum())
        rows.append({
            "year": yr,
            "month": mo,
            "enrollment_count": grp[pol].nunique() if pol in grp.columns else len(grp),
            "enrollee_count": grp[mem].nunique() if mem in grp.columns else len(grp),
            "subscriber_count": sub_count,
        })
    return pd.DataFrame(rows)


def _pct_match(xml_val: float, az_val: float) -> float:
    if xml_val == 0 and az_val == 0:
        return 100.0
    if xml_val == 0:
        return 0.0
    diff = abs(xml_val - az_val)
    return max(0.0, 100.0 * (1.0 - diff / max(xml_val, az_val)))


def _status_distribution(df: pd.DataFrame, *, status_col: str = "canonical_status") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["year", "month", "status", "count"])
    work = df.copy()
    work["year"] = _col_series(work, "coverage_year", "year").astype(str)
    work["month"] = _col_series(work, "snapshot_month", "month").astype(str).map(_zmonth)
    if status_col in work.columns:
        work["status"] = work[status_col].astype(str).map(normalize_status)
    else:
        work["status"] = _col_series(work, status_col).astype(str).map(normalize_status)
    mem = "enrollee_id" if "enrollee_id" in work.columns else "member_id"
    return (
        work.groupby(["year", "month", "status"], dropna=False)[mem]
        .nunique().reset_index(name="count")
        if mem in work.columns else pd.DataFrame()
    )


def _member_key_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["join_issuer"] = _col_series(work, "issuer", "issuer_id").astype(str)
    work["join_policy"] = _col_series(work, "enrollment_id", "policy_id").astype(str)
    work["join_member"] = _col_series(work, "enrollee_id", "member_id").astype(str)
    work["join_insurance"] = _col_series(work, "insurance_type", "insurance_type_code").map(normalize_insurance_type)
    work["_join_key"] = (
        work["join_issuer"] + "|" + work["join_policy"] + "|"
        + work["join_member"] + "|" + work["join_insurance"]
    )
    return work


def learn_status_mapping(xml_df: pd.DataFrame, az_df: pd.DataFrame) -> dict[str, str]:
    """Learn XML→Azure status mapping by maximizing agreement on matched join keys."""
    if xml_df.empty or az_df.empty:
        return {}
    xl = _member_key_frame(xml_df)
    az = _member_key_frame(az_df)
    xl["xml_status_raw"] = _col_series(xl, "canonical_status", "additional_maint_reason_code").astype(str)
    az["az_status_raw"] = _col_series(az, "canonical_status", "enrollment_status").astype(str)

    merged = xl.merge(
        az, on="_join_key", how="inner", suffixes=("_xml", "_az"),
    )
    if merged.empty:
        return {}

    mapping: dict[str, str] = {}
    for xml_st, grp in merged.groupby("xml_status_raw"):
        if not str(xml_st).strip():
            continue
        counts = grp["az_status_raw"].value_counts()
        if counts.empty:
            continue
        best_az = str(counts.index[0])
        mapping[str(xml_st).upper()] = normalize_status(best_az)
    logger.info("Learned status mapping (%d pairs): %s", len(mapping), mapping)
    return mapping


def _apply_status_mapping(series: pd.Series, mapping: dict[str, str]) -> pd.Series:
    def _map_val(raw: str) -> str:
        key = str(raw).strip().upper()
        if key in mapping:
            return mapping[key]
        return normalize_status(raw)
    return series.map(_map_val)


def _compare_monthly_totals(xml_monthly: pd.DataFrame, az_monthly: pd.DataFrame, metric: str) -> float:
    if xml_monthly.empty or az_monthly.empty or metric not in xml_monthly.columns:
        return 0.0
    merged = xml_monthly.merge(az_monthly, on=["year", "month"], how="outer", suffixes=("_xml", "_az"))
    scores = []
    for _, row in merged.iterrows():
        xv = float(row.get(f"{metric}_xml", 0) or 0)
        av = float(row.get(f"{metric}_az", 0) or 0)
        scores.append(_pct_match(xv, av))
    return sum(scores) / len(scores) if scores else 0.0


def _compare_status_distribution(xml_dist: pd.DataFrame, az_dist: pd.DataFrame) -> float:
    if xml_dist.empty or az_dist.empty:
        return 0.0
    merged = xml_dist.merge(
        az_dist, on=["year", "month", "status"], how="outer", suffixes=("_xml", "_az")
    ).fillna(0)
    scores = []
    for _, row in merged.iterrows():
        scores.append(_pct_match(float(row.get("count_xml", 0)), float(row.get("count_az", 0))))
    return sum(scores) / len(scores) if scores else 0.0


def _monthly_transition_match(xml_monthly: pd.DataFrame, az_monthly: pd.DataFrame) -> float:
    """Compare month-over-month enrollee movement (aggregate lifecycle proxy)."""
    if xml_monthly.empty or az_monthly.empty:
        return 0.0
    xm = xml_monthly.sort_values(["year", "month"]).copy()
    am = az_monthly.sort_values(["year", "month"]).copy()
    xm["transition"] = xm["enrollee_count"].diff().fillna(0).astype(int).astype(str)
    am["transition"] = am["enrollee_count"].diff().fillna(0).astype(int).astype(str)
    merged = xm.merge(am, on=["year", "month"], how="outer", suffixes=("_xml", "_az"))
    scores = []
    for _, row in merged.iterrows():
        xv = abs(float(row.get("transition_xml", 0) or 0))
        av = abs(float(row.get("transition_az", 0) or 0))
        scores.append(_pct_match(xv, av) if xv or av else 100.0)
    return sum(scores) / len(scores) if scores else 0.0


def _azure_frame_for_mapping(
    table_df: pd.DataFrame,
    profile: TableProfile,
    status_column: str,
) -> pd.DataFrame:
    if table_df.empty:
        return table_df
    az = table_df.copy()
    if profile.issuer_col and profile.issuer_col in az.columns:
        az["issuer"] = az[profile.issuer_col]
    if profile.policy_col and profile.policy_col in az.columns:
        az["enrollment_id"] = az[profile.policy_col]
    if profile.member_col and profile.member_col in az.columns:
        az["enrollee_id"] = az[profile.member_col]
    st_col = status_column or profile.status_col or profile.action_col or ""
    if st_col and st_col in az.columns:
        az["canonical_status"] = az[st_col].astype(str).map(normalize_status)
    elif profile.status_col and profile.status_col in az.columns:
        az["canonical_status"] = az[profile.status_col].astype(str).map(normalize_status)
    return az


def _timeline_match(xml_monthly: pd.DataFrame, az_monthly: pd.DataFrame) -> float:
    if xml_monthly.empty or az_monthly.empty:
        return 0.0
    xm = xml_monthly.sort_values(["year", "month"]).copy()
    am = az_monthly.sort_values(["year", "month"]).copy()
    xm["delta"] = xm["enrollee_count"].diff().fillna(0)
    am["delta"] = am["enrollee_count"].diff().fillna(0)
    merged = xm.merge(am, on=["year", "month"], how="outer", suffixes=("_xml", "_az"))
    scores = []
    for _, row in merged.iterrows():
        scores.append(_pct_match(abs(float(row.get("delta_xml", 0))), abs(float(row.get("delta_az", 0))) or 1))
    return sum(scores) / len(scores) if scores else 0.0


def _join_key_quality(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    work = _member_key_frame(df)
    if work.empty or "_join_key" not in work.columns:
        return 0.0
    dup = int(work.duplicated(subset=["_join_key"]).sum())
    total = len(work)
    if total == 0:
        return 0.0
    return max(0.0, 100.0 * (1.0 - dup / total))


def _issuer_month_accuracy(
    xml_lifecycle: pd.DataFrame,
    az_monthly: pd.DataFrame,
    partitions: list[Partition],
) -> float:
    scores: list[float] = []
    for part in historical_partitions(partitions):
        if xml_lifecycle.empty:
            continue
        yr = _col_series(xml_lifecycle, "coverage_year", "year").astype(str)
        mo = _col_series(xml_lifecycle, "snapshot_month", "month").astype(str).str.zfill(2)
        xml_p = xml_lifecycle[(yr == part.year) & (mo == _zmonth(part.month))]
        if xml_p.empty:
            continue
        xm = _aggregate_monthly(xml_p)
        am = az_monthly[
            (az_monthly["year"].astype(str) == part.year)
            & (az_monthly["month"].astype(str).str.zfill(2) == _zmonth(part.month))
        ] if not az_monthly.empty else pd.DataFrame()
        scores.append(_compare_monthly_totals(xm, am, "enrollee_count"))
    return sum(scores) / len(scores) if scores else 0.0


def _strategy_monthly(strat_df: pd.DataFrame) -> pd.DataFrame:
    if strat_df.empty:
        return pd.DataFrame(columns=["year", "month", "enrollment_count", "enrollee_count", "subscriber_count"])
    work = strat_df.copy()
    work["year"] = work["year"].astype(str)
    work["month"] = work["month"].astype(str).apply(_zmonth)
    agg: dict[str, tuple[str, str]] = {
        "enrollment_count": ("enrollment_count", "sum"),
        "enrollee_count": ("enrollee_count", "sum"),
    }
    if "subscriber_count" in work.columns:
        agg["subscriber_count"] = ("subscriber_count", "sum")
    return work.groupby(["year", "month"], dropna=False).agg(**agg).reset_index()


def _strategy_status_dist(strat_df: pd.DataFrame) -> pd.DataFrame:
    if strat_df.empty:
        return pd.DataFrame(columns=["year", "month", "status", "count"])
    work = strat_df.copy()
    work["year"] = work["year"].astype(str)
    work["month"] = work["month"].astype(str).apply(_zmonth)
    work["status"] = _col_series(work, "status").astype(str).map(normalize_status)
    return work.groupby(["year", "month", "status"], dropna=False)["enrollee_count"].sum().reset_index(name="count")


def _learn_status_mapping_from_distributions(
    xml_dist: pd.DataFrame, az_dist: pd.DataFrame,
) -> dict[str, str]:
    """Learn mapping by aligning status frequency vectors (no hardcoding)."""
    if xml_dist.empty or az_dist.empty:
        return {}
    x_tot = xml_dist.groupby("status")["count"].sum()
    a_tot = az_dist.groupby("status")["count"].sum()
    mapping: dict[str, str] = {}
    used_az: set[str] = set()
    for xml_st in x_tot.index:
        xml_canon = normalize_status(str(xml_st))
        best_az, best_score = None, -1.0
        for az_st in a_tot.index:
            az_canon = normalize_status(str(az_st))
            if az_canon in used_az:
                continue
            score = min(x_tot[xml_st], a_tot[az_st])
            if score > best_score:
                best_score = score
                best_az = az_canon
        if best_az:
            mapping[str(xml_st).upper()] = best_az
            used_az.add(best_az)
    return mapping


def compare_strategy(
    *,
    strategy_id: str,
    source_table: str,
    strat_df: pd.DataFrame,
    profile: TableProfile,
    xml_lifecycle: pd.DataFrame,
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
    date_column: str,
    status_column: str,
    table_df: pd.DataFrame | None = None,
) -> ComparisonResult:
    """Run all 9 comparison levels for one strategy/table."""
    pol = profile.policy_col or "enrollment_id"
    mem = profile.member_col or "enrollee_id"
    join_key = f"issuer + {pol} + {mem} + insurance_type"

    az_monthly = _strategy_monthly(strat_df)
    xml_monthly = _aggregate_monthly(xml_lifecycle)
    xml_dist = _status_distribution(xml_lifecycle)
    az_dist = _strategy_status_dist(strat_df)
    status_mapping = _learn_status_mapping_from_distributions(xml_dist, az_dist)
    if table_df is not None and not table_df.empty and not xml_raw.empty:
        az_frame = _azure_frame_for_mapping(table_df, profile, status_column)
        key_mapping = learn_status_mapping(xml_raw, az_frame)
        if key_mapping:
            status_mapping = key_mapping

    scores = {
        "enrollment_match": _compare_monthly_totals(xml_monthly, az_monthly, "enrollment_count"),
        "enrollee_match": _compare_monthly_totals(xml_monthly, az_monthly, "enrollee_count"),
        "subscriber_match": _compare_monthly_totals(xml_monthly, az_monthly, "subscriber_count"),
        "status_match": _compare_status_distribution(xml_dist, az_dist),
        "lifecycle_match": _monthly_transition_match(xml_monthly, az_monthly),
        "timeline_match": _timeline_match(xml_monthly, az_monthly),
        "join_key_match": _join_key_quality(_member_key_frame(xml_raw)) if not xml_raw.empty else 50.0,
        "issuer_accuracy": _issuer_month_accuracy(xml_lifecycle, az_monthly, partitions),
        "business_timeline_match": _timeline_match(xml_monthly, az_monthly),
    }
    if table_df is not None and not table_df.empty:
        az_keys = table_df.copy()
        if profile.issuer_col and profile.issuer_col in az_keys.columns:
            az_keys["issuer"] = az_keys[profile.issuer_col]
        if pol in az_keys.columns:
            az_keys["enrollment_id"] = az_keys[pol]
        if mem in az_keys.columns:
            az_keys["enrollee_id"] = az_keys[mem]
        scores["join_key_match"] = (
            _join_key_quality(_member_key_frame(xml_raw))
            + _join_key_quality(_member_key_frame(az_keys))
        ) / 2.0

    overall = sum(scores[k] * LEVEL_WEIGHTS.get(k, 0) for k in scores)
    penalty, bonus, behavior_notes = _strategy_behavior_adjustment(
        strategy_id=strategy_id,
        strat_df=strat_df,
        profile=profile,
        table_df=table_df if table_df is not None else pd.DataFrame(),
        date_column=date_column,
    )
    overall = overall - penalty + bonus
    overall = round(min(100.0, max(0.0, overall)), 2)

    reason = ""
    if overall < 90.0:
        weak = sorted(scores.items(), key=lambda x: x[1])[:3]
        reason = (
            f"Overall below 90% — weakest levels: "
            + ", ".join(f"{k}={v:.1f}%" for k, v in weak)
        )
        if behavior_notes:
            reason += f"; behavior: {behavior_notes}"
        if penalty > 0:
            reason += f"; snapshot penalty={penalty:.1f}%"

    detail = []
    xml_by_ym = {
        (str(r["year"]), str(r["month"]).zfill(2)): r
        for _, r in xml_monthly.iterrows()
    }
    for r in strat_df.to_dict("records"):
        ym = (str(r.get("year")), str(r.get("month")).zfill(2))
        xm = xml_by_ym.get(ym, {})
        detail.append({
            "strategy_id": strategy_id,
            "source_table": source_table,
            "year": r.get("year"),
            "month": r.get("month"),
            "xml_enrollment": xm.get("enrollment_count"),
            "azure_enrollment": r.get("enrollment_count"),
            "xml_enrollee": xm.get("enrollee_count"),
            "azure_enrollee": r.get("enrollee_count"),
            "status": r.get("status"),
        })

    return ComparisonResult(
        strategy_id=strategy_id,
        source_table=source_table,
        date_column=date_column,
        status_column=status_column,
        join_key=join_key,
        status_mapping=status_mapping,
        scores=scores,
        overall_accuracy=overall,
        detail_rows=detail,
        low_confidence_reason=reason,
        behavior_penalty=penalty,
        behavior_bonus=bonus,
        behavior_notes=behavior_notes,
    )


def run_fast_final_comparison(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    xml_raw: pd.DataFrame,
    xml_lifecycle: pd.DataFrame,
    schema: str = "dbo",
) -> dict[str, Any]:
    """FAST_MODE: fixed dbo.834_Inbound_test / Strategy C + record-level comparison."""
    from azure_reconciliation.azure_client import list_table_columns
    from azure_reconciliation.fixed_azure_candidate import (
        FIXED_DATE_COL,
        FIXED_STRATEGY,
        build_fixed_profile,
        fetch_fixed_azure_data,
        run_fixed_strategy_c,
        fixed_table_name,
    )
    from azure_reconciliation.record_comparison import (
        build_canonical_azure_records,
        build_canonical_xml_records,
        compare_records,
        export_record_debug_csvs,
    )

    hist = historical_partitions(partitions)
    issuer_parts = [p for p in hist if p.issuer == issuer]

    if xml_lifecycle.empty and not xml_raw.empty:
        xml_lifecycle = build_all_lifecycle_snapshots(xml_raw, issuer_parts)

    meta: dict[str, Any] = {
        "xml_raw_rows": len(xml_raw),
        "xml_lifecycle_rows": len(xml_lifecycle),
        "fast_mode": True,
        "historical_window": historical_window_label(issuer_parts),
        "rejected": [],
    }

    if xml_raw.empty:
        meta["cannot_calculate_reason"] = "missing XML data"
        return {"winner": None, "results": [], **meta}

    cols = list_table_columns(engine, schema, fixed_table_name())
    profile = build_fixed_profile(cols)
    table_df = fetch_fixed_azure_data(engine, issuer=issuer, partitions=issuer_parts, profile=profile)
    meta["azure_raw_rows"] = len(table_df)

    if table_df.empty:
        meta["cannot_calculate_reason"] = "missing Azure data — 834_Inbound_test returned zero rows"
        return {"winner": None, "results": [], **meta}

    strat_df = run_fixed_strategy_c(table_df, profile, issuer_parts)
    if strat_df.empty:
        meta["cannot_calculate_reason"] = "Strategy C produced zero rows on 834_Inbound_test"
        return {"winner": None, "results": [], **meta}

    try:
        result = compare_strategy(
            strategy_id=FIXED_STRATEGY,
            source_table=profile.full_name,
            strat_df=strat_df,
            profile=profile,
            xml_lifecycle=xml_lifecycle,
            xml_raw=xml_raw,
            partitions=issuer_parts,
            date_column=FIXED_DATE_COL,
            status_column=profile.status_col or "enrolleeStatus",
            table_df=table_df,
        )
    except Exception as exc:
        logger.exception("FAST compare_strategy failed: %s", exc)
        meta["cannot_calculate_reason"] = str(exc)
        return {"winner": None, "results": [], **meta}

    xml_records = build_canonical_xml_records(xml_raw)
    az_records = build_canonical_azure_records(table_df, profile, date_col=FIXED_DATE_COL)
    record_stats = compare_records(xml_records, az_records)
    record_paths = export_record_debug_csvs(record_stats)

    meta.update({
        "winner": result,
        "results": [result],
        "azure_rows_sampled": len(table_df),
        "azure_lifecycle_rows": len(az_records),
        "record_stats": record_stats,
        "record_paths": record_paths,
        "match_count": record_stats["match_count"],
        "xml_not_in_azure_count": record_stats["xml_not_in_azure_count"],
        "azure_not_in_xml_count": record_stats["azure_not_in_xml_count"],
        "status_diff_count": record_stats["status_diff_count"],
        "date_diff_count": record_stats["date_diff_count"],
    })
    return meta


def run_final_comparison(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    xml_raw: pd.DataFrame,
    xml_lifecycle: pd.DataFrame,
    schema: str = "dbo",
) -> dict[str, Any]:
    """
    Evaluate Azure vs XML. FAST_MODE uses fixed 834_Inbound_test / Strategy C only.
    Full discovery only when ENABLE_FULL_DISCOVERY=true.
    """
    if settings.use_fixed_azure_candidate and not settings.enable_full_discovery:
        logger.info("FAST_MODE final comparison — dbo.834_Inbound_test / Strategy C")
        return run_fast_final_comparison(
            engine,
            issuer=issuer,
            partitions=partitions,
            xml_raw=xml_raw,
            xml_lifecycle=xml_lifecycle,
            schema=schema,
        )

    hist = historical_partitions(partitions)
    issuer_parts = [p for p in hist if p.issuer == issuer]
    years = sorted({p.year for p in issuer_parts})

    if xml_lifecycle.empty and not xml_raw.empty:
        xml_lifecycle = build_all_lifecycle_snapshots(xml_raw, issuer_parts)

    table_names = discover_candidate_tables(engine, schema)
    results: list[ComparisonResult] = []
    rejected: list[dict[str, str]] = []

    if xml_raw.empty:
        return {
            "winner": None,
            "results": [],
            "rejected": [{"reason": "missing XML data"}],
            "historical_window": historical_window_label(issuer_parts),
            "xml_raw_rows": 0,
            "xml_lifecycle_rows": len(xml_lifecycle),
            "cannot_calculate_reason": "missing XML data",
        }

    for table in table_names:
        profile = inspect_table(engine, schema, table)
        if not profile.available or not profile.issuer_col:
            continue

        parts: list[pd.DataFrame] = []
        for year in years:
            df, _, _ = fetch_issuer_year_sample(engine, profile, issuer=issuer, year=year, limit=15000)
            if not df.empty:
                parts.append(df)
        if not parts:
            rejected.append({
                "source_table": profile.full_name,
                "strategy_id": "all",
                "reason": "Azure table returned zero rows for issuer/years",
            })
            continue
        table_df = pd.concat(parts, ignore_index=True)
        all_strategies = run_applicable_strategies(table_df, profile, issuer_parts)

        for sid in FINAL_STRATEGIES:
            strat_df = all_strategies.get(sid)
            if strat_df is None or strat_df.empty:
                rejected.append({
                    "source_table": profile.full_name,
                    "strategy_id": sid,
                    "reason": "strategy produced zero rows",
                })
                continue
            date_col = ""
            status_col = profile.status_col or profile.action_col or ""
            if "source_date_column" in strat_df.columns and not strat_df.empty:
                date_col = str(strat_df["source_date_column"].iloc[0])
            if "source_status_column" in strat_df.columns and not strat_df.empty:
                status_col = str(strat_df["source_status_column"].iloc[0])

            logger.info("Final comparison: %s / %s on %s", sid, profile.full_name, table)
            try:
                result = compare_strategy(
                    strategy_id=sid,
                    source_table=profile.full_name,
                    strat_df=strat_df,
                    profile=profile,
                    xml_lifecycle=xml_lifecycle,
                    xml_raw=xml_raw,
                    partitions=issuer_parts,
                    date_column=date_col,
                    status_column=status_col,
                    table_df=table_df,
                )
                results.append(result)
            except Exception as exc:
                logger.warning("Comparison skipped %s/%s: %s", sid, profile.full_name, exc)
                rejected.append({
                    "source_table": profile.full_name,
                    "strategy_id": sid,
                    "reason": str(exc),
                })

    meta = {
        "xml_raw_rows": len(xml_raw),
        "xml_lifecycle_rows": len(xml_lifecycle),
        "azure_rows_sampled": sum(len(r.detail_rows) for r in results),
        "rejected": rejected,
        "historical_window": historical_window_label(issuer_parts),
        "all_results_count": len(results),
    }

    if not results:
        reason = "missing Azure data"
        if not table_names:
            reason = "no Azure candidate tables discovered"
        elif rejected:
            reason = rejected[0].get("reason", reason)
        return {"winner": None, "results": [], "cannot_calculate_reason": reason, **meta}

    winner = max(results, key=lambda r: r.overall_accuracy)
    return {"winner": winner, "results": results, **meta}


def print_final_result(comparison: dict[str, Any]) -> None:
    """Print acceptance-criteria final result block."""
    winner: ComparisonResult | None = comparison.get("winner")
    lines = ["=" * 51, "FINAL RESULT"]

    if comparison.get("fast_mode"):
        lines.append("Mode: FAST (dbo.834_Inbound_test / Strategy C)")

    lines.append(f"XML rows: {comparison.get('xml_raw_rows', 'n/a')}")
    lines.append(f"XML lifecycle rows: {comparison.get('xml_lifecycle_rows', 'n/a')}")
    lines.append(f"Azure rows: {comparison.get('azure_raw_rows', comparison.get('azure_rows_sampled', 'n/a'))}")
    lines.append(f"Azure lifecycle rows: {comparison.get('azure_lifecycle_rows', 'n/a')}")

    if "match_count" in comparison:
        lines.extend([
            f"MATCH count: {comparison.get('match_count', 0)}",
            f"XML_NOT_IN_AZURE count: {comparison.get('xml_not_in_azure_count', 0)}",
            f"AZURE_NOT_IN_XML count: {comparison.get('azure_not_in_xml_count', 0)}",
            f"STATUS_DIFF count: {comparison.get('status_diff_count', 0)}",
            f"DATE_DIFF count: {comparison.get('date_diff_count', 0)}",
        ])

    if winner is None:
        reason = comparison.get("cannot_calculate_reason", "unknown")
        lines.append("Overall Accuracy: cannot calculate")
        lines.append(f"Reason: {reason}")
        rejected = comparison.get("rejected", [])
        if rejected:
            lines.append("Rejected strategies:")
            for r in rejected[:10]:
                lines.append(f"  {r.get('source_table', '?')} / {r.get('strategy_id', '?')}: {r.get('reason', '')}")
        _append_output_paths(lines, comparison)
        lines.append("=" * 51)
        for line in lines:
            print(line)
            logger.info(line)
        return

    s = winner.scores
    lines.extend([
        f"Selected Azure Table: {winner.source_table}",
        f"Selected Strategy: Strategy {winner.strategy_id} ({STRATEGY_META.get(winner.strategy_id, ('',))[0]})",
        f"Date Column: {winner.date_column or 'n/a'}",
        f"Join Key: {winner.join_key}",
        "Status Mapping",
    ])
    for xk, av in winner.status_mapping.items():
        lines.append(f"  {xk} → {av}")
    if not winner.status_mapping:
        lines.append("  (learned from matched keys — none found)")
    lines.extend([
        f"Historical Window: {comparison.get('historical_window', 'n/a')}",
        f"Enrollment Match: {s.get('enrollment_match', 0):.1f}%",
        f"Enrollee Match: {s.get('enrollee_match', 0):.1f}%",
        f"Subscriber Match: {s.get('subscriber_match', 0):.1f}%",
        f"Status Match: {s.get('status_match', 0):.1f}%",
        f"Lifecycle Match: {s.get('lifecycle_match', 0):.1f}%",
        f"Timeline Match: {s.get('timeline_match', 0):.1f}%",
        f"Overall Accuracy: {winner.overall_accuracy:.1f}%",
    ])
    if winner.behavior_penalty or winner.behavior_bonus:
        lines.append(
            f"Behavior adjustment: penalty={winner.behavior_penalty:.1f}% "
            f"bonus={winner.behavior_bonus:.1f}% ({winner.behavior_notes})"
        )
    if winner.overall_accuracy < 90.0:
        lines.append(f"WARNING (below 90%): {winner.low_confidence_reason}")
    _append_output_paths(lines, comparison)
    lines.append("=" * 51)
    for line in lines:
        print(line)
        logger.info(line)


def _append_output_paths(lines: list[str], comparison: dict[str, Any]) -> None:
    lines.append("Output paths:")
    for key in ("record_paths", "final_comparison_paths", "output_paths"):
        paths = comparison.get(key)
        if isinstance(paths, dict):
            for k, v in paths.items():
                lines.append(f"  {k}: {v}")
    lines.append("  outputs/comparison/final_result.html")
    lines.append("  outputs/comparison/final_result.xlsx")
    lines.append("  outputs/comparison/final_result.csv")
    lines.append("  outputs/debug/final_validation.txt")
    lines.append("  outputs/issuer_reports/index.html")
