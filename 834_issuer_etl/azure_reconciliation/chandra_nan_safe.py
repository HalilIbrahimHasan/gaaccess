"""
NaN-safe cleanup for Chandra report-only exports.

Used by chandra_report_only runner — does not change parser or business pipeline logic.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from azure_reconciliation.chandra_business_format import (
    CHANDRA_BUSINESS_COLUMNS_CORE,
    safe_count_int,
    safe_month_str,
    safe_status_id,
    safe_year_str,
)

NAN_STRINGS = frozenset({"", "nan", "none", "<na>", "nat"})


def _is_na(val: object) -> bool:
    if val is None:
        return True
    try:
        return bool(pd.isna(val))
    except (TypeError, ValueError):
        return False


def _record_nan_audit(
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str,
    column_name: str,
    nan_count: int,
    cleanup_action: str,
) -> None:
    if nan_count <= 0:
        return
    audit.append({
        "issuer": issuer,
        "year": year,
        "month": month,
        "column_name": column_name,
        "nan_count": nan_count,
        "cleanup_action": cleanup_action,
    })


def _series_nan_count(series: pd.Series) -> int:
    if series.empty:
        return 0
    mask = series.isna() | series.astype(str).str.strip().str.lower().isin(NAN_STRINGS)
    return int(mask.sum())


def sanitize_business_ready_df(
    df: pd.DataFrame,
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str = "",
) -> pd.DataFrame:
    """NaN-safe cleanup for business-ready record exports."""
    if df.empty:
        return df

    out = df.copy()

    if "year" in out.columns:
        n = _series_nan_count(out["year"])
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="year", nan_count=n, cleanup_action="coerce_to_string_year",
            )
        out["year"] = out["year"].apply(safe_year_str)

    if "month" in out.columns:
        n = _series_nan_count(out["month"])
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="month", nan_count=n, cleanup_action="coerce_to_zfill_month_string",
            )
        out["month"] = out["month"].apply(safe_month_str)

    if "business_month" in out.columns and "year" in out.columns and "month" in out.columns:
        out["business_month"] = out["year"].astype(str) + "-" + out["month"].astype(str)

    if "status_Id" in out.columns:
        n = int(out["status_Id"].isna().sum()) if out["status_Id"].dtype != object else _series_nan_count(out["status_Id"])
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="status_Id", nan_count=n, cleanup_action="derive_from_enrolleeStatus_or_zero",
            )
        enrollee = out["enrolleeStatus"] if "enrolleeStatus" in out.columns else pd.Series([None] * len(out))
        out["status_Id"] = [
            safe_status_id(sid, enrollee_status=est)
            for sid, est in zip(out["status_Id"], enrollee, strict=False)
        ]

    if "benefit_effective_year" in out.columns:
        n = int(out["benefit_effective_year"].isna().sum())
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="benefit_effective_year", nan_count=n, cleanup_action="fill_blank_string",
            )
        out["benefit_effective_year"] = out["benefit_effective_year"].apply(
            lambda v: "" if _is_na(v) or str(v).strip().lower() in NAN_STRINGS else safe_year_str(v),
        )

    for col in ("raw_transaction_count",):
        if col in out.columns:
            n = int(out[col].isna().sum())
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month=month,
                    column_name=col, nan_count=n, cleanup_action="fill_zero_int",
                )
            out[col] = out[col].apply(lambda v: safe_count_int(v, 0))

    return out


def sanitize_chandra_summary_df(
    df: pd.DataFrame,
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str = "",
) -> pd.DataFrame:
    """NaN-safe cleanup for Chandra 8-column summary exports."""
    if df.empty:
        return pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)

    out = df.copy()

    if "Coverage_Year" in out.columns:
        n = _series_nan_count(out["Coverage_Year"])
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="Coverage_Year", nan_count=n, cleanup_action="coerce_to_string_year",
            )
        out["Coverage_Year"] = out["Coverage_Year"].apply(safe_year_str)

    if "GAA_Load_Date" in out.columns:
        n = _series_nan_count(out["GAA_Load_Date"])
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="GAA_Load_Date", nan_count=n, cleanup_action="fill_blank_string",
            )
        out["GAA_Load_Date"] = out["GAA_Load_Date"].fillna("").astype(str).replace("nan", "")

    if "status_Id" in out.columns:
        n = int(out["status_Id"].isna().sum())
        if n:
            _record_nan_audit(
                audit, issuer=issuer, year=year, month=month,
                column_name="status_Id", nan_count=n, cleanup_action="derive_from_enrolleeStatus_or_zero",
            )
        enrollee = out["enrolleeStatus"] if "enrolleeStatus" in out.columns else pd.Series([None] * len(out))
        out["status_Id"] = [
            safe_status_id(sid, enrollee_status=est)
            for sid, est in zip(out["status_Id"], enrollee, strict=False)
        ]

    for col in ("Enrollment_Count", "Enrollee_Count"):
        if col in out.columns:
            n = int(pd.to_numeric(out[col], errors="coerce").isna().sum())
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month=month,
                    column_name=col, nan_count=n, cleanup_action="fill_zero_int",
                )
            out[col] = out[col].apply(lambda v: safe_count_int(v, 0))

    cols = [c for c in CHANDRA_BUSINESS_COLUMNS_CORE if c in out.columns]
    return out[cols]


def sanitize_dashboard_summary_df(
    df: pd.DataFrame,
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
) -> pd.DataFrame:
    """NaN-safe cleanup for business_ready_summary dashboard frame."""
    if df.empty:
        return df
    out = df.copy()
    for col in ("year",):
        if col in out.columns:
            out[col] = out[col].apply(safe_year_str)
    for col in ("month",):
        if col in out.columns:
            out[col] = out[col].apply(safe_month_str)
    for col in ("business_ready_records", "distinct_enrollment_ids", "distinct_enrollee_ids"):
        if col in out.columns:
            n = int(pd.to_numeric(out[col], errors="coerce").isna().sum())
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month="",
                    column_name=col, nan_count=n, cleanup_action="fill_zero_int",
                )
            out[col] = out[col].apply(lambda v: safe_count_int(v, 0))
    return out
