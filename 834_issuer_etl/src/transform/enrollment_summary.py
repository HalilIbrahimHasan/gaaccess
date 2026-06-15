"""
Build Hari-format enrollment summary reports.

Output columns (fixed order):
    Coverage_Year, GAA_HIOS_ID, GAA_Load_Date, Insurance_Type,
    status_id, enrolleeStatus, Enrollment_Count, Enrollee_Count
"""

from __future__ import annotations

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

OUTPUT_COLUMNS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
]

STATUS_TO_ID = {
    "CONFIRM": 1,
    "CANCEL": 2,
    "TERM": 3,
}

INSURANCE_TYPE_DISPLAY = {
    "HLT": "Health",
    "HEALTH": "Health",
    "H": "Health",
    "DEN": "Dental",
    "DENTAL": "Dental",
    "VIS": "Vision",
    "VISION": "Vision",
}


def _gaa_load_date(year: str | int, month: str | int) -> str:
    """Format load date as M/D/YYYY (first day of partition month)."""
    return f"{int(month)}/1/{int(year)}"


def _insurance_type_display(code: str | None) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "Health"
    key = str(code).strip().upper()
    if not key:
        return "Health"
    return INSURANCE_TYPE_DISPLAY.get(key, "Health")


def _resolve_enrollee_status(row: pd.Series) -> str | None:
    """Map a row to CONFIRM, CANCEL, or TERM."""
    reason = str(row.get("additional_maint_reason_code") or "").strip().upper()
    if reason in STATUS_TO_ID:
        return reason

    txn = str(row.get("transaction_classification") or "").strip().upper()
    if txn == "CONFIRMATION":
        return "CONFIRM"
    if txn == "CANCELLATION":
        return "CANCEL"
    if txn == "TERMINATION":
        return "TERM"

    action_desc = str(row.get("action_code_description") or "").upper()
    action_code = str(row.get("event_type_code") or row.get("action_code") or "").upper()

    for text in (reason, action_desc, action_code):
        if "CONFIRM" in text:
            return "CONFIRM"
        if "CANCEL" in text:
            return "CANCEL"
        if "TERM" in text:
            return "TERM"

    return None


def _member_column(df: pd.DataFrame) -> str:
    if "exchg_indiv_identifier" in df.columns:
        return "exchg_indiv_identifier"
    if "member_id" in df.columns:
        return "member_id"
    raise ValueError("No member identifier column found for enrollee count")


def _issuer_column(df: pd.DataFrame) -> str:
    if "issuer_id" in df.columns:
        return "issuer_id"
    if "issuer" in df.columns:
        return "issuer"
    raise ValueError("No issuer column found")


def build_enrollment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate enrollee rows into Hari-format summary grouped by
    coverage year, issuer, load date, insurance type, and status.
    """
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work = df.copy()
    issuer_col = _issuer_column(work)
    member_col = _member_column(work)

    if "source_year" not in work.columns and "year" in work.columns:
        work["source_year"] = work["year"].astype(str)
    if "source_month" not in work.columns and "month" in work.columns:
        work["source_month"] = work["month"].astype(str)

    work["Coverage_Year"] = work["source_year"].astype(str)
    work["GAA_HIOS_ID"] = work[issuer_col].astype(str)
    work["GAA_Load_Date"] = work.apply(
        lambda r: _gaa_load_date(r["source_year"], r["source_month"]),
        axis=1,
    )
    if "insurance_type_code" in work.columns:
        work["Insurance_Type"] = work["insurance_type_code"].map(_insurance_type_display)
    else:
        work["Insurance_Type"] = "Health"

    work["enrolleeStatus"] = work.apply(_resolve_enrollee_status, axis=1)
    work = work[work["enrolleeStatus"].notna()].copy()
    if work.empty:
        logger.warning("No CONFIRM/CANCEL/TERM rows found for enrollment summary")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work["status_id"] = work["enrolleeStatus"].map(STATUS_TO_ID)

    grouped = (
        work.groupby(
            [
                "Coverage_Year",
                "GAA_HIOS_ID",
                "GAA_Load_Date",
                "Insurance_Type",
                "status_id",
                "enrolleeStatus",
            ],
            dropna=False,
        )
        .agg(
            Enrollment_Count=(member_col, "count"),
            Enrollee_Count=(member_col, "nunique"),
        )
        .reset_index()
    )

    grouped["status_id"] = grouped["status_id"].astype(int)
    grouped["Enrollment_Count"] = grouped["Enrollment_Count"].astype(int)
    grouped["Enrollee_Count"] = grouped["Enrollee_Count"].astype(int)

    result = grouped[OUTPUT_COLUMNS].sort_values(
        ["Coverage_Year", "GAA_Load_Date", "status_id"]
    ).reset_index(drop=True)

    logger.info(
        "Built enrollment summary: %d row(s) for issuer(s) %s",
        len(result),
        ", ".join(sorted(result["GAA_HIOS_ID"].unique())),
    )
    return result
