"""
90-day cancellation / termination business rules layer.

Rules:
- Cancel within 90 days of coverage effective date → cancellation, refund may apply.
- Cancel after 90 days → reclassified as termination, no refund.
- Explicit termination → no refund for valid covered months.
- Computes timing vs effective date, transaction date, benefit end, reporting month.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from config.config import settings
from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)

WINDOW_DAYS = 90  # overridden by settings at runtime

# Action classification
CLASS_CONFIRMATION = "CONFIRMATION"
CLASS_CANCELLATION = "CANCELLATION"
CLASS_TERMINATION = "TERMINATION"
CLASS_OTHER = "OTHER"

STATUS_WITHIN = "WITHIN_90_DAYS"
STATUS_OUTSIDE = "OUTSIDE_90_DAYS"
STATUS_UNKNOWN = "UNKNOWN_DATE"
STATUS_NA = "N/A"

REFUND_REQUIRED = "REFUND_REQUIRED"
NO_REFUND = "NO_REFUND_TERMINATION"
REVIEW = "REVIEW_REQUIRED"
REFUND_NA = "N/A"


def parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    cleaned = s[:10].replace("-", "")
    if len(cleaned) >= 8 and cleaned[:8].isdigit():
        try:
            return datetime.strptime(cleaned[:8], "%Y%m%d")
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s[:10])
    except ValueError:
        return None


def _is_cancel(action: str, reason: str) -> bool:
    return reason == "CANCEL" or "CANCEL" in action


def _is_term(action: str, reason: str) -> bool:
    return reason == "TERM" or "TERM" in action


def _is_confirm(action: str, reason: str) -> bool:
    return reason == "CONFIRM" or "CONFIRM" in action


def classify_record(row: dict[str, Any], window_days: int) -> dict[str, Any]:
    """
    Apply business rules to a single staging record.

    Returns dict of fields to UPDATE on stg_834_records.
    """
    action = (row.get("action_code_description") or "").upper()
    reason = (row.get("additional_maint_reason_code") or "").upper()

    eff = parse_date(row.get("benefit_effective_date"))
    txn = parse_date(row.get("member_maint_effective_date"))
    end = parse_date(row.get("benefit_end_date"))

    year = str(row.get("year", ""))
    month = str(row.get("month", "")).zfill(2)
    reporting_month = f"{year}-{month}" if year and month else None

    days_eff_to_txn = None
    months_eff_to_txn = None
    days_eff_to_end = None
    days_txn_to_end = None

    if eff and txn:
        days_eff_to_txn = (txn - eff).days
        months_eff_to_txn = round(days_eff_to_txn / 30.44, 1)
    if eff and end:
        days_eff_to_end = (end - eff).days
    if txn and end:
        days_txn_to_end = (end - txn).days

    window_status = STATUS_UNKNOWN
    refund = REVIEW
    txn_class = CLASS_OTHER
    revenue_at_risk = 0.0
    withheld_fee = 0.0
    expected_fee = float(row.get("expected_user_fee") or row.get("user_fee_amount") or 0)

    if _is_confirm(action, reason):
        txn_class = CLASS_CONFIRMATION
        window_status = STATUS_NA
        refund = REFUND_NA

    elif _is_cancel(action, reason) or _is_term(action, reason):
        if not eff or not txn:
            window_status = STATUS_UNKNOWN
            refund = REVIEW
            txn_class = CLASS_CANCELLATION if _is_cancel(action, reason) else CLASS_TERMINATION
        elif _is_term(action, reason):
            # Explicit termination — always outside window, no refund
            txn_class = CLASS_TERMINATION
            window_status = STATUS_OUTSIDE
            refund = NO_REFUND
        elif days_eff_to_txn <= window_days:
            # Cancel within 90 days → true cancellation, refund eligible
            txn_class = CLASS_CANCELLATION
            window_status = STATUS_WITHIN
            refund = REFUND_REQUIRED
            revenue_at_risk = expected_fee
        else:
            # Cancel after 90 days → reclassify as termination, no refund
            txn_class = CLASS_TERMINATION
            window_status = STATUS_OUTSIDE
            refund = NO_REFUND
            withheld_fee = expected_fee  # fee collected, no refund owed

    return {
        "days_between_effective_and_cancel": days_eff_to_txn,
        "months_between_effective_and_cancel": months_eff_to_txn,
        "days_between_effective_and_end": days_eff_to_end,
        "days_between_transaction_and_end": days_txn_to_end,
        "reporting_month": reporting_month,
        "cancellation_window_status": window_status,
        "refund_eligibility": refund,
        "transaction_classification": txn_class,
        "revenue_at_risk": revenue_at_risk,
        "withheld_user_fee": withheld_fee,
        "non_refundable_user_fee": expected_fee if refund == NO_REFUND else 0.0,
        "refund_eligible_user_fee": expected_fee if refund == REFUND_REQUIRED else 0.0,
    }


def apply_business_rules(db: Database) -> int:
    """Apply 90-day cancellation/termination rules to all staging records."""
    window_days = settings.cancellation_window_days
    df = pd.read_sql_query(
        """SELECT record_id, issuer, year, month,
                  benefit_effective_date, benefit_end_date, member_maint_effective_date,
                  action_code_description, additional_maint_reason_code,
                  expected_user_fee, user_fee_amount
           FROM stg_834_records""",
        db.conn,
    )
    updated = 0
    for _, row in df.iterrows():
        result = classify_record(row.to_dict(), window_days)
        db.execute(
            """UPDATE stg_834_records SET
               days_between_effective_and_cancel = ?,
               months_between_effective_and_cancel = ?,
               days_between_effective_and_end = ?,
               days_between_transaction_and_end = ?,
               reporting_month = ?,
               cancellation_window_status = ?,
               refund_eligibility = ?,
               transaction_classification = ?,
               revenue_at_risk = ?,
               withheld_user_fee = ?,
               non_refundable_user_fee = ?,
               refund_eligible_user_fee = ?
               WHERE record_id = ?""",
            (
                result["days_between_effective_and_cancel"],
                result["months_between_effective_and_cancel"],
                result["days_between_effective_and_end"],
                result["days_between_transaction_and_end"],
                result["reporting_month"],
                result["cancellation_window_status"],
                result["refund_eligibility"],
                result["transaction_classification"],
                result["revenue_at_risk"],
                result["withheld_user_fee"],
                result["non_refundable_user_fee"],
                result["refund_eligible_user_fee"],
                row["record_id"],
            ),
        )
        updated += 1
    db.commit()
    logger.info("Business rules applied to %d records (window=%d days)", updated, window_days)
    return updated
