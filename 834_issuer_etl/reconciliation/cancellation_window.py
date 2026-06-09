"""90-day cancellation window and refund eligibility rules."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from config.config import settings
from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)


def _parse_date(s: str | None) -> datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10].replace("-", "")[:8], "%Y%m%d" if fmt == "%Y%m%d" else "%Y-%m-%d")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s[:10])
    except ValueError:
        return None


def apply_cancellation_window(db: Database) -> int:
    window = settings.cancellation_window_days
    df = pd.read_sql_query(
        """SELECT record_id, benefit_effective_date, member_maint_effective_date,
                  action_code_description, additional_maint_reason_code
           FROM stg_834_records""",
        db.conn,
    )
    updated = 0
    for _, row in df.iterrows():
        eff = _parse_date(row["benefit_effective_date"])
        txn = _parse_date(row["member_maint_effective_date"])
        action = (row["action_code_description"] or "").upper()
        reason = (row["additional_maint_reason_code"] or "").upper()

        days = months = None
        window_status = "UNKNOWN_DATE"
        refund = "REVIEW_REQUIRED"

        if eff and txn:
            days = (txn - eff).days
            months = round(days / 30.44, 1)
            if reason in ("CANCEL",) or "CANCEL" in action:
                if days <= window:
                    window_status = "WITHIN_90_DAYS"
                    refund = "REFUND_REQUIRED"
                else:
                    window_status = "OUTSIDE_90_DAYS"
                    refund = "NO_REFUND_TERMINATION"
            elif reason in ("TERM",) or "TERM" in action:
                window_status = "OUTSIDE_90_DAYS"
                refund = "NO_REFUND_TERMINATION"
            else:
                window_status = "N/A"
                refund = "N/A"

        db.execute(
            """UPDATE stg_834_records SET
               days_between_effective_and_cancel=?,
               months_between_effective_and_cancel=?,
               cancellation_window_status=?,
               refund_eligibility=?
               WHERE record_id=?""",
            (days, months, window_status, refund, row["record_id"]),
        )
        updated += 1
    db.commit()
    logger.info("Cancellation window applied to %d records", updated)
    return updated


def cancellation_window_summary(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, cancellation_window_status,
                   refund_eligibility, COUNT(*) AS record_count,
                   SUM(expected_user_fee) AS user_fee_total
            FROM stg_834_records {where}
            GROUP BY issuer, year, month, cancellation_window_status, refund_eligibility
            ORDER BY issuer, year, month""",
        db.conn, params=params,
    )
