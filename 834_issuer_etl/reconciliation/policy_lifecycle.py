"""Policy lifecycle KPIs — confirmed, cancelled, terminated, household counts."""

from __future__ import annotations

import pandas as pd

from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)


def issuer_kpi_summary(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""SELECT issuer, year, month,
            SUM(CASE WHEN action_code_description LIKE '%Confirm%'
                      OR additional_maint_reason_code = 'CONFIRM' THEN 1 ELSE 0 END)
                AS confirmed_policies,
            SUM(CASE WHEN action_code_description LIKE '%Cancel%'
                      OR additional_maint_reason_code = 'CANCEL' THEN 1 ELSE 0 END)
                AS cancelled_policies,
            SUM(CASE WHEN action_code_description LIKE '%Term%'
                      OR additional_maint_reason_code = 'TERM' THEN 1 ELSE 0 END)
                AS terminated_policies,
            SUM(CASE WHEN cancellation_window_status = 'WITHIN_90_DAYS' THEN 1 ELSE 0 END)
                AS cancellations_within_90_days,
            SUM(CASE WHEN cancellation_window_status = 'OUTSIDE_90_DAYS'
                      AND (action_code_description LIKE '%Cancel%'
                           OR additional_maint_reason_code = 'CANCEL') THEN 1 ELSE 0 END)
                AS cancellations_outside_90_days,
            SUM(CASE WHEN refund_eligibility = 'REFUND_REQUIRED' THEN 1 ELSE 0 END)
                AS refund_eligible_count,
            SUM(CASE WHEN refund_eligibility = 'REFUND_REQUIRED'
                     THEN COALESCE(expected_user_fee, 0) ELSE 0 END)
                AS refund_eligible_user_fee,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members,
            COUNT(*) AS total_records
        FROM stg_834_records {where}
        GROUP BY issuer, year, month
        ORDER BY issuer, year, month""",
        db.conn, params=params,
    )


def household_member_counts(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, policy_id,
                   COUNT(DISTINCT member_id) AS member_count,
                   SUM(CASE WHEN subscriber_flag = 'Y' THEN 1 ELSE 0 END) AS subscriber_count
            FROM stg_834_records {where}
            GROUP BY issuer, year, month, policy_id
            HAVING subscriber_count > 1 OR member_count > 1
            ORDER BY member_count DESC""",
        db.conn, params=params,
    )


def rolling_3_month_kpi(db: Database, issuer: str | None = None) -> pd.DataFrame:
    """Rolling 3-month period summaries (Jan-Mar, Feb-Apr, ...)."""
    monthly = issuer_kpi_summary(db, issuer)
    if monthly.empty:
        return monthly

    monthly["period_key"] = monthly["year"] + "-" + monthly["month"]
    monthly = monthly.sort_values(["issuer", "year", "month"])
    rows = []
    kpi_cols = [
        c for c in monthly.columns
        if c not in ("issuer", "year", "month", "period_key")
    ]

    for issuer_name, grp in monthly.groupby("issuer"):
        grp = grp.reset_index(drop=True)
        for i in range(len(grp)):
            window = grp.iloc[max(0, i - 2): i + 1]
            if len(window) < 1:
                continue
            row = {"issuer": issuer_name, "end_year": grp.iloc[i]["year"],
                   "end_month": grp.iloc[i]["month"],
                   "rolling_period": "-".join(window["period_key"].tolist())}
            for col in kpi_cols:
                row[col] = window[col].sum()
            rows.append(row)

    result = pd.DataFrame(rows)
    logger.info("Rolling 3-month KPI: %d period(s)", len(result))
    return result
