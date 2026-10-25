#!/usr/bin/env python3
"""
Export issuer × enrollee status reconciliation for Hari.

Reads dbo.inbound_automation and writes Excel with:
  - hari_template_distinct_enrollees  (issuer | Confirm | Cancel | Term)
  - hari_template_raw_rows
  - by_issuer_month_status
  - all_statuses_long
  - latest_status_per_member          (optional proxy, not business-ready collapse)

Usage:
    python run_issuer_enrollee_status_export.py --year 2026
    python run_issuer_enrollee_status_export.py --year 2025 --year 2026
    python run_issuer_enrollee_status_export.py --year 2026 --output-dir "last reports"
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from azure_reconciliation.safe_export import safe_write_excel
from inbound_automation.azure_common import connect_automation_engine
from utils.logger import get_logger

logger = get_logger(__name__)

HARI_TEMPLATE_COLS = [
    "issuer",
    "enrollee_count_confirm",
    "enrollee_count_cancel",
    "enrollee_count_term",
]

SQL_DISTINCT_TEMPLATE = """
SELECT
    issuer,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus = 'CONFIRM' AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_confirm,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus = 'CANCEL' AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_cancel,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus = 'TERM' AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_term,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus NOT IN ('CONFIRM', 'CANCEL', 'TERM')
             AND member_id IS NOT NULL THEN member_id
    END) AS enrollee_count_other_status,
    COUNT(DISTINCT CASE WHEN member_id IS NOT NULL THEN member_id END)
        AS distinct_member_id_any_status,
    COUNT(*) AS total_raw_rows
FROM dbo.inbound_automation
WHERE folder_year = :folder_year
GROUP BY issuer
ORDER BY issuer
"""

SQL_RAW_ROWS_TEMPLATE = """
SELECT
    issuer,
    SUM(CASE WHEN enrolleeStatus = 'CONFIRM' THEN 1 ELSE 0 END) AS raw_row_count_confirm,
    SUM(CASE WHEN enrolleeStatus = 'CANCEL' THEN 1 ELSE 0 END) AS raw_row_count_cancel,
    SUM(CASE WHEN enrolleeStatus = 'TERM' THEN 1 ELSE 0 END) AS raw_row_count_term,
    SUM(CASE WHEN enrolleeStatus NOT IN ('CONFIRM', 'CANCEL', 'TERM') THEN 1 ELSE 0 END)
        AS raw_row_count_other_status,
    COUNT(*) AS total_raw_rows
FROM dbo.inbound_automation
WHERE folder_year = :folder_year
GROUP BY issuer
ORDER BY issuer
"""

SQL_BY_ISSUER_MONTH = """
SELECT
    issuer,
    folder_year,
    folder_month,
    enrolleeStatus,
    COUNT(*) AS raw_row_count,
    COUNT(DISTINCT member_id) AS distinct_member_id,
    COUNT(DISTINCT policy_id) AS distinct_policy_id,
    COUNT(DISTINCT COALESCE(policy_id, health_coverage_policy_no)) AS distinct_policy_any
FROM dbo.inbound_automation
WHERE folder_year = :folder_year
GROUP BY issuer, folder_year, folder_month, enrolleeStatus
ORDER BY issuer, folder_month, enrolleeStatus
"""

SQL_ALL_STATUSES = """
SELECT
    issuer,
    enrolleeStatus,
    COUNT(*) AS raw_row_count,
    COUNT(DISTINCT member_id) AS distinct_member_id
FROM dbo.inbound_automation
WHERE folder_year = :folder_year
GROUP BY issuer, enrolleeStatus
ORDER BY issuer, enrolleeStatus
"""

SQL_LATEST_STATUS = """
WITH ranked AS (
    SELECT
        issuer,
        member_id,
        enrolleeStatus,
        benefit_effective_date,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY issuer, member_id
            ORDER BY
                benefit_effective_date DESC,
                loaded_at DESC,
                id DESC
        ) AS rn
    FROM dbo.inbound_automation
    WHERE folder_year = :folder_year
      AND member_id IS NOT NULL
)
SELECT
    issuer,
    COUNT(DISTINCT CASE WHEN enrolleeStatus = 'CONFIRM' THEN member_id END)
        AS latest_status_enrollee_count_confirm,
    COUNT(DISTINCT CASE WHEN enrolleeStatus = 'CANCEL' THEN member_id END)
        AS latest_status_enrollee_count_cancel,
    COUNT(DISTINCT CASE WHEN enrolleeStatus = 'TERM' THEN member_id END)
        AS latest_status_enrollee_count_term,
    COUNT(DISTINCT CASE
        WHEN enrolleeStatus NOT IN ('CONFIRM', 'CANCEL', 'TERM') THEN member_id
    END) AS latest_status_enrollee_count_other,
    COUNT(DISTINCT member_id) AS latest_status_distinct_members
FROM ranked
WHERE rn = 1
GROUP BY issuer
ORDER BY issuer
"""


def _read(engine, sql: str, folder_year: int) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"folder_year": folder_year})


def _totals_row(df: pd.DataFrame, label_col: str = "issuer") -> pd.Series:
    numeric = df.select_dtypes(include="number")
    totals = numeric.sum()
    out = pd.Series({label_col: "TOTAL"}, dtype=object)
    for col in numeric.columns:
        out[col] = totals[col]
    return out


def export_year(engine, folder_year: int, output_dir: Path) -> Path:
    distinct_df = _read(engine, SQL_DISTINCT_TEMPLATE, folder_year)
    raw_df = _read(engine, SQL_RAW_ROWS_TEMPLATE, folder_year)
    month_df = _read(engine, SQL_BY_ISSUER_MONTH, folder_year)
    long_df = _read(engine, SQL_ALL_STATUSES, folder_year)
    latest_df = _read(engine, SQL_LATEST_STATUS, folder_year)

    hari_df = distinct_df[HARI_TEMPLATE_COLS + ["enrollee_count_other_status", "total_raw_rows"]].copy()
    hari_with_totals = pd.concat(
        [hari_df, pd.DataFrame([_totals_row(hari_df)])],
        ignore_index=True,
    )

    out_path = output_dir / f"issuer_enrollee_status_reconciliation_{folder_year}.xlsx"
    safe_write_excel(
        out_path,
        {
            "hari_template_distinct_enrollees": hari_with_totals,
            "hari_template_raw_rows": pd.concat(
                [raw_df, pd.DataFrame([_totals_row(raw_df)])],
                ignore_index=True,
            ),
            "by_issuer_month_status": month_df,
            "all_statuses_long": long_df,
            "latest_status_per_member": latest_df,
            "notes": pd.DataFrame(
                [
                    {
                        "sheet": "hari_template_distinct_enrollees",
                        "description": (
                            "Hari template. enrollee_count_* = COUNT(DISTINCT member_id) "
                            "for rows with that enrolleeStatus. Same member may appear "
                            "in multiple status columns if they have multiple transaction types."
                        ),
                    },
                    {
                        "sheet": "hari_template_raw_rows",
                        "description": (
                            "Raw transaction row counts per status (COUNT(*)), not distinct enrollees."
                        ),
                    },
                    {
                        "sheet": "latest_status_per_member",
                        "description": (
                            "One status per member using latest benefit_effective_date. "
                            "Proxy only — business-ready uses lifecycle/collapse rules."
                        ),
                    },
                    {
                        "sheet": "all_statuses_long",
                        "description": "Includes UNMAPPED and any non-CONFIRM/CANCEL/TERM values.",
                    },
                    {
                        "field": "enrolleeStatus source",
                        "description": (
                            "Derived at raw ingest in enrich_parser_row() from "
                            "additional_maint_reason_code, action_code_description, action_code."
                        ),
                    },
                ]
            ),
        },
        drop_duplicate_value_columns=False,
    )
    logger.info("Wrote %s", out_path)
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export issuer enrollee status reconciliation Excel for Hari",
    )
    parser.add_argument(
        "--year",
        action="append",
        type=int,
        required=True,
        help="Folder year(s) to export (repeatable), e.g. --year 2025 --year 2026",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "last reports",
        help="Output directory (default: 834_issuer_etl/last reports)",
    )
    args = parser.parse_args(argv)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    engine = connect_automation_engine()
    paths: list[Path] = []
    for year in args.year:
        paths.append(export_year(engine, year, output_dir))

    print(f"Exported {len(paths)} file(s) to {output_dir}")
    for p in paths:
        print(f"  - {p.name}")
    print(f"Generated at {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
