import sqlite3
import csv
from pathlib import Path

DB_PATH = Path("data/issuer_834.db")
OUT_DIR = Path("query_outputs")
OUT_DIR.mkdir(exist_ok=True)

ISSUER = "15105"
YEAR = "2026"
MONTHS = ("01", "02", "03", "04", "05")

CUSTOM_QUERY = """
SELECT
    year,
    month,
    additional_maint_reason_code AS status,
    COUNT(DISTINCT policy_id) AS enrollment_count,
    COUNT(DISTINCT member_id) AS enrollee_count
FROM stg_834_records
WHERE issuer = '15105'
  AND year = '2026'
  AND month IN ('01','02','03','04','05')
  AND insurance_type_code = 'HLT'
GROUP BY year, month, additional_maint_reason_code
ORDER BY year, month, status;
"""


def connect():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH.resolve()}")
    return sqlite3.connect(f"file:{DB_PATH.resolve()}?mode=ro", uri=True)


def run_sql(conn, sql, title, export_name=None, limit=200):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []

    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)

    if not cols:
        print("No result set.")
        return rows, cols

    print(" | ".join(cols))
    print("-" * 120)

    for row in rows[:limit]:
        print(" | ".join("" if v is None else str(v) for v in row))

    print(f"\nRows shown: {min(len(rows), limit)} / {len(rows)}")

    if export_name:
        path = OUT_DIR / export_name
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            writer.writerows(rows)
        print(f"CSV exported: {path}")

    return rows, cols


def get_columns(conn, table="stg_834_records"):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]


def existing(cols, candidates):
    return [c for c in candidates if c in cols]


def show_tables(conn):
    run_sql(
        conn,
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
        """,
        "TABLES",
        "00_tables.csv"
    )


def show_columns(conn):
    run_sql(
        conn,
        "PRAGMA table_info(stg_834_records);",
        "STG_834_RECORDS COLUMNS",
        "01_stg_columns.csv",
        limit=300
    )


def show_sample_rows(conn):
    cols = get_columns(conn)

    wanted = [
        "record_id", "id",
        "issuer", "year", "month",
        "policy_id", "member_id", "subscriber_id",
        "insurance_type_code",
        "additional_maint_reason_code",
        "maintenance_type_code",
        "enrollee_event_type_code",
        "enrollee_event_reason_code",
        "transaction_classification",
        "coverage_status",
        "relationship",
        "subscriber_flag",
        "benefit_effective_date",
        "benefit_end_date",
        "member_maint_effective_date",
        "request_submit_timestamp",
        "file_name",
        "raw_xml_path",
    ]

    selected = existing(cols, wanted)

    sql = f"""
    SELECT {", ".join(selected)}
    FROM stg_834_records
    WHERE issuer = '{ISSUER}'
      AND year = '{YEAR}'
    LIMIT 50;
    """

    run_sql(conn, sql, "SAMPLE STAGING ROWS", "02_sample_staging_rows.csv", limit=50)


def show_field_distributions(conn):
    cols = get_columns(conn)

    optional_group_cols = existing(cols, [
        "insurance_type_code",
        "additional_maint_reason_code",
        "maintenance_type_code",
        "enrollee_event_type_code",
        "enrollee_event_reason_code",
        "transaction_classification",
        "coverage_status",
        "subscriber_flag",
        "relationship",
    ])

    select_cols = ["year", "month"] + optional_group_cols
    group_cols = ", ".join(select_cols)

    sql = f"""
    SELECT
        {group_cols},
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT policy_id) AS policies,
        COUNT(DISTINCT member_id) AS members,
        COUNT(DISTINCT subscriber_id) AS subscribers
    FROM stg_834_records
    WHERE issuer = '{ISSUER}'
      AND year = '{YEAR}'
      AND month IN ('01','02','03','04','05')
    GROUP BY {group_cols}
    ORDER BY year, month, raw_rows DESC;
    """

    run_sql(conn, sql, "FIELD DISTRIBUTIONS", "03_field_distributions.csv", limit=300)


def our_dashboard_monthly_logic(conn):
    """
    This reproduces the current dashboard/enrollment-summary style logic:
    - use staging rows
    - normalize status from additional_maint_reason_code
    - HLT => Health, DEN => Dental
    - reporting-level dedup by issuer/year/month/insurance/status/member_id
    - keep latest by member_maint_effective_date/request_submit_timestamp/file name
    - Enrollment_Count = distinct policy_id
    - Enrollee_Count = distinct member_id
    """
    cols = get_columns(conn)

    order_parts = []
    for c in ["member_maint_effective_date", "request_submit_timestamp", "file_name"]:
        if c in cols:
            order_parts.append(f"COALESCE({c}, '') DESC")
    order_parts.append("rowid DESC")
    order_sql = ", ".join(order_parts)

    sql = f"""
    WITH base AS (
        SELECT
            *,
            CASE
                WHEN insurance_type_code = 'DEN' THEN 'Dental'
                ELSE 'Health'
            END AS Insurance_Type,
            CASE
                WHEN UPPER(COALESCE(additional_maint_reason_code,'')) LIKE '%CONFIRM%' THEN 'CONFIRM'
                WHEN UPPER(COALESCE(additional_maint_reason_code,'')) LIKE '%REINSTATE%' THEN 'CONFIRM'
                WHEN UPPER(COALESCE(additional_maint_reason_code,'')) LIKE '%CANCEL%' THEN 'CANCEL'
                WHEN UPPER(COALESCE(additional_maint_reason_code,'')) LIKE '%TERM%' THEN 'TERM'
                ELSE NULL
            END AS enrolleeStatus
        FROM stg_834_records
        WHERE issuer = '{ISSUER}'
          AND year = '{YEAR}'
          AND month IN ('01','02','03','04','05')
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY issuer, year, month, Insurance_Type, enrolleeStatus, member_id
                ORDER BY {order_sql}
            ) AS rn
        FROM base
        WHERE enrolleeStatus IS NOT NULL
    ),
    dedup AS (
        SELECT *
        FROM ranked
        WHERE rn = 1
    )
    SELECT
        year AS Coverage_Year,
        issuer AS GAA_HIOS_ID,
        month,
        Insurance_Type,
        CASE enrolleeStatus
            WHEN 'CONFIRM' THEN 1
            WHEN 'CANCEL' THEN 2
            WHEN 'TERM' THEN 3
        END AS status_id,
        enrolleeStatus,
        COUNT(DISTINCT policy_id) AS Enrollment_Count,
        COUNT(DISTINCT member_id) AS Enrollee_Count,
        COUNT(*) AS dedup_rows_used
    FROM dedup
    GROUP BY year, issuer, month, Insurance_Type, status_id, enrolleeStatus
    ORDER BY year, month, Insurance_Type, status_id;
    """

    run_sql(
        conn,
        sql,
        "OUR CURRENT DASHBOARD / ENROLLMENT SUMMARY LOGIC - MONTHLY",
        "04_our_dashboard_monthly_logic.csv",
        limit=300
    )


def our_dashboard_rollup_logic(conn):
    """
    Same logic as monthly, but shows all months together for one issuer.
    Useful to show Chandra/Hari the all-month rollup view.
    """
    our_dashboard_monthly_logic(conn)


def run_chandra_custom_query(conn):
    run_sql(
        conn,
        CUSTOM_QUERY,
        "CUSTOM QUERY RESULT - CHANDRA CAN EDIT CUSTOM_QUERY",
        "05_custom_query_result.csv",
        limit=500
    )


def main():
    conn = connect()

    show_tables(conn)
    show_columns(conn)
    show_sample_rows(conn)

    # This is the most important explanation piece:
    # It shows the exact logic we currently reflect in dashboard/monthly output.
    our_dashboard_monthly_logic(conn)

    # Optional supporting outputs
    show_field_distributions(conn)

    # Chandra can replace CUSTOM_QUERY at the top and run this.
    run_chandra_custom_query(conn)

    conn.close()


if __name__ == "__main__":
    main()
