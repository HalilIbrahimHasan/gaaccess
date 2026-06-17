import sqlite3
import csv
from pathlib import Path

DB_PATH = Path("data/issuer_834.db")
OUTPUT_DIR = Path("query_outputs")
OUTPUT_DIR.mkdir(exist_ok=True)


def connect():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH.resolve()}")
    return sqlite3.connect(DB_PATH)


def get_columns(conn, table_name="stg_834_records"):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def show_tables(conn):
    print("\n=== TABLES ===")
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for row in cur.fetchall():
        print(row[0])


def show_columns(conn, table_name="stg_834_records"):
    print(f"\n=== COLUMNS OF {table_name} ===")
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    for cid, name, col_type, *_ in cur.fetchall():
        print(f"{cid:02d} | {name} | {col_type}")


def run_query(conn, sql, output_name="custom_query_output.csv", limit_print=50):
    print("\n=== RUNNING QUERY ===")
    print(sql)

    cur = conn.cursor()
    cur.execute(sql)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    print(f"\nRows returned: {len(rows)}")
    print(" | ".join(columns))
    print("-" * 120)

    for row in rows[:limit_print]:
        print(" | ".join("" if v is None else str(v) for v in row))

    out_path = OUTPUT_DIR / output_name
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"\nCSV exported: {out_path}")


def show_sample_staging_rows(conn):
    columns = get_columns(conn)

    wanted = [
        "record_id",
        "issuer",
        "year",
        "month",
        "policy_id",
        "member_id",
        "subscriber_id",
        "relationship",
        "subscriber_flag",
        "insurance_type_code",
        "additional_maint_reason_code",
        "maintenance_type_code",
        "enrollee_event_type_code",
        "enrollee_event_reason_code",
        "transaction_classification",
        "coverage_status",
        "benefit_effective_date",
        "benefit_end_date",
        "member_maint_effective_date",
        "request_submit_timestamp",
        "raw_xml_path",
    ]

    selected = [c for c in wanted if c in columns]

    sql = f"""
    SELECT {", ".join(selected)}
    FROM stg_834_records
    WHERE issuer = '15105'
      AND year = '2026'
      AND month = '02'
    LIMIT 100
    """

    run_query(conn, sql, "sample_staging_rows.csv")


def show_our_dashboard_logic(conn):
    columns = get_columns(conn)

    if "raw_xml_path" in columns:
        file_col = "raw_xml_path"
    else:
        file_col = "record_id"

    sql = f"""
    WITH base AS (
        SELECT
            issuer,
            year,
            month,
            CASE
                WHEN insurance_type_code = 'DEN' THEN 'Dental'
                ELSE 'Health'
            END AS Insurance_Type,
            CASE
                WHEN additional_maint_reason_code = 'REINSTATE' THEN 'CONFIRM'
                ELSE additional_maint_reason_code
            END AS enrolleeStatus,
            CASE
                WHEN additional_maint_reason_code IN ('CONFIRM', 'REINSTATE') THEN 1
                WHEN additional_maint_reason_code = 'CANCEL' THEN 2
                WHEN additional_maint_reason_code = 'TERM' THEN 3
            END AS status_id,
            policy_id,
            member_id,
            subscriber_id,
            household_or_employee_case_id,
            member_maint_effective_date,
            benefit_effective_date,
            {file_col} AS file_sort
        FROM stg_834_records
        WHERE additional_maint_reason_code IN ('CONFIRM', 'CANCEL', 'TERM', 'REINSTATE')
    ),
    keyed AS (
        SELECT
            *,
            CASE
                WHEN policy_id IS NOT NULL AND policy_id != '' THEN 'policy:' || policy_id
                WHEN subscriber_id IS NOT NULL AND subscriber_id != '' THEN 'subscriber:' || subscriber_id
                WHEN household_or_employee_case_id IS NOT NULL AND household_or_employee_case_id != '' THEN 'household:' || household_or_employee_case_id
                ELSE NULL
            END AS enrollment_key
        FROM base
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY issuer, year, month, Insurance_Type, enrolleeStatus, member_id
                ORDER BY
                    COALESCE(member_maint_effective_date, benefit_effective_date, '') DESC,
                    file_sort DESC
            ) AS rn
        FROM keyed
    )
    SELECT
        year,
        month,
        Insurance_Type,
        enrolleeStatus,
        status_id,
        COUNT(DISTINCT enrollment_key) AS Enrollment_Count,
        COUNT(DISTINCT member_id) AS Enrollee_Count
    FROM ranked
    WHERE rn = 1
      AND issuer = '15105'
    GROUP BY year, month, Insurance_Type, enrolleeStatus, status_id
    ORDER BY year, month, status_id
    """

    run_query(conn, sql, "our_dashboard_monthly_logic.csv")


def show_simple_status_counts(conn):
    sql = """
    SELECT
        year,
        month,
        additional_maint_reason_code AS status,
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT policy_id) AS distinct_policies,
        COUNT(DISTINCT member_id) AS distinct_members,
        COUNT(DISTINCT subscriber_id) AS distinct_subscribers
    FROM stg_834_records
    WHERE issuer = '15105'
      AND year = '2026'
      AND additional_maint_reason_code IN ('CONFIRM', 'CANCEL', 'TERM')
    GROUP BY year, month, additional_maint_reason_code
    ORDER BY year, month, status
    """

    run_query(conn, sql, "simple_status_counts.csv")


def show_duplicate_members(conn):
    sql = """
    SELECT
        year,
        month,
        additional_maint_reason_code AS status,
        member_id,
        COUNT(*) AS row_count,
        COUNT(DISTINCT policy_id) AS policy_count,
        MIN(member_maint_effective_date) AS first_maint_date,
        MAX(member_maint_effective_date) AS latest_maint_date
    FROM stg_834_records
    WHERE issuer = '15105'
      AND year = '2026'
      AND additional_maint_reason_code IN ('CONFIRM', 'CANCEL', 'TERM')
    GROUP BY year, month, additional_maint_reason_code, member_id
    HAVING COUNT(*) > 1
    ORDER BY year, month, status, row_count DESC
    LIMIT 300
    """

    run_query(conn, sql, "duplicate_members.csv")


def chandra_query_workspace(conn):
    """
    Buraya Chandra kendi query'sini yazabilir.
    Sadece SQL'i değiştirip çalıştırmak yeterli.
    """

    sql = """
    SELECT
        year,
        month,
        additional_maint_reason_code AS status,
        COUNT(DISTINCT policy_id) AS Enrollment_Count,
        COUNT(DISTINCT member_id) AS Enrollee_Count
    FROM stg_834_records
    WHERE issuer = '15105'
      AND year = '2026'
      AND additional_maint_reason_code IN ('CONFIRM', 'CANCEL', 'TERM')
    GROUP BY year, month, additional_maint_reason_code
    ORDER BY year, month, status
    """

    run_query(conn, sql, "chandra_custom_query_result.csv")


def main():
    conn = connect()

    show_tables(conn)
    show_columns(conn)

    show_sample_staging_rows(conn)
    show_our_dashboard_logic(conn)
    show_simple_status_counts(conn)
    show_duplicate_members(conn)

    # Chandra kendi query'sini burada değiştirebilir:
    chandra_query_workspace(conn)

    conn.close()


if __name__ == "__main__":
    main()
