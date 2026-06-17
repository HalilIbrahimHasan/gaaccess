import sqlite3
from pathlib import Path
from textwrap import dedent


DB_PATH = r"C:\Users\SelmaKazanci\Downloads\gaaccess-develop6\gaaccess-develop6\834_issuer_etl\data\issuer_834.db"

ISSUER = "15105"
YEAR = "2026"
MONTHS = ("01", "02", "03", "04", "05", "06")


def connect():
    db = Path(DB_PATH)
    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")
    return sqlite3.connect(db)


def print_title(title):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def run_query(conn, title, sql, params=None, limit=None):
    print_title(title)

    cur = conn.cursor()
    cur.execute(sql, params or {})
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    if limit:
        rows = rows[:limit]

    print(" | ".join(cols))
    print("-" * 100)

    for row in rows:
        print(" | ".join("" if v is None else str(v) for v in row))

    print(f"\nRows shown: {len(rows)}")


def database_overview(conn):
    run_query(
        conn,
        "1. Database tables",
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
        """
    )

    run_query(
        conn,
        "2. Row counts by table",
        """
        SELECT 'stg_834_records' AS table_name, COUNT(*) AS row_count FROM stg_834_records
        UNION ALL
        SELECT 'raw_file_inventory', COUNT(*) FROM raw_file_inventory
        UNION ALL
        SELECT 'parse_errors', COUNT(*) FROM parse_errors;
        """
    )


def file_inventory_summary(conn):
    run_query(
        conn,
        "3. Files loaded by issuer/year/month",
        """
        SELECT
            s.issuer,
            s.year,
            s.month,
            COUNT(DISTINCT s.file_id) AS file_count,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT s.policy_id) AS distinct_policies,
            COUNT(DISTINCT s.member_id) AS distinct_members
        FROM stg_834_records s
        WHERE s.issuer = :issuer
          AND s.year = :year
          AND s.month IN ('01','02','03','04','05','06')
        GROUP BY s.issuer, s.year, s.month
        ORDER BY s.year, s.month;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def status_summary_raw(conn):
    run_query(
        conn,
        "4. Raw status summary by month",
        """
        SELECT
            year,
            month,
            insurance_type_code,
            additional_maint_reason_code,
            maintenance_type_code,
            transaction_classification,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members,
            COUNT(DISTINCT exchg_assigned_enrollee_id) AS distinct_enrollee_ids
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05','06')
        GROUP BY
            year,
            month,
            insurance_type_code,
            additional_maint_reason_code,
            maintenance_type_code,
            transaction_classification
        ORDER BY
            year,
            month,
            additional_maint_reason_code,
            maintenance_type_code,
            transaction_classification;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def status_bucket_distribution(conn):
    run_query(
        conn,
        "5. Status bucket distribution only",
        """
        SELECT
            year,
            month,
            additional_maint_reason_code AS status,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05','06')
        GROUP BY year, month, additional_maint_reason_code
        ORDER BY year, month, status;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def compare_member_vs_enrollee_id(conn):
    run_query(
        conn,
        "6. Member ID vs REF*4A Enrollee ID comparison",
        """
        SELECT
            year,
            month,
            additional_maint_reason_code AS status,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT member_id) AS distinct_member_ids,
            COUNT(DISTINCT exchg_assigned_enrollee_id) AS distinct_enrollee_ids,
            COUNT(*) - COUNT(DISTINCT member_id) AS duplicate_member_rows
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05','06')
        GROUP BY year, month, additional_maint_reason_code
        ORDER BY year, month, status;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def duplicate_members_by_month_status(conn):
    run_query(
        conn,
        "7. Duplicate members by month/status",
        """
        WITH member_counts AS (
            SELECT
                issuer,
                year,
                month,
                insurance_type_code,
                additional_maint_reason_code AS status,
                member_id,
                COUNT(*) AS row_count,
                COUNT(DISTINCT policy_id) AS policy_count,
                COUNT(DISTINCT maintenance_type_code) AS maint_type_count,
                MIN(member_maint_effective_date) AS first_maint_date,
                MAX(member_maint_effective_date) AS last_maint_date
            FROM stg_834_records
            WHERE issuer = :issuer
              AND year = :year
              AND month IN ('01','02','03','04','05','06')
            GROUP BY issuer, year, month, insurance_type_code, status, member_id
            HAVING COUNT(*) > 1
        )
        SELECT
            year,
            month,
            insurance_type_code,
            status,
            COUNT(*) AS duplicated_members,
            SUM(row_count) AS duplicate_raw_rows,
            SUM(row_count - 1) AS extra_rows_removed_if_deduped
        FROM member_counts
        GROUP BY year, month, insurance_type_code, status
        ORDER BY year, month, status;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def duplicate_policy_by_month_status(conn):
    run_query(
        conn,
        "8. Duplicate policies by month/status",
        """
        WITH policy_counts AS (
            SELECT
                issuer,
                year,
                month,
                insurance_type_code,
                additional_maint_reason_code AS status,
                policy_id,
                COUNT(*) AS row_count,
                COUNT(DISTINCT member_id) AS member_count,
                COUNT(DISTINCT maintenance_type_code) AS maint_type_count
            FROM stg_834_records
            WHERE issuer = :issuer
              AND year = :year
              AND month IN ('01','02','03','04','05','06')
              AND policy_id IS NOT NULL
            GROUP BY issuer, year, month, insurance_type_code, status, policy_id
            HAVING COUNT(*) > 1
        )
        SELECT
            year,
            month,
            insurance_type_code,
            status,
            COUNT(*) AS duplicated_policies,
            SUM(row_count) AS raw_rows_under_duplicated_policies,
            SUM(member_count) AS distinct_members_under_policies
        FROM policy_counts
        GROUP BY year, month, insurance_type_code, status
        ORDER BY year, month, status;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def feb_confirm_detail_distribution(conn):
    run_query(
        conn,
        "9. February 2026 CONFIRM — maintenance/event distribution",
        """
        SELECT
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month = '02'
          AND additional_maint_reason_code = 'CONFIRM'
        GROUP BY
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification
        ORDER BY raw_rows DESC;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def feb_term_detail_distribution(conn):
    run_query(
        conn,
        "10. February 2026 TERM — maintenance/event distribution",
        """
        SELECT
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month = '02'
          AND additional_maint_reason_code = 'TERM'
        GROUP BY
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification
        ORDER BY raw_rows DESC;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def march_term_detail_distribution(conn):
    run_query(
        conn,
        "11. March 2026 TERM — maintenance/event distribution",
        """
        SELECT
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month = '03'
          AND additional_maint_reason_code = 'TERM'
        GROUP BY
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification
        ORDER BY raw_rows DESC;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def sample_duplicate_members_feb_confirm(conn):
    run_query(
        conn,
        "12. Sample duplicate members — February CONFIRM",
        """
        WITH dup_members AS (
            SELECT member_id
            FROM stg_834_records
            WHERE issuer = :issuer
              AND year = :year
              AND month = '02'
              AND additional_maint_reason_code = 'CONFIRM'
            GROUP BY member_id
            HAVING COUNT(*) > 1
            LIMIT 20
        )
        SELECT
            s.member_id,
            s.policy_id,
            s.subscriber_id,
            s.insurance_type_code,
            s.additional_maint_reason_code,
            s.maintenance_type_code,
            s.enrollee_event_type_code,
            s.enrollee_event_reason_code,
            s.member_maint_effective_date,
            s.request_submit_timestamp,
            f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f ON s.file_id = f.file_id
        WHERE s.member_id IN (SELECT member_id FROM dup_members)
          AND s.issuer = :issuer
          AND s.year = :year
          AND s.month = '02'
          AND s.additional_maint_reason_code = 'CONFIRM'
        ORDER BY s.member_id, s.member_maint_effective_date, f.file_name;
        """,
        {"issuer": ISSUER, "year": YEAR},
        limit=200
    )


def sample_policy_family_feb_confirm(conn):
    run_query(
        conn,
        "13. Sample policy/member family view — February CONFIRM",
        """
        WITH sample_policies AS (
            SELECT policy_id
            FROM stg_834_records
            WHERE issuer = :issuer
              AND year = :year
              AND month = '02'
              AND additional_maint_reason_code = 'CONFIRM'
              AND policy_id IS NOT NULL
            GROUP BY policy_id
            HAVING COUNT(DISTINCT member_id) > 1
            LIMIT 10
        )
        SELECT
            s.policy_id,
            s.member_id,
            s.subscriber_id,
            s.relationship,
            s.subscriber_flag,
            s.additional_maint_reason_code,
            s.maintenance_type_code,
            s.member_maint_effective_date,
            f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f ON s.file_id = f.file_id
        WHERE s.policy_id IN (SELECT policy_id FROM sample_policies)
          AND s.issuer = :issuer
          AND s.year = :year
          AND s.month = '02'
          AND s.additional_maint_reason_code = 'CONFIRM'
        ORDER BY s.policy_id, s.member_id, s.member_maint_effective_date;
        """,
        {"issuer": ISSUER, "year": YEAR},
        limit=200
    )


def reproduce_current_summary_approx(conn):
    run_query(
        conn,
        "14. Approx current Enrollment Summary from DB",
        """
        WITH base AS (
            SELECT
                issuer,
                year,
                month,
                CASE
                    WHEN insurance_type_code = 'DEN' THEN 'Dental'
                    ELSE 'Health'
                END AS Insurance_Type,
                additional_maint_reason_code AS status,
                CASE
                    WHEN policy_id IS NOT NULL AND policy_id <> '' THEN 'policy:' || policy_id
                    WHEN subscriber_id IS NOT NULL AND subscriber_id <> '' THEN 'subscriber:' || subscriber_id
                    WHEN household_or_employee_case_id IS NOT NULL AND household_or_employee_case_id <> '' THEN 'household:' || household_or_employee_case_id
                    ELSE NULL
                END AS enrollment_key,
                member_id,
                member_maint_effective_date,
                file_id,
                ROW_NUMBER() OVER (
                    PARTITION BY issuer, year, month, insurance_type_code, additional_maint_reason_code, member_id
                    ORDER BY member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM stg_834_records
            WHERE issuer = :issuer
              AND year = :year
              AND month IN ('01','02','03','04','05','06')
              AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
        ),
        dedup AS (
            SELECT *
            FROM base
            WHERE rn = 1
        )
        SELECT
            year,
            month,
            Insurance_Type,
            status,
            CASE status
                WHEN 'CONFIRM' THEN 1
                WHEN 'CANCEL' THEN 2
                WHEN 'TERM' THEN 3
            END AS status_id,
            COUNT(DISTINCT enrollment_key) AS Enrollment_Count,
            COUNT(DISTINCT member_id) AS Enrollee_Count
        FROM dedup
        GROUP BY year, month, Insurance_Type, status
        ORDER BY year, month, status_id;
        """,
        {"issuer": ISSUER, "year": YEAR}
    )


def compare_chandra_known_values(conn):
    print_title("15. Manual Chandra comparison helper")
    print(dedent("""
    Chandra known values:
    2026-01 CONFIRM: Enrollment 1240 / Enrollee 1690
    2026-01 CANCEL : Enrollment 47   / Enrollee 47
    2026-02 CONFIRM: Enrollment 490  / Enrollee 744
    2026-02 CANCEL : Enrollment 734  / Enrollee 709
    2026-02 TERM   : Enrollment 1435 / Enrollee 1435
    2026-03 CONFIRM: Enrollment 453  / Enrollee 584
    2026-03 CANCEL : Enrollment 278  / Enrollee 271
    2026-03 TERM   : Enrollment 20   / Enrollee 20
    2026-04 CONFIRM: Enrollment 463  / Enrollee 559
    2026-04 CANCEL : Enrollment 236  / Enrollee 233
    2026-04 TERM   : Enrollment 4255 / Enrollee 4255
    2026-05 CONFIRM: Enrollment 340  / Enrollee 418
    2026-05 CANCEL : Enrollment 64   / Enrollee 63
    2026-05 TERM   : Enrollment 179  / Enrollee 179
    """))
def test_chandra_matching_rules(conn):
    print_title("16. Test possible Chandra matching rules")

    sql = """
    WITH base AS (
        SELECT
            issuer,
            year,
            month,
            insurance_type_code,
            additional_maint_reason_code AS status,
            policy_id,
            member_id,
            subscriber_id,
            member_maint_effective_date,
            request_submit_timestamp,
            file_id,
            CASE
                WHEN policy_id IS NOT NULL AND policy_id <> '' THEN policy_id
                ELSE subscriber_id
            END AS enrollment_key
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05')
          AND insurance_type_code = 'HLT'
          AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    raw_counts AS (
        SELECT
            year, month, status,
            'A_raw_no_dedup' AS rule,
            COUNT(DISTINCT enrollment_key) AS enrollment_count,
            COUNT(DISTINCT member_id) AS enrollee_count
        FROM base
        GROUP BY year, month, status
    ),

    dedup_member_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, member_id
                    ORDER BY member_maint_effective_date DESC, request_submit_timestamp DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    rule_member_status AS (
        SELECT
            year, month, status,
            'B_latest_per_member_status' AS rule,
            COUNT(DISTINCT enrollment_key) AS enrollment_count,
            COUNT(DISTINCT member_id) AS enrollee_count
        FROM dedup_member_status
        GROUP BY year, month, status
    ),

    dedup_member_policy_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, member_id, policy_id
                    ORDER BY member_maint_effective_date DESC, request_submit_timestamp DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    rule_member_policy_status AS (
        SELECT
            year, month, status,
            'C_latest_per_member_policy_status' AS rule,
            COUNT(DISTINCT enrollment_key) AS enrollment_count,
            COUNT(DISTINCT member_id) AS enrollee_count
        FROM dedup_member_policy_status
        GROUP BY year, month, status
    ),

    dedup_policy_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, policy_id
                    ORDER BY member_maint_effective_date DESC, request_submit_timestamp DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    rule_policy_status AS (
        SELECT
            year, month, status,
            'D_latest_per_policy_status' AS rule,
            COUNT(DISTINCT enrollment_key) AS enrollment_count,
            COUNT(DISTINCT member_id) AS enrollee_count
        FROM dedup_policy_status
        GROUP BY year, month, status
    ),

    chandra AS (
        SELECT '2026' AS year, '01' AS month, 'CONFIRM' AS status, 1240 AS ch_enrollment, 1690 AS ch_enrollee UNION ALL
        SELECT '2026','01','CANCEL',47,47 UNION ALL
        SELECT '2026','02','CONFIRM',490,744 UNION ALL
        SELECT '2026','02','CANCEL',734,709 UNION ALL
        SELECT '2026','02','TERM',1435,1435 UNION ALL
        SELECT '2026','03','CONFIRM',453,584 UNION ALL
        SELECT '2026','03','CANCEL',278,271 UNION ALL
        SELECT '2026','03','TERM',20,20 UNION ALL
        SELECT '2026','04','CONFIRM',463,559 UNION ALL
        SELECT '2026','04','CANCEL',236,233 UNION ALL
        SELECT '2026','04','TERM',4255,4255 UNION ALL
        SELECT '2026','05','CONFIRM',340,418 UNION ALL
        SELECT '2026','05','CANCEL',64,63 UNION ALL
        SELECT '2026','05','TERM',179,179
    ),

    all_rules AS (
        SELECT * FROM raw_counts
        UNION ALL SELECT * FROM rule_member_status
        UNION ALL SELECT * FROM rule_member_policy_status
        UNION ALL SELECT * FROM rule_policy_status
    )

    SELECT
        r.year,
        r.month,
        r.status,
        r.rule,
        r.enrollment_count,
        c.ch_enrollment,
        r.enrollment_count - c.ch_enrollment AS enrollment_delta,
        r.enrollee_count,
        c.ch_enrollee,
        r.enrollee_count - c.ch_enrollee AS enrollee_delta,
        ABS(r.enrollment_count - c.ch_enrollment) + ABS(r.enrollee_count - c.ch_enrollee) AS total_delta
    FROM all_rules r
    JOIN chandra c
      ON r.year = c.year
     AND r.month = c.month
     AND r.status = c.status
    ORDER BY r.year, r.month, r.status, total_delta;
    """

    run_query(conn, "16. Test possible Chandra matching rules", sql, {"issuer": ISSUER, "year": YEAR})

def main():
    conn = connect()

    test_chandra_matching_rules(conn)

    conn.close()

if __name__ == "__main__":
    main()
