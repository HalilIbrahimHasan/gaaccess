import sqlite3
import csv
from pathlib import Path


DB_PATH = r"C:\Users\SelmaKazanci\Downloads\gaaccess-develop6\gaaccess-develop6\834_issuer_etl\data\issuer_834.db"

ISSUER = "15105"
YEAR = "2026"

OUT_DIR = Path("investigation_outputs")
OUT_DIR.mkdir(exist_ok=True)


CHANDRA = [
    ("01", "CONFIRM", 1240, 1690),
    ("01", "CANCEL", 47, 47),
    ("02", "CONFIRM", 490, 744),
    ("02", "CANCEL", 734, 709),
    ("02", "TERM", 1435, 1435),
    ("03", "CONFIRM", 453, 584),
    ("03", "CANCEL", 278, 271),
    ("03", "TERM", 20, 20),
    ("04", "CONFIRM", 463, 559),
    ("04", "CANCEL", 236, 233),
    ("04", "TERM", 4255, 4255),
    ("05", "CONFIRM", 340, 418),
    ("05", "CANCEL", 64, 63),
    ("05", "TERM", 179, 179),
]


def connect():
    db = Path(DB_PATH)
    if not db.exists():
        raise FileNotFoundError(f"Database not found: {db}")
    return sqlite3.connect(db)


def print_title(title):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)


def fetch_rows(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or {})
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows


def print_rows(title, cols, rows, limit=100):
    print_title(title)
    print(" | ".join(cols))
    print("-" * 120)

    for row in rows[:limit]:
        print(" | ".join("" if v is None else str(v) for v in row))

    print(f"\nRows shown: {min(len(rows), limit)} / {len(rows)}")


def export_csv(filename, cols, rows):
    path = OUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"CSV exported: {path}")


def chandra_cte():
    parts = []
    for month, status, enrollment, enrollee in CHANDRA:
        parts.append(
            f"SELECT '{YEAR}' AS year, '{month}' AS month, '{status}' AS status, "
            f"{enrollment} AS ch_enrollment, {enrollee} AS ch_enrollee"
        )
    return "\nUNION ALL\n".join(parts)


def show_columns(conn):
    cols, rows = fetch_rows(conn, "PRAGMA table_info(stg_834_records);")
    print_rows("STAGING COLUMNS", cols, rows, limit=100)


def test_all_rules(conn):
    sql = f"""
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
            relationship,
            subscriber_flag,
            maintenance_type_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            coverage_status,
            benefit_effective_date,
            benefit_end_date,
            member_maint_effective_date,
            request_submit_timestamp,
            file_id,
            household_or_employee_case_id,
            raw_xml_path,
            CASE
                WHEN policy_id IS NOT NULL AND policy_id <> '' THEN policy_id
                WHEN subscriber_id IS NOT NULL AND subscriber_id <> '' THEN subscriber_id
                WHEN household_or_employee_case_id IS NOT NULL AND household_or_employee_case_id <> '' THEN household_or_employee_case_id
                ELSE NULL
            END AS enrollment_key
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05')
          AND insurance_type_code = 'HLT'
          AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    exact_distinct AS (
        SELECT DISTINCT
            year,
            month,
            status,
            policy_id,
            member_id,
            subscriber_id,
            relationship,
            subscriber_flag,
            maintenance_type_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            coverage_status,
            benefit_effective_date,
            benefit_end_date,
            member_maint_effective_date,
            request_submit_timestamp,
            file_id,
            household_or_employee_case_id,
            enrollment_key
        FROM base
    ),

    latest_member_status AS (
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

    latest_member_policy_status AS (
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

    latest_member_policy_effective AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, member_id, policy_id, member_maint_effective_date
                    ORDER BY request_submit_timestamp DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    latest_policy_status AS (
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

    latest_policy_subscriber_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, policy_id, subscriber_id
                    ORDER BY member_maint_effective_date DESC, request_submit_timestamp DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    latest_request_member_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, member_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    latest_request_policy_member_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, policy_id, member_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    latest_request_policy_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, policy_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    subscriber_only AS (
        SELECT *
        FROM base
        WHERE subscriber_flag = 'Y'
    ),

    relationship_18_only AS (
        SELECT *
        FROM base
        WHERE relationship = '18'
    ),

    transaction_classification_based AS (
        SELECT
            *,
            CASE
                WHEN transaction_classification = 'CONFIRMATION' THEN 'CONFIRM'
                WHEN transaction_classification = 'CANCELLATION' THEN 'CANCEL'
                WHEN transaction_classification = 'TERMINATION' THEN 'TERM'
                ELSE status
            END AS normalized_status
        FROM base
        WHERE transaction_classification IN ('CONFIRMATION','CANCELLATION','TERMINATION')
    ),

    latest_tx_member AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, normalized_status, member_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM transaction_classification_based
        )
        WHERE rn = 1
    ),

    all_rules AS (
        SELECT
            year,
            month,
            status,
            'A_raw_distinct_policy_member' AS rule,
            COUNT(DISTINCT enrollment_key) AS enrollment_count,
            COUNT(DISTINCT member_id) AS enrollee_count
        FROM base
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'B_exact_distinct_rows' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM exact_distinct
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'C_latest_member_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_member_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'D_latest_member_policy_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_member_policy_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'E_latest_member_policy_effective' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_member_policy_effective
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'F_latest_policy_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_policy_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'G_latest_policy_subscriber_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_policy_subscriber_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'H_latest_request_member_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_request_member_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'I_latest_request_policy_member_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_request_policy_member_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'J_latest_request_policy_status' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_request_policy_status
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'K_subscriber_flag_Y_only' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM subscriber_only
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'L_relationship_18_only' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM relationship_18_only
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'M_policy_id_as_enrollment_raw' AS rule,
            COUNT(DISTINCT policy_id),
            COUNT(DISTINCT member_id)
        FROM base
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            status,
            'N_subscriber_id_as_enrollment_raw' AS rule,
            COUNT(DISTINCT subscriber_id),
            COUNT(DISTINCT member_id)
        FROM base
        GROUP BY year, month, status

        UNION ALL
        SELECT
            year,
            month,
            normalized_status AS status,
            'O_transaction_classification_raw' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM transaction_classification_based
        GROUP BY year, month, normalized_status

        UNION ALL
        SELECT
            year,
            month,
            normalized_status AS status,
            'P_latest_tx_class_member' AS rule,
            COUNT(DISTINCT enrollment_key),
            COUNT(DISTINCT member_id)
        FROM latest_tx_member
        GROUP BY year, month, normalized_status
    ),

    chandra AS (
        {chandra_cte()}
    ),

    scored AS (
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
            ABS(r.enrollment_count - c.ch_enrollment)
              + ABS(r.enrollee_count - c.ch_enrollee) AS total_delta
        FROM all_rules r
        JOIN chandra c
          ON r.year = c.year
         AND r.month = c.month
         AND r.status = c.status
    )

    SELECT *
    FROM scored
    ORDER BY year, month, status, total_delta, rule;
    """

    cols, rows = fetch_rows(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_rows("ALL CHANDRA MATCHING RULE TESTS", cols, rows, limit=500)
    export_csv("all_chandra_rule_tests.csv", cols, rows)


def best_rule_per_month_status(conn):
    sql = f"""
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
            relationship,
            subscriber_flag,
            maintenance_type_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            coverage_status,
            benefit_effective_date,
            benefit_end_date,
            member_maint_effective_date,
            request_submit_timestamp,
            file_id,
            household_or_employee_case_id,
            CASE
                WHEN policy_id IS NOT NULL AND policy_id <> '' THEN policy_id
                WHEN subscriber_id IS NOT NULL AND subscriber_id <> '' THEN subscriber_id
                WHEN household_or_employee_case_id IS NOT NULL AND household_or_employee_case_id <> '' THEN household_or_employee_case_id
                ELSE NULL
            END AS enrollment_key
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05')
          AND insurance_type_code = 'HLT'
          AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    latest_member_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, member_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    latest_policy_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, policy_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    latest_policy_member_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, policy_id, member_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM base
        )
        WHERE rn = 1
    ),

    all_rules AS (
        SELECT year, month, status, 'RAW_POLICY_MEMBER' AS rule,
               COUNT(DISTINCT enrollment_key) AS enrollment_count,
               COUNT(DISTINCT member_id) AS enrollee_count
        FROM base GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'LATEST_MEMBER_STATUS' AS rule,
               COUNT(DISTINCT enrollment_key), COUNT(DISTINCT member_id)
        FROM latest_member_status GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'LATEST_POLICY_STATUS' AS rule,
               COUNT(DISTINCT enrollment_key), COUNT(DISTINCT member_id)
        FROM latest_policy_status GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'LATEST_POLICY_MEMBER_STATUS' AS rule,
               COUNT(DISTINCT enrollment_key), COUNT(DISTINCT member_id)
        FROM latest_policy_member_status GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'SUBSCRIBER_ONLY' AS rule,
               COUNT(DISTINCT enrollment_key), COUNT(DISTINCT member_id)
        FROM base WHERE subscriber_flag='Y' GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'RELATIONSHIP_18_ONLY' AS rule,
               COUNT(DISTINCT enrollment_key), COUNT(DISTINCT member_id)
        FROM base WHERE relationship='18' GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'SUBSCRIBER_ID_AS_ENROLLMENT' AS rule,
               COUNT(DISTINCT subscriber_id), COUNT(DISTINCT member_id)
        FROM base GROUP BY year, month, status
    ),

    chandra AS (
        {chandra_cte()}
    ),

    scored AS (
        SELECT
            r.*,
            c.ch_enrollment,
            c.ch_enrollee,
            ABS(r.enrollment_count - c.ch_enrollment)
              + ABS(r.enrollee_count - c.ch_enrollee) AS total_delta
        FROM all_rules r
        JOIN chandra c
          ON r.year = c.year
         AND r.month = c.month
         AND r.status = c.status
    ),

    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY year, month, status
                ORDER BY total_delta ASC, rule
            ) AS best_rank
        FROM scored
    )

    SELECT *
    FROM ranked
    WHERE best_rank = 1
    ORDER BY year, month, status;
    """

    cols, rows = fetch_rows(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_rows("BEST MATCHING RULE PER MONTH/STATUS", cols, rows, limit=100)
    export_csv("best_rule_per_month_status.csv", cols, rows)


def count_grain_comparison(conn):
    sql = """
    SELECT
        year,
        month,
        additional_maint_reason_code AS status,
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT policy_id) AS distinct_policy_id,
        COUNT(DISTINCT subscriber_id) AS distinct_subscriber_id,
        COUNT(DISTINCT member_id) AS distinct_member_id,
        COUNT(DISTINCT policy_id || '|' || member_id) AS distinct_policy_member,
        COUNT(DISTINCT policy_id || '|' || member_id || '|' || member_maint_effective_date) AS distinct_policy_member_effective,
        COUNT(DISTINCT policy_id || '|' || member_id || '|' || file_id) AS distinct_policy_member_file,
        COUNT(DISTINCT policy_id || '|' || member_id || '|' || member_maint_effective_date || '|' || file_id) AS distinct_policy_member_effective_file
    FROM stg_834_records
    WHERE issuer = :issuer
      AND year = :year
      AND month IN ('01','02','03','04','05')
      AND insurance_type_code = 'HLT'
      AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    GROUP BY year, month, additional_maint_reason_code
    ORDER BY year, month, status;
    """

    cols, rows = fetch_rows(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_rows("COUNT GRAIN COMPARISON", cols, rows, limit=100)
    export_csv("count_grain_comparison.csv", cols, rows)


def main():
    conn = connect()

    # Uncomment if you need schema again:
    # show_columns(conn)

    test_all_rules(conn)
    best_rule_per_month_status(conn)
    count_grain_comparison(conn)

    conn.close()


if __name__ == "__main__":
    main()
