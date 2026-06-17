import sqlite3
import csv
from pathlib import Path


DB_PATH = r"C:\Users\SelmaKazanci\Downloads\gaaccess-develop6\gaaccess-develop6\834_issuer_etl\data\issuer_834.db"

ISSUER = "15105"
YEAR = "2026"

OUT_DIR = Path("investigation_outputs")
OUT_DIR.mkdir(exist_ok=True)


CHANDRA = {
    ("01", "CONFIRM"): (1240, 1690),
    ("01", "CANCEL"): (47, 47),
    ("02", "CONFIRM"): (490, 744),
    ("02", "CANCEL"): (734, 709),
    ("02", "TERM"): (1435, 1435),
    ("03", "CONFIRM"): (453, 584),
    ("03", "CANCEL"): (278, 271),
    ("03", "TERM"): (20, 20),
    ("04", "CONFIRM"): (463, 559),
    ("04", "CANCEL"): (236, 233),
    ("04", "TERM"): (4255, 4255),
    ("05", "CONFIRM"): (340, 418),
    ("05", "CANCEL"): (64, 63),
    ("05", "TERM"): (179, 179),
}


def connect():
    db = Path(DB_PATH)
    if not db.exists():
        raise FileNotFoundError(f"DB not found: {db}")
    return sqlite3.connect(db)


def run(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or {})
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows


def export_csv(name, cols, rows):
    path = OUT_DIR / name
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"CSV exported: {path}")


def print_table(title, cols, rows, limit=80):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)
    print(" | ".join(cols))
    print("-" * 120)
    for r in rows[:limit]:
        print(" | ".join("" if v is None else str(v) for v in r))
    print(f"Rows shown: {min(len(rows), limit)} / {len(rows)}")


def chandra_cte():
    parts = []
    for (month, status), (enroll, enrollee) in CHANDRA.items():
        parts.append(
            f"SELECT '{YEAR}' year, '{month}' month, '{status}' status, "
            f"{enroll} ch_enrollment, {enrollee} ch_enrollee"
        )
    return "\nUNION ALL\n".join(parts)


def monthly_baseline(conn):
    sql = f"""
    WITH base AS (
        SELECT
            year,
            month,
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
            raw_xml_path
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05')
          AND insurance_type_code = 'HLT'
          AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    latest_member AS (
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

    summary AS (
        SELECT
            year,
            month,
            status,
            COUNT(DISTINCT policy_id) AS our_enrollment,
            COUNT(DISTINCT member_id) AS our_enrollee
        FROM latest_member
        GROUP BY year, month, status
    ),

    chandra AS (
        {chandra_cte()}
    )

    SELECT
        s.year,
        s.month,
        s.status,
        s.our_enrollment,
        c.ch_enrollment,
        s.our_enrollment - c.ch_enrollment AS enrollment_gap,
        s.our_enrollee,
        c.ch_enrollee,
        s.our_enrollee - c.ch_enrollee AS enrollee_gap
    FROM summary s
    JOIN chandra c
      ON s.year = c.year
     AND s.month = c.month
     AND s.status = c.status
    ORDER BY s.year, s.month, s.status;
    """
    cols, rows = run(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_table("1. BASELINE VS CHANDRA USING LATEST MEMBER STATUS", cols, rows)
    export_csv("01_baseline_vs_chandra.csv", cols, rows)


def gap_feature_profile(conn):
    sql = f"""
    WITH base AS (
        SELECT
            *,
            additional_maint_reason_code AS status
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05')
          AND insurance_type_code = 'HLT'
          AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    latest_member AS (
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
    )

    SELECT
        year,
        month,
        status,
        maintenance_type_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        subscriber_flag,
        relationship,
        COUNT(*) AS rows,
        COUNT(DISTINCT policy_id) AS distinct_policies,
        COUNT(DISTINCT member_id) AS distinct_members,
        COUNT(DISTINCT subscriber_id) AS distinct_subscribers
    FROM latest_member
    GROUP BY
        year,
        month,
        status,
        maintenance_type_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        subscriber_flag,
        relationship
    ORDER BY year, month, status, rows DESC;
    """
    cols, rows = run(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_table("2. FEATURE PROFILE AFTER LATEST MEMBER DEDUP", cols, rows, limit=150)
    export_csv("02_feature_profile_after_latest_member.csv", cols, rows)


def test_business_filters(conn):
    sql = f"""
    WITH base AS (
        SELECT
            *,
            additional_maint_reason_code AS status
        FROM stg_834_records
        WHERE issuer = :issuer
          AND year = :year
          AND month IN ('01','02','03','04','05')
          AND insurance_type_code = 'HLT'
          AND additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    latest_member AS (
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

    rules AS (
        SELECT year, month, status, 'A_latest_member_all' rule,
               COUNT(DISTINCT policy_id) enrollment_count,
               COUNT(DISTINCT member_id) enrollee_count
        FROM latest_member
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'B_exclude_reason_28',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE IFNULL(enrollee_event_reason_code,'') <> '28'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'C_only_reason_28',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE enrollee_event_reason_code = '28'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'D_exclude_reason_59',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE IFNULL(enrollee_event_reason_code,'') <> '59'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'E_only_reason_59',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE enrollee_event_reason_code = '59'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'F_only_subscriber_Y',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE subscriber_flag = 'Y'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'G_exclude_subscriber_Y',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE IFNULL(subscriber_flag,'') <> 'Y'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'H_only_relationship_18',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE relationship = '18'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'I_exclude_relationship_18',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE IFNULL(relationship,'') <> '18'
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'J_only_same_begin_end',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE benefit_effective_date = benefit_end_date
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'K_exclude_same_begin_end',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE benefit_end_date IS NULL
           OR benefit_end_date = ''
           OR benefit_effective_date <> benefit_end_date
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'L_only_null_end_date',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE benefit_end_date IS NULL OR benefit_end_date = ''
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'M_exclude_null_end_date',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE benefit_end_date IS NOT NULL AND benefit_end_date <> ''
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'N_only_policy_equals_subscriber',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE policy_id = subscriber_id
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'O_exclude_policy_equals_subscriber',
               COUNT(DISTINCT policy_id), COUNT(DISTINCT member_id)
        FROM latest_member
        WHERE policy_id <> subscriber_id
        GROUP BY year, month, status
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
        FROM rules r
        JOIN chandra c
          ON r.year = c.year
         AND r.month = c.month
         AND r.status = c.status
    )

    SELECT *
    FROM scored
    ORDER BY year, month, status, total_delta, rule;
    """
    cols, rows = run(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_table("3. BUSINESS FILTER TESTS VS CHANDRA", cols, rows, limit=250)
    export_csv("03_business_filter_tests.csv", cols, rows)


def identify_month_file_patterns(conn):
    sql = """
    SELECT
        s.year,
        s.month,
        s.additional_maint_reason_code AS status,
        f.file_name,
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT s.policy_id) AS policies,
        COUNT(DISTINCT s.member_id) AS members,
        MIN(s.member_maint_effective_date) AS min_maint_date,
        MAX(s.member_maint_effective_date) AS max_maint_date,
        MIN(s.request_submit_timestamp) AS min_request_ts,
        MAX(s.request_submit_timestamp) AS max_request_ts
    FROM stg_834_records s
    LEFT JOIN raw_file_inventory f
      ON s.file_id = f.file_id
    WHERE s.issuer = :issuer
      AND s.year = :year
      AND s.month IN ('01','02','03','04','05')
      AND s.insurance_type_code = 'HLT'
      AND s.additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    GROUP BY s.year, s.month, s.additional_maint_reason_code, f.file_name
    ORDER BY s.year, s.month, status, f.file_name;
    """
    cols, rows = run(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_table("4. FILE LEVEL PATTERNS", cols, rows, limit=150)
    export_csv("04_file_level_patterns.csv", cols, rows)


def latest_file_only_test(conn):
    sql = f"""
    WITH base AS (
        SELECT
            s.*,
            s.additional_maint_reason_code AS status,
            f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f
          ON s.file_id = f.file_id
        WHERE s.issuer = :issuer
          AND s.year = :year
          AND s.month IN ('01','02','03','04','05')
          AND s.insurance_type_code = 'HLT'
          AND s.additional_maint_reason_code IN ('CONFIRM','CANCEL','TERM')
    ),

    latest_file_by_status AS (
        SELECT *
        FROM (
            SELECT
                *,
                DENSE_RANK() OVER (
                    PARTITION BY year, month, status
                    ORDER BY file_name DESC
                ) AS file_rank
            FROM base
        )
        WHERE file_rank = 1
    ),

    latest_file_then_member AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, member_id
                    ORDER BY request_submit_timestamp DESC, member_maint_effective_date DESC, file_id DESC
                ) AS rn
            FROM latest_file_by_status
        )
        WHERE rn = 1
    ),

    rules AS (
        SELECT year, month, status, 'LATEST_FILE_ONLY' AS rule,
               COUNT(DISTINCT policy_id) AS enrollment_count,
               COUNT(DISTINCT member_id) AS enrollee_count
        FROM latest_file_by_status
        GROUP BY year, month, status

        UNION ALL
        SELECT year, month, status, 'LATEST_FILE_THEN_MEMBER' AS rule,
               COUNT(DISTINCT policy_id),
               COUNT(DISTINCT member_id)
        FROM latest_file_then_member
        GROUP BY year, month, status
    ),

    chandra AS (
        {chandra_cte()}
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
        ABS(r.enrollment_count - c.ch_enrollment)
        + ABS(r.enrollee_count - c.ch_enrollee) AS total_delta
    FROM rules r
    JOIN chandra c
      ON r.year = c.year
     AND r.month = c.month
     AND r.status = c.status
    ORDER BY r.year, r.month, r.status, total_delta;
    """
    cols, rows = run(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_table("5. LATEST FILE TESTS VS CHANDRA", cols, rows, limit=120)
    export_csv("05_latest_file_tests.csv", cols, rows)


def suspect_gap_records(conn):
    sql = """
    WITH base AS (
        SELECT
            s.*,
            s.additional_maint_reason_code AS status,
            f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f
          ON s.file_id = f.file_id
        WHERE s.issuer = :issuer
          AND s.year = :year
          AND s.month IN ('02','03')
          AND s.insurance_type_code = 'HLT'
          AND s.additional_maint_reason_code IN ('CONFIRM','TERM')
    ),

    latest_member AS (
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
    )

    SELECT
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
        file_name,
        raw_xml_path
    FROM latest_member
    ORDER BY month, status, policy_id, member_id
    LIMIT 500;
    """
    cols, rows = run(conn, sql, {"issuer": ISSUER, "year": YEAR})
    print_table("6. SUSPECT GAP RECORD SAMPLE - FEB/MAR CONFIRM/TERM", cols, rows, limit=100)
    export_csv("06_suspect_gap_records_feb_mar.csv", cols, rows)


def main():
    conn = connect()

    monthly_baseline(conn)
    gap_feature_profile(conn)
    test_business_filters(conn)
    identify_month_file_patterns(conn)
    latest_file_only_test(conn)
    suspect_gap_records(conn)

    conn.close()


if __name__ == "__main__":
    main()
