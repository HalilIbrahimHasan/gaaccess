import sqlite3
import csv
from pathlib import Path
from datetime import datetime

DB_PATH = Path("data/issuer_834.db")
OUT_DIR = Path("investigation_outputs_final")
OUT_DIR.mkdir(exist_ok=True)

ISSUER = "15105"

CHANDRA = {
    ("2026", "01", "CONFIRM"): (1240, 1690),
    ("2026", "01", "CANCEL"): (47, 47),
    ("2026", "02", "CONFIRM"): (490, 744),
    ("2026", "02", "CANCEL"): (734, 709),
    ("2026", "02", "TERM"): (1435, 1435),
    ("2026", "03", "CONFIRM"): (453, 584),
    ("2026", "03", "CANCEL"): (278, 271),
    ("2026", "03", "TERM"): (20, 20),
    ("2026", "04", "CONFIRM"): (463, 559),
    ("2026", "04", "CANCEL"): (236, 233),
    ("2026", "04", "TERM"): (4255, 4255),
    ("2026", "05", "CONFIRM"): (340, 418),
    ("2026", "05", "CANCEL"): (64, 63),
    ("2026", "05", "TERM"): (179, 179),
}

STATUS_SQL = """
CASE
    WHEN UPPER(COALESCE(additional_maint_reason_code, '')) LIKE '%CONFIRM%' THEN 'CONFIRM'
    WHEN UPPER(COALESCE(additional_maint_reason_code, '')) LIKE '%REINSTATE%' THEN 'CONFIRM'
    WHEN UPPER(COALESCE(additional_maint_reason_code, '')) LIKE '%CANCEL%' THEN 'CANCEL'
    WHEN UPPER(COALESCE(additional_maint_reason_code, '')) LIKE '%TERM%' THEN 'TERM'

    WHEN UPPER(COALESCE(transaction_classification, '')) LIKE '%CONFIRM%' THEN 'CONFIRM'
    WHEN UPPER(COALESCE(transaction_classification, '')) LIKE '%CANCEL%' THEN 'CANCEL'
    WHEN UPPER(COALESCE(transaction_classification, '')) LIKE '%TERM%' THEN 'TERM'

    WHEN UPPER(COALESCE(coverage_status, '')) LIKE '%CONFIRM%' THEN 'CONFIRM'
    WHEN UPPER(COALESCE(coverage_status, '')) LIKE '%EFFECT%' THEN 'CONFIRM'
    WHEN UPPER(COALESCE(coverage_status, '')) LIKE '%CANCEL%' THEN 'CANCEL'
    WHEN UPPER(COALESCE(coverage_status, '')) LIKE '%TERM%' THEN 'TERM'

    ELSE NULL
END
"""


def connect():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH.resolve()}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_columns(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(stg_834_records)")
    return [r["name"] for r in cur.fetchall()]


def pick_col(cols, *candidates, required=True):
    for c in candidates:
        if c in cols:
            return c
    if required:
        raise Exception(f"None of these columns found: {candidates}")
    return None


def q(col):
    return f'"{col}"'


def run_query(conn, title, sql, params=None, export_name=None, limit_print=80):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)

    cur = conn.cursor()
    cur.execute(sql, params or {})
    rows = cur.fetchall()

    if not rows:
        print("No rows.")
        return rows

    headers = rows[0].keys()
    print(" | ".join(headers))
    print("-" * 120)

    for row in rows[:limit_print]:
        print(" | ".join("" if row[h] is None else str(row[h]) for h in headers))

    print(f"\nRows shown: {min(len(rows), limit_print)} / {len(rows)}")

    if export_name:
        path = OUT_DIR / export_name
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in rows:
                writer.writerow([row[h] for h in headers])
        print(f"CSV exported: {path}")

    return rows


def build_base_temp(conn):
    cols = get_columns(conn)

    policy_col = pick_col(cols, "policy_id", "exchg_assigned_policy_id", "health_coverage_policy_no")
    member_col = pick_col(cols, "member_id", "exchg_indiv_identifier", "issuer_indiv_identifier")
    subscriber_col = pick_col(cols, "subscriber_id", "exchg_subscriber_identifier", "issuer_subscriber_identifier", required=False)
    enrollee_col = pick_col(cols, "exchg_assigned_enrollee_id", "enrollee_id", required=False)
    file_name_col = pick_col(cols, "file_name", required=False)
    raw_path_col = pick_col(cols, "raw_xml_path", required=False)

    subscriber_expr = q(subscriber_col) if subscriber_col else "NULL"
    enrollee_expr = q(enrollee_col) if enrollee_col else "NULL"
    file_name_expr = q(file_name_col) if file_name_col else "NULL"
    raw_path_expr = q(raw_path_col) if raw_path_col else "NULL"

    conn.execute("DROP TABLE IF EXISTS inv_base")
    conn.execute(f"""
        CREATE TEMP TABLE inv_base AS
        SELECT
            record_id,
            issuer,
            year,
            printf('%02d', CAST(month AS INTEGER)) AS month,

            {q(policy_col)} AS policy_id,
            {q(member_col)} AS member_id,
            {subscriber_expr} AS subscriber_id,
            {enrollee_expr} AS enrollee_id,

            relationship,
            subscriber_flag,
            maintenance_type_code,
            additional_maint_reason_code,
            coverage_status,
            benefit_effective_date,
            benefit_end_date,
            member_maint_effective_date,
            request_submit_timestamp,
            insurance_type_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,

            {file_name_expr} AS file_name,
            {raw_path_expr} AS raw_xml_path,

            {STATUS_SQL} AS status
        FROM stg_834_records
        WHERE issuer = ?
    """, (ISSUER,))

    conn.execute("DROP TABLE IF EXISTS inv_latest_member_status")
    conn.execute("""
        CREATE TEMP TABLE inv_latest_member_status AS
        SELECT *
        FROM (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, insurance_type_code, member_id
                    ORDER BY
                        COALESCE(member_maint_effective_date, benefit_effective_date, '') DESC,
                        COALESCE(request_submit_timestamp, '') DESC,
                        COALESCE(file_name, '') DESC,
                        record_id DESC
                ) AS rn
            FROM inv_base b
            WHERE status IS NOT NULL
        )
        WHERE rn = 1
    """)

    conn.execute("DROP TABLE IF EXISTS inv_latest_policy_member_status")
    conn.execute("""
        CREATE TEMP TABLE inv_latest_policy_member_status AS
        SELECT *
        FROM (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, insurance_type_code, policy_id, member_id
                    ORDER BY
                        COALESCE(member_maint_effective_date, benefit_effective_date, '') DESC,
                        COALESCE(request_submit_timestamp, '') DESC,
                        COALESCE(file_name, '') DESC,
                        record_id DESC
                ) AS rn
            FROM inv_base b
            WHERE status IS NOT NULL
        )
        WHERE rn = 1
    """)

    conn.execute("DROP TABLE IF EXISTS inv_latest_policy_status")
    conn.execute("""
        CREATE TEMP TABLE inv_latest_policy_status AS
        SELECT *
        FROM (
            SELECT
                b.*,
                ROW_NUMBER() OVER (
                    PARTITION BY year, month, status, insurance_type_code, policy_id
                    ORDER BY
                        COALESCE(member_maint_effective_date, benefit_effective_date, '') DESC,
                        COALESCE(request_submit_timestamp, '') DESC,
                        COALESCE(file_name, '') DESC,
                        record_id DESC
                ) AS rn
            FROM inv_base b
            WHERE status IS NOT NULL
        )
        WHERE rn = 1
    """)


def show_schema(conn):
    run_query(conn, "1. Tables", """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name
    """)

    run_query(conn, "2. Columns of stg_834_records", """
        PRAGMA table_info(stg_834_records)
    """, export_name="00_columns.csv")


def baseline_vs_chandra(conn):
    rows = []

    sql = """
        SELECT
            year,
            month,
            status,
            COUNT(DISTINCT policy_id) AS enrollment_count,
            COUNT(DISTINCT member_id) AS enrollee_count
        FROM inv_latest_member_status
        WHERE year = '2026'
          AND month IN ('01','02','03','04','05')
          AND status IN ('CONFIRM','CANCEL','TERM')
        GROUP BY year, month, status
        ORDER BY year, month,
            CASE status WHEN 'CONFIRM' THEN 1 WHEN 'CANCEL' THEN 2 WHEN 'TERM' THEN 3 ELSE 9 END
    """

    cur = conn.cursor()
    cur.execute(sql)
    for r in cur.fetchall():
        key = (r["year"], r["month"], r["status"])
        ch_enr, ch_mem = CHANDRA.get(key, (None, None))
        rows.append({
            "year": r["year"],
            "month": r["month"],
            "status": r["status"],
            "our_enrollment": r["enrollment_count"],
            "ch_enrollment": ch_enr,
            "enrollment_gap": None if ch_enr is None else r["enrollment_count"] - ch_enr,
            "our_enrollee": r["enrollee_count"],
            "ch_enrollee": ch_mem,
            "enrollee_gap": None if ch_mem is None else r["enrollee_count"] - ch_mem,
        })

    export_dict_rows("01_baseline_vs_chandra.csv", rows)
    print_dict_rows("3. BASELINE VS CHANDRA - latest member/status dedup", rows)


def export_dict_rows(filename, rows):
    if not rows:
        return
    path = OUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"CSV exported: {path}")


def print_dict_rows(title, rows, limit=80):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)

    if not rows:
        print("No rows.")
        return

    headers = list(rows[0].keys())
    print(" | ".join(headers))
    print("-" * 120)

    for row in rows[:limit]:
        print(" | ".join("" if row[h] is None else str(row[h]) for h in headers))

    print(f"\nRows shown: {min(len(rows), limit)} / {len(rows)}")


def feature_profile(conn):
    run_query(conn, "4. Feature profile after latest member dedup", """
        SELECT
            year,
            month,
            status,
            insurance_type_code,
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            coverage_status,
            subscriber_flag,
            relationship,
            COUNT(*) AS rows,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members
        FROM inv_latest_member_status
        WHERE year = '2026'
          AND month IN ('01','02','03','04','05')
        GROUP BY
            year, month, status, insurance_type_code,
            maintenance_type_code, additional_maint_reason_code,
            enrollee_event_type_code, enrollee_event_reason_code,
            transaction_classification, coverage_status,
            subscriber_flag, relationship
        ORDER BY year, month, status, rows DESC
    """, export_name="02_feature_profile_after_latest_member.csv")


def business_filter_tests(conn):
    rules = {
        "A_latest_member_all": "",
        "B_exclude_reason_28": "AND COALESCE(enrollee_event_reason_code,'') <> '28'",
        "C_only_reason_28": "AND COALESCE(enrollee_event_reason_code,'') = '28'",
        "D_exclude_reason_59": "AND COALESCE(enrollee_event_reason_code,'') <> '59'",
        "E_only_reason_59": "AND COALESCE(enrollee_event_reason_code,'') = '59'",
        "F_only_subscriber_Y": "AND COALESCE(subscriber_flag,'') = 'Y'",
        "G_exclude_subscriber_Y": "AND COALESCE(subscriber_flag,'') <> 'Y'",
        "H_only_relationship_18": "AND COALESCE(relationship,'') = '18'",
        "I_exclude_relationship_18": "AND COALESCE(relationship,'') <> '18'",
        "J_only_same_begin_end": "AND COALESCE(benefit_effective_date,'') = COALESCE(benefit_end_date,'')",
        "K_exclude_same_begin_end": "AND COALESCE(benefit_effective_date,'') <> COALESCE(benefit_end_date,'')",
        "L_only_null_end_date": "AND (benefit_end_date IS NULL OR TRIM(benefit_end_date) = '')",
        "M_exclude_null_end_date": "AND NOT (benefit_end_date IS NULL OR TRIM(benefit_end_date) = '')",
        "N_only_policy_equals_subscriber": "AND COALESCE(policy_id,'') = COALESCE(subscriber_id,'')",
        "O_exclude_policy_equals_subscriber": "AND COALESCE(policy_id,'') <> COALESCE(subscriber_id,'')",
    }

    all_rows = []

    for rule_name, where_extra in rules.items():
        sql = f"""
            SELECT
                year,
                month,
                status,
                '{rule_name}' AS rule,
                COUNT(DISTINCT policy_id) AS enrollment_count,
                COUNT(DISTINCT member_id) AS enrollee_count
            FROM inv_latest_member_status
            WHERE year = '2026'
              AND month IN ('01','02','03','04','05')
              AND status IN ('CONFIRM','CANCEL','TERM')
              {where_extra}
            GROUP BY year, month, status
        """
        cur = conn.cursor()
        cur.execute(sql)
        for r in cur.fetchall():
            key = (r["year"], r["month"], r["status"])
            ch_enr, ch_mem = CHANDRA.get(key, (None, None))
            if ch_enr is None:
                continue

            enrollment_delta = r["enrollment_count"] - ch_enr
            enrollee_delta = r["enrollee_count"] - ch_mem

            all_rows.append({
                "year": r["year"],
                "month": r["month"],
                "status": r["status"],
                "rule": r["rule"],
                "enrollment_count": r["enrollment_count"],
                "ch_enrollment": ch_enr,
                "enrollment_delta": enrollment_delta,
                "enrollee_count": r["enrollee_count"],
                "ch_enrollee": ch_mem,
                "enrollee_delta": enrollee_delta,
                "total_delta": abs(enrollment_delta) + abs(enrollee_delta),
            })

    all_rows.sort(key=lambda x: (x["year"], x["month"], x["status"], x["total_delta"], x["rule"]))

    export_dict_rows("03_business_filter_tests.csv", all_rows)
    print_dict_rows("5. BUSINESS FILTER TESTS VS CHANDRA", all_rows, limit=160)


def grain_tests(conn):
    sql = """
        SELECT
            year,
            month,
            status,
            COUNT(*) AS raw_rows,
            COUNT(DISTINCT policy_id) AS distinct_policy_id,
            COUNT(DISTINCT subscriber_id) AS distinct_subscriber_id,
            COUNT(DISTINCT member_id) AS distinct_member_id,
            COUNT(DISTINCT policy_id || '|' || member_id) AS distinct_policy_member,
            COUNT(DISTINCT policy_id || '|' || member_id || '|' || member_maint_effective_date) AS distinct_policy_member_effective,
            COUNT(DISTINCT member_id || '|' || member_maint_effective_date) AS distinct_member_effective,
            COUNT(DISTINCT file_name || '|' || policy_id) AS distinct_file_policy,
            COUNT(DISTINCT file_name || '|' || member_id) AS distinct_file_member
        FROM inv_base
        WHERE year = '2026'
          AND month IN ('01','02','03','04','05')
          AND status IN ('CONFIRM','CANCEL','TERM')
        GROUP BY year, month, status
        ORDER BY year, month,
            CASE status WHEN 'CONFIRM' THEN 1 WHEN 'CANCEL' THEN 2 WHEN 'TERM' THEN 3 ELSE 9 END
    """

    run_query(conn, "6. COUNT GRAIN COMPARISON", sql, export_name="04_count_grain_comparison.csv")


def latest_file_tests(conn):
    conn.execute("DROP TABLE IF EXISTS inv_latest_file")
    conn.execute("""
        CREATE TEMP TABLE inv_latest_file AS
        SELECT *
        FROM inv_base
        WHERE file_name IN (
            SELECT file_name
            FROM (
                SELECT
                    year,
                    month,
                    status,
                    file_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY year, month, status
                        ORDER BY COALESCE(file_name,'') DESC
                    ) AS rn
                FROM inv_base
                WHERE status IS NOT NULL
                GROUP BY year, month, status, file_name
            )
            WHERE rn = 1
        )
    """)

    rows = []

    for table_name, rule in [
        ("inv_latest_file", "LATEST_FILE_ONLY"),
        ("inv_latest_file", "LATEST_FILE_THEN_MEMBER"),
    ]:
        if rule == "LATEST_FILE_ONLY":
            sql = """
                SELECT
                    year,
                    month,
                    status,
                    COUNT(DISTINCT policy_id) AS enrollment_count,
                    COUNT(DISTINCT member_id) AS enrollee_count
                FROM inv_latest_file
                WHERE year = '2026'
                  AND month IN ('01','02','03','04','05')
                  AND status IN ('CONFIRM','CANCEL','TERM')
                GROUP BY year, month, status
            """
        else:
            sql = """
                WITH x AS (
                    SELECT *
                    FROM (
                        SELECT
                            lf.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY year, month, status, insurance_type_code, member_id
                                ORDER BY
                                    COALESCE(member_maint_effective_date, benefit_effective_date, '') DESC,
                                    COALESCE(request_submit_timestamp, '') DESC,
                                    record_id DESC
                            ) AS rn2
                        FROM inv_latest_file lf
                    )
                    WHERE rn2 = 1
                )
                SELECT
                    year,
                    month,
                    status,
                    COUNT(DISTINCT policy_id) AS enrollment_count,
                    COUNT(DISTINCT member_id) AS enrollee_count
                FROM x
                WHERE year = '2026'
                  AND month IN ('01','02','03','04','05')
                  AND status IN ('CONFIRM','CANCEL','TERM')
                GROUP BY year, month, status
            """

        cur = conn.cursor()
        cur.execute(sql)
        for r in cur.fetchall():
            key = (r["year"], r["month"], r["status"])
            ch_enr, ch_mem = CHANDRA.get(key, (None, None))
            if ch_enr is None:
                continue

            rows.append({
                "year": r["year"],
                "month": r["month"],
                "status": r["status"],
                "rule": rule,
                "enrollment_count": r["enrollment_count"],
                "ch_enrollment": ch_enr,
                "enrollment_delta": r["enrollment_count"] - ch_enr,
                "enrollee_count": r["enrollee_count"],
                "ch_enrollee": ch_mem,
                "enrollee_delta": r["enrollee_count"] - ch_mem,
                "total_delta": abs(r["enrollment_count"] - ch_enr) + abs(r["enrollee_count"] - ch_mem),
            })

    rows.sort(key=lambda x: (x["year"], x["month"], x["status"], x["total_delta"]))
    export_dict_rows("05_latest_file_tests.csv", rows)
    print_dict_rows("7. LATEST FILE TESTS VS CHANDRA", rows)


def best_rule_summary(conn):
    files = [
        OUT_DIR / "03_business_filter_tests.csv",
        OUT_DIR / "05_latest_file_tests.csv",
    ]

    all_rows = []

    for file in files:
        if not file.exists():
            continue
        with open(file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                r["total_delta"] = int(r["total_delta"])
                all_rows.append(r)

    best = {}
    for r in all_rows:
        key = (r["year"], r["month"], r["status"])
        if key not in best or r["total_delta"] < best[key]["total_delta"]:
            best[key] = r

    rows = list(best.values())
    rows.sort(key=lambda x: (x["year"], x["month"], x["status"]))

    export_dict_rows("06_best_rule_per_month_status.csv", rows)
    print_dict_rows("8. BEST MATCHING RULE PER MONTH/STATUS", rows)


def suspect_gap_records(conn):
    run_query(conn, "9. Feb 2026 CONFIRM raw sample", """
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
        FROM inv_base
        WHERE year = '2026'
          AND month = '02'
          AND status = 'CONFIRM'
        ORDER BY policy_id, member_id, member_maint_effective_date, file_name
        LIMIT 500
    """, export_name="07_feb_confirm_raw_sample.csv", limit_print=120)

    run_query(conn, "10. Feb/Mar TERM raw sample", """
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
        FROM inv_base
        WHERE year = '2026'
          AND month IN ('02','03')
          AND status = 'TERM'
        ORDER BY month, policy_id, member_id, member_maint_effective_date, file_name
        LIMIT 500
    """, export_name="08_feb_mar_term_raw_sample.csv", limit_print=120)


def final_recommendation():
    print("\n" + "=" * 120)
    print("FINAL RECOMMENDATION FOR CURSOR / PIPELINE UPDATE")
    print("=" * 120)

    print("""
1) Do NOT rely on raw row count.
2) Keep staging as-is. Do not remove rows during load.
3) For Enrollment Summary, use business-level dedup only.

Recommended official logic:
- Status:
    CONFIRM from additional_maint_reason_code / transaction_classification / coverage_status.
    CANCEL from additional_maint_reason_code / transaction_classification / coverage_status.
    TERM from additional_maint_reason_code / transaction_classification / coverage_status.
- Insurance Type:
    HLT => Health, DEN => Dental, null => Health only if business accepts default.
- Dedup:
    Dedup grain = issuer + year + month + insurance_type + status + member_id.
    Keep latest record by:
        1. member_maint_effective_date DESC
        2. benefit_effective_date DESC
        3. request_submit_timestamp DESC
        4. file_name DESC
        5. record_id DESC
- Counts:
    Enrollment_Count = COUNT(DISTINCT policy_id) after dedup.
    Enrollee_Count = COUNT(DISTINCT member_id) after dedup.

What we found:
- Chandra does NOT match one single consistent rule across all months/statuses.
- Some Chandra rows are close to latest member/status dedup.
- Some rows are closer to policy/member dedup or special filters.
- The biggest mismatch is mainly Feb/Mar TERM and Feb CONFIRM, which suggests Chandra is likely using a different file window, effective-date window, or a subset of files.
- Therefore the gap is not just code bug. It is a business-rule / source-scope mismatch.

Ask Chandra/Hari:
1. Are counts based on all files in the month or only the latest file/snapshot?
2. Are counts based on file date, member maintenance effective date, benefit effective date, or reporting month?
3. Is Enrollee_Count based on member_id or REF*4A enrollee_id?
4. Are subscriber-only rows counted differently?
5. Are rows with benefit effective date outside the load month excluded?
6. Are TERM rows counted only if benefit_end_date is inside that month?

Use exported CSVs under investigation_outputs_final for evidence.
""")


def main():
    print(f"Started: {datetime.now()}")
    print(f"DB Path: {DB_PATH.resolve()}")
    print(f"Output Folder: {OUT_DIR.resolve()}")

    conn = connect()

    try:
        build_base_temp(conn)

        show_schema(conn)
        baseline_vs_chandra(conn)
        feature_profile(conn)
        business_filter_tests(conn)
        grain_tests(conn)
        latest_file_tests(conn)
        best_rule_summary(conn)
        suspect_gap_records(conn)
        final_recommendation()

    finally:
        conn.close()

    print(f"\nFinished: {datetime.now()}")


if __name__ == "__main__":
    main()
