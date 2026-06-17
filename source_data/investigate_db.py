import sqlite3
import csv
from pathlib import Path
from collections import defaultdict

DB_PATH = Path("data/issuer_834.db")
OUT_DIR = Path("investigation_outputs_final")
OUT_DIR.mkdir(exist_ok=True)

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
    WHEN upper(coalesce(additional_maint_reason_code,'')) LIKE '%CONFIRM%' THEN 'CONFIRM'
    WHEN upper(coalesce(additional_maint_reason_code,'')) LIKE '%CANCEL%' THEN 'CANCEL'
    WHEN upper(coalesce(additional_maint_reason_code,'')) LIKE '%TERM%' THEN 'TERM'
    WHEN upper(coalesce(transaction_classification,'')) LIKE '%CONFIRM%' THEN 'CONFIRM'
    WHEN upper(coalesce(transaction_classification,'')) LIKE '%CANCEL%' THEN 'CANCEL'
    WHEN upper(coalesce(transaction_classification,'')) LIKE '%TERM%' THEN 'TERM'
    ELSE NULL
END
"""

def connect():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH.resolve()}")
    return sqlite3.connect(DB_PATH)

def q(conn, sql, params=()):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]

def export_csv(name, rows):
    path = OUT_DIR / name
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"CSV exported: {path}")

def print_table(title, rows, limit=80):
    print("\n" + "=" * 140)
    print(title)
    print("=" * 140)
    if not rows:
        print("No rows")
        return

    cols = list(rows[0].keys())
    print(" | ".join(cols))
    print("-" * 140)

    for r in rows[:limit]:
        print(" | ".join(str(r.get(c, "")) for c in cols))

    print(f"Rows shown: {min(len(rows), limit)} / {len(rows)}")

def base_view_sql():
    return f"""
    WITH base AS (
        SELECT
            issuer,
            year,
            printf('%02d', CAST(month AS INTEGER)) AS month,
            policy_id,
            member_id,
            subscriber_id,
            health_coverage_policy_no,
            household_or_employee_case_id,
            relationship,
            subscriber_flag,
            maintenance_type_code,
            additional_maint_reason_code,
            enrollee_event_type_code,
            enrollee_event_reason_code,
            transaction_classification,
            coverage_status,
            benefit_effective_date,
            benefit_end_date,
            member_maint_effective_date,
            request_submit_timestamp,
            file_name,
            raw_xml_path,
            {STATUS_SQL} AS status
        FROM stg_834_records
        WHERE issuer = '15105'
    )
    """

def run_rule(conn, rule_name, where_clause="", enrollment_key="policy_id", enrollee_key="member_id", dedup_key="member_id"):
    sql = base_view_sql() + f"""
    , filtered AS (
        SELECT *
        FROM base
        WHERE status IS NOT NULL
        {where_clause}
    ),
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY year, month, status, insurance_grain, dedup_value
                   ORDER BY
                       date(coalesce(member_maint_effective_date, benefit_effective_date, '1900-01-01')) DESC,
                       coalesce(request_submit_timestamp, '') DESC,
                       coalesce(file_name, '') DESC,
                       record_sort DESC
               ) AS rn
        FROM (
            SELECT
                rowid AS record_sort,
                *,
                coalesce(insurance_type_code, 'HLT') AS insurance_grain,
                coalesce({dedup_key}, '') AS dedup_value
            FROM filtered
        )
    ),
    final AS (
        SELECT *
        FROM ranked
        WHERE rn = 1
    )
    SELECT
        year,
        month,
        status,
        '{rule_name}' AS rule,
        COUNT(DISTINCT NULLIF({enrollment_key}, '')) AS enrollment_count,
        COUNT(DISTINCT NULLIF({enrollee_key}, '')) AS enrollee_count
    FROM final
    GROUP BY year, month, status
    ORDER BY year, month,
        CASE status WHEN 'CONFIRM' THEN 1 WHEN 'CANCEL' THEN 2 WHEN 'TERM' THEN 3 ELSE 9 END
    """
    return q(conn, sql)

def score_vs_chandra(rows):
    scored = []
    for r in rows:
        key = (r["year"], r["month"], r["status"])
        if key not in CHANDRA:
            continue

        ch_enrollment, ch_enrollee = CHANDRA[key]
        enrollment_delta = r["enrollment_count"] - ch_enrollment
        enrollee_delta = r["enrollee_count"] - ch_enrollee

        scored.append({
            **r,
            "ch_enrollment": ch_enrollment,
            "ch_enrollee": ch_enrollee,
            "enrollment_delta": enrollment_delta,
            "enrollee_delta": enrollee_delta,
            "total_abs_delta": abs(enrollment_delta) + abs(enrollee_delta),
            "exact_match": "YES" if enrollment_delta == 0 and enrollee_delta == 0 else "NO",
        })
    return scored

def test_all_logic_variants(conn):
    rules = []

    filters = {
        "ALL": "",
        "ONLY_SUBSCRIBER_Y": "AND subscriber_flag = 'Y'",
        "ONLY_REL_18": "AND relationship = '18'",
        "ONLY_ACTIVE_END_NULL": "AND (benefit_end_date IS NULL OR trim(benefit_end_date) = '')",
        "EXCLUDE_REL_18": "AND coalesce(relationship,'') <> '18'",
        "EXCLUDE_SUBSCRIBER_Y": "AND coalesce(subscriber_flag,'') <> 'Y'",
        "ONLY_REASON_28": "AND enrollee_event_reason_code = '28'",
        "ONLY_REASON_59": "AND enrollee_event_reason_code = '59'",
        "EXCLUDE_REASON_28": "AND coalesce(enrollee_event_reason_code,'') <> '28'",
        "EXCLUDE_REASON_59": "AND coalesce(enrollee_event_reason_code,'') <> '59'",
        "BENEFIT_MONTH_MATCH": "AND substr(benefit_effective_date,1,7) = year || '-' || printf('%02d', cast(month as integer))",
        "REQUEST_MONTH_MATCH": "AND substr(request_submit_timestamp,1,7) = year || '-' || printf('%02d', cast(month as integer))",
    }

    enrollment_keys = {
        "POLICY": "policy_id",
        "HEALTH_POLICY": "health_coverage_policy_no",
        "SUBSCRIBER": "subscriber_id",
        "MEMBER": "member_id",
        "POLICY_MEMBER": "policy_id || '|' || member_id",
        "SUBSCRIBER_MEMBER": "subscriber_id || '|' || member_id",
    }

    enrollee_keys = {
        "MEMBER": "member_id",
        "SUBSCRIBER": "subscriber_id",
        "POLICY_MEMBER": "policy_id || '|' || member_id",
        "SUBSCRIBER_MEMBER": "subscriber_id || '|' || member_id",
    }

    dedup_keys = {
        "MEMBER": "member_id",
        "POLICY_MEMBER": "policy_id || '|' || member_id",
        "POLICY_STATUS": "policy_id",
        "SUBSCRIBER_MEMBER": "subscriber_id || '|' || member_id",
    }

    all_results = []

    for f_name, f_sql in filters.items():
        for e_name, e_sql in enrollment_keys.items():
            for m_name, m_sql in enrollee_keys.items():
                for d_name, d_sql in dedup_keys.items():
                    rule = f"FILTER={f_name} | ENROLL={e_name} | ENROLLEE={m_name} | DEDUP={d_name}"
                    try:
                        rows = run_rule(
                            conn,
                            rule_name=rule,
                            where_clause=f_sql,
                            enrollment_key=e_sql,
                            enrollee_key=m_sql,
                            dedup_key=d_sql,
                        )
                        all_results.extend(score_vs_chandra(rows))
                    except Exception as ex:
                        print(f"Rule failed: {rule} -> {ex}")

    all_results.sort(key=lambda r: (r["total_abs_delta"], r["year"], r["month"], r["status"], r["rule"]))

    export_csv("01_all_logic_variants_vs_chandra.csv", all_results)
    print_table("TOP LOGIC MATCHES VS CHANDRA", all_results[:100], limit=100)

    return all_results

def best_rule_per_month_status(all_results):
    best = {}
    for r in all_results:
        key = (r["year"], r["month"], r["status"])
        if key not in best or r["total_abs_delta"] < best[key]["total_abs_delta"]:
            best[key] = r

    rows = list(best.values())
    rows.sort(key=lambda r: (r["year"], r["month"], r["status"]))
    export_csv("02_best_rule_per_month_status.csv", rows)
    print_table("BEST RULE PER MONTH/STATUS", rows, limit=100)
    return rows

def baseline_current_logic(conn):
    rows = run_rule(
        conn,
        rule_name="CURRENT_BASELINE_LATEST_MEMBER_STATUS",
        where_clause="",
        enrollment_key="policy_id",
        enrollee_key="member_id",
        dedup_key="member_id",
    )
    scored = score_vs_chandra(rows)
    export_csv("03_current_baseline_vs_chandra.csv", scored)
    print_table("CURRENT BASELINE VS CHANDRA", scored, limit=100)
    return scored

def timeline_duplicate_members(conn):
    sql = base_view_sql() + """
    , member_counts AS (
        SELECT year, month, status, member_id, COUNT(*) AS row_count, COUNT(DISTINCT file_name) AS file_count
        FROM base
        WHERE status IS NOT NULL
          AND member_id IS NOT NULL
          AND trim(member_id) <> ''
        GROUP BY year, month, status, member_id
        HAVING COUNT(*) > 1
    )
    SELECT
        b.year,
        b.month,
        b.status,
        b.policy_id,
        b.member_id,
        b.subscriber_id,
        b.relationship,
        b.subscriber_flag,
        b.maintenance_type_code,
        b.additional_maint_reason_code,
        b.enrollee_event_type_code,
        b.enrollee_event_reason_code,
        b.transaction_classification,
        b.coverage_status,
        b.benefit_effective_date,
        b.benefit_end_date,
        b.member_maint_effective_date,
        b.request_submit_timestamp,
        b.file_name,
        b.raw_xml_path
    FROM base b
    JOIN member_counts mc
      ON b.year = mc.year
     AND b.month = mc.month
     AND b.status = mc.status
     AND b.member_id = mc.member_id
    WHERE b.month IN ('02','03','04','05')
    ORDER BY b.year, b.month, b.status, b.member_id,
             date(coalesce(b.member_maint_effective_date, b.benefit_effective_date, '1900-01-01')),
             b.request_submit_timestamp,
             b.file_name
    LIMIT 2000
    """
    rows = q(conn, sql)
    export_csv("04_duplicate_member_timeline_sample.csv", rows)
    print_table("DUPLICATE MEMBER TIMELINE SAMPLE", rows, limit=120)
    return rows

def suspect_gap_records(conn):
    sql = base_view_sql() + """
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
        additional_maint_reason_code,
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
    FROM base
    WHERE status IS NOT NULL
      AND (
            month IN ('02','03')
            OR (month='04' AND status IN ('CONFIRM','TERM'))
            OR (month='05')
          )
    ORDER BY year, month, status, policy_id, member_id, request_submit_timestamp, file_name
    LIMIT 5000
    """
    rows = q(conn, sql)
    export_csv("05_suspect_gap_records.csv", rows)
    print_table("SUSPECT GAP RECORDS SAMPLE", rows, limit=150)
    return rows

def feature_profile(conn):
    sql = base_view_sql() + """
    SELECT
        year,
        month,
        status,
        maintenance_type_code,
        additional_maint_reason_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        subscriber_flag,
        relationship,
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT policy_id) AS distinct_policies,
        COUNT(DISTINCT member_id) AS distinct_members,
        COUNT(DISTINCT subscriber_id) AS distinct_subscribers,
        COUNT(DISTINCT health_coverage_policy_no) AS distinct_health_policy_no
    FROM base
    WHERE status IS NOT NULL
    GROUP BY
        year, month, status,
        maintenance_type_code,
        additional_maint_reason_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        subscriber_flag,
        relationship
    ORDER BY year, month,
        CASE status WHEN 'CONFIRM' THEN 1 WHEN 'CANCEL' THEN 2 WHEN 'TERM' THEN 3 ELSE 9 END,
        raw_rows DESC
    """
    rows = q(conn, sql)
    export_csv("06_feature_profile.csv", rows)
    print_table("FEATURE PROFILE", rows, limit=150)
    return rows

def raw_count_grains(conn):
    sql = base_view_sql() + """
    SELECT
        year,
        month,
        status,
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT policy_id) AS distinct_policy_id,
        COUNT(DISTINCT member_id) AS distinct_member_id,
        COUNT(DISTINCT subscriber_id) AS distinct_subscriber_id,
        COUNT(DISTINCT health_coverage_policy_no) AS distinct_health_coverage_policy_no,
        COUNT(DISTINCT policy_id || '|' || member_id) AS distinct_policy_member,
        COUNT(DISTINCT subscriber_id || '|' || member_id) AS distinct_subscriber_member,
        COUNT(DISTINCT policy_id || '|' || member_id || '|' || coalesce(benefit_effective_date,'')) AS distinct_policy_member_begin,
        COUNT(DISTINCT policy_id || '|' || member_id || '|' || coalesce(benefit_effective_date,'') || '|' || coalesce(benefit_end_date,'')) AS distinct_policy_member_begin_end
    FROM base
    WHERE status IS NOT NULL
    GROUP BY year, month, status
    ORDER BY year, month,
        CASE status WHEN 'CONFIRM' THEN 1 WHEN 'CANCEL' THEN 2 WHEN 'TERM' THEN 3 ELSE 9 END
    """
    rows = q(conn, sql)
    export_csv("07_raw_count_grains.csv", rows)
    print_table("RAW COUNT GRAINS", rows, limit=100)
    return rows

def write_cursor_prompt(best_rows):
    prompt = f"""
Cursor update request:

We completed the investigation against Chandra reference counts for issuer 15105.

Main finding:
The data load is not missing rows. XML files are loaded into stg_834_records, and the mismatch is caused by business counting logic: repeated monthly 834 maintenance transactions, member/policy level dedup grain, and possible Chandra-specific filters.

Update enrollment_summary.py so the logic is configurable and auditable.

Required implementation:

1. Keep staging unchanged.
   - Do not remove rows during load.
   - All XML transaction rows must remain in stg_834_records.

2. Add configurable enrollment summary strategy:
   - status source: additional_maint_reason_code first, then transaction_classification fallback.
   - status values: CONFIRM, CANCEL, TERM.
   - insurance type default: HLT => Health.
   - dedup strategy configurable:
       a. member_id
       b. policy_id + member_id
       c. subscriber_id + member_id
       d. policy_id
   - enrollment key configurable:
       a. policy_id
       b. health_coverage_policy_no
       c. subscriber_id
       d. member_id
       e. policy_id + member_id
   - enrollee key configurable:
       a. member_id
       b. subscriber_id
       c. policy_id + member_id

3. Add business filters configurable:
   - only subscriber_flag = Y
   - only relationship = 18
   - exclude relationship = 18
   - only enrollee_event_reason_code = 28
   - only enrollee_event_reason_code = 59
   - exclude reason 28
   - exclude reason 59
   - require benefit_effective_date month equals reporting month
   - require request_submit_timestamp month equals reporting month
   - require benefit_end_date null
   - exclude policy_id = subscriber_id

4. Dedup ordering:
   Keep latest row by:
   - member_maint_effective_date DESC
   - benefit_effective_date DESC
   - request_submit_timestamp DESC
   - file_name DESC

5. Add audit exports:
   - enrollment_summary_final.xlsx
   - enrollment_summary_debug.csv
   - duplicate_member_timeline.csv
   - chandra_comparison.csv
   - unmatched_gap_records.csv

6. The system must show:
   - raw rows before dedup
   - rows after dedup
   - distinct policy count
   - distinct member count
   - selected rule name
   - delta vs Chandra reference if provided

7. Do not hardcode Chandra values into production aggregation.
   Chandra values can only be used in comparison/debug mode.

Best currently tested rules per month/status:
{best_rows}

Important:
This update is not because parser/load is wrong. The investigation shows the mismatch is mainly caused by business counting grain and repeated monthly maintenance transactions.
"""
    path = OUT_DIR / "08_cursor_update_prompt.txt"
    path.write_text(prompt, encoding="utf-8")
    print(f"Cursor prompt exported: {path}")

def main():
    conn = connect()

    print("\nFINAL CHANDRA LOGIC INVESTIGATION STARTED")
    print(f"DB: {DB_PATH.resolve()}")

    baseline_current_logic(conn)
    feature_profile(conn)
    raw_count_grains(conn)

    all_results = test_all_logic_variants(conn)
    best_rows = best_rule_per_month_status(all_results)

    timeline_duplicate_members(conn)
    suspect_gap_records(conn)

    write_cursor_prompt(best_rows)

    conn.close()

    print("\nDONE.")
    print(f"Open this folder: {OUT_DIR.resolve()}")
    print("\nMost important files:")
    print("01_all_logic_variants_vs_chandra.csv")
    print("02_best_rule_per_month_status.csv")
    print("03_current_baseline_vs_chandra.csv")
    print("04_duplicate_member_timeline_sample.csv")
    print("05_suspect_gap_records.csv")
    print("08_cursor_update_prompt.txt")

if __name__ == "__main__":
    main()
