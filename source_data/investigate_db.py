import sqlite3
import csv
from pathlib import Path

DB_PATH = Path("data/issuer_834.db")
OUT_DIR = Path("query_outputs")
OUT_DIR.mkdir(exist_ok=True)

# Chandra kendi SQL'ini buraya yazabilir.
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

    # Read-only connection. This script will not change data.
    return sqlite3.connect(f"file:{DB_PATH.resolve()}?mode=ro", uri=True)


def print_title(title):
    print("\n" + "=" * 120)
    print(title)
    print("=" * 120)


def run_sql(conn, sql, title="QUERY RESULT", export_name=None, limit=200):
    cur = conn.cursor()
    cur.execute(sql)

    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []

    print_title(title)

    if not cols:
        print("Query executed. No result set.")
        return

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


def show_tables(conn):
    sql = """
    SELECT name
    FROM sqlite_master
    WHERE type='table'
    ORDER BY name;
    """
    run_sql(conn, sql, "TABLES", "tables.csv")


def show_columns(conn, table_name="stg_834_records"):
    sql = f"""
    PRAGMA table_info({table_name});
    """
    run_sql(conn, sql, f"COLUMNS: {table_name}", f"{table_name}_columns.csv", limit=300)


def show_sample_rows(conn):
    sql = """
    SELECT
        record_id,
        issuer,
        year,
        month,
        policy_id,
        member_id,
        subscriber_id,
        insurance_type_code,
        additional_maint_reason_code,
        maintenance_type_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        relationship,
        subscriber_flag,
        benefit_effective_date,
        benefit_end_date,
        member_maint_effective_date,
        request_submit_timestamp,
        file_name,
        raw_xml_path
    FROM stg_834_records
    WHERE issuer = '15105'
      AND year = '2026'
    LIMIT 50;
    """
    run_sql(conn, sql, "SAMPLE STAGING ROWS", "sample_staging_rows.csv", limit=50)


def show_field_distributions(conn):
    sql = """
    SELECT
        year,
        month,
        insurance_type_code,
        additional_maint_reason_code,
        maintenance_type_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        subscriber_flag,
        relationship,
        COUNT(*) AS raw_rows,
        COUNT(DISTINCT policy_id) AS policies,
        COUNT(DISTINCT member_id) AS members,
        COUNT(DISTINCT subscriber_id) AS subscribers
    FROM stg_834_records
    WHERE issuer = '15105'
      AND year = '2026'
      AND month IN ('01','02','03','04','05')
    GROUP BY
        year,
        month,
        insurance_type_code,
        additional_maint_reason_code,
        maintenance_type_code,
        enrollee_event_type_code,
        enrollee_event_reason_code,
        transaction_classification,
        coverage_status,
        subscriber_flag,
        relationship
    ORDER BY year, month, additional_maint_reason_code, raw_rows DESC;
    """
    run_sql(conn, sql, "FIELD DISTRIBUTIONS", "field_distributions.csv", limit=300)


def run_chandra_query(conn):
    run_sql(
        conn,
        CUSTOM_QUERY,
        "CUSTOM CHANDRA QUERY RESULT",
        "custom_chandra_query_result.csv",
        limit=500
    )


def main():
    conn = connect()

    # 1. Chandra'ya göstermek için database structure
    show_tables(conn)
    show_columns(conn, "stg_834_records")

    # 2. Örnek data
    show_sample_rows(conn)

    # 3. Field distribution: relationship/status/reason/member/policy dağılımı
    show_field_distributions(conn)

    # 4. Chandra'nın kendi query'sini çalıştıracağı yer
    run_chandra_query(conn)

    conn.close()


if __name__ == "__main__":
    main()
