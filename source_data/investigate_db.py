import sqlite3
from pathlib import Path

# ==========================================================
# UPDATE THIS PATH
# ==========================================================
DB_PATH = r"C:\Users\SelmaKazanci\Downloads\gaaccess-develop6\gaaccess-develop6\834_issuer_etl\assets\15105\rollups\sqlite\15105_rollup.db"
# ==========================================================

db = Path(DB_PATH)

if not db.exists():
    print(f"\nDatabase not found:\n{db}")
    exit()

print("=" * 80)
print("DATABASE")
print("=" * 80)
print(db)

conn = sqlite3.connect(db)
cursor = conn.cursor()

# ----------------------------------------------------------
# List tables
# ----------------------------------------------------------
print("\nTABLES\n")

cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='table'
ORDER BY name;
""")

tables = [r[0] for r in cursor.fetchall()]

for t in tables:
    print(f" - {t}")

print("\n" + "=" * 80)

# ----------------------------------------------------------
# Show columns for every table
# ----------------------------------------------------------
for table in tables:

    print(f"\nTABLE: {table}")
    print("-" * 80)

    cursor.execute(f"PRAGMA table_info({table})")

    cols = cursor.fetchall()

    for col in cols:
        print(f"{col[1]:35} {col[2]}")

    # row count
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        cnt = cursor.fetchone()[0]
        print(f"\nRows : {cnt}")
    except:
        pass

print("\nDone.")

conn.close()
