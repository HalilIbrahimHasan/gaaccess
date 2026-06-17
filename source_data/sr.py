import sqlite3
from pathlib import Path

DB_PATH = r"C:\Users\SelmaKazanci\Downloads\gaaccess-develop6\gaaccess-develop6\834_issuer_etl\data\issuer_834.db"

db = Path(DB_PATH)
print("DB exists:", db.exists())
print("DB path:", db)

conn = sqlite3.connect(db)
cur = conn.cursor()

print("\nTABLES:")
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
tables = [r[0] for r in cur.fetchall()]
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM {t}")
    print(f"- {t}: {cur.fetchone()[0]} rows")

print("\nSTAGING COLUMNS:")
cur.execute("PRAGMA table_info(stg_834_records);")
for col in cur.fetchall():
    print(col[1])

conn.close()
