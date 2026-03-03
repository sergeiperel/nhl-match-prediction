import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]

DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"
OUT_PATH = BASE_DIR / "data" / "processed" / "match_features2.csv"

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(DB_PATH)

query = """
SELECT * FROM match_features
"""

df = pd.read_sql(query, conn)
df.to_csv(OUT_PATH, index=False)

conn.close()

print(f"Exported {len(df)} rows to {OUT_PATH}")
