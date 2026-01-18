import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]

DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"
DATA_PATH = BASE_DIR / "data" / "processed"

conn = sqlite3.connect(DB_PATH)

tables = {
    "games": "games.csv",
    "team_game_stats": "team_game_stats.csv",
    "goalie_game_stats": "goalie_game_stats.csv",
    "standings_daily": "standings_daily.csv",
}

for table_name, file_name in tables.items():
    file_path = DATA_PATH / file_name

    if not file_path.exists():
        print(f"{file_name} not found, skipping")
        continue

    print(f"Loading {file_name} → {table_name}")
    df = pd.read_csv(file_path)

    df.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=50_000)

conn.close()
print("All tables loaded into SQLite")
