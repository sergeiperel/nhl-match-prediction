import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"
SEASON_END = 7

today = date.today()
year = today.year
month = today.month

if month >= SEASON_END:
    season_start = year
    season_end = year + 1
else:
    season_start = year - 1
    season_end = year

season_id = f"{season_start}{season_end}"


def create_team_logo_table():
    con = sqlite3.connect(DB_PATH)

    con.execute("""DROP TABLE IF EXISTS team_logo;""")

    con.execute("""
    CREATE TABLE IF NOT EXISTS team_logo (
        team_abbr TEXT PRIMARY KEY,
        team_name TEXT,
        logo_url TEXT
    )
    """)

    con.commit()
    con.close()


def populate_team_logo():
    con = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query(
        f"""
        SELECT DISTINCT
            team_abbrev as team_abbr,
            team_name as team_name,
            team_logo as logo_url
        FROM standings_daily
        WHERE season_id = '{season_id}'
    """,
        con,
    )

    df.to_sql("team_logo", con, if_exists="replace", index=False)

    con.close()


if __name__ == "__main__":
    create_team_logo_table()
    populate_team_logo()

    print("✅ team_logo table created")
