import sqlite3
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"


def get_upcoming_matches() -> pd.DataFrame:
    con = sqlite3.connect(str(DB_PATH))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA cache_size=1000000;")

    # ------------------------------------------------------------------------------------------
    # UPCOMING GAMES
    # ------------------------------------------------------------------------------------------

    df = pd.read_sql_query(
        """
        SELECT
            u.*,
            h.logo_url AS home_logo,
            a.logo_url AS away_logo,
			ar.Arena AS arena

        FROM upcoming_match_features u
        LEFT JOIN team_logo h
            ON u.home_team_abbr = h.team_abbr
        LEFT JOIN team_logo a
            ON u.away_team_abbr = a.team_abbr
		LEFT JOIN arenas_data ar
			ON u.home_team_abbr = ar.team_abbr
        """,
        con,
    )

    con.close()

    return df


if __name__ == "__main__":
    df = get_upcoming_matches()
    print(df.head())
