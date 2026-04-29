import pandas as pd

from nhl_match_prediction.etl_pipeline.connection import get_engine


def get_upcoming_matches() -> pd.DataFrame:
    engine = get_engine()

    query = """
        SELECT
            u.*,
            h.logo_url AS home_logo,
            a.logo_url AS away_logo,
            ar.arena AS arena
        FROM upcoming_match_features u
        LEFT JOIN team_logo h
            ON u.home_team_abbr = h.team_abbr
        LEFT JOIN team_logo a
            ON u.away_team_abbr = a.team_abbr
        LEFT JOIN arenas_data ar
            ON u.home_team_abbr = ar.team_abbr
    """

    return pd.read_sql(query, engine)


if __name__ == "__main__":
    df = get_upcoming_matches()
    print(df.head())
