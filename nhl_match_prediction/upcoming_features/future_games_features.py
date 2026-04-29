import pandas as pd
from sqlalchemy import text

from nhl_match_prediction.etl_pipeline.connection import get_engine
from nhl_match_prediction.feature_engineering.games_features.elo_matches import add_elo_features
from nhl_match_prediction.feature_engineering.games_features.geo_location_data import (
    timezone_change,
    travel_distance,
)


def build_future_games_features():
    engine = get_engine()

    with engine.begin() as con:
        games = pd.read_sql(
            """
            SELECT
                game_id,
                date,
                game_type,
                neutral_site,
                home_team_id,
                away_team_id,
                home_team_abbr,
                away_team_abbr,
                home_win,
                ABS(home_score - away_score) AS goal_diff
            FROM games
        """,
            con,
        )

        schedule = pd.read_sql(
            """
            SELECT
                game_id,
                game_date,
                home_team_id,
                away_team_id,
                home_team_abbr,
                away_team_abbr,
                game_state
            FROM schedule_games
            WHERE game_state = 'FUT'
        """,
            con,
        )

    # elo + trends on history
    games = games.sort_values("date").reset_index(drop=True)
    games = add_elo_features(games)

    # extract last elo
    home_elo = games[["home_team_id", "home_elo"]].rename(
        columns={"home_team_id": "team_id", "home_elo": "elo"}
    )

    away_elo = games[["away_team_id", "away_elo"]].rename(
        columns={"away_team_id": "team_id", "away_elo": "elo"}
    )

    teams_elo = pd.concat([home_elo, away_elo], ignore_index=True)
    teams_elo = teams_elo.groupby("team_id").last().reset_index()

    # extract last trends
    home_trend = games[["home_team_id", "home_elo_trend_last5"]].rename(
        columns={"home_team_id": "team_id", "home_elo_trend_last5": "trend"}
    )

    away_trend = games[["away_team_id", "away_elo_trend_last5"]].rename(
        columns={"away_team_id": "team_id", "away_elo_trend_last5": "trend"}
    )

    teams_trend = pd.concat([home_trend, away_trend], ignore_index=True)
    teams_trend = teams_trend.groupby("team_id").last().reset_index()

    # BUILD FUTURE FEATURES
    future = schedule.merge(
        teams_elo.rename(columns={"team_id": "home_team_id", "elo": "home_elo"}),
        on="home_team_id",
        how="left",
    )

    future = future.merge(
        teams_elo.rename(columns={"team_id": "away_team_id", "elo": "away_elo"}),
        on="away_team_id",
        how="left",
    )

    future = future.merge(
        teams_trend.rename(columns={"team_id": "home_team_id", "trend": "home_elo_trend_last5"}),
        on="home_team_id",
        how="left",
    )

    future = future.merge(
        teams_trend.rename(columns={"team_id": "away_team_id", "trend": "away_elo_trend_last5"}),
        on="away_team_id",
        how="left",
    )

    future["elo_diff"] = future["home_elo"] - future["away_elo"]

    future["elo_trend_diff"] = future["home_elo_trend_last5"] - future["away_elo_trend_last5"]

    future["travel_distance_away_team"] = future.apply(
        lambda x: travel_distance(x["home_team_abbr"], x["away_team_abbr"]), axis=1
    )

    future["timezone_change"] = future.apply(
        lambda x: timezone_change(x["home_team_abbr"], x["away_team_abbr"]), axis=1
    )

    future = future[
        [
            "game_id",
            "game_state",
            "game_date",
            "home_team_id",
            "away_team_id",
            "home_team_abbr",
            "away_team_abbr",
            "home_elo",
            "away_elo",
            "elo_diff",
            "home_elo_trend_last5",
            "away_elo_trend_last5",
            "elo_trend_diff",
            "travel_distance_away_team",
            "timezone_change",
        ]
    ]

    with engine.begin() as con:
        future.to_sql(
            "future_games_features",
            con,
            if_exists="replace",
            index=False,
            method="multi",
        )

        con.execute(
            text("""
            CREATE INDEX IF NOT EXISTS idx_future_game_id
            ON future_games_features(game_id)
        """)
        )

    print("✅ future_games_features built successfully!")


if __name__ == "__main__":
    build_future_games_features()
