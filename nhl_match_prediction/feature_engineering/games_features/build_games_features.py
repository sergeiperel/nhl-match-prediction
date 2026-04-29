import numpy as np
import pandas as pd

from nhl_match_prediction.etl_pipeline.connection import get_engine

from .elo_matches import add_elo_features
from .fatigue_features import add_fatigue_features
from .geo_location_data import timezone_change, travel_distance
from .performance_features import add_performance_features


def add_travel_features(games):
    missing_teams = set()

    def safe_distance(row):
        home = row["home_team_abbr"]
        away = row["away_team_abbr"]

        try:
            return travel_distance(home, away)
        except KeyError:
            missing_teams.add(home)
            missing_teams.add(away)
            return np.nan

    def safe_timezone(row):
        home = row["home_team_abbr"]
        away = row["away_team_abbr"]

        try:
            return timezone_change(home, away)
        except KeyError:
            missing_teams.add(home)
            missing_teams.add(away)
            return np.nan

    games["travel_distance_away_team"] = games.apply(safe_distance, axis=1)
    games["geo_timezone_change"] = games.apply(safe_timezone, axis=1)

    return games


def build_games_with_features(engine=None, mode="full"):
    if engine is None:
        engine = get_engine()
    # --- load ---
    games = pd.read_sql("SELECT * FROM games", engine)
    games = games.sort_values("date").reset_index(drop=True)

    # --- features ---
    games = add_performance_features(games)
    games = add_travel_features(games)
    games = add_fatigue_features(games)
    games = add_elo_features(games)

    # --- save (always full rebuild) ---
    games.to_sql("games_with_features", engine, if_exists="replace", index=False)

    print("✅ games_with_features updated")


if __name__ == "__main__":
    build_games_with_features()
