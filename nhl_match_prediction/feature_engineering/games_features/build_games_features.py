from pathlib import Path

import numpy as np
import pandas as pd

from .elo_matches import add_elo_features
from .fatigue_features import add_fatigue_features
from .geo_location_data import timezone_change, travel_distance
from .performance_features import add_performance_features

BASE_DIR = Path(__file__).resolve().parents[2]

GAMES_PATH = BASE_DIR / "data" / "processed" / "games.csv"
OUT_PATH = BASE_DIR / "data" / "processed" / "games.csv"  # games_with_features


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
    games["timezone_change"] = games.apply(safe_timezone, axis=1)

    return games


def build_games_with_features():
    games = pd.read_csv(GAMES_PATH)

    games = add_performance_features(games)
    games = add_travel_features(games)
    games = add_fatigue_features(games)
    games = add_elo_features(games)

    games.to_csv(OUT_PATH, index=False)

    print(f"Saved to {OUT_PATH}")


if __name__ == "__main__":
    build_games_with_features()
