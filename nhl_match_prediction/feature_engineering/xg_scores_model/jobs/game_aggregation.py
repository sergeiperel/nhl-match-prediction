import numpy as np
import pandas as pd


def aggregate_games(df: pd.DataFrame):
    df = df[df["event_type"].isin(["shot-on-goal", "goal", "missed-shot"])].copy()

    home = (
        df[df["is_home"] == 1]
        .groupby(["game_id", "game_date", "team_id"])
        .agg(
            home_xg=("xg", "sum"),
            home_shots=("xg", "count"),
            home_goals=("goal", "sum"),
        )
        .reset_index()
        .rename(columns={"team_id": "home_team_id"})
    )

    away = (
        df[df["is_home"] == 0]
        .groupby(["game_id", "game_date", "team_id"])
        .agg(
            away_xg=("xg", "sum"),
            away_shots=("xg", "count"),
            away_goals=("goal", "sum"),
        )
        .reset_index()
        .rename(columns={"team_id": "away_team_id"})
    )

    game = home.merge(away, on=["game_id", "game_date"], how="inner")

    # ratios
    game["home_xg_per_shot"] = game["home_xg"] / game["home_shots"].replace(0, np.nan)
    game["away_xg_per_shot"] = game["away_xg"] / game["away_shots"].replace(0, np.nan)

    return game


def add_rolling_features(game: pd.DataFrame):
    game = game.sort_values("game_date").reset_index(drop=True)

    game["home_xg_ewm"] = game.groupby("home_team_id")["home_xg"].transform(
        lambda x: x.shift(1).ewm(alpha=0.2).mean()
    )

    game["away_xg_ewm"] = game.groupby("away_team_id")["away_xg"].transform(
        lambda x: x.shift(1).ewm(alpha=0.2).mean()
    )

    game["home_xg_last5"] = game.groupby("home_team_id")["home_xg"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    game["away_xg_last5"] = game.groupby("away_team_id")["away_xg"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    game["home_shots_last5"] = game.groupby("home_team_id")["home_shots"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    game["away_shots_last5"] = game.groupby("away_team_id")["away_shots"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    game["xg_diff_last5"] = game["home_xg_last5"] - game["away_xg_last5"]
    game["shots_diff_last5"] = game["home_shots_last5"] - game["away_shots_last5"]

    game["xg_diff_last5"] = game["home_xg_last5"] - game["away_xg_last5"]

    return game
