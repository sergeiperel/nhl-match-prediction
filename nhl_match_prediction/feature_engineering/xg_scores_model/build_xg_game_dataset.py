import numpy as np
import pandas as pd
from catboost import CatBoostClassifier

from nhl_match_prediction.feature_engineering.xg_scores_model.config import (
    LOG_PATH,
    MODEL_FILE,
    PROCESSED_PATH,
    XG_SHOTS_DATASET_PATH,
)
from nhl_match_prediction.feature_engineering.xg_scores_model.logger import setup_logger

logger = setup_logger("xg_game_dataset", LOG_PATH / "build_game_dataset.log")


OUTPUT_PATH = PROCESSED_PATH / "xg_game_dataset.csv"


# ======================
# LOAD
# ======================
def load_data():
    df = pd.read_csv(XG_SHOTS_DATASET_PATH)
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["game_id", "period", "game_time"]).reset_index(drop=True)

    logger.info(f"Loaded shots dataset: {df.shape}")
    return df


# ======================
# LOAD MODEL
# ======================
def load_model():
    model = CatBoostClassifier()
    model.load_model(MODEL_FILE)

    logger.info(f"Loaded xG model from {MODEL_FILE}")
    return model


# ======================
# PREP FEATURES
# ======================
def prepare_features(df, model):
    feature_names = model.feature_names_

    # categorical
    for col in ["shot_type", "situation_compact", "prev_event_type"]:
        df[col] = df[col].fillna("unknown").astype(str)

    # numeric
    for col in feature_names:
        if col not in df.columns:
            continue
        df[col] = df[col].fillna(0)

    return df[feature_names]


# ======================
# PREDICT XG
# ======================
def add_xg(df, model):
    X = prepare_features(df, model)  # noqa: N806
    df["xg"] = model.predict_proba(X)[:, 1]

    logger.info("Computed xG for all shots")

    return df


# ======================
# AGGREGATE GAME LEVEL
# ======================
def aggregate_games(df):
    df = df[df["event_type"].isin(["shot-on-goal", "goal", "missed-shot"])].copy()

    game_home = (
        df[df["is_home"] == 1]
        .groupby(["game_id", "game_date", "team_id"])
        .agg(home_xg=("xg", "sum"), home_shots=("xg", "count"), home_goals=("goal", "sum"))
        .reset_index()
        .rename(columns={"team_id": "home_team_id"})
    )

    game_away = (
        df[df["is_home"] == 0]
        .groupby(["game_id", "game_date", "team_id"])
        .agg(away_xg=("xg", "sum"), away_shots=("xg", "count"), away_goals=("goal", "sum"))
        .reset_index()
        .rename(columns={"team_id": "away_team_id"})
    )

    game = game_home.merge(game_away, on=["game_id", "game_date"], how="outer")

    game = game.dropna(subset=["home_team_id", "away_team_id"])

    # ratios (safe)
    game["home_xg_per_shot"] = game["home_xg"] / game["home_shots"].replace(0, np.nan)
    game["away_xg_per_shot"] = game["away_xg"] / game["away_shots"].replace(0, np.nan)

    # targets
    # game["home_win"] = (game["home_goals"] > game["away_goals"]).astype(int)

    # diffs
    # game["xg_diff"] = game["home_xg"] - game["away_xg"]
    # game["shots_diff"] = game["home_shots"] - game["away_shots"]
    # game["goal_diff"] = game["home_goals"] - game["away_goals"]

    logger.info(f"Aggregated game dataset: {game.shape}")

    return game


# ======================
# ADD ROLLING FEATURES
# ======================
def add_rolling_features(game):
    game = game.sort_values("game_date").reset_index(drop=True)

    # EWM
    game["home_xg_ewm"] = game.groupby("home_team_id")["home_xg"].transform(
        lambda x: x.shift(1).ewm(alpha=0.2).mean()
    )

    game["away_xg_ewm"] = game.groupby("away_team_id")["away_xg"].transform(
        lambda x: x.shift(1).ewm(alpha=0.2).mean()
    )

    # rolling 5
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

    # diffs
    game["xg_diff_last5"] = game["home_xg_last5"] - game["away_xg_last5"]
    game["shots_diff_last5"] = game["home_shots_last5"] - game["away_shots_last5"]

    logger.info("Added rolling features")

    return game


# ======================
# SAVE
# ======================
def save_dataset(game):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    game.to_csv(OUTPUT_PATH, index=False)

    logger.info(f"Saved final dataset to {OUTPUT_PATH}")


# ======================
# MAIN
# ======================
def main():
    df = load_data()
    model = load_model()

    df = add_xg(df, model)

    game = aggregate_games(df)
    game = add_rolling_features(game)

    save_dataset(game)

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
