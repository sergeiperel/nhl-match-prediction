import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from nhl_match_prediction.etl_pipeline.connection import get_engine
from nhl_match_prediction.feature_engineering.xg_scores_model.config import (
    LOG_PATH,
    MODEL_FILE,
)
from nhl_match_prediction.feature_engineering.xg_scores_model.logger import setup_logger

logger = setup_logger("xg_game_dataset", LOG_PATH / "build_game_dataset.log")


# ======================
# LOAD MODEL
# ======================
def load_model():
    model = CatBoostClassifier()
    model.load_model(MODEL_FILE)

    logger.info(f"Loaded xG model from {MODEL_FILE}")
    return model


# ======================
# LOAD NEW SHOTS
# ======================


def load_new_shots(engine):
    existing = pd.read_sql("SELECT DISTINCT game_id FROM game_xg_features", engine)[
        "game_id"
    ].tolist()

    query = """
        SELECT *
        FROM pbp_shots_features
    """

    if existing:
        query += f" WHERE game_id NOT IN ({','.join(map(str, existing))})"

    df = pd.read_sql(query, engine)

    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"])

    df = df.sort_values(["game_date", "game_id", "period", "game_time"]).reset_index(drop=True)

    logger.info(f"Loaded new shots: {df.shape}")
    return df


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
# FEATURES + XG
# ======================
def add_xg(df, model):
    feature_names = model.feature_names_

    for col in ["shot_type", "situation_compact", "prev_event_type"]:
        df[col] = df[col].fillna("unknown").astype(str)

    for col in feature_names:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    X = df[feature_names]
    df["xg"] = model.predict_proba(X)[:, 1]

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

    game = game_home.merge(game_away, on=["game_id", "game_date"], how="inner")

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
# LOAD HISTORY FOR ROLLING
# ======================
def load_history(engine):
    df = pd.read_sql("SELECT * FROM game_xg_features", engine)

    if df.empty:
        return df

    df["game_date"] = pd.to_datetime(df["game_date"])

    return df.sort_values(["game_date", "game_id"]).reset_index(drop=True)


# ======================
# ADD ROLLING FEATURES
# ======================
def add_rolling(history, new):
    df = pd.concat([history, new], ignore_index=True)

    df = df.sort_values(["game_date", "game_id"]).reset_index(drop=True)

    # EWM
    df["home_xg_ewm"] = df.groupby("home_team_id")["home_xg"].transform(
        lambda x: x.shift(1).ewm(alpha=0.2).mean()
    )

    df["away_xg_ewm"] = df.groupby("away_team_id")["away_xg"].transform(
        lambda x: x.shift(1).ewm(alpha=0.2).mean()
    )

    # rolling 5
    df["home_xg_last5"] = df.groupby("home_team_id")["home_xg"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    df["away_xg_last5"] = df.groupby("away_team_id")["away_xg"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    df["home_shots_last5"] = df.groupby("home_team_id")["home_shots"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    df["away_shots_last5"] = df.groupby("away_team_id")["away_shots"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=3).mean()
    )

    # diffs
    df["xg_diff_last5"] = df["home_xg_last5"] - df["away_xg_last5"]
    df["shots_diff_last5"] = df["home_shots_last5"] - df["away_shots_last5"]

    logger.info("Added rolling features")

    return df


# ======================
# UPSERT
# ======================
def upsert(df, engine):
    df = df.where(pd.notnull(df), None)

    metadata = MetaData()
    table = Table("game_xg_features", metadata, autoload_with=engine)

    pk = ["game_id"]

    stmt = insert(table).values(df.to_dict(orient="records"))

    update_dict = {c.name: stmt.excluded[c.name] for c in table.columns if c.name not in pk}

    stmt = stmt.on_conflict_do_update(index_elements=pk, set_=update_dict)

    with engine.begin() as conn:
        conn.execute(stmt)


# ======================
# MAIN
# ======================


def main():
    engine = get_engine()
    model = load_model()

    df = load_new_shots(engine)

    if df.empty:
        logger.info("No new data")
        return

    df = add_xg(df, model)

    new_games = aggregate_games(df)

    history = load_history(engine)

    # убираем дубли уже существующих игр
    new_games = new_games[~new_games["game_id"].isin(history["game_id"])]

    full = add_rolling(history, new_games)

    result = full[full["game_id"].isin(new_games["game_id"])]

    upsert(result, engine)

    logger.info(f"✅ Inserted {result.shape}")


if __name__ == "__main__":
    main()
