import pandas as pd
from sqlalchemy import text

from nhl_match_prediction.etl_pipeline.connection import get_engine

from .jobs.game_aggregation import add_rolling_features, aggregate_games
from .jobs.xg_scoring import run_xg_scoring


def load_shots(engine):
    return pd.read_sql("SELECT * FROM pbp_shots_features", engine)


def save_table(df, engine, table_name):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )


def run_xg_pipeline(mode="full"):
    engine = get_engine()

    # ======================
    # 1. LOAD
    # ======================
    df = load_shots(engine)

    # ======================
    # 2. XG SCORING
    # ======================
    df = run_xg_scoring(df, mode=mode)

    save_table(df, engine, "shot_xg_predictions")

    # ======================
    # 3. GAME AGGREGATION
    # ======================
    game = aggregate_games(df)
    game = add_rolling_features(game)

    save_table(game, engine, "game_xg_features")

    print("PIPELINE DONE")


if __name__ == "__main__":
    run_xg_pipeline(mode="full")
