import pandas as pd
from sqlalchemy import text

from nhl_match_prediction.etl_pipeline.connection import get_engine

from .player_features import add_player_features


def get_existing_game_ids(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT game_id FROM player_stats_features"))
        return {row[0] for row in result}


def truncate_table(engine):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE player_stats_features"))


def build_player_features(engine=None, mode="incremental"):
    if engine is None:
        engine = get_engine()

    print(f"⚙️ build_player_features | mode={mode}")

    # --- load ---
    df = pd.read_sql("SELECT * FROM player_stats", engine)

    # -------------------------
    # FULL MODE
    # -------------------------
    if mode == "full":
        print("🔄 FULL rebuild")
        truncate_table(engine)

    # -------------------------
    # INCREMENTAL MODE
    # -------------------------
    else:
        existing_games = get_existing_game_ids(engine)

        before = len(df)
        df = df[~df["game_id"].isin(existing_games)]
        after = len(df)

        print(f"📊 filtered: {before} → {after}")

        if df.empty:
            print("✅ No new games for player_features")
            return

    # --- build ---
    features_df = add_player_features(df)

    # --- save ---
    features_df.to_sql(
        "player_stats_features",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print(f"✅ Added {len(features_df)} rows to player_stats_features")


if __name__ == "__main__":
    build_player_features()
