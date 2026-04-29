import pandas as pd

from nhl_match_prediction.etl_pipeline.connection import get_engine

from .goalie_features import add_goalie_features


def build_goalie_features(engine=None, mode="full"):
    if engine is None:
        engine = get_engine()

    # --- load ---
    df = pd.read_sql("SELECT * FROM goalie_game_stats", engine)

    # --- features ---
    df = add_goalie_features(df)

    # --- save (full rebuild) ---
    df.to_sql(
        "goalie_game_stats_features",
        engine,
        if_exists="replace",
        index=False,
    )

    print(f"✅ goalie_game_stats_features rebuilt: {len(df)} rows")


if __name__ == "__main__":
    build_goalie_features()
