from pathlib import Path

import pandas as pd

from .player_features import add_player_features

BASE_DIR = Path(__file__).resolve().parents[2]
PLAYER_STATS_PATH = BASE_DIR / "data" / "processed" / "player_stats.csv"
OUT_PATH = BASE_DIR / "data" / "processed" / "player_stats_features.csv"


def build_player_features():
    player_features_df = pd.read_csv(PLAYER_STATS_PATH)

    player_features_df = add_player_features(player_features_df)

    player_features_df.to_csv(OUT_PATH, index=False)

    print(f"Saved to {OUT_PATH}")


if __name__ == "__main__":
    build_player_features()
