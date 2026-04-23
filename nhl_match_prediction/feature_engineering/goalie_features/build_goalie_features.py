from pathlib import Path

import pandas as pd

from .goalie_features import add_goalie_features

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_PATH = BASE_DIR / "data" / "processed" / "goalie_game_stats.csv"
OUT_PATH = BASE_DIR / "data" / "processed" / "goalie_game_stats.csv"  # goalie_game_stats_features


def build_goalie_features():
    df = pd.read_csv(INPUT_PATH)

    df = add_goalie_features(df)

    df.to_csv(OUT_PATH, index=False)

    print(f"Saved to {OUT_PATH}")


if __name__ == "__main__":
    build_goalie_features()
