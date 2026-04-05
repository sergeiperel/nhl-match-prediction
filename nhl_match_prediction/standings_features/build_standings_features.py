from pathlib import Path

import pandas as pd

from .schedule_strength import add_schedule_strength

BASE_DIR = Path(__file__).resolve().parents[2]
STANDINGS_PATH = BASE_DIR / "data" / "processed" / "standings_daily.csv"
OUT_PATH = BASE_DIR / "data" / "processed" / "standings_daily.csv"  # standings_daily_features


def build_standings_daily_features():
    standings = pd.read_csv(STANDINGS_PATH, parse_dates=["date"])

    standings = add_schedule_strength(standings)

    standings.to_csv(OUT_PATH, index=False)

    print(f"Saved to {OUT_PATH}")


if __name__ == "__main__":
    build_standings_daily_features()
