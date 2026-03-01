# nhl_match_prediction/features/build_features.py

import json
from pathlib import Path

import pandas as pd

from .additional_features import extract_additional_features
from .event_features import extract_event_features
from .goalie_features import extract_goalie_features
from .spatial_features import extract_spatial_features
from .special_teams import extract_special_teams_features

BASE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = BASE_DIR / "data" / "raw" / "playbyplay"
OUT_DIR = BASE_DIR / "data" / "processed"

OUTPUT_PATH = OUT_DIR / "play_by_play_stats.csv"


def build_play_by_play_dataset():
    rows = []

    for file_path in RAW_DIR.iterdir():
        if file_path.suffix != ".json":
            continue

        game_id = file_path.stem

        with file_path.open() as f:
            pbp_json = json.load(f)

        features = {"game_id": game_id}

        features.update(extract_event_features(pbp_json))
        features.update(extract_spatial_features(pbp_json))
        features.update(extract_special_teams_features(pbp_json))
        features.update(extract_goalie_features(pbp_json))
        features.update(extract_additional_features(pbp_json))

        rows.append(features)

    df = pd.DataFrame(rows)
    df.fillna(0, inplace=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    build_play_by_play_dataset()
