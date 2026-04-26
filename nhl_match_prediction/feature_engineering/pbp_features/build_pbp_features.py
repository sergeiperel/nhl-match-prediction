import json
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from nhl_match_prediction.etl_pipeline.connection import get_engine

from .additional_features import extract_additional_features
from .event_features import extract_event_features
from .goalie_features import extract_goalie_features
from .spatial_features import extract_spatial_features
from .special_teams import extract_special_teams_features

BASE_DIR = Path(__file__).resolve().parents[3]
RAW_DIR = BASE_DIR / "data" / "raw" / "playbyplay"


def get_existing_games(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT game_id, game_state FROM play_by_play_stats"))
        return {row[0]: row[1] for row in result}


def truncate_table(engine):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE play_by_play_stats"))


def delete_game(engine, game_id):
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM play_by_play_stats WHERE game_id = :gid"),
            {"gid": game_id},
        )


def write_rows(df, engine):
    df.to_sql(
        "play_by_play_stats",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )


def build_play_by_play_dataset(mode="incremental", engine=None):
    if engine is None:
        engine = get_engine()

    # -------------------------
    # FULL MODE
    # -------------------------
    if mode == "full":
        print("🔄 FULL rebuild started")
        truncate_table(engine)
        existing_games = {}
    else:
        print("⚡ INCREMENTAL update started")
        existing_games = get_existing_games(engine)

    rows = []

    for file_path in sorted(RAW_DIR.iterdir()):
        if file_path.suffix != ".json":
            continue

        game_id = int(file_path.stem)

        with file_path.open() as f:
            pbp_json = json.load(f)

        game_state = pbp_json.get("gameState")
        db_state = existing_games.get(game_id)

        # -------------------------
        # INCREMENTAL LOGIC
        # -------------------------
        if mode == "incremental":
            # 1. матч уже финализирован -> пропускаем
            if game_state == "OFF" and db_state == "OFF":
                continue

            # 2. если матч есть -> удаляем (пересобираем)
            if game_id in existing_games:
                delete_game(engine, game_id)

        # -------------------------
        # FEATURE BUILDING
        # -------------------------

        features = {
            "game_id": game_id,
            "game_state": game_state,
        }

        features.update(extract_event_features(pbp_json))
        features.update(extract_spatial_features(pbp_json))
        features.update(extract_special_teams_features(pbp_json))
        features.update(extract_goalie_features(pbp_json))
        features.update(extract_additional_features(pbp_json))

        rows.append(features)

    # -------------------------
    # WRITE
    # -------------------------

    if not rows:
        print("No updates needed")
        return

    df = pd.DataFrame(rows)
    df.fillna(0, inplace=True)

    write_rows(df, engine)

    print(f"✅ Updated {len(df)} games in play_by_play_stats")


if __name__ == "__main__":
    build_play_by_play_dataset()
