import json
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm

from nhl_match_prediction.etl_pipeline.connection import get_engine
from nhl_match_prediction.feature_engineering.xg_scores_model.config import (
    LOG_PATH,
    PBP_PATH,
    VALID_EVENTS,
)
from nhl_match_prediction.feature_engineering.xg_scores_model.logger import setup_logger
from nhl_match_prediction.feature_engineering.xg_scores_model.xg_utils import get_time_in_game

logger = setup_logger("xg_dataset", LOG_PATH / "build_dataset.log")


def load_json(path):
    with Path(path).open(encoding="utf-8") as f:
        return json.load(f)


def process_game_pbp(game_data):
    home_id = game_data.get("homeTeam", {}).get("id")
    plays = game_data.get("plays", [])
    plays = sorted(plays, key=lambda x: (x["periodDescriptor"]["number"], x["timeInPeriod"]))
    if not plays:
        return None

    period_sides = {}
    for play in plays:
        p = play.get("periodDescriptor", {}).get("number")
        side = play.get("homeTeamDefendingSide")
        if side and p and p not in period_sides:
            period_sides[p] = side

    # valid_events = [
    # 'shot', 'shot-on-goal', 'goal',
    # 'missed-shot', 'blocked-shot', 'failed-shot-attempt']

    rows = []

    prev_event = None

    for play in plays:
        # if play.get('typeDescKey') not in valid_events:
        #     continue

        details = play.get("details", {})
        x, y = details.get("xCoord"), details.get("yCoord")
        team_id = details.get("eventOwnerTeamId")

        if x is None or y is None or team_id is None:
            continue

        period = play["periodDescriptor"]["number"]
        time_str = play["timeInPeriod"]
        game_time = get_time_in_game(period, time_str)

        defending_side = period_sides.get(period)
        if not defending_side:
            defending_side = "left" if period % 2 != 0 else "right"

        is_home = team_id == home_id

        # нормализация координат

        should_flip = defending_side == "right" if is_home else defending_side == "left"

        x_norm = -x if should_flip else x
        y_norm = -y if should_flip else y

        # геометрия
        dist = np.sqrt((89 - x_norm) ** 2 + y_norm**2)
        angle = np.arctan2(abs(y_norm), (89 - x_norm))

        # target
        goal = 1 if play["typeDescKey"] == "goal" else 0

        # динамика
        delta_t = None
        delta_d = None
        speed = None
        delta_angle = None
        prev_event_type = None

        if prev_event and game_time is not None:
            if prev_event["time"] is not None:
                delta_t = game_time - prev_event["time"]
                if delta_t is not None and delta_t > 0:
                    delta_d = np.sqrt(
                        (x_norm - prev_event["x"]) ** 2 + (y_norm - prev_event["y"]) ** 2
                    )
                    speed = delta_d / delta_t
                    delta_angle = abs(angle - prev_event["angle"])

            prev_event_type = prev_event["type"]

        # ситуация (manpower)
        situation = play.get("situationCode")

        shot_type = details.get("shotType")

        shot_type = shot_type.lower() if shot_type else "unknown"

        rows.append(
            {
                "game_id": game_data.get("id"),
                "game_date": game_data.get("gameDate"),
                "team_id": team_id,
                "is_home": int(is_home),
                "period": period,
                "game_time": game_time,
                # geometry
                "x": x_norm,
                "y": y_norm,
                "distance": dist,
                "angle": angle,
                # shot info
                "shot_type": shot_type,
                "event_type": play["typeDescKey"],
                # context
                "situation": situation,
                # dynamics
                "delta_t": delta_t,
                "delta_d": delta_d,
                "speed": speed,
                "delta_angle": delta_angle,
                "prev_event_type": prev_event_type,
                # target
                "goal": goal,
            }
        )

        # обновляем prev_event
        prev_event = {
            "x": x_norm,
            "y": y_norm,
            "time": game_time,
            "angle": angle,
            "type": play["typeDescKey"],
        }

    return pd.DataFrame(rows) if rows else None


def build_dataset():
    all_games = sorted(PBP_PATH.glob("*.json"))
    dfs = []

    for game_path in tqdm(all_games, desc="Processing games"):
        try:
            data = load_json(game_path)
        except Exception as e:
            logger.warning(f"Failed to load {game_path}: {e}")
            continue

        df_game = process_game_pbp(data)

        if df_game is not None:
            dfs.append(df_game)

    if not dfs:
        raise ValueError("No PBP data loaded")

    logger.info(f"Total games processed: {len(dfs)}")

    df_full = pd.concat(dfs, ignore_index=True)
    df_full = df_full.sort_values(["game_id", "period", "game_time"]).reset_index(drop=True)

    logger.info(f"Shape of full PBP dataset events: {df_full.shape}")

    return df_full


def add_features(df):
    df = df.sort_values(["game_id", "period", "game_time"]).reset_index(drop=True)

    codes = df["situation"].fillna(0).astype(str).str.zfill(4)

    df["team_skaters"] = pd.to_numeric(codes.str[1], errors="coerce").fillna(5)
    df["opp_skaters"] = pd.to_numeric(codes.str[2], errors="coerce").fillna(5)

    df["man_diff"] = df["team_skaters"] - df["opp_skaters"]
    df["total_skaters"] = df["team_skaters"] + df["opp_skaters"]

    df["is_powerplay"] = (df["man_diff"] > 0).astype(int)
    df["is_penalty_kill"] = (df["man_diff"] < 0).astype(int)
    df["is_even"] = (df["man_diff"] == 0).astype(int)

    df["is_empty_net"] = ((df["team_skaters"] == 6) | (df["opp_skaters"] == 6)).astype(int)  # noqa: PLR2004

    # --- rebound ---
    df["prev_shot_event"] = df.groupby("game_id")["event_type"].shift(1)
    df["prev_time"] = df.groupby("game_id")["game_time"].shift(1)

    df["delta_t_rebound"] = df["game_time"] - df["prev_time"]

    df["is_rebound"] = (
        (df["delta_t_rebound"].fillna(999) <= 2)  # noqa: PLR2004
        & (df["prev_shot_event"].isin(["shot", "shot-on-goal", "goal"]))
    ).astype(int)

    # --- compact ---
    df["situation_compact"] = (
        df["team_skaters"].astype(int).astype(str) + "v" + df["opp_skaters"].astype(int).astype(str)
    )

    df["is_shot_event"] = (
        df["event_type"].isin(["shot", "shot-on-goal", "goal", "missed-shot"])
    ).astype(int)

    for col in ["team_skaters", "opp_skaters", "man_diff", "total_skaters"]:
        df[col] = df[col].astype(float)

    logger.info("Feature engineering completed")
    logger.info(f"Columns: {list(df.columns)}")

    return df


def upsert_dataframe(df, table_name, engine):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    df = df.copy()
    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")

    stmt = insert(table).values(records)

    pk_cols = ["game_id", "period", "game_time", "team_id"]

    update_dict = {c.name: stmt.excluded[c.name] for c in table.columns if c.name not in pk_cols}

    stmt = stmt.on_conflict_do_update(index_elements=pk_cols, set_=update_dict)

    with engine.begin() as conn:
        conn.execute(stmt)


def main():
    engine = get_engine()

    df = build_dataset()
    df = add_features(df)
    df = df.drop_duplicates(subset=["game_id", "period", "game_time", "team_id"])

    df_shots = df[df["event_type"].isin(VALID_EVENTS)].copy()
    df_shots = df_shots.drop_duplicates(subset=["game_id", "period", "game_time", "team_id"])

    # upsert_dataframe(df, "pbp_events_features", engine)
    # upsert_dataframe(df_shots, "pbp_shots_features", engine)

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE pbp_events_features"))
        conn.execute(text("TRUNCATE TABLE pbp_shots_features"))

    df.to_sql(
        "pbp_events_features",
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )

    df_shots.to_sql(
        "pbp_shots_features",
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )


if __name__ == "__main__":
    main()
