import numpy as np
import pandas as pd

ELITE_GOALIE_SAVE_PCT = 0.92
BAD_GOALIE_SAVE_PCT = 0.82
MINUTES_TO_PULL = 60

HIGH_SHOT_GAME = 35
HIGH_SHOT_GAME_SAVE_PCT = 0.90


def add_goalie_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Базовые флаги ---
    df["backup_goalie_flag"] = np.where(df["starter"], 0, 1)

    df["played_flag"] = np.where(df["toi_minutes"] > 0, 1, 0)

    # --- Save pct ---
    df["save_pct"] = df["save_pct"].fillna(0)

    df["elite_goalie_game"] = (df["save_pct"] >= ELITE_GOALIE_SAVE_PCT).astype(int)
    df["bad_goalie_game"] = (df["save_pct"] <= BAD_GOALIE_SAVE_PCT).astype(int)

    # --- Нагрузка ---
    df["high_shot_game"] = (df["shots_against"] >= HIGH_SHOT_GAME).astype(int)

    # --- Quality start ---
    df["quality_start"] = (
        (df["shots_against"] >= HIGH_SHOT_GAME - 5) & (df["save_pct"] >= HIGH_SHOT_GAME_SAVE_PCT)
    ).astype(int)

    # --- Pulled goalie ---
    df["pulled_flag"] = ((df["starter"]) & (df["toi_minutes"] < MINUTES_TO_PULL)).astype(int)

    return df
