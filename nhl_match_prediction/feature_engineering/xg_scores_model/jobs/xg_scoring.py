import pandas as pd
from catboost import CatBoostClassifier

from nhl_match_prediction.feature_engineering.xg_scores_model.config import MODEL_FILE
from nhl_match_prediction.feature_engineering.xg_scores_model.logger import setup_logger

logger = setup_logger("xg_scoring")


def load_model():
    model = CatBoostClassifier()
    model.load_model(MODEL_FILE)
    return model


def prepare_features(df, model):
    feature_names = model.feature_names_

    for col in ["shot_type", "situation_compact", "prev_event_type"]:
        if col in df.columns:
            df[col] = df[col].fillna("unknown").astype(str)

    for col in feature_names:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df[feature_names]


def run_xg_scoring(df: pd.DataFrame, mode="full"):
    """
    mode:
        - full
        - incremental
    """

    model = load_model()

    X = prepare_features(df, model)
    df = df.copy()

    df["xg"] = model.predict_proba(X)[:, 1]

    logger.info(f"xG computed for {len(df)} rows")

    return df
