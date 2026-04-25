import json
from datetime import datetime
from pathlib import Path

import optuna
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import log_loss, roc_auc_score

from nhl_match_prediction.feature_engineering.xg_scores_model.config import (
    LOG_PATH,
    MODEL_FILE,
    MODEL_META,
    PLOTS_PATH,
    RANDOM_STATE,
    XG_SHOTS_DATASET_PATH,
)
from nhl_match_prediction.feature_engineering.xg_scores_model.logger import setup_logger
from nhl_match_prediction.feature_engineering.xg_scores_model.xg_utils import (
    plot_calibration,
    plot_feature_importance,
    plot_prediction_distribution,
)

logger = setup_logger("xg_train", LOG_PATH / "train.log")


# ======================
# LOAD
# ======================
def load_data():
    df = pd.read_csv(XG_SHOTS_DATASET_PATH)
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["game_id", "period", "game_time"]).reset_index(drop=True)
    logger.info(f"Loaded dataset: {df.shape}")
    return df


# ======================
# FEATURES
# ======================
def prepare_features(df):
    cat_features = ["shot_type", "situation_compact", "prev_event_type"]

    num_features = [
        "distance",
        "angle",
        "x",
        "y",
        "delta_t",
        "delta_d",
        "speed",
        "delta_angle",
        # situation features (ОЧЕНЬ ВАЖНЫ)
        # "team_skaters",
        # "opp_skaters",
        "man_diff",
        "total_skaters",
        # "is_powerplay",
        # "is_penalty_kill",
        "is_even",
        "is_empty_net",
        # rebound
        "is_rebound",
        # "is_shot_event"
    ]

    # categorical
    for col in cat_features:
        df[col] = df[col].fillna("unknown").astype(str)

    # numeric
    for col in num_features:
        df[col] = df[col].fillna(0)

    # target
    y = df["goal"]
    X = df[cat_features + num_features]

    return X, y, cat_features


# ======================
# SPLIT
# ======================
def split_data(df, X, y):
    game_order = df.groupby("game_id")["game_date"].min().reset_index().sort_values("game_date")

    games = game_order["game_id"].values
    n = len(games)

    train_games = games[: int(n * 0.7)]
    val_games = games[int(n * 0.7) : int(n * 0.85)]
    test_games = games[int(n * 0.85) :]

    logger.info(
        f"Games split | train={train_games[:3]}... val={val_games[:3]}... test={test_games[:3]}..."
    )

    def mask(games_subset):
        return df["game_id"].isin(games_subset)

    X_train, y_train = X[mask(train_games)], y[mask(train_games)]
    X_val, y_val = X[mask(val_games)], y[mask(val_games)]
    X_test, y_test = X[mask(test_games)], y[mask(test_games)]

    assert len(X_train) > 0, "Empty train set"
    assert len(X_val) > 0, "Empty val set"
    assert len(X_test) > 0, "Empty test set"

    logger.info(
        f"Split stats | train={len(train_games)} val={len(val_games)} test={len(test_games)}"
    )
    logger.info(f"Goal rate: {df['goal'].mean():.4f}")

    return (
        X_train.reset_index(drop=True),
        y_train.reset_index(drop=True),
        X_val.reset_index(drop=True),
        y_val.reset_index(drop=True),
        X_test.reset_index(drop=True),
        y_test.reset_index(drop=True),
    )


# ======================
# OPTUNA
# ======================
def run_optuna(X_train, y_train, X_val, y_val, cat_features):
    def objective(trial):
        params = {
            "iterations": 2000,
            "depth": trial.suggest_int("depth", 4, 6),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.08, log=True),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 5, 25),
            "random_strength": trial.suggest_float("random_strength", 3, 10),
            "loss_function": "Logloss",
            "eval_metric": "AUC",
            "random_state": RANDOM_STATE,
            "verbose": False,
            "allow_writing_files": False,
            "auto_class_weights": "Balanced",
        }

        logger.debug(f"Trial params: {params}")

        model = CatBoostClassifier(**params)

        model.fit(
            X_train,
            y_train,
            eval_set=(X_val, y_val),
            cat_features=cat_features,
            early_stopping_rounds=200,
            verbose=False,
        )

        preds = model.predict_proba(X_val)[:, 1]

        return roc_auc_score(y_val, preds)

    study = optuna.create_study(
        direction="maximize", sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE)
    )
    study.optimize(objective, n_trials=50)

    trials_df = study.trials_dataframe()
    trials_df.to_csv(LOG_PATH / "optuna_trials.csv", index=False)

    logger.info(f"Best AUC: {study.best_value}")
    logger.info(f"Best params: {study.best_params}")

    return study.best_params, study


def train_final_model(data, cat_features, params):
    (X_train, y_train, X_val, y_val) = data
    model = CatBoostClassifier(
        **params, loss_function="Logloss", eval_metric="AUC", random_state=RANDOM_STATE, verbose=200
    )

    logger.info("Training final model...")

    model.fit(
        X_train,
        y_train,
        eval_set=(X_val, y_val),
        cat_features=cat_features,
        early_stopping_rounds=200,
    )

    return model


def evaluate(model, X, y, name):
    preds = model.predict_proba(X)[:, 1]
    logger.info(f"{name} AUC: {roc_auc_score(y, preds):.5f}")
    logger.info(f"{name} LOGLOSS: {log_loss(y, preds):.5f}")
    return preds


def save_model(model, metadata):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    model_path = MODEL_FILE.parent / f"xg_model_{timestamp}.cbm"
    meta_path = MODEL_META.parent / f"xg_model_{timestamp}.json"

    model.save_model(model_path)

    with Path(meta_path).open("w") as f:
        json.dump(metadata, f, indent=4)

    logger.info(f"Model saved to {model_path}")
    logger.info(f"Metadata saved to {meta_path}")

    fi = pd.DataFrame(
        {"feature": model.feature_names_, "importance": model.get_feature_importance()}
    ).sort_values("importance", ascending=False)

    fi.to_csv(LOG_PATH / f"feature_importance_{timestamp}.csv", index=False)


def main():
    df = load_data()

    X, y, cat_features = prepare_features(df)

    X_train, y_train, X_val, y_val, X_test, y_test = split_data(df, X, y)

    best_params, _ = run_optuna(X_train, y_train, X_val, y_val, cat_features)

    # train final
    X_full = pd.concat([X_train, X_val])
    y_full = pd.concat([y_train, y_val])

    model = train_final_model((X_full, y_full, X_val, y_val), cat_features, best_params)

    # eval
    train_preds = evaluate(model, X_full, y_full, "TRAIN")
    test_preds = evaluate(model, X_test, y_test, "TEST")

    plot_feature_importance(model, list(X.columns), PLOTS_PATH / "feature_importance.png", logger)
    plot_prediction_distribution(test_preds, PLOTS_PATH / "test_pred_dist.png", logger)
    plot_calibration(y_test, test_preds, PLOTS_PATH / "calibration.png", logger)

    metadata = {
        "features": list(X.columns),
        "cat_features": cat_features,
        "params": best_params,
        "train_auc": roc_auc_score(y_full, train_preds),
        "test_auc": roc_auc_score(y_test, test_preds),
        "train_logloss": log_loss(y_full, train_preds),
        "test_logloss": log_loss(y_test, test_preds),
    }

    save_model(model, metadata)


if __name__ == "__main__":
    main()
