from pathlib import Path

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)

BASE_DIR = Path(__file__).resolve().parents[2]
THRESHOLD = 0.5


def evaluate_model(y_true, proba):
    pred = (proba >= THRESHOLD).astype(int)

    return {
        "accuracy": accuracy_score(y_true, pred),
        "roc_auc": roc_auc_score(y_true, proba),
        "log_loss": log_loss(y_true, proba),
        "brier_score": brier_score_loss(y_true, proba),
    }


def main():
    predictions_path = BASE_DIR / "predictions" / "upcoming_predictions.csv"

    results_path = BASE_DIR / "data" / "processed" / "match_features.csv"

    preds = pd.read_csv(predictions_path)
    results = pd.read_csv(results_path)

    df = preds.merge(results, on="game_id")

    y_true = df["home_win"]

    models = {
        "logistic": df["logistic_proba"],
        "random_forest": df["random_forest_proba"],
        "ensemble": df["avg_proba"],
    }

    for model_name, proba in models.items():
        metrics = evaluate_model(y_true, proba)

        print(f"=== {model_name.upper()} ===")

        for name, value in metrics.items():
            print(f"{name}: {value:.4f}")


if __name__ == "__main__":
    main()
