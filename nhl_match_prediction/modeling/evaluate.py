from pathlib import Path

import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"

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
    engine = create_engine(f"sqlite:///{DB_PATH}")

    df = pd.read_sql(
        """
        SELECT pr.*, home_win
        FROM predictions pr
        LEFT JOIN match_features m
            on pr.game_id = m.game_id
        WHERE home_win IS NOT NULL
    """,
        engine,
    )

    df["game_date"] = pd.to_datetime(df["game_date"]).dt.date

    models = {
        "logistic": df["logistic_proba"],
        "random_forest": df["random_forest_proba"],
        "ensemble": df["avg_proba"],
    }

    all_metrics = []

    for model_name, proba in models.items():
        metrics = evaluate_model(df["home_win"], proba)

        print(f"=== {model_name.upper()} ===")
        print(metrics)

        all_metrics.append({"model": model_name, **metrics})

    metrics_df = pd.DataFrame(all_metrics)

    metrics_df.to_sql("metrics", engine, if_exists="replace", index=False)

    print("Saved to DB → metrics table")


if __name__ == "__main__":
    main()
