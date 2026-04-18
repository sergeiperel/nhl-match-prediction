from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import roc_auc_score, roc_curve
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"

ACC_COLOR = "#2563EB"
CONF_COLOR = "#EF4444"


def load_data():
    engine = create_engine(f"sqlite:///{DB_PATH}")

    return pd.read_sql(
        """
        SELECT
            p.game_id,
            p.logistic_proba,
            m.home_win
        FROM predictions p
        JOIN games m USING(game_id)
        WHERE m.home_win IS NOT NULL
        AND m.game_type in (1, 2, 3)
    """,
        engine,
    )


def main():
    df = load_data()

    df = df.dropna(subset=["logistic_proba", "home_win"])

    y_true = df["home_win"]
    y_score = df["logistic_proba"]

    roc_auc = roc_auc_score(y_true, y_score)
    print(f"ROC AUC: {roc_auc:.4f}")

    fpr, tpr, _ = roc_curve(y_true, y_score)

    plt.figure(figsize=(8, 8))

    plt.plot(fpr, tpr, color=ACC_COLOR, linewidth=2, label=f"ROC curve (AUC = {roc_auc:.3f})")

    plt.plot([0, 1], [0, 1], linestyle="--", color=CONF_COLOR, linewidth=2, label="Random model")

    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")

    plt.legend(loc="lower right")
    plt.grid(True, linestyle="--", alpha=0.4)

    output_dir = BASE_DIR / "visualizations"
    output_dir.mkdir(exist_ok=True)

    plt.savefig(output_dir / "roc_curve.png", dpi=150, bbox_inches="tight")

    plt.show()


if __name__ == "__main__":
    main()
