from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"

THRESHOLD = 0.5

ACC_COLOR = "#2563EB"
CONF_COLOR = "#EF4444"
GAMES_COLOR = "#6B7280"


def load_data() -> pd.DataFrame:
    engine = create_engine(f"sqlite:///{DB_PATH}")

    return pd.read_sql(
        """
        SELECT
            p.game_id,
            DATE(p.game_date) as game_day,
            p.logistic_proba,
            m.home_win
        FROM predictions p
        JOIN games m USING(game_id)
        WHERE m.home_win IS NOT NULL
        AND m.game_type IN (1, 2, 3)
    """,
        engine,
    )


def prepare_predictions(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    df = df.copy()

    df["game_day"] = pd.to_datetime(df["game_day"])

    df["pred"] = (df["logistic_proba"] >= threshold).astype(int)
    df["correct"] = (df["pred"] == df["home_win"]).astype(int)
    df["confidence"] = df["logistic_proba"].apply(lambda x: max(x, 1 - x))

    return df


def prepare_daily_metrics(
    df: pd.DataFrame, min_games_per_day: int = 3, days: int = 30
) -> pd.DataFrame:
    df = df.copy()

    daily = (
        df.groupby("game_day")
        .agg(
            games=("game_id", "count"),
            accuracy=("correct", "mean"),
            confidence=("confidence", "mean"),
        )
        .reset_index()
    )

    if daily.empty:
        return daily

    # фильтр по количеству игр
    daily = daily[daily["games"] >= min_games_per_day]

    if daily.empty:
        return daily

    last_date = daily["game_day"].max() - pd.Timedelta(days=days)
    daily = daily[daily["game_day"] >= last_date]

    if daily.empty:
        return daily

    daily = daily.sort_values("game_day")

    daily["accuracy"] *= 100
    daily["confidence"] *= 100

    daily["acc_smooth"] = daily["accuracy"].rolling(5, min_periods=2).mean()
    daily["conf_smooth"] = daily["confidence"].rolling(5, min_periods=2).mean()

    return daily


def plot_daily_accuracy():
    df = load_data()
    df = prepare_predictions(df, THRESHOLD)

    daily = prepare_daily_metrics(df)

    if daily.empty:
        print("No data for plotting")
        return

    _, ax1 = plt.subplots(figsize=(14, 7))

    ax1.plot(
        daily["game_day"], daily["acc_smooth"], label="Accuracy", linewidth=2.5, color=ACC_COLOR
    )

    ax1.plot(
        daily["game_day"],
        daily["conf_smooth"],
        linestyle="--",
        label="Confidence",
        linewidth=2.5,
        color=CONF_COLOR,
    )

    ax1.set_ylabel("Percent")
    ax1.set_ylim(0, 100)
    ax1.set_yticks(range(0, 101, 10))

    ax2 = ax1.twinx()

    ax2.bar(
        daily["game_day"], daily["games"], alpha=0.3, width=0.8, label="Games", color=GAMES_COLOR
    )

    ax2.set_ylabel("Games")

    ax1.grid(True, linestyle="--", alpha=0.4)

    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()

    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left", framealpha=0.6)

    plt.xticks(rotation=45)
    plt.tight_layout()

    output_dir = BASE_DIR / "visualizations"
    output_dir.mkdir(exist_ok=True)

    plt.savefig(output_dir / "daily_accuracy.png", dpi=150, bbox_inches="tight")

    plt.show()


if __name__ == "__main__":
    plot_daily_accuracy()
