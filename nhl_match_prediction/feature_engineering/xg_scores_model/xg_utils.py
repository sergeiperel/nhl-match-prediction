from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from sklearn.calibration import calibration_curve


def parse_time(time_str):
    if not time_str:
        return None
    try:
        m, s = map(int, time_str.split(":"))
        return m * 60 + s
    except (ValueError, AttributeError, TypeError):
        return None


def get_time_in_game(period, time_str):
    """Общее время матча в секундах"""
    t = parse_time(time_str)
    if t is None:
        return None
    return (period - 1) * 20 * 60 + t


def rolling_mean(series, window):
    return series.shift(1).rolling(window, min_periods=3).mean()


def ewma(series, alpha=0.2):
    return series.shift(1).ewm(alpha=alpha).mean()


def save_plot(fig, path: Path, logger=None):
    fig.tight_layout()
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    if logger:
        logger.info(f"Saved plot: {path}")


def plot_feature_importance(model, feature_names, path, logger=None):
    importance = model.get_feature_importance()

    df_imp = pd.DataFrame({"feature": feature_names, "importance": importance}).sort_values(
        "importance", ascending=True
    )

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.barh(df_imp["feature"], df_imp["importance"])
    ax.set_title("Feature Importance")

    save_plot(fig, path, logger)


def plot_prediction_distribution(preds, path, logger=None):
    fig, ax = plt.subplots()
    ax.hist(preds, bins=50)
    ax.set_title("Prediction Distribution")
    ax.set_xlabel("Predicted probability")

    save_plot(fig, path, logger)


def plot_calibration(y_true, preds, path, logger=None):
    prob_true, prob_pred = calibration_curve(y_true, preds, n_bins=20)

    fig, ax = plt.subplots()
    ax.plot(prob_pred, prob_true, marker="o", label="Model")
    ax.plot([0, 1], [0, 1], linestyle="--", label="Perfect")

    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.set_title("Calibration Curve")
    ax.legend()

    save_plot(fig, path, logger)
