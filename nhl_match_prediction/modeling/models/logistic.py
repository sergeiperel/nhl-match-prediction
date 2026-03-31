# nhl_match_prediction/modeling/models/logistic.py

from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def load_dataset(data_path: Path) -> pd.DataFrame:
    return pd.read_csv(data_path).sort_values("game_date").reset_index(drop=True)


def prepare_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    drop_cols = [
        "game_id",
        "game_date",
        "home_team_abbr",
        "away_team_abbr",
        "neutral_site",
        "season",
        "game_type",
        "home_team_id",
        "away_team_id",
    ]

    dup_cols = [c for c in df.columns if ":1" in c]
    drop_cols += dup_cols

    x = df.drop(columns=[c for c in drop_cols if c in df.columns] + ["home_win"])
    y = df["home_win"]

    return x, y


def time_split(
    x: pd.DataFrame,
    y: pd.Series,
    test_size: float = 0.3,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    split_idx = int(len(x) * (1 - test_size))

    x_train = x.iloc[:split_idx]
    x_test = x.iloc[split_idx:]
    y_train = y.iloc[:split_idx]
    y_test = y.iloc[split_idx:]

    return x_train, x_test, y_train, y_test


def build_model(params: dict) -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("logreg", LogisticRegression(**params)),
        ]
    )


def evaluate_model(
    model: Pipeline,
    x_test: pd.DataFrame,
    y_test: pd.Series,
) -> dict[str, float]:
    y_pred = model.predict(x_test)
    y_proba = model.predict_proba(x_test)[:, 1]

    return {
        "accuracy": accuracy_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
        "log_loss": log_loss(y_test, y_proba),
    }


def plot_feature_importance(
    model,
    feature_names,
    top_n: int = 10,
    figsize=(8, 6),
):
    coefs = model.named_steps["logreg"].coef_[0]

    feature_importance = (
        pd.Series(coefs, index=feature_names).sort_values(key=abs, ascending=False).head(top_n)
    )

    plt.figure(figsize=figsize)

    ax = sns.barplot(
        x=feature_importance.values,
        y=feature_importance.index,
        color="#a2d2ff",
    )

    plt.title("Feature Importance (Logistic Regression)")
    plt.xlabel("Coefficient")
    plt.ylabel("")
    sns.despine()

    for i, value in enumerate(feature_importance.values):
        ax.text(
            value,  # x позиция
            i,  # y позиция
            f"{value:.3f}",  # формат числа
            va="center",
            ha="left" if value > 0 else "right",
            fontsize=8,
            color="black",
        )

    offset = max(abs(feature_importance.values)) * 0.25
    plt.xlim(
        feature_importance.values.min() - offset,
        feature_importance.values.max() + offset,
    )

    plt.tight_layout()
    plt.show()

    return feature_importance


def train_logistic(
    data_path: Path, model_output_path: Path, params: dict | None = None
) -> dict[str, float]:
    df = load_dataset(data_path)

    x, y = prepare_data(df)

    x_train, x_test, y_train, y_test = time_split(x, y)

    print(params)

    model = build_model(params)
    model.fit(x_train, y_train)

    metrics = evaluate_model(model, x_test, y_test)

    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_output_path)

    return metrics
