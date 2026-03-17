import sqlite3
from pathlib import Path

import joblib

from nhl_match_prediction.modeling.models.logistic import (
    evaluate_model,
    load_dataset,
    prepare_data,
    time_split,
    train_logistic,
)
from nhl_match_prediction.modeling.models.random_forest import build_model

from .celery_app import celery_app

BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "sql" / "nhl.db"
DATA_PATH = BASE_DIR / "data" / "processed" / "match_features.csv"
MODEL_DIR = BASE_DIR / "nhl_match_prediction" / "modeling" / "artifacts"


def update_task_status(task_id, status, result=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if status == "in_progress":
        cursor.execute(
            "UPDATE tasks SET status=?, started_at=datetime('now') WHERE id=?",
            (status, task_id),
        )
    elif status in ["success", "failure"]:
        cursor.execute(
            "UPDATE tasks SET status=?, finished_at=datetime('now'), result=? WHERE id=?",
            (status, result, task_id),
        )
    else:
        cursor.execute(
            "UPDATE tasks SET status=? WHERE id=?",
            (status, task_id),
        )
    conn.commit()
    conn.close()


@celery_app.task(bind=True)
def train_model_task(self, task_id: str, model_type: str = "logistic"):
    """
    Запускает обучение модели.

    Args:
        task_id: ID задачи из базы tasks
        model_type: 'logistic' или 'random_forest'
    """
    try:
        update_task_status(task_id, "in_progress")
        print(f"🚀 Task {task_id} started training {model_type} model...")

        MODEL_DIR.mkdir(exist_ok=True)
        model_path = MODEL_DIR / f"{model_type}_model.joblib"

        if model_type == "logistic":
            metrics = train_logistic(DATA_PATH, model_path)
        elif model_type == "random_forest":
            df = load_dataset(DATA_PATH)
            x, y = prepare_data(df)
            x_train, x_test, y_train, y_test = time_split(x, y)

            model = build_model(params={"n_estimators": 500, "random_state": 42})
            model.fit(x_train, y_train)

            metrics = evaluate_model(model, x_test, y_test)
            joblib.dump(model, model_path)
        else:
            raise ValueError(f"Unknown model_type: {model_type}")

        result = f"Model trained successfully: {metrics}"
        update_task_status(task_id, "success", result=result)
        print(f"✅ Task {task_id} finished")

        return result
    except Exception as e:
        update_task_status(task_id, "failure", result=str(e))
        raise
